use super::*;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
};
use serde_json::json;
use std::io::Read;

const REGISTRY: &str = "http://127.0.0.1:18081";

fn registry() -> ureq::Agent {
    ureq::Agent::config_builder()
        .proxy(None)
        .max_redirects(0)
        .timeout_global(Some(Duration::from_secs(5)))
        .build()
        .into()
}

fn register(subject: &str, body: serde_json::Value) -> u32 {
    let mut response = registry()
        .post(format!("{REGISTRY}/subjects/{subject}/versions"))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .send(body.to_string())
        .unwrap();
    let mut bytes = Vec::new();
    response
        .body_mut()
        .as_reader()
        .take(65537)
        .read_to_end(&mut bytes)
        .unwrap();
    assert!(bytes.len() <= 65536);
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    body["id"].as_u64().unwrap().try_into().unwrap()
}

fn message(n: u8, ids: [u32; 2]) -> Vec<u8> {
    let mut raw = vec![0];
    raw.extend(ids[usize::from(n % 2)].to_be_bytes());
    if n.is_multiple_of(3) {
        raw.extend([0, 8, n]); // shortcut [0], Event
        if n % 2 == 1 {
            raw.extend([26, 1, b'x']);
        }
    } else {
        raw.extend([4, 2, 0, 10, 2, 8, n]); // [1, 0], Outer.Inner with imported Child
    }
    raw
}

#[tokio::test]
#[ignore = "creates a test-owned topic and Protobuf subjects on disposable Redpanda"]
async fn protobuf_registry_versions_imports_indexes_replay_and_following() {
    let topic = format!("onetui_proto_registry_{}", std::process::id());
    let config = {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:29092")
            .set("allow.auto.create.topics", "false")
            .set("message.timeout.ms", "5000");
        config
    };
    let admin: AdminClient<_> = config.create().unwrap();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    for result in admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let owned = topic.clone();
    let tested = tokio::spawn(async move {
        let child = format!("{owned}_child");
        register(&child, json!({"schemaType":"PROTOBUF", "schema":r#"syntax="proto3"; package demo; message Child {uint32 id=1;}"#}));
        let mut ids = [0; 2];
        for (version, id) in ids.iter_mut().enumerate() {
            let extra = if version == 1 { "string added=3;" } else { "" };
            let source = format!("syntax=\"proto3\"; package demo; import \"child.proto\"; message Event {{uint32 id=1; {extra}}} message Outer {{message Inner {{Child child=1;}}}}");
            *id = register(&format!("{owned}_value"), json!({"schemaType":"PROTOBUF","schema":source,"references":[{"name":"child.proto","subject":child,"version":1}]}));
        }
        assert_ne!(ids[0], ids[1]);
        let producer: FutureProducer = config.create().unwrap();
        for n in 0..125 {
            producer.send(FutureRecord::to(&owned).partition(0).key("demo").payload(&message(n, ids)), Duration::from_secs(5)).await.unwrap();
        }
        let options = toml::from_str(&format!("bootstrap_servers=['127.0.0.1:29092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic={owned:?}\nfield='value'\nformat='protobuf'\nframing='confluent'\nregistry={{url={REGISTRY:?}}}")).unwrap();
        let mut executor = KafkaProvider.configure(&options, &|_| panic!("no credentials")).unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        let resource = Resource::new("kafka.records", vec![owned.clone(), "0".into()]);
        let first = fetch(&executor, resource.clone(), None).await;
        assert_eq!(first.rows.len(), 100);
        for (n, row) in first.rows.iter().enumerate() {
            assert_eq!(row.cells[3], Some(Value::Bytes(message(n as u8, ids))));
            assert_eq!(row.cells[7], None, "{:?}", row.cells);
            let value: serde_json::Value = serde_json::from_str(row.cells[5].as_ref().unwrap().text().unwrap()).unwrap();
            // Protobuf JSON omits zero-valued scalar fields.
            if n > 0 { assert_eq!(if n % 3 == 0 { &value["id"] } else { &value["child"]["id"] }, n); }
            assert!(row.cells[6].as_ref().unwrap().text().unwrap().contains(&format!("#id={}&message=demo.", ids[n % 2])));
        }
        let tail = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(tail.rows.len(), 25);
        assert_eq!(fetch(&executor, resource.clone(), first.continuation.clone()).await.rows[0].cells, tail.rows[0].cells);
        let start = follow(&executor, resource.clone(), None).await;
        assert!(start.rows.is_empty());
        for raw in [vec![0, 0, 0, 0, 1, 128], vec![0, 0x7f, 255, 255, 255, 0], message(125, ids)] {
            producer.send(FutureRecord::to(&owned).partition(0).key("demo").payload(&raw), Duration::from_secs(5)).await.unwrap();
        }
        let live = follow(&executor, resource.clone(), start.continuation).await;
        assert_eq!(live.rows.len(), 3);
        assert!(live.rows[0].cells[7].is_some());
        assert!(live.rows[1].cells[7].as_ref().unwrap().text().unwrap().contains("40403"));
        assert_eq!(live.rows[2].cells[7], None);
        assert_eq!(live.rows[2].cells[3], Some(Value::Bytes(message(125, ids))));
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor.query_page(QueryRequest {page:PageRequest {resource:Resource::new("kafka.query", resource.path), continuation:None}, text:r#"{"offset":1,"end_offset":3}"#.into()}, context).await.unwrap();
        assert_eq!(replay.rows[0].cells, first.rows[1].cells);
        executor.shutdown(ShutdownContext::new(Duration::from_secs(3))).await.unwrap();
    }).await;
    // Only this test's topic and subjects are removed; demo data stays untouched.
    for result in admin.delete_topics(&[&topic], &opts).await.unwrap() {
        result.unwrap();
    }
    for suffix in ["value", "child"] {
        registry()
            .delete(format!("{REGISTRY}/subjects/{topic}_{suffix}"))
            .call()
            .unwrap();
    }
    tested.unwrap();
}
