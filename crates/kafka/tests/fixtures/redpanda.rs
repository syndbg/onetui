use super::*;
#[path = "../../examples/support/redpanda.rs"]
mod fixture;
use rdkafka::{
    admin::{AdminClient, AdminOptions},
    producer::{FutureProducer, FutureRecord},
};

#[tokio::test]
#[ignore = "requires disposable Redpanda and its real Schema Registry"]
async fn redpanda_registry_versions_references_paging_and_following() {
    let topic = format!("onetui_registry_{}", std::process::id());
    let owned = topic.clone();
    let tested = tokio::spawn(async move {
        fixture::seed(&owned, 125).await.unwrap();
        // Repeated setup must not append duplicate demo records.
        fixture::seed(&owned, 125).await.unwrap();
        let ids = fixture::schemas(&owned).unwrap();
        assert_ne!(ids[0], ids[1]);
        let options = toml::from_str(&format!("bootstrap_servers=[{:?}]\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic={owned:?}\nfield='value'\nformat='avro'\nframing='confluent'\n[decoders.registry]\nurl={:?}", fixture::BROKER, fixture::REGISTRY)).unwrap();
        let mut executor = KafkaProvider.configure(&options, &|_| panic!("fixture needs no credentials")).unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        let resource = Resource::new("kafka.records", vec![owned.clone(), "0".into()]);
        let first = fetch(&executor, resource.clone(), None).await;
        assert_eq!(first.rows.len(), 100);
        for (n, row) in first.rows.iter().enumerate() {
            assert_eq!(row.cells[3], Some(Value::Bytes(fixture::message(n as u32, ids).unwrap())));
            assert_eq!(row.cells[7], None, "{:?}", row.cells);
            let decoded: serde_json::Value = serde_json::from_str(row.cells[5].as_ref().unwrap().text().unwrap()).unwrap();
            assert_eq!(decoded["id"], n);
            assert_eq!(decoded["customer"]["id"], n % 23);
            assert_eq!(decoded.get("note").is_some(), n % 2 == 1);
            assert!(row.cells[6].as_ref().unwrap().text().unwrap().ends_with(&format!("#id={}", ids[n % 2])));
        }
        let tail = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(tail.rows.len(), 25);
        assert_eq!(fetch(&executor, resource.clone(), first.continuation).await.rows[0].cells, tail.rows[0].cells);
        let start = follow(&executor, resource.clone(), None).await;
        assert!(start.rows.is_empty());
        let producer: FutureProducer = fixture::config().create().unwrap();
        let raw = fixture::message(125, ids).unwrap();
        producer.send(FutureRecord::to(&owned).partition(0).key("demo").payload(&raw), Duration::from_secs(5)).await.unwrap();
        let live = follow(&executor, resource.clone(), start.continuation).await;
        assert_eq!(live.rows.len(), 1);
        assert_eq!(live.rows[0].cells[3], Some(Value::Bytes(raw)));
        assert!(live.rows[0].cells[5].is_some());
        assert_eq!(live.rows[0].cells[7], None);
        let missing = [0u8, 0x7f, 0xff, 0xff, 0xff, 14];
        for raw in [&missing[..], &[0u8, 0][..], &fixture::message(126, ids).unwrap()[..]] {
            producer.send(FutureRecord::to(&owned).partition(0).key("demo").payload(raw), Duration::from_secs(5)).await.unwrap();
        }
        let continued = follow(&executor, resource.clone(), live.continuation).await;
        assert_eq!(continued.rows.len(), 3);
        assert_eq!(continued.rows[0].cells[3], Some(Value::Bytes(missing.to_vec())));
        assert!(continued.rows[0].cells[7].as_ref().unwrap().text().unwrap().contains("40403"));
        assert!(continued.rows[1].cells[7].is_some());
        assert!(continued.rows[2].cells[5].is_some());
        assert_eq!(continued.rows[2].cells[7], None);
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor.query_page(QueryRequest {page:PageRequest { resource:Resource::new("kafka.query",resource.path), continuation:None }, text:r#"{"offset":1,"end_offset":3}"#.into()}, context).await.unwrap();
        assert_eq!(replay.rows.len(), 2);
        assert_eq!(replay.rows[0].cells, first.rows[1].cells);
        executor.shutdown(ShutdownContext::new(Duration::from_secs(3))).await.unwrap();
    }).await;
    let admin: AdminClient<_> = fixture::config().create().unwrap();
    let deleted = admin
        .delete_topics(
            &[&topic],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await;
    // The exact test-owned subjects are disposable; shared demo schemas remain untouched.
    for suffix in ["value", "customer"] {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .proxy(None)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .into();
        let response = agent
            .delete(format!("{}/subjects/{topic}_{suffix}", fixture::REGISTRY))
            .call();
        assert!(response.is_ok(), "{response:?}");
    }
    for result in deleted.unwrap() {
        result.unwrap();
    }
    tested.unwrap();
}
