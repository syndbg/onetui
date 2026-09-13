use super::*;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
};

#[tokio::test]
#[ignore = "creates one topic on disposable Kafka and a local registry protocol fixture"]
async fn avro_registry_browsing_following_replay_and_cancellation() {
    let registry = server::Server::start(true, |request| {
        if !request.contains("Bearer fixture-token") {
            return (401, "not authorized".into());
        }
        let body = match request.split_whitespace().nth(1).unwrap() {
            "/schemas/types" => serde_json::json!(["AVRO"]),
            "/schemas/ids/1" => serde_json::json!({"schema":"\"long\""}),
            "/schemas/ids/2" => serde_json::json!({"schema":"\"string\""}),
            "/schemas/ids/6" => {
                std::thread::sleep(Duration::from_millis(1500));
                serde_json::json!({"schema":"\"long\""})
            }
            _ => {
                return (
                    404,
                    r#"{"error_code":40403,"message":"Schema not found"}"#.into(),
                );
            }
        };
        (200, body.to_string())
    });
    let topic = format!("onetui_registry_{}", std::process::id());
    let options = toml::from_str(&format!("bootstrap_servers=['127.0.0.1:19092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic={topic:?}\nfield='value'\nformat='avro'\nframing='confluent'\n[decoders.registry]\nurl={:?}\nca_file={:?}\ntoken_env='FIXTURE_REGISTRY_TOKEN'", registry.url, registry.ca_file.as_deref().unwrap())).unwrap();
    let mut native = ClientConfig::new();
    native
        .set("bootstrap.servers", "127.0.0.1:19092")
        .set("message.timeout.ms", "5000");
    let admin: AdminClient<_> = native.create().unwrap();
    let admin_options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    for result in admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &admin_options,
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let task_topic = topic.clone();
    let tested = tokio::spawn(async move {
        let topic = task_topic;
        let mut executor = KafkaProvider
            .configure(&options, &|name| {
                assert_eq!(name, "FIXTURE_REGISTRY_TOKEN");
                Some("fixture-token".into())
            })
            .unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        let resource = Resource::new("kafka.records", vec![topic.clone(), "0".into()]);
        let start = follow(&executor, resource.clone(), None).await;
        assert!(start.rows.is_empty());
        let producer: FutureProducer = native.create().unwrap();
        for n in 0..125 {
            let raw = match n {
                1 => vec![0, 0, 0, 0, 2, 2, b'a'],
                2 => vec![0, 0, 0, 0, 4, 14],
                3 => vec![0, 0],
                _ => vec![0, 0, 0, 0, 1, 14],
            };
            producer
                .send(
                    FutureRecord::to(&topic)
                        .partition(0)
                        .key("plain")
                        .payload(&raw),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        let first = fetch(&executor, resource.clone(), None).await;
        assert_eq!(first.rows.len(), 100);
        assert_eq!(
            first.rows[0].cells[3],
            Some(Value::Bytes(vec![0, 0, 0, 0, 1, 14]))
        );
        assert_eq!(first.rows[0].cells[5], Some(Value::Json("7".into())));
        assert_eq!(first.rows[1].cells[5], Some(Value::Json("\"a\"".into())));
        assert!(
            first.rows[2].cells[7]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains("40403")
        );
        assert!(first.rows[3].cells[7].is_some());
        let second = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(second.rows.len(), 25);
        assert_eq!(
            fetch(&executor, resource.clone(), first.continuation)
                .await
                .rows[0]
                .cells,
            second.rows[0].cells
        );
        let live = follow(&executor, resource.clone(), start.continuation).await;
        assert_eq!(live.rows.len(), 100);
        assert!(live.rows[2].cells[7].is_some());
        let tail = follow(&executor, resource.clone(), live.continuation).await;
        assert_eq!(tail.rows.len(), 25);
        let quiet = follow(&executor, resource.clone(), tail.continuation.clone()).await;
        assert!(quiet.rows.is_empty());
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor
            .query_page(
                QueryRequest {
                    page: PageRequest {
                        resource: Resource::new("kafka.query", resource.path.clone()),
                        continuation: None,
                    },
                    text: r#"{"offset":1,"end_offset":5}"#.into(),
                },
                context,
            )
            .await
            .unwrap();
        assert_eq!(replay.rows.len(), 4);
        assert_eq!(registry.requests.lock().unwrap().len(), 4); // types + three distinct IDs
        producer
            .send(
                FutureRecord::to(&topic)
                    .partition(0)
                    .key("plain")
                    .payload(&[0u8, 0, 0, 0, 6, 14][..]),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let (cancel, context) = RequestContext::new(Duration::from_secs(5));
        let started = std::time::Instant::now();
        let (result, ()) = tokio::join!(
            executor.follow_page(
                PageRequest {
                    resource,
                    continuation: tail.continuation
                },
                context
            ),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = cancel.send(());
            }
        );
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_millis(500));
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(3)))
            .await
            .unwrap();
        assert_eq!(
            *executor.status().borrow(),
            onetui_core::provider::ConnectionStatus::Closed
        );
    })
    .await;
    for result in admin
        .delete_topics(&[&topic], &admin_options)
        .await
        .unwrap()
    {
        result.unwrap();
    }
    tested.unwrap();
}
