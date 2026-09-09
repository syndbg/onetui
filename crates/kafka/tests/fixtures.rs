use onetui_core::provider::{Executor, PageRequest, Provider, RequestContext, ShutdownContext};
use onetui_core::{Page, Resource, Value};
use onetui_kafka::{KafkaExecutor, KafkaProvider};
use std::time::Duration;

fn executor() -> KafkaExecutor {
    let options =
        toml::from_str("bootstrap_servers=['127.0.0.1:19092']\nsecurity_protocol='PLAINTEXT'")
            .unwrap();
    KafkaProvider
        .configure(&options, &|_| panic!("no fixture credentials"))
        .unwrap()
}

async fn fetch(executor: &KafkaExecutor, resource: Resource, continuation: Option<String>) -> Page {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .fetch_page(
            PageRequest {
                resource,
                continuation,
            },
            context,
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the disposable seeded Kafka fixture; read-only"]
async fn kafka_browses_seeded_partitions_and_refetches_old_bookmarks() {
    let mut executor = executor();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    assert!(
        executor
            .check(context)
            .await
            .unwrap()
            .summary
            .contains("metadata readable")
    );
    let topics = fetch(&executor, Resource::new("kafka.topics", vec![]), None).await;
    let target = topics
        .rows
        .iter()
        .find(|row| row.cells[0] == Some("demo_events".into()))
        .unwrap()
        .target
        .clone()
        .unwrap();
    let partitions = fetch(&executor, target, None).await;
    assert_eq!(partitions.rows.len(), 3);
    let resource = partitions.rows[0].target.clone().unwrap();
    let first = fetch(&executor, resource.clone(), None).await;
    assert_eq!(first.rows.len(), 100, "{}", first.notice);
    assert_eq!(first.rows[0].cells[0], Some("0".into()));
    assert!(matches!(first.rows[0].cells[3], Some(Value::Bytes(_))));
    let mut token = first.continuation.clone();
    let mut count = first.rows.len();
    while let Some(next) = token {
        let page = fetch(&executor, resource.clone(), Some(next)).await;
        count += page.rows.len();
        token = page.continuation;
    }
    assert_eq!(count, 500);
    let previous = fetch(&executor, resource.clone(), first.continuation).await;
    assert_eq!(previous.rows[0].cells[0], Some("100".into()));
    let beginning = fetch(&executor, resource, None).await;
    assert_eq!(beginning.rows[0].cells[0], Some("0".into()));
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the disposable seeded Kafka fixture; read-only"]
async fn kafka_preserves_null_empty_binary_headers_and_empty_partitions() {
    let mut executor = executor();
    let tombstones = fetch(
        &executor,
        Resource::new("kafka.records", vec!["demo_tombstones".into(), "0".into()]),
        None,
    )
    .await;
    assert_eq!(tombstones.rows.len(), 64, "{}", tombstones.notice);
    assert_eq!(tombstones.rows[0].cells[3], None);
    assert_eq!(tombstones.rows[1].cells[3], Some(Value::Bytes(vec![])));
    let headers: serde_json::Value = serde_json::from_str(
        tombstones.rows[0].cells[4]
            .as_ref()
            .unwrap()
            .text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(headers[1]["name"], headers[2]["name"]);
    assert_eq!(headers[1]["value"], serde_json::json!([255, 0]));
    assert!(headers[2]["value"].is_null());
    let binary = fetch(
        &executor,
        Resource::new("kafka.records", vec!["demo_binary".into(), "0".into()]),
        None,
    )
    .await;
    assert!(std::str::from_utf8(binary.rows[0].cells[3].as_ref().unwrap().bytes()).is_err());
    let empty = fetch(
        &executor,
        Resource::new("kafka.records", vec!["demo_empty".into(), "0".into()]),
        None,
    )
    .await;
    assert!(empty.rows.is_empty() && !empty.next);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "writes only a uniquely named topic/group on the disposable Kafka fixture"]
async fn kafka_transactions_limits_and_application_offsets() {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    use rdkafka::{Offset, TopicPartitionList};
    let topic = format!(
        "onetui_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", "127.0.0.1:19092");
    let admin: AdminClient<_> = config.create().unwrap();
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));
    for result in admin
        .create_topics(
            &[NewTopic::new(&topic, 2, TopicReplication::Fixed(1))],
            &options,
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let test_topic = topic.clone();
    let result = tokio::spawn(async move {
        let mut executor = executor();
        let resource = Resource::new("kafka.records", vec![test_topic.clone(), "0".into()]);
        let producer: FutureProducer = config
            .clone()
            .set("transactional.id", &test_topic)
            .set("compression.type", "zstd")
            .set("message.max.bytes", "2097152")
            .create()
            .unwrap();
        producer.init_transactions(Duration::from_secs(10)).unwrap();
        producer.begin_transaction().unwrap();
        for n in 0..120u32 {
            producer
                .send(
                    FutureRecord::to(&test_topic)
                        .partition(0)
                        .key("key")
                        .payload(&n.to_be_bytes()[..]),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        producer.begin_transaction().unwrap();
        producer
            .send(
                FutureRecord::to(&test_topic)
                    .partition(0)
                    .key("key")
                    .payload("aborted"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        producer.abort_transaction(Duration::from_secs(10)).unwrap();
        producer.begin_transaction().unwrap();
        let near_limit = vec![b'x'; onetui_core::PAGE_BYTES - 4096];
        producer
            .send(
                FutureRecord::to(&test_topic)
                    .partition(1)
                    .key("key")
                    .payload(&near_limit),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let near_limit_page = fetch(
            &executor,
            Resource::new("kafka.records", vec![test_topic.clone(), "1".into()]),
            None,
        )
        .await;
        assert_eq!(near_limit_page.rows.len(), 1);
        assert_eq!(
            near_limit_page.rows[0].cells[3].as_ref().unwrap().bytes(),
            near_limit
        );
        let first = fetch(&executor, resource.clone(), None).await;
        let second = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(first.rows.len() + second.rows.len(), 120);
        assert!(
            !second.next,
            "control/aborted offsets must not create an endless page"
        );
        assert!(
            !second
                .rows
                .iter()
                .any(|row| row.cells[3].as_ref().unwrap().bytes() == b"aborted")
        );
        let observer: BaseConsumer = config
            .clone()
            .set("group.id", format!("{test_topic}_application"))
            .set("enable.auto.commit", "false")
            .create()
            .unwrap();
        let mut offsets = TopicPartitionList::new();
        offsets
            .add_partition_offset(&test_topic, 0, Offset::Offset(7))
            .unwrap();
        observer.commit(&offsets, CommitMode::Sync).unwrap();
        let replay = fetch(&executor, resource.clone(), first.continuation).await;
        assert_eq!(replay.rows.len(), 20);
        assert_eq!(
            observer
                .committed_offsets(offsets, Duration::from_secs(5))
                .unwrap()
                .find_partition(&test_topic, 0)
                .unwrap()
                .offset(),
            Offset::Offset(7)
        );
        {
            let groups = observer
                .fetch_group_list(None, Duration::from_secs(5))
                .unwrap();
            assert!(
                !groups
                    .groups()
                    .iter()
                    .any(|g| g.name().starts_with("onetui-")),
                "browser must not join its internal group"
            );
        }
        producer.begin_transaction().unwrap();
        producer
            .send(
                FutureRecord::to(&test_topic)
                    .partition(0)
                    .key("key")
                    .payload("open transaction"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let first = fetch(&executor, resource.clone(), None).await;
        let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
        let stable = executor
            .fetch_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: first.continuation,
                },
                context,
            )
            .await
            .unwrap();
        assert_eq!(
            stable.rows.len(),
            20,
            "window ends before the open transaction"
        );
        assert!(!stable.next);
        assert!(
            !stable
                .rows
                .iter()
                .any(|row| row.cells[3].as_ref().unwrap().bytes() == b"open transaction")
        );
        producer.abort_transaction(Duration::from_secs(10)).unwrap();
        producer.begin_transaction().unwrap();
        let oversized = vec![b'x'; onetui_core::PAGE_BYTES + 1];
        producer
            .send(
                FutureRecord::to(&test_topic)
                    .partition(0)
                    .key("key")
                    .payload(&oversized),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let first = fetch(&executor, resource.clone(), None).await;
        let token = first.continuation.unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .fetch_page(
                PageRequest {
                    resource,
                    continuation: Some(token),
                },
                context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1 MiB"), "{error:#}");
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
        drop(observer);
    })
    .await;
    // Clean the exact test-owned topic/group even when the assertions panic.
    let groups = admin
        .delete_groups(&[&format!("{topic}_application")], &options)
        .await;
    let topics = admin.delete_topics(&[&topic], &options).await;
    for deleted in topics.unwrap() {
        deleted.unwrap();
    }
    for deleted in groups.unwrap() {
        if let Err((_, error)) = deleted {
            assert_eq!(error, rdkafka::error::RDKafkaErrorCode::GroupIdNotFound);
        }
    }
    result.unwrap();
}
