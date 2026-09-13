use super::*;
use std::collections::BTreeSet;

fn keys(page: &Page) -> Vec<(i32, i64)> {
    page.rows
        .iter()
        .map(|row| {
            (
                row.cells[0]
                    .as_ref()
                    .unwrap()
                    .text()
                    .unwrap()
                    .parse()
                    .unwrap(),
                row.cells[1]
                    .as_ref()
                    .unwrap()
                    .text()
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires seeded Kafka; reads only disposable demo data"]
async fn kafka_topic_pages_preserve_partition_offsets_and_bookmarks() {
    let mut executor = executor();
    let resource = Resource::new("kafka.records", vec!["demo_events".into()]);
    let first = fetch(&executor, resource.clone(), None).await;
    assert_eq!(first.columns[0].name, "partition");
    assert_eq!(first.rows.len(), 100);
    assert_eq!(
        keys(&first).iter().map(|k| k.0).collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2])
    );
    let bookmark = first.continuation.clone();
    let second = fetch(&executor, resource.clone(), bookmark.clone()).await;
    let mut all = keys(&first);
    let mut next = first.continuation.clone();
    let mut pages = 1;
    while next.is_some() {
        let page = fetch(&executor, resource.clone(), next).await;
        assert!(page.rows.len() <= 100 && page.bytes() <= onetui_core::PAGE_BYTES);
        all.extend(keys(&page));
        next = page.continuation;
        pages += 1;
        assert!(pages <= 20, "cursor must make progress");
    }
    assert_eq!(all.len(), 1500);
    assert_eq!(all.iter().copied().collect::<BTreeSet<_>>().len(), 1500);
    for partition in 0..3 {
        assert_eq!(
            all.iter()
                .filter(|k| k.0 == partition)
                .map(|k| k.1)
                .collect::<Vec<_>>(),
            (0..500).collect::<Vec<_>>()
        );
    }
    assert_eq!(
        keys(&fetch(&executor, resource.clone(), bookmark).await),
        keys(&second)
    );
    assert_eq!(keys(&fetch(&executor, resource, None).await), keys(&first));
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "creates and removes one unique topic on the disposable Kafka fixture"]
async fn kafka_topic_follow_transactions_limits_and_partition_changes() {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
    use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
    let topic = format!(
        "onetui_multi_{}_{}",
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
            &[NewTopic::new(&topic, 3, TopicReplication::Fixed(1))],
            &options,
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let owned = topic.clone();
    let tested = tokio::spawn(async move {
        let mut executor = executor();
        let resource = Resource::new("kafka.records", vec![owned.clone()]);
        let tail = follow(&executor, resource.clone(), None).await;
        assert!(tail.rows.is_empty());
        let producer: FutureProducer = config
            .set("transactional.id", &owned)
            .set("compression.type", "zstd")
            .create()
            .unwrap();
        producer.init_transactions(Duration::from_secs(10)).unwrap();
        producer.begin_transaction().unwrap();
        for n in 0..240 {
            producer
                .send(
                    FutureRecord::to(&owned)
                        .partition(n % 2)
                        .key("multi")
                        .payload(&n.to_string()),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        let open = follow(&executor, resource.clone(), tail.continuation).await;
        assert!(open.rows.is_empty(), "open transactions must remain hidden");
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let mut token = open.continuation;
        let mut all = Vec::new();
        // A commit acknowledgement can precede visibility at each partition.
        // Empty live batches are valid; assert delivery, not a poll count.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while all.len() < 240 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "committed records did not arrive: {}",
                all.len()
            );
            let page = follow(&executor, resource.clone(), token).await;
            assert!(page.rows.len() <= 100 && page.bytes() <= onetui_core::PAGE_BYTES);
            if page.rows.is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            all.extend(keys(&page));
            token = page.continuation;
        }
        assert_eq!(all.len(), 240);
        assert_eq!(all.iter().copied().collect::<BTreeSet<_>>().len(), 240);
        assert_eq!(all.iter().filter(|k| k.0 == 0).count(), 120);
        assert_eq!(all.iter().filter(|k| k.0 == 1).count(), 120);
        assert_eq!(all.iter().filter(|k| k.0 == 2).count(), 0);
        producer.begin_transaction().unwrap();
        producer
            .send(
                FutureRecord::to(&owned)
                    .partition(2)
                    .key("aborted")
                    .payload("hidden"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        producer.abort_transaction(Duration::from_secs(10)).unwrap();
        let quiet = follow(&executor, resource.clone(), token).await;
        assert!(quiet.rows.is_empty());
        producer.begin_transaction().unwrap();
        for partition in 0..2 {
            producer
                .send(
                    FutureRecord::to(&owned)
                        .partition(partition)
                        .key("large")
                        .payload(&vec![b'x'; 600_000]),
                    Duration::from_secs(5),
                )
                .await
                .unwrap();
        }
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let one = follow(&executor, resource.clone(), quiet.continuation).await;
        let two = follow(&executor, resource.clone(), one.continuation.clone()).await;
        assert_eq!(one.rows.len(), 1);
        assert_eq!(two.rows.len(), 1);
        assert_ne!(keys(&one)[0].0, keys(&two)[0].0);
        assert_eq!(
            one.rows[0].cells[4].as_ref().unwrap().bytes().len(),
            600_000
        );
        let (cancel, context) = RequestContext::new(Duration::from_secs(5));
        cancel.send(()).unwrap();
        assert!(
            executor
                .follow_page(
                    PageRequest {
                        resource: resource.clone(),
                        continuation: two.continuation.clone()
                    },
                    context
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
        let unchanged = follow(&executor, resource.clone(), two.continuation.clone()).await;
        assert!(unchanged.rows.is_empty());
        let admin: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:19092")
            .create()
            .unwrap();
        for result in admin
            .create_partitions(&[NewPartitions::new(&owned, 4)], &AdminOptions::new())
            .await
            .unwrap()
        {
            result.unwrap();
        }
        // The controller can acknowledge expansion before the new partition is readable.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let ready = producer
                .client()
                .fetch_metadata(Some(&owned), Duration::from_millis(200))
                .is_ok_and(|metadata| {
                    metadata.topics().iter().any(|topic| {
                        topic.name() == owned
                            && topic.error().is_none()
                            && topic.partitions().len() == 4
                            && topic.partitions().iter().all(|partition| {
                                partition.error().is_none() && partition.leader() >= 0
                            })
                    })
                });
            if ready
                && producer
                    .client()
                    .fetch_watermarks(&owned, 3, Duration::from_millis(200))
                    .is_ok()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "new partition did not become readable"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .follow_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: unchanged.continuation,
                },
                context,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("partition set changed"),
            "{error:#}"
        );
        let restarted = follow(&executor, resource, None).await;
        assert!(restarted.rows.is_empty());
        assert!(restarted.notice.contains("4 partitions"));
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
    })
    .await;
    for result in admin.delete_topics(&[&topic], &options).await.unwrap() {
        result.unwrap();
    }
    tested.unwrap();
}
