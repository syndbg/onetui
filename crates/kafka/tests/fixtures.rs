use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, QueryRequest, RequestContext,
    ShutdownContext,
};
use onetui_core::{Page, Resource, Value};
use onetui_kafka::{KafkaExecutor, KafkaProvider};
use std::time::Duration;
#[path = "support/registry.rs"]
mod server;

#[cfg(unix)]
#[path = "fixtures/terminal.rs"]
mod terminal;

#[path = "fixtures/topic.rs"]
mod topic;

#[path = "fixtures/decoding_avro.rs"]
mod decoding_avro;
#[path = "fixtures/decoding_protobuf.rs"]
mod decoding_protobuf;
#[path = "fixtures/demo_avro.rs"]
mod demo_avro;
#[path = "fixtures/demo_protobuf.rs"]
mod demo_protobuf;
#[path = "fixtures/kerberos.rs"]
mod kerberos;
#[path = "fixtures/mtls.rs"]
mod mtls;
#[path = "fixtures/oauth.rs"]
mod oauth;
#[path = "fixtures/redpanda.rs"]
mod redpanda;
#[path = "fixtures/registry_avro.rs"]
mod registry_avro;
#[path = "fixtures/registry_protobuf.rs"]
mod registry_protobuf;

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

async fn follow(
    executor: &KafkaExecutor,
    resource: Resource,
    continuation: Option<String>,
) -> Page {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .follow_page(
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
#[ignore = "pauses/unpauses only the disposable Kafka fixture; no data reset"]
async fn kafka_broker_stall_cancellation_deadline_and_recovery() {
    fn broker(action: &str) {
        let output = std::process::Command::new("docker")
            .args([
                "compose",
                "--project-name",
                "onetui-fixtures",
                "--env-file",
                "/dev/null",
                "-f",
                "hack/compose.yaml",
                action,
                "kafka",
            ])
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut current = executor();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    current.check(context).await.unwrap();
    broker("pause");
    // Await the assertion task as a result so panic cannot skip unpausing the fixture.
    let tested = tokio::spawn(async move {
        for resource in [
            Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
            Resource::new("kafka.topic_config", vec!["demo_events".into()]),
            Resource::new("kafka.offsets", vec!["fixture-cancelled-group".into()]),
        ] {
            let started = tokio::time::Instant::now();
            let (cancel, context) = RequestContext::new(Duration::from_secs(5));
            let (result, ()) = tokio::join!(
                async {
                    let following = resource.id == "kafka.records";
                    let request = PageRequest {
                        resource,
                        continuation: None,
                    };
                    if following {
                        current.follow_page(request, context).await
                    } else {
                        current.fetch_page(request, context).await
                    }
                },
                async {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let _ = cancel.send(());
                }
            );
            let error = result.expect_err("cancelled request");
            assert!(error.to_string().contains("cancelled"), "{error:#}");
            assert!(started.elapsed() < Duration::from_millis(750));
            current
                .shutdown(ShutdownContext::new(Duration::from_secs(2)))
                .await
                .unwrap();
            current = executor();
        }
        let started = tokio::time::Instant::now();
        let (_cancel, context) = RequestContext::new(Duration::from_millis(250));
        let error = current
            .check(context)
            .await
            .err()
            .expect("stalled broker deadline");
        assert!(error.to_string().contains("timed out"), "{error:#}");
        assert!(started.elapsed() < Duration::from_millis(750));
        current
    })
    .await;
    broker("unpause");
    let mut current = tested.unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    current.check(context).await.unwrap();
    let page = fetch(
        &current,
        Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
        None,
    )
    .await;
    assert_eq!(page.rows.len(), 100);
    current
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the disposable Kafka TLS/SASL fixture; read-only"]
async fn kafka_verified_tls_sasl_and_native_auth_errors() {
    use std::io::Write;
    let cert = std::process::Command::new("docker")
        .args([
            "compose",
            "--project-name",
            "onetui-fixtures",
            "--env-file",
            "/dev/null",
            "-f",
            "hack/compose.yaml",
            "exec",
            "-T",
            "kafka",
            "cat",
            "/tmp/onetui-kafka-tls/ca.crt",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap();
    assert!(
        cert.status.success(),
        "{}",
        String::from_utf8_lossy(&cert.stderr)
    );
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&cert.stdout).unwrap();
    // Each case exercises the provider, including native teardown before the next alias.
    for (host, protocol, mechanism, username, password, trust, expected) in [
        ("localhost:19093", "SSL", None, "", "", true, None),
        (
            "localhost:19094",
            "SASL_SSL",
            Some("PLAIN"),
            "fixture-reader",
            "fixture-reader-only",
            true,
            None,
        ),
        (
            "localhost:19094",
            "SASL_SSL",
            Some("SCRAM-SHA-256"),
            "fixture-reader",
            "fixture-reader-only",
            true,
            None,
        ),
        (
            "localhost:19094",
            "SASL_SSL",
            Some("SCRAM-SHA-512"),
            "fixture-reader",
            "fixture-reader-only",
            true,
            None,
        ),
        (
            "localhost:19094",
            "SASL_SSL",
            Some("PLAIN"),
            "fixture-reader",
            "wrong-fixture-secret",
            true,
            Some("Invalid username or password"),
        ),
        (
            "127.0.0.1:19093",
            "SSL",
            None,
            "",
            "",
            true,
            Some("certificate verify failed"),
        ),
        (
            "localhost:19093",
            "SSL",
            None,
            "",
            "",
            false,
            Some("certificate verify failed"),
        ),
        (
            "localhost:19094",
            "SASL_SSL",
            Some("PLAIN"),
            "fixture-denied",
            "fixture-denied-only",
            true,
            Some("RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED"),
        ),
    ] {
        let mut options: toml::Table = toml::from_str(&format!(
            "bootstrap_servers=['{host}']\nsecurity_protocol='{protocol}'"
        ))
        .unwrap();
        if trust {
            options.insert("ca_file".into(), ca.path().to_str().unwrap().into());
        }
        if let Some(mechanism) = mechanism {
            options.insert("sasl_mechanism".into(), mechanism.into());
            options.insert("username_env".into(), "FIXTURE_USER".into());
            options.insert("password_env".into(), "FIXTURE_PASSWORD".into());
        }
        let mut executor = KafkaProvider
            .configure(&options, &|name| match name {
                "FIXTURE_USER" => Some(username.into()),
                "FIXTURE_PASSWORD" => Some(password.into()),
                _ => None,
            })
            .unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let result = executor
            .fetch_page(
                PageRequest {
                    resource: Resource::new(
                        "kafka.records",
                        vec!["demo_events".into(), "0".into()],
                    ),
                    continuation: None,
                },
                context,
            )
            .await;
        if expected.is_none() && mechanism.is_some() {
            let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
            let denied = executor
                .fetch_page(
                    PageRequest {
                        resource: Resource::new(
                            "kafka.members",
                            vec!["fixture-protected-group".into()],
                        ),
                        continuation: None,
                    },
                    context,
                )
                .await
                .unwrap_err();
            // ListGroups hides unauthorized names before DescribeGroups can return
            // a per-group error. Absence must not become a successful empty view.
            assert!(
                denied
                    .to_string()
                    .contains("Kafka group missing from metadata"),
                "omitted group must not look like an empty membership: {denied:#}"
            );
            assert!(!denied.to_string().contains(password));
            for (id, name, expected_code) in [
                (
                    "kafka.topic_config",
                    "demo_events",
                    "TOPIC_AUTHORIZATION_FAILED",
                ),
                ("kafka.broker_config", "1", "CLUSTER_AUTHORIZATION_FAILED"),
                (
                    "kafka.offsets",
                    "fixture-protected-group",
                    "GROUP_AUTHORIZATION_FAILED",
                ),
            ] {
                let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
                let denied = executor
                    .fetch_page(
                        PageRequest {
                            resource: Resource::new(id, vec![name.into()]),
                            continuation: None,
                        },
                        context,
                    )
                    .await
                    .unwrap_err();
                assert!(
                    denied.to_string().contains(expected_code),
                    "{id}: {denied:#}"
                );
                assert!(!denied.to_string().contains(password));
            }
        }
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
        match expected {
            Some(expected) => {
                let error = format!("{:#}", result.unwrap_err());
                assert!(error.contains(expected), "{host} {mechanism:?}: {error}");
                assert!(!error.contains("wrong-fixture-secret"));
            }
            None => match result {
                Ok(page) => assert_eq!(page.rows.len(), 100, "{host} {mechanism:?}"),
                Err(error) => panic!("{host} {mechanism:?}: {error:#}"),
            },
        }
    }
}

#[tokio::test]
#[ignore = "requires the disposable seeded Kafka fixture; read-only"]
async fn kafka_replays_offsets_timestamps_and_exclusive_ranges() {
    let mut executor = executor();
    let request = |text: &str, continuation: Option<String>| QueryRequest {
        page: PageRequest {
            resource: Resource::new("kafka.query", vec!["demo_events".into(), "0".into()]),
            continuation,
        },
        // The verb line carries the whole read; each case supplies only its range.
        text: format!("CONSUME demo_events/0 {text}"),
    };
    let text = "offsets 123..250";
    let mut token = None;
    let mut bookmark = None;
    for (start, count) in [(123, 100), (223, 27)] {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let page = executor
            .query_page(request(text, token), context)
            .await
            .unwrap();
        assert_eq!(page.rows.len(), count);
        assert_eq!(page.rows[0].cells[0], Some(start.to_string().into()));
        if start == 123 {
            bookmark = page.continuation.clone();
        }
        token = page.continuation;
    }
    assert!(token.is_none());
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let again = executor
        .query_page(request(text, bookmark.clone()), context)
        .await
        .unwrap();
    assert_eq!(again.rows[0].cells[0], Some("223".into()));
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    assert!(
        executor
            .query_page(request("offsets ..", bookmark), context)
            .await
            .is_err()
    );
    // The fixture assigns event n to partition n % 3 with timestamp base + n.
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let timed = executor
        .query_page(request("time 1750000000369..130", None), context)
        .await
        .unwrap();
    assert_eq!(timed.rows.len(), 7);
    assert_eq!(timed.rows[0].cells[0], Some("123".into()));
    for text in [
        "time 1750009999999..",
        "offsets 250..250",
        "time 1750000000369..100",
    ] {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let page = executor
            .query_page(request(text, None), context)
            .await
            .unwrap();
        assert!(page.rows.is_empty() && !page.next);
    }
    for text in ["offsets 501..", "offsets ..501"] {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .query_page(request(text, None), context)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("available"), "{error:#}");
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
    }
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    cancel.send(()).unwrap();
    assert!(
        executor
            .query_page(request("offsets ..", None), context)
            .await
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
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
    let brokers = fetch(&executor, Resource::new("kafka.brokers", vec![]), None).await;
    assert_eq!(brokers.rows.len(), 1);
    assert_eq!(brokers.rows[0].cells[0], Some("1".into()));
    assert_eq!(brokers.rows[0].cells[2], Some("19092".into()));
    let target = topics
        .rows
        .iter()
        .find(|row| row.cells[0] == Some("demo_events".into()))
        .unwrap()
        .target
        .clone()
        .unwrap();
    let topic_menu = fetch(&executor, target, None).await;
    assert_eq!(topic_menu.rows.len(), 3);
    let config = fetch(&executor, topic_menu.rows[1].target.clone().unwrap(), None).await;
    let cleanup = config
        .rows
        .iter()
        .find(|row| row.cells[0] == Some("cleanup.policy".into()))
        .unwrap();
    assert_eq!(cleanup.cells[1], Some("delete".into()));
    assert_eq!(config.columns[1].datatype, "text");
    assert_eq!(config.columns[6].name, "synonyms");
    let synonyms: serde_json::Value =
        serde_json::from_slice(cleanup.cells[6].as_ref().unwrap().bytes()).unwrap();
    assert!(!synonyms.as_array().unwrap().is_empty());
    assert_eq!(synonyms[0]["value"], "delete");
    assert!(synonyms[0]["source"].is_string());
    let broker_resource = brokers.rows[0].target.clone().unwrap();
    let first_config = fetch(&executor, broker_resource.clone(), None).await;
    assert_eq!(first_config.rows.len(), 100);
    assert!(first_config.next);
    let mut config_page = first_config.clone();
    let mut sensitive = 0;
    loop {
        for row in &config_page.rows {
            if row.cells[5] == Some("true".into()) {
                sensitive += 1;
                assert_eq!(row.cells[1], None, "sensitive config must be withheld");
                let synonyms: serde_json::Value =
                    serde_json::from_slice(row.cells[6].as_ref().unwrap().bytes()).unwrap();
                assert!(
                    synonyms
                        .as_array()
                        .unwrap()
                        .iter()
                        .all(|s| s["value"].is_null())
                );
            }
        }
        let Some(token) = config_page.continuation else {
            break;
        };
        config_page = fetch(&executor, broker_resource.clone(), Some(token)).await;
    }
    assert!(sensitive > 0);
    let again = fetch(&executor, broker_resource, None).await;
    assert_eq!(again.rows.len(), first_config.rows.len());
    for (actual, expected) in again.rows.iter().zip(&first_config.rows) {
        assert_eq!(actual.cells, expected.cells);
    }
    let partitions = fetch(&executor, topic_menu.rows[0].target.clone().unwrap(), None).await;
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
        let tail = follow(&executor, resource.clone(), None).await;
        assert!(tail.rows.is_empty());
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
        let large_resource = Resource::new("kafka.records", vec![test_topic.clone(), "1".into()]);
        let large_tail = follow(&executor, large_resource.clone(), None).await;
        producer.begin_transaction().unwrap();
        for _ in 0..2 {
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
        }
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let large_first = follow(&executor, large_resource.clone(), large_tail.continuation).await;
        assert_eq!(
            large_first.rows.len(),
            1,
            "byte budget must split a batch without skipping its next record"
        );
        let large_second = follow(&executor, large_resource, large_first.continuation).await;
        assert_eq!(large_second.rows.len(), 1);
        assert_eq!(
            large_first.rows[0].cells[3].as_ref().unwrap().bytes(),
            near_limit
        );
        assert_eq!(
            large_second.rows[0].cells[3].as_ref().unwrap().bytes(),
            near_limit
        );
        assert_ne!(large_first.rows[0].cells[0], large_second.rows[0].cells[0]);
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
        observer.subscribe(&[&test_topic]).unwrap();
        let joined_by = std::time::Instant::now() + Duration::from_secs(10);
        while observer.assignment().unwrap().count() == 0 {
            let _ = observer.poll(Duration::from_millis(100));
            assert!(
                std::time::Instant::now() < joined_by,
                "fixture consumer did not join its own test group"
            );
        }
        let groups = fetch(&executor, Resource::new("kafka.groups", vec![]), None).await;
        let group_name = format!("{test_topic}_application");
        let group = groups
            .rows
            .iter()
            .find(|row| row.cells[0] == Some(group_name.clone().into()))
            .expect("fixture application group");
        assert_eq!(group.cells[5], Some("1".into()));
        assert_eq!(group.cells[6], Some("127.0.0.1".into()));
        assert_eq!(group.cells[7], Some("19092".into()));
        let group_menu = fetch(&executor, group.target.clone().unwrap(), None).await;
        let members = fetch(&executor, group_menu.rows[0].target.clone().unwrap(), None).await;
        assert_eq!(members.rows.len(), 1);
        assert!(
            matches!(&members.rows[0].cells[3], Some(Value::Bytes(value)) if !value.is_empty())
        );
        assert!(
            matches!(&members.rows[0].cells[4], Some(Value::Bytes(value)) if !value.is_empty())
        );
        observer.unsubscribe();
        let inspected = fetch(&executor, group_menu.rows[1].target.clone().unwrap(), None).await;
        assert_eq!(inspected.rows.len(), 1);
        assert_eq!(inspected.rows[0].cells[0], Some(test_topic.clone().into()));
        assert_eq!(inspected.rows[0].cells[2], Some("7".into()));
        let (_, stable_end) = observer
            .fetch_watermarks(&test_topic, 0, Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            inspected.rows[0].cells[4],
            Some(stable_end.to_string().into())
        );
        assert_eq!(
            inspected.rows[0].cells[5],
            Some((stable_end - 7).to_string().into())
        );
        let empty = fetch(
            &executor,
            Resource::new("kafka.offsets", vec![format!("{test_topic}_absent")]),
            None,
        )
        .await;
        assert!(empty.rows.is_empty());
        let live_first = follow(&executor, resource.clone(), tail.continuation.clone()).await;
        let live_second = follow(&executor, resource.clone(), live_first.continuation).await;
        assert_eq!(live_first.rows.len() + live_second.rows.len(), 120);
        assert!(
            !live_second
                .rows
                .iter()
                .any(|row| row.cells[3].as_ref().unwrap().bytes() == b"aborted")
        );
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
        let quiet = follow(&executor, resource.clone(), live_second.continuation).await;
        assert!(
            quiet.rows.is_empty(),
            "aborted transaction must not appear in follow"
        );
        producer.begin_transaction().unwrap();
        producer
            .send(
                FutureRecord::to(&test_topic)
                    .partition(0)
                    .key("key")
                    .payload("live commit"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let open = follow(&executor, resource.clone(), quiet.continuation).await;
        assert!(
            open.rows.is_empty(),
            "open transaction must not appear in follow"
        );
        producer
            .commit_transaction(Duration::from_secs(10))
            .unwrap();
        let committed = follow(&executor, resource.clone(), open.continuation).await;
        assert_eq!(committed.rows.len(), 1);
        assert_eq!(
            committed.rows[0].cells[3].as_ref().unwrap().bytes(),
            b"live commit"
        );
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
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .follow_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: committed.continuation,
                },
                context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1 MiB"), "{error:#}");
        let first = fetch(&executor, resource.clone(), None).await;
        let token = first.continuation.unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .fetch_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: Some(token.clone()),
                },
                context,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("1 MiB"), "{error:#}");
        let retention: AdminClient<_> = config.create().unwrap();
        let mut truncate = TopicPartitionList::new();
        truncate
            .add_partition_offset(&test_topic, 0, Offset::Offset(110))
            .unwrap();
        let deleted = retention
            .delete_records(
                &truncate,
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
            )
            .await
            .unwrap();
        {
            let partition = deleted.find_partition(&test_topic, 0).unwrap();
            partition.error().unwrap();
            assert_eq!(partition.offset(), Offset::Offset(110));
        }
        let retained = fetch(&executor, group_menu.rows[1].target.clone().unwrap(), None).await;
        assert_eq!(retained.rows[0].cells[2], Some("7".into()));
        assert_eq!(retained.rows[0].cells[3], Some("110".into()));
        assert_eq!(retained.rows[0].cells[5], None);
        assert_eq!(
            retained.rows[0].cells[6],
            Some("before retained start".into())
        );
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let error = executor
            .follow_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: tail.continuation,
                },
                context,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("offsets unavailable"),
            "{error:#}"
        );
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
        assert!(
            error.to_string().contains("offsets unavailable"),
            "{error:#}"
        );
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
