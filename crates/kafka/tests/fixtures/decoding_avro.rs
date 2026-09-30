use super::*;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
};

#[tokio::test]
#[ignore = "creates and removes one avro topic on the disposable Kafka fixture"]
async fn kafka_avro_bindings_browse_replay_and_follow_without_losing_raw_data() {
    exercise_binding(false).await;
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "creates and removes one avro catalog topic on the disposable Kafka fixture"]
async fn kafka_avro_catalog_browse_replay_and_follow() {
    exercise_binding(true).await;
}

fn avro_options(
    topic: &str,
    dir: &std::path::Path,
    schema: &std::path::Path,
    reader: &std::path::Path,
    catalog: bool,
) -> toml::Table {
    let mut options = toml::Table::new();
    options.insert(
        "bootstrap_servers".into(),
        toml::Value::Array(vec!["127.0.0.1:19092".into()]),
    );
    options.insert("security_protocol".into(), "PLAINTEXT".into());
    let mut binding = toml::Table::new();
    binding.insert("topic".into(), topic.into());
    binding.insert("field".into(), "value".into());
    binding.insert("format".into(), "avro".into());
    binding.insert("framing".into(), "raw".into());
    binding.insert("reader_schema_file".into(), reader.to_str().unwrap().into());
    if catalog {
        let mut source = toml::Table::new();
        source.insert("directory".into(), dir.to_str().unwrap().into());
        source.insert("schema".into(), "event".into());
        binding.insert("catalog".into(), source.into());
    } else {
        binding.insert("schema_file".into(), schema.to_str().unwrap().into());
    }

    options.insert(
        "decoders".into(),
        toml::Value::Array(vec![toml::Value::Table(binding)]),
    );
    options
}

async fn seed_avro_records(producer: &FutureProducer, topic: &str) {
    for n in 0..125 {
        let raw: Option<&[u8]> = match n {
            1 => Some(&[255]),
            2 => Some(&[]),
            3 => None,
            _ => Some(&[14]),
        };
        let mut record = FutureRecord::<[u8], [u8]>::to(topic)
            .partition(0)
            .key(b"unchanged");
        if let Some(raw) = raw {
            record = record.payload(raw);
        }
        producer.send(record, Duration::from_secs(5)).await.unwrap();
    }
}

async fn check_first_avro_page(
    executor: &KafkaExecutor,
    resource: &Resource,
    catalog: bool,
    schema: &std::path::Path,
) -> Page {
    let first = fetch(executor, resource.clone(), None).await;
    assert_eq!(first.rows.len(), 100);
    if catalog {
        assert!(
            first.rows[0].cells[6]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains("#schema=event:")
        );
        // Refresh and follow must keep the binding snapshot after a file edit.
        std::fs::write(schema, b"invalid replacement").unwrap();
    }
    assert_eq!(first.rows[0].cells[5], Some(Value::Json("7".into())));
    let native: serde_json::Value =
        serde_json::from_slice(first.rows[0].cells[8].as_ref().unwrap().bytes()).unwrap();
    assert_eq!(native["writer"]["type"], "int");
    assert_eq!(native["reader"]["type"], "long");
    assert!(
        first.rows[0].cells[6]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("&reader=")
    );
    assert!(first.rows[1].cells[5].is_none() && first.rows[1].cells[7].is_some());
    assert!(first.rows[2].cells[7].is_some());
    assert!(first.rows[3].cells[5].is_none() && first.rows[3].cells[7].is_none());
    assert_eq!(first.rows[0].cells[3], Some(Value::Bytes(vec![14])));
    assert_eq!(
        first.rows[0].cells[2],
        Some(Value::Bytes(b"unchanged".to_vec()))
    );
    first
}

async fn check_second_avro_page(executor: &KafkaExecutor, resource: &Resource, first: &Page) {
    let second = fetch(executor, resource.clone(), first.continuation.clone()).await;
    assert_eq!(second.rows.len(), 25);
    assert_eq!(
        second.rows.iter().map(|r| &r.cells).collect::<Vec<_>>(),
        fetch(executor, resource.clone(), first.continuation.clone())
            .await
            .rows
            .iter()
            .map(|r| &r.cells)
            .collect::<Vec<_>>()
    );
}

async fn check_avro_following(
    executor: &KafkaExecutor,
    resource: &Resource,
    start: Page,
    first: &Page,
    columns: &[String],
) {
    let live = follow(executor, resource.clone(), start.continuation).await;
    assert_eq!(live.rows.len(), 100);
    assert_eq!(
        live.columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
        columns
    );
    assert!(live.rows[1].cells[7].is_some());
    let next = follow(executor, resource.clone(), live.continuation).await;
    assert_eq!(next.rows.len(), 25);
    assert!(next.rows[0].cells[5].is_some());
    assert_eq!(next.rows[0].cells[6], first.rows[0].cells[6]);
    let quiet = follow(executor, resource.clone(), next.continuation).await;
    assert!(quiet.rows.is_empty());
    assert_eq!(
        quiet
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
        columns
    );
}

async fn check_avro_replay(executor: &KafkaExecutor, resource: &Resource, topic: &str) {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let replay = executor
        .query_page(
            QueryRequest {
                page: PageRequest {
                    resource: Resource::new("kafka.query", resource.path.clone()),
                    continuation: None,
                },
                text: format!(
                    "CONSUME {}/{} offsets 1..5",
                    resource.path[0], resource.path[1]
                ),
            },
            context,
        )
        .await
        .unwrap();
    assert_eq!(replay.rows.len(), 4);
    assert!(replay.rows[0].cells[7].is_some());
    let whole = fetch(
        executor,
        Resource::new("kafka.records", vec![topic.to_owned()]),
        None,
    )
    .await;
    assert_eq!(whole.columns[0].name, "partition");
    assert_eq!(whole.rows[0].cells[6], Some(Value::Json("7".into())));
}

async fn exercise_avro(
    topic: String,
    options: toml::Table,
    native: ClientConfig,
    schema: std::path::PathBuf,
    catalog: bool,
) {
    let mut executor = KafkaProvider.configure(&options, &|_| None).unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let resource = Resource::new("kafka.records", vec![topic.clone(), "0".into()]);
    let start = follow(&executor, resource.clone(), None).await;
    assert!(start.rows.is_empty());
    let columns = start
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<_>>();
    assert!(columns.contains(&"value_decoded".into()));
    let producer: FutureProducer = native.create().unwrap();
    seed_avro_records(&producer, &topic).await;
    let first = check_first_avro_page(&executor, &resource, catalog, &schema).await;
    check_second_avro_page(&executor, &resource, &first).await;
    check_avro_following(&executor, &resource, start, &first, &columns).await;
    check_avro_replay(&executor, &resource, &topic).await;
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
    #[cfg(unix)]
    if !catalog {
        super::terminal::assert_avro_projection(options, &topic);
    }
}

async fn exercise_binding(catalog: bool) {
    let topic = format!("onetui_avro_{}_{}", std::process::id(), catalog);
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("event.avsc");
    std::fs::write(&schema, r#""int""#).unwrap();
    let reader = dir.path().join("reader.avsc");
    std::fs::write(&reader, r#""long""#).unwrap();
    let options = avro_options(&topic, dir.path(), &schema, &reader, catalog);
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
    let tested = tokio::spawn(exercise_avro(task_topic, options, native, schema, catalog)).await;
    for result in admin
        .delete_topics(&[&topic], &admin_options)
        .await
        .unwrap()
    {
        result.unwrap();
    }
    tested.unwrap();
}
