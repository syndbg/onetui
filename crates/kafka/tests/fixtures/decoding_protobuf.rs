use super::*;
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    producer::{FutureProducer, FutureRecord},
};

#[tokio::test]
#[ignore = "creates and removes one protobuf topic on the disposable Kafka fixture"]
async fn kafka_protobuf_bindings_browse_replay_and_follow_without_losing_raw_data() {
    let topic = format!("onetui_protobuf_{}", std::process::id());
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("event.pb");
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    let descriptor = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Event".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    r#type: Some(3),
                    label: Some(1),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    std::fs::write(&schema, descriptor).unwrap();
    let mut options = toml::Table::new();
    options.insert(
        "bootstrap_servers".into(),
        toml::Value::Array(vec!["127.0.0.1:19092".into()]),
    );
    options.insert("security_protocol".into(), "PLAINTEXT".into());
    let mut binding = toml::Table::new();
    binding.insert("topic".into(), topic.clone().into());
    binding.insert("field".into(), "key".into());
    binding.insert("format".into(), "protobuf".into());
    binding.insert("framing".into(), "raw".into());
    binding.insert("schema_file".into(), schema.to_str().unwrap().into());
    binding.insert("message_name".into(), "demo.Event".into());
    options.insert(
        "decoders".into(),
        toml::Value::Array(vec![toml::Value::Table(binding)]),
    );
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
        assert!(columns.contains(&"key_decoded".into()));
        let producer: FutureProducer = native.create().unwrap();
        for n in 0..125 {
            let raw: Option<&[u8]> = match n {
                1 => Some(&[255]),
                2 => Some(&[]),
                3 => None,
                _ => Some(&[8, 7]),
            };
            let mut record = FutureRecord::<[u8], [u8]>::to(&topic)
                .partition(0)
                .payload(b"unchanged");
            if let Some(raw) = raw {
                record = record.key(raw);
            }
            producer.send(record, Duration::from_secs(5)).await.unwrap();
        }
        let first = fetch(&executor, resource.clone(), None).await;
        assert_eq!(first.rows.len(), 100);
        assert_eq!(
            first.rows[0].cells[5],
            Some(Value::Json(r#"{"id":"7"}"#.into()))
        );
        assert!(first.rows[1].cells[5].is_none() && first.rows[1].cells[7].is_some());
        assert_eq!(first.rows[2].cells[5], Some(Value::Json("{}".into())));
        assert!(first.rows[3].cells[5].is_none() && first.rows[3].cells[7].is_none());
        assert_eq!(first.rows[0].cells[2], Some(Value::Bytes(vec![8, 7])));
        assert_eq!(
            first.rows[0].cells[3],
            Some(Value::Bytes(b"unchanged".to_vec()))
        );
        let second = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(second.rows.len(), 25);
        assert_eq!(
            second.rows.iter().map(|r| &r.cells).collect::<Vec<_>>(),
            fetch(&executor, resource.clone(), first.continuation)
                .await
                .rows
                .iter()
                .map(|r| &r.cells)
                .collect::<Vec<_>>()
        );
        let live = follow(&executor, resource.clone(), start.continuation).await;
        assert_eq!(live.rows.len(), 100);
        assert_eq!(
            live.columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            columns
        );
        assert!(live.rows[1].cells[7].is_some());
        let next = follow(&executor, resource.clone(), live.continuation).await;
        assert_eq!(next.rows.len(), 25);
        let quiet = follow(&executor, resource.clone(), next.continuation).await;
        assert!(quiet.rows.is_empty());
        assert_eq!(
            quiet
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
            columns
        );
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor
            .query_page(
                QueryRequest {
                    page: PageRequest {
                        resource: Resource::new("kafka.query", resource.path),
                        continuation: None,
                    },
                    text: r#"{"offset":1,"end_offset":5}"#.into(),
                },
                context,
            )
            .await
            .unwrap();
        assert_eq!(replay.rows.len(), 4);
        assert!(replay.rows[0].cells[7].is_some());
        let whole = fetch(&executor, Resource::new("kafka.records", vec![topic]), None).await;
        assert_eq!(whole.columns[0].name, "partition");
        assert_eq!(
            whole.rows[0].cells[6],
            Some(Value::Json(r#"{"id":"7"}"#.into()))
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
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
