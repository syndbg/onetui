use anyhow::{Result, ensure};
use apache_avro::{Schema, types::Value};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use serde_json::json;
use std::{io::Read, time::Duration};

pub const BROKER: &str = "127.0.0.1:29092";
pub const REGISTRY: &str = "http://127.0.0.1:18081";
const CUSTOMER: &str = r#"{"type":"record","name":"Customer","namespace":"demo","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

pub fn config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", BROKER)
        .set("allow.auto.create.topics", "false")
        .set("message.timeout.ms", "5000");
    config
}

fn writer(version: usize) -> String {
    let mut fields = vec![
        json!({"name":"id","type":"long"}),
        json!({"name":"customer","type":"demo.Customer"}),
        json!({"name":"tags","type":{"type":"array","items":"string"}}),
    ];
    if version == 1 {
        fields.push(json!({"name":"note","type":["null","string"],"default":null}));
    }
    json!({"type":"record","name":"Event","namespace":"demo","fields":fields}).to_string()
}

pub fn schemas(topic: &str) -> Result<[u32; 2]> {
    // These fixture-only names become URL segments, never an arbitrary endpoint.
    ensure!(
        !topic.is_empty()
            && topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "Invalid fixture topic"
    );
    let register = |subject: &str, body: serde_json::Value| -> Result<u32> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .proxy(None)
            .max_redirects(0)
            .timeout_global(Some(Duration::from_secs(5)))
            .build()
            .into();
        let mut response = agent
            .post(format!("{REGISTRY}/subjects/{subject}/versions"))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .send(body.to_string())?;
        let mut bytes = Vec::new();
        response
            .body_mut()
            .as_reader()
            .take(65537)
            .read_to_end(&mut bytes)?;
        ensure!(bytes.len() <= 65536, "Fixture registry response too large");
        let body: serde_json::Value = serde_json::from_slice(&bytes)?;
        let id = body["id"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing registry schema ID"))?;
        ensure!(
            id > 0 && id <= i32::MAX as u64,
            "Invalid registry schema ID"
        );
        Ok(id as u32)
    };
    let subject = format!("{topic}_customer");
    register(&subject, json!({"schemaType":"AVRO","schema":CUSTOMER}))?;
    let mut ids = [0; 2];
    for (version, id) in ids.iter_mut().enumerate() {
        *id = register(
            &format!("{topic}_value"),
            json!({"schemaType":"AVRO","schema":writer(version),"references":[{"name":"demo.Customer","subject":subject,"version":1}]}),
        )?;
    }
    Ok(ids)
}

pub fn message(n: u32, ids: [u32; 2]) -> Result<Vec<u8>> {
    let version = (n % 2) as usize;
    let root = writer(version);
    let schemas = Schema::parse_list([CUSTOMER, &root])?;
    let mut fields = vec![
        ("id".into(), Value::Long(i64::from(n))),
        (
            "customer".into(),
            Value::Record(vec![
                ("id".into(), Value::Long(i64::from(n % 23))),
                (
                    "name".into(),
                    Value::String(format!("София / 東京 / São Paulo {n}")),
                ),
            ]),
        ),
        (
            "tags".into(),
            Value::Array(vec![
                Value::String("synthetic".into()),
                Value::String("registry".into()),
            ]),
        ),
    ];
    if version == 1 {
        fields.push((
            "note".into(),
            Value::Union(
                1,
                Box::new(Value::String(
                    "Added in writer version 2\nSafe terminal text".into(),
                )),
            ),
        ));
    }
    let mut raw = vec![0];
    raw.extend(ids[version].to_be_bytes());
    raw.extend(
        apache_avro::writer::datum::GenericDatumWriter::builder(&schemas[1])
            .schemata(schemas.iter().collect())?
            .build()?
            .write_value_to_vec(Value::Record(fields))?,
    );
    Ok(raw)
}

pub async fn seed(topic: &str, count: u32) -> Result<()> {
    let config = config();
    let admin: AdminClient<_> = config.create()?;
    let reader: BaseConsumer = config.create()?;
    let existing = reader.fetch_metadata(None, Duration::from_secs(5))?;
    if let Some(metadata) = existing.topics().iter().find(|t| t.name() == topic) {
        ensure!(
            metadata.error().is_none() && metadata.partitions().len() == 1,
            "Unexpected {topic} metadata; inspect before resetting"
        );
        let offsets = reader.fetch_watermarks(topic, 0, Duration::from_secs(5))?;
        ensure!(
            offsets == (0, i64::from(count)),
            "Unexpected {topic} offsets {offsets:?}; inspect before resetting"
        );
        println!("Preserved {topic}: {count} records");
        return Ok(());
    }
    let ids = schemas(topic)?;
    for result in admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))
                .set("retention.ms", "-1")
                .set("retention.bytes", "-1")],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await?
    {
        result.map_err(|(_, e)| anyhow::anyhow!("Create fixture topic: {e:?}"))?;
    }
    let producer: FutureProducer = config.create()?;
    for n in 0..count {
        let raw = message(n, ids)?;
        producer
            .send(
                FutureRecord::to(topic)
                    .partition(0)
                    .key("demo")
                    .payload(&raw),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!(
        "Seeded {topic}: {count} Avro records, writer schema IDs {ids:?}, referenced Customer schema"
    );
    Ok(())
}
