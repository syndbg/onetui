use anyhow::{Result, ensure};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    producer::{FutureProducer, FutureRecord},
};
use std::{io::Read, time::Duration};

pub const BROKER: &str = "127.0.0.1:29092";
pub const REGISTRY: &str = "http://127.0.0.1:18081";

pub fn config() -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", BROKER)
        .set("allow.auto.create.topics", "false")
        .set("message.timeout.ms", "5000");
    config
}

pub fn register(subject: &str, body: serde_json::Value) -> Result<u32> {
    ensure!(
        !subject.is_empty()
            && subject
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "Invalid fixture subject"
    );
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
}

pub async fn send_keyed(
    producer: &FutureProducer,
    topic: &str,
    key: &[u8],
    raw: &[u8],
) -> Result<i64> {
    let delivery = producer
        .send(
            FutureRecord::to(topic).partition(0).key(key).payload(raw),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(error, _)| error)?;
    Ok(delivery.offset)
}

#[allow(dead_code)]
pub async fn seed(topic: &str, count: u32, message: impl Fn(u32) -> Result<Vec<u8>>) -> Result<()> {
    seed_keyed(topic, count, |n| Ok((b"demo".to_vec(), message(n)?))).await
}

pub async fn seed_keyed(
    topic: &str,
    count: u32,
    message: impl Fn(u32) -> Result<(Vec<u8>, Vec<u8>)>,
) -> Result<()> {
    let config = config();
    let reader: BaseConsumer = config.create()?;
    let existing = reader.fetch_metadata(None, Duration::from_secs(5))?;
    if let Some(metadata) = existing.topics().iter().find(|t| t.name() == topic) {
        ensure!(
            metadata.error().is_none() && metadata.partitions().len() == 1,
            "Unexpected {topic} metadata; inspect before resetting"
        );
        let (low, high) = reader.fetch_watermarks(topic, 0, Duration::from_secs(5))?;
        // Traffic appends to seeded topics. Never duplicate records or erase that traffic.
        ensure!(
            low == 0 && high >= i64::from(count),
            "Incomplete {topic} dataset ({low}, {high}); inspect before resetting"
        );
        println!("Preserved {topic}: {high} records");
        return Ok(());
    }
    let admin: AdminClient<_> = config.create()?;
    for result in admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))
                .set("retention.ms", "-1")
                .set("retention.bytes", "-1")],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
        )
        .await?
    {
        result.map_err(|(_, error)| anyhow::anyhow!("Create fixture topic: {error:?}"))?;
    }
    let producer: FutureProducer = config.create()?;
    for n in 0..count {
        let (key, value) = message(n)?;
        send_keyed(&producer, topic, &key, &value).await?;
    }
    println!("Seeded {topic}: {count} records");
    Ok(())
}
