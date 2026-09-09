//! Seed only the fixed disposable Kafka fixture; never overwrite an existing topic.
use anyhow::{Result, ensure};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "127.0.0.1:19092")
        .set("allow.auto.create.topics", "false")
        .set("message.timeout.ms", "5000");
    let admin: AdminClient<_> = config.create()?;
    let reader: BaseConsumer = config.create()?;
    let producer: FutureProducer = config.clone().set("compression.type", "zstd").create()?;
    let existing = reader.fetch_metadata(None, Duration::from_secs(5))?;
    for (name, partitions, count) in [
        ("demo_events", 3, 1500u32),
        ("demo_binary", 2, 512),
        ("demo_tombstones", 1, 64),
        ("demo_empty", 1, 0),
    ] {
        if let Some(topic) = existing.topics().iter().find(|topic| topic.name() == name) {
            ensure!(
                topic.error().is_none() && topic.partitions().len() == partitions as usize,
                "Existing {name} has unexpected metadata; inspect before resetting fixtures"
            );
            let mut offsets = 0;
            for partition in 0..partitions {
                let (low, high) =
                    reader.fetch_watermarks(name, partition, Duration::from_secs(5))?;
                ensure!(
                    low == 0,
                    "Existing {name} has lost initial records; inspect before resetting fixtures"
                );
                offsets += high;
            }
            ensure!(
                offsets == i64::from(count),
                "Existing {name} has {offsets} offsets, expected {count}; inspect before resetting fixtures"
            );
            println!("Preserved {name}: {count} records");
            continue;
        }
        for result in admin
            .create_topics(
                &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
            )
            .await?
        {
            result.map_err(|(_, error)| anyhow::anyhow!("Kafka create topic: {error:?}"))?;
        }
        for n in 0..count {
            let key = n.to_be_bytes();
            let payload = match name {
                "demo_binary" => Some(
                    (0..256)
                        .map(|byte| (byte as u8).wrapping_add(n as u8))
                        .collect(),
                ),
                "demo_tombstones" if n % 3 == 0 => None,
                "demo_tombstones" if n % 3 == 1 => Some(Vec::new()),
                _ => Some(serde_json::to_vec(&serde_json::json!({
                    "id": n, "name": format!("Synthetic event {n}"), "active": n % 2 == 0,
                    "city": "София / 東京 / São Paulo", "optional": null, "empty": "",
                    "tags": ["synthetic", "kafka"], "nested": {"sequence": n, "scores": [0.5, 1.25]},
                    "message": "Line one\nLine two\u{1b}[31m", "large_integer": "9007199254740993"
                }))?),
            };
            let headers = OwnedHeaders::new()
                .insert(Header {
                    key: "format",
                    value: Some(name),
                })
                .insert(Header {
                    key: "duplicate",
                    value: Some(&[255u8, 0][..]),
                })
                .insert(Header::<&[u8]> {
                    key: "duplicate",
                    value: None,
                });
            let mut record = FutureRecord::to(name)
                .partition((n % partitions as u32) as i32)
                .key(&key[..])
                .timestamp(1_750_000_000_000 + i64::from(n))
                .headers(headers);
            if let Some(payload) = payload.as_deref() {
                record = record.payload(payload);
            }
            producer
                .send(record, Duration::from_secs(5))
                .await
                .map_err(|(error, _)| error)?;
        }
        println!("Seeded {name}: {count} records across {partitions} partitions");
    }
    producer.flush(Duration::from_secs(5))?;
    Ok(())
}
