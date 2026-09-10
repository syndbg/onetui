//! Produce only to the fixed disposable fixture, never to configured user connections.
use anyhow::{Result, bail, ensure};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let count = match args.as_slice() {
        [] => None,
        [flag, count] if flag == "--count" => {
            let count: u64 = count.parse()?;
            ensure!(count > 0, "--count must be a positive integer");
            Some(count)
        }
        _ => bail!("Usage: produce_demo [--count POSITIVE_INTEGER]"),
    };
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", "127.0.0.1:19092")
        .set("allow.auto.create.topics", "false")
        .set("message.timeout.ms", "5000");
    let admin: AdminClient<_> = config.create()?;
    let reader: BaseConsumer = config.create()?;
    let metadata = reader.fetch_metadata(None, Duration::from_secs(5))?;
    if let Some(topic) = metadata
        .topics()
        .iter()
        .find(|topic| topic.name() == "demo_live")
    {
        ensure!(
            topic.error().is_none() && topic.partitions().len() == 1,
            "Existing demo_live must have one healthy partition; no data was reset"
        );
    } else {
        for result in admin
            .create_topics(
                &[NewTopic::new("demo_live", 1, TopicReplication::Fixed(1))
                    .set("retention.ms", "3600000")
                    .set("retention.bytes", "67108864")],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
            )
            .await?
        {
            result.map_err(|(_, error)| anyhow::anyhow!("Kafka create topic: {error:?}"))?;
        }
    }
    let producer: FutureProducer = config.set("enable.idempotence", "true").create()?;
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0u64;
    loop {
        tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => { signal?; break; }
            _ = interval.tick() => {}
        }
        let payload = serde_json::to_vec(&serde_json::json!({
            "sequence": sequence, "sent_at_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "message": "Synthetic live event", "source": "onetui fixture producer"
        }))?;
        let delivery = producer
            .send(
                FutureRecord::to("demo_live")
                    .partition(0)
                    .key("demo")
                    .payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(error, _)| error)?;
        println!(
            "demo_live partition={} offset={} sequence={sequence}",
            delivery.partition, delivery.offset
        );
        sequence += 1;
        if count == Some(sequence) {
            break;
        }
    }
    Ok(())
}
