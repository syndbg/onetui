use anyhow::{Result, anyhow};
use onetui_core::provider::{WriteOutcome, WriteResult};
use rdkafka::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::Mutex;
use std::time::Duration;

use crate::statement::{Record, Target};

/// Delivery reports arrive on the producer's own queue; poll it rather than sleeping.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// One delivery outcome per submitted record, indexed by its position in the batch.
/// The native callback runs on the polling thread, so a plain mutex is enough.
#[derive(Default)]
pub(crate) struct Reports(Mutex<Vec<Option<Outcome>>>);

pub(crate) enum Outcome {
    Delivered { partition: i32, offset: i64 },
    Failed(String),
}

impl Reports {
    pub fn begin(&self, records: usize) {
        let mut reports = self.0.lock().unwrap_or_else(|e| e.into_inner());
        reports.clear();
        reports.resize_with(records, || None);
    }
    pub fn record(&self, index: usize, outcome: Outcome) {
        let mut reports = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(slot) = reports.get_mut(index) {
            *slot = Some(outcome);
        }
    }
    fn take(&self) -> Vec<Option<Outcome>> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

/// Producing shares the connection settings but never the consumer's fetch and group
/// behavior. Retries stay off: ADR-0014 leaves resubmission to the user, because a
/// retried produce can duplicate a record that the first attempt already appended.
pub(crate) fn config(base: &ClientConfig) -> ClientConfig {
    let mut config = base.clone();
    for consumer_only in [
        "group.id",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "auto.offset.reset",
        "isolation.level",
        "enable.partition.eof",
        "queued.min.messages",
        "fetch.queue.backoff.ms",
        "queued.max.messages.kbytes",
        "fetch.message.max.bytes",
        "fetch.max.bytes",
        "allow.auto.create.topics",
    ] {
        config.remove(consumer_only);
    }
    config
        // No automatic resubmission: a failed produce is reported, never retried.
        .set("retries", "0")
        .set("enable.idempotence", "false")
        // Wait for all in-sync replicas so a reported offset is durable.
        .set("acks", "all")
        // Send the batch immediately; the editor submits one bounded batch at a time.
        .set("linger.ms", "0")
        .set("message.timeout.ms", "10000");
    config
}

/// Send every record, then wait for all delivery reports within the request deadline.
pub(crate) fn send<C>(
    producer: &BaseProducer<C>,
    reports: &Reports,
    target: &Target,
    records: &[Record],
    remaining: &dyn Fn() -> Result<Duration>,
) -> Result<WriteResult>
where
    C: rdkafka::producer::ProducerContext<
            rdkafka::producer::NoCustomPartitioner,
            DeliveryOpaque = usize,
        >,
{
    reports.begin(records.len());
    for (index, record) in records.iter().enumerate() {
        remaining()?;
        // Decoded bytes must outlive the borrowing record below.
        let key = record.key()?;
        let value = record.value()?;
        let mut outbound: BaseRecord<'_, [u8], [u8], usize> =
            BaseRecord::with_opaque_to(&target.topic, index);
        // Per-record partition wins; then the verb line's; then the broker's partitioner,
        // which hashes the key and round-robins when there is none.
        if let Some(partition) = record.partition.or(target.partition) {
            outbound = outbound.partition(partition);
        }
        if let Some(key) = &key {
            outbound = outbound.key(key.as_slice());
        }
        // A record with no value is a tombstone; sending no payload is what stores the
        // null value that marks the key deleted.
        if let Some(value) = &value {
            outbound = outbound.payload(value.as_slice());
        }
        if let Some(headers) = &record.headers {
            let mut owned = OwnedHeaders::new_with_capacity(headers.len());
            for (key, value) in headers {
                owned = owned.insert(Header {
                    key,
                    value: Some(value.as_str()),
                });
            }
            outbound = outbound.headers(owned);
        }
        producer
            .send(outbound)
            .map_err(|(error, _)| anyhow!("Kafka produce rejected locally: {error}"))?;
    }
    // Every enqueued record yields a delivery report, so an empty queue means completion.
    while producer.in_flight_count() > 0 {
        producer.poll(POLL_INTERVAL.min(remaining()?));
    }
    Ok(result(target, reports.take()))
}

/// Summarize one submission. A batch can be partly delivered, and the outcome has no
/// partial state, so anything short of a clean result reports Unknown: resending the
/// batch would duplicate whichever records did land, and only the user can judge that.
/// Applied means every record was delivered, Rejected that every one was refused by a
/// completed broker response.
fn result(target: &Target, reports: Vec<Option<Outcome>>) -> WriteResult {
    let total = reports.len();
    let mut delivered = Vec::new();
    let mut failures = Vec::new();
    let mut unknown = 0;
    for report in reports {
        match report {
            Some(Outcome::Delivered { partition, offset }) => delivered.push((partition, offset)),
            Some(Outcome::Failed(error)) => failures.push(error),
            None => unknown += 1,
        }
    }
    let target = match target.partition {
        Some(partition) => format!("{}/{partition}", target.topic),
        None => target.topic.clone(),
    };
    let outcome = match (delivered.is_empty(), failures.is_empty(), unknown) {
        (false, true, 0) => WriteOutcome::Applied,
        (true, false, 0) => WriteOutcome::Rejected,
        _ => WriteOutcome::Unknown,
    };
    // Offsets are the only record of where each message landed, so keep them in the
    // summary rather than dropping them with the row view.
    let mut summary = match outcome {
        WriteOutcome::Applied => format!("Published {total} records to {target}"),
        WriteOutcome::Rejected => format!("Kafka rejected all {total} records for {target}"),
        WriteOutcome::Unknown => format!(
            "Publish partly completed for {target}: {} of {total} records delivered, \
             {} failed, {unknown} without a delivery report. Resending duplicates the \
             delivered records; inspect the topic before retrying",
            delivered.len(),
            failures.len()
        ),
    };
    if !delivered.is_empty() {
        let offsets: Vec<_> = delivered
            .iter()
            .map(|(partition, offset)| format!("{partition}:{offset}"))
            .collect();
        summary.push_str(&format!(
            "; delivered at partition:offset {}",
            offsets.join(", ")
        ));
    }
    if !failures.is_empty() {
        // Repeated broker errors collapse to one mention each.
        let mut distinct: Vec<&String> = Vec::new();
        for error in &failures {
            if !distinct.contains(&error) {
                distinct.push(error);
            }
        }
        summary.push_str(&format!(
            "; {} failed: {}",
            failures.len(),
            distinct
                .iter()
                .map(|error| error.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    WriteResult { outcome, summary }
}

/// Flush before dropping a producer so an abandoned request cannot append later.
pub(crate) fn flush<C>(producer: &BaseProducer<C>, wait: Duration)
where
    C: rdkafka::producer::ProducerContext<
            rdkafka::producer::NoCustomPartitioner,
            DeliveryOpaque = usize,
        >,
{
    let _ = producer.flush(Timeout::After(wait));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produce_config_drops_consumer_settings_and_never_retries() {
        let mut base = ClientConfig::new();
        base.set("bootstrap.servers", "broker:9093")
            .set("security.protocol", "SASL_SSL")
            .set("group.id", "onetui-1-2")
            .set("isolation.level", "read_committed")
            .set("fetch.max.bytes", "1048576");
        let config = config(&base);
        // Connection settings carry over; consumer fetch and group settings do not.
        assert_eq!(config.get("bootstrap.servers"), Some("broker:9093"));
        assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
        assert_eq!(config.get("group.id"), None);
        assert_eq!(config.get("isolation.level"), None);
        assert_eq!(config.get("fetch.max.bytes"), None);
        assert_eq!(
            config.get("retries"),
            Some("0"),
            "ADR-0014 forbids retrying"
        );
        assert_eq!(config.get("acks"), Some("all"));
        config.create_native_config().unwrap();
    }

    #[test]
    fn batch_outcome_is_the_weakest_record_and_keeps_offsets() {
        let target = Target {
            topic: "events".into(),
            partition: None,
        };
        let delivered = |partition, offset| Some(Outcome::Delivered { partition, offset });

        let applied = result(&target, vec![delivered(2, 41), delivered(0, 7)]);
        assert_eq!(applied.outcome, WriteOutcome::Applied);
        assert!(applied.summary.starts_with("Published 2 records to events"));
        assert!(applied.summary.contains("2:41, 0:7"), "{}", applied.summary);

        // Every record refused by a completed response is a rejection, not an unknown.
        let rejected = result(
            &target,
            vec![
                Some(Outcome::Failed("broker refused".into())),
                Some(Outcome::Failed("broker refused".into())),
            ],
        );
        assert_eq!(rejected.outcome, WriteOutcome::Rejected);
        assert!(rejected.summary.contains("2 failed: broker refused"));

        // One missing report makes the whole submission unknown, even beside a success.
        let unknown = result(&target, vec![delivered(0, 1), None]);
        assert_eq!(unknown.outcome, WriteOutcome::Unknown);
        assert!(unknown.summary.contains("1 without a delivery report"));
        assert!(
            unknown
                .summary
                .contains("inspect the topic before retrying")
        );
        assert!(
            unknown.summary.contains("0:1"),
            "a partial batch keeps its offsets"
        );

        // A partly delivered batch is neither applied nor rejected: resending it would
        // duplicate the records that did land.
        let partial = result(
            &target,
            vec![delivered(0, 1), Some(Outcome::Failed("too large".into()))],
        );
        assert_eq!(partial.outcome, WriteOutcome::Unknown);
        assert!(partial.summary.contains("1 of 2 records delivered"));
        assert!(partial.summary.contains("1 failed: too large"));
    }
}
