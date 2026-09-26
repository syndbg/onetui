use anyhow::{Result, anyhow, ensure};
use serde::Deserialize;

/// A query editor submission: a verb line naming the target, then an optional body.
pub(crate) enum Statement {
    Replay {
        target: Target,
        replay: Replay,
    },
    Produce {
        target: Target,
        records: Vec<Record>,
    },
}

/// The topic, and optionally the partition, named on the verb line.
pub(crate) struct Target {
    pub topic: String,
    pub partition: Option<i32>,
}

#[derive(Debug, Default)]
pub(crate) struct Replay {
    pub offset: Option<i64>,
    pub timestamp_ms: Option<i64>,
    pub end_offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    #[serde(default)]
    pub key: Option<String>,
    // An explicit null value is a tombstone, which differs from omitting the field, so
    // the two cases must stay distinguishable after deserializing.
    #[serde(default, deserialize_with = "present")]
    pub value: Option<Option<String>>,
    #[serde(default)]
    pub partition: Option<i32>,
    #[serde(default)]
    pub headers: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default)]
    pub key_encoding: Option<Encoding>,
    #[serde(default)]
    pub value_encoding: Option<Encoding>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Encoding {
    #[serde(rename = "utf-8", alias = "utf8")]
    Utf8,
    Base64,
    Hex,
}

fn present<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(Some)
}

impl Encoding {
    /// Absent encoding sends the text as UTF-8, which is what most publishes want.
    fn decode(self, text: &str) -> Result<Vec<u8>> {
        // Whitespace lets a long encoded payload stay readable.
        let compact: String = text.split_whitespace().collect();
        match self {
            Encoding::Utf8 => Ok(text.as_bytes().to_vec()),
            Encoding::Base64 => {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(&compact)
                    .map_err(|error| anyhow!("Invalid base64 payload: {error}"))
            }
            Encoding::Hex => {
                ensure!(
                    compact.len().is_multiple_of(2),
                    "Hex payload must have an even number of digits"
                );
                (0..compact.len())
                    .step_by(2)
                    .map(|i| {
                        u8::from_str_radix(&compact[i..i + 2], 16)
                            .map_err(|error| anyhow!("Invalid hex payload: {error}"))
                    })
                    .collect()
            }
        }
    }
}

impl Record {
    /// The key bytes, or None for a record published without a key.
    pub fn key(&self) -> Result<Option<Vec<u8>>> {
        self.key
            .as_deref()
            .map(|key| self.key_encoding.unwrap_or(Encoding::Utf8).decode(key))
            .transpose()
    }

    /// The value bytes. None is a tombstone: an explicit null, or a record with only a
    /// key, both of which Kafka stores as a null value.
    pub fn value(&self) -> Result<Option<Vec<u8>>> {
        self.value
            .as_ref()
            .and_then(Option::as_deref)
            .map(|value| self.value_encoding.unwrap_or(Encoding::Utf8).decode(value))
            .transpose()
    }
}

/// A produce submission sends every record or none; librdkafka accepts a bounded batch.
pub(crate) const PRODUCE_RECORDS: usize = 100;

pub(crate) fn parse(text: &str) -> Result<Statement> {
    let (line, rest) = text.split_once('\n').unwrap_or((text, ""));
    let mut parts = line.split_whitespace();
    let verb = parts
        .next()
        .ok_or_else(|| anyhow!("Enter CONSUME or PRODUCE topic[/partition]"))?;
    let target = parts
        .next()
        .ok_or_else(|| anyhow!("Name a topic on the first line: {verb} topic[/partition]"))
        .and_then(target_of)?;
    let body = match rest {
        "" => "",
        rest => rest
            .strip_prefix('\n')
            .ok_or_else(|| anyhow!("Leave a blank line before the body"))?,
    };
    match verb {
        "CONSUME" => {
            // The verb line carries the whole read: what to address, and how much of it.
            let replay = range(&mut parts)?;
            ensure!(
                body.trim().is_empty(),
                "CONSUME takes no body; put the range on the first line as offsets or time start..end"
            );
            ensure!(
                target.partition.is_some(),
                "CONSUME reads one partition; name it as topic/partition"
            );
            Ok(Statement::Replay { target, replay })
        }
        "PRODUCE" => {
            ensure!(
                parts.next().is_none(),
                "Enter PRODUCE topic[/partition] on the first line"
            );
            let mut records = Vec::new();
            for line in body.lines().filter(|line| !line.trim().is_empty()) {
                let record: Record = serde_json::from_str(line)?;
                ensure!(
                    record.key.is_some() || record.value.is_some(),
                    "Each record needs a key or a value"
                );
                // A tombstone is keyed by definition: the broker needs the key to know
                // which record the deletion marker retires.
                ensure!(
                    !(record.key.is_none() && record.value == Some(None)),
                    "A null value is a tombstone and needs a key"
                );
                // Decoding here rejects a malformed payload before anything is sent.
                record.key()?;
                record.value()?;
                ensure!(
                    record.partition.is_none_or(|p| p >= 0),
                    "Kafka partitions are nonnegative 32-bit integers"
                );
                records.push(record);
            }
            ensure!(
                !records.is_empty(),
                "Enter one JSON record per line after a blank line"
            );
            ensure!(
                records.len() <= PRODUCE_RECORDS,
                "PRODUCE accepts at most {PRODUCE_RECORDS} records per submission"
            );
            Ok(Statement::Produce { target, records })
        }
        other => Err(anyhow!(
            "Unknown Kafka verb {other}; use CONSUME or PRODUCE"
        )),
    }
}

/// An optional `offsets start..end` or `time start..end` clause. Omitting it reads the
/// whole retained partition; either side of the range may be left out.
fn range<'a>(parts: &mut impl Iterator<Item = &'a str>) -> Result<Replay> {
    let Some(kind) = parts.next() else {
        return Ok(Replay::default());
    };
    let span = parts
        .next()
        .ok_or_else(|| anyhow!("Enter a range as {kind} start..end"))?;
    ensure!(
        parts.next().is_none(),
        "Enter one range on the first line: offsets or time start..end"
    );
    let (start, end) = span
        .split_once("..")
        .ok_or_else(|| anyhow!("Enter a range as {kind} start..end"))?;
    let bound = |text: &str, what: &str| -> Result<Option<i64>> {
        if text.is_empty() {
            return Ok(None);
        }
        let value: i64 = text
            .parse()
            .map_err(|_| anyhow!("Invalid Kafka {what} {text}"))?;
        ensure!(
            value >= 0,
            "Kafka offsets and timestamps must be nonnegative signed 64-bit integers"
        );
        Ok(Some(value))
    };
    let end_offset = bound(end, "end offset")?;
    let replay = match kind {
        "offsets" => Replay {
            offset: bound(start, "offset")?,
            timestamp_ms: None,
            end_offset,
        },
        // A time range still ends at an offset: the start resolves to one, and there is
        // no per-record time filter.
        "time" => Replay {
            offset: None,
            timestamp_ms: bound(start, "timestamp")?,
            end_offset,
        },
        other => anyhow::bail!("Unknown Kafka range {other}; use offsets or time"),
    };
    ensure!(
        replay
            .offset
            .zip(replay.end_offset)
            .is_none_or(|(start, end)| start <= end),
        "Kafka offset exceeds end_offset"
    );
    Ok(replay)
}

fn target_of(text: &str) -> Result<Target> {
    let (topic, partition) = match text.rsplit_once('/') {
        Some((topic, partition)) => {
            let partition = partition
                .parse::<i32>()
                .map_err(|_| anyhow!("Invalid Kafka partition {partition}"))?;
            ensure!(
                partition >= 0,
                "Kafka partitions are nonnegative 32-bit integers"
            );
            (topic, Some(partition))
        }
        None => (text, None),
    };
    ensure!(
        crate::browse::valid_topic(topic),
        "Invalid Kafka topic {topic}"
    );
    Ok(Target {
        topic: topic.into(),
        partition,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_verb_line_carries_the_whole_read_and_bodies_are_strict() {
        // CONSUME addresses a partition and states its range on the same line.
        let Statement::Replay { target, replay } =
            parse("CONSUME events/0 offsets 123..250").unwrap()
        else {
            panic!("expected replay")
        };
        assert_eq!(target.topic, "events");
        assert_eq!(target.partition, Some(0));
        assert_eq!((replay.offset, replay.end_offset), (Some(123), Some(250)));
        assert_eq!(replay.timestamp_ms, None);

        // A time range resolves its start to an offset and may still bound the end.
        let Statement::Replay { replay, .. } =
            parse("CONSUME events/0 time 1750000000369..250").unwrap()
        else {
            panic!("expected replay")
        };
        assert_eq!(replay.timestamp_ms, Some(1750000000369));
        assert_eq!((replay.offset, replay.end_offset), (None, Some(250)));

        // Either side may be omitted, and so may the range itself.
        for (text, expected) in [
            ("CONSUME events/0 offsets 123..", (Some(123), None)),
            ("CONSUME events/0 offsets ..250", (None, Some(250))),
            ("CONSUME events/0 offsets ..", (None, None)),
            ("CONSUME events/0", (None, None)),
        ] {
            let Statement::Replay { replay, .. } = parse(text).unwrap() else {
                panic!("expected replay")
            };
            assert_eq!((replay.offset, replay.end_offset), expected, "{text}");
        }

        // PRODUCE takes one JSON record per line; blank lines separate, not terminate.
        let Statement::Produce { target, records } = parse(concat!(
            "PRODUCE events\n\n",
            "{\"key\":\"a\",\"value\":\"1\",\"partition\":2}\n",
            "\n",
            "{\"value\":\"2\",\"headers\":{\"src\":\"tui\"}}\n",
        ))
        .unwrap() else {
            panic!("expected produce")
        };
        assert_eq!(target.topic, "events");
        assert_eq!(target.partition, None, "broker partitions by key hash");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].partition, Some(2), "per-record partition wins");
        assert_eq!(records[1].headers.as_ref().unwrap()["src"], "tui");

        for text in [
            "",
            "CONSUME",                                 // no target
            "CONSUME events",                          // consume needs a partition
            "CONSUME events/0 offsets",                // range keyword without a span
            "CONSUME events/0 123..250",               // range needs its kind
            "CONSUME events/0 offsets 123",            // not a range
            "CONSUME events/0 offsets 123..250 extra", // one range per line
            "CONSUME events/0 seq 1..2",               // NATS spelling, not Kafka's
            "CONSUME events/0 offsets -1..",
            "CONSUME events/0 offsets 2..1",
            "CONSUME events/0 offsets a..b",
            "CONSUME events/0\n\n{\"offset\":1}", // the body is gone
            "CONSUME events/-1",
            "PRODUCE events",                                   // no records
            "PRODUCE events offsets 1..2\n\n{\"value\":\"a\"}", // produce takes no range
            "PRODUCE events\n\n{}",                             // neither key nor value
            "PRODUCE events\n\n{\"value\":\"a\",\"partition\":-1}",
            "PRODUCE events\n\n{\"value\":\"a\",\"offset\":1}", // unknown field
            "PRODUCE events\n\n[{\"value\":\"a\"}]",            // not JSONL
            "PRODUCE events\n\n{\"value\":null}",               // tombstone needs a key
            "PRODUCE events\n\n{\"value\":\"!!\",\"value_encoding\":\"base64\"}",
            "PRODUCE events\n\n{\"value\":\"abc\",\"value_encoding\":\"hex\"}",
            "PRODUCE events\n\n{\"value\":\"a\",\"value_encoding\":\"rot13\"}",
            "SEND events\n\n{\"value\":\"a\"}", // unknown verb
        ] {
            assert!(parse(text).is_err(), "{text:?} must be rejected");
        }
        let many = std::iter::repeat_n("{\"value\":\"a\"}", PRODUCE_RECORDS + 1)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(parse(&format!("PRODUCE events\n\n{many}")).is_err());
    }

    #[test]
    fn encodings_decode_payloads_and_a_null_value_is_a_tombstone() {
        let record = |text: &str| {
            let source = format!("PRODUCE events\n\n{text}");
            let Statement::Produce { mut records, .. } = parse(&source).unwrap() else {
                panic!("expected produce")
            };
            records.pop().unwrap()
        };

        // Absent encoding keeps today's UTF-8 behavior.
        let plain = record(r#"{"key":"k","value":"hello"}"#);
        assert_eq!(plain.key().unwrap(), Some(b"k".to_vec()));
        assert_eq!(plain.value().unwrap(), Some(b"hello".to_vec()));

        // Key and value decode independently.
        let mixed = record(
            r#"{"key":"6b","key_encoding":"hex","value":"aGVsbG8=","value_encoding":"base64"}"#,
        );
        assert_eq!(mixed.key().unwrap(), Some(b"k".to_vec()));
        assert_eq!(mixed.value().unwrap(), Some(b"hello".to_vec()));

        // Bytes that are not valid UTF-8 are exactly what encoding is for.
        let binary = record(r#"{"value":"ff00","value_encoding":"hex"}"#);
        assert_eq!(binary.value().unwrap(), Some(vec![255, 0]));

        // An explicit null value is a tombstone; omitting the field is the same on the
        // wire, but only the keyed form is accepted.
        let tombstone = record(r#"{"key":"k","value":null}"#);
        assert_eq!(tombstone.key().unwrap(), Some(b"k".to_vec()));
        assert_eq!(tombstone.value().unwrap(), None);
        let keyed = record(r#"{"key":"k"}"#);
        assert_eq!(keyed.value().unwrap(), None);

        // An empty value is stored as empty bytes, which differs from a tombstone.
        let empty = record(r#"{"key":"k","value":""}"#);
        assert_eq!(empty.value().unwrap(), Some(Vec::new()));
    }
}
