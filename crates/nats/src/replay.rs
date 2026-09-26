use anyhow::{Result, anyhow, ensure};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Replay {
    #[serde(default = "all_subjects")]
    pub subject: String,
    pub start_sequence: Option<u64>,
    pub end_sequence: Option<u64>,
    pub start_time: Option<String>,
}

fn all_subjects() -> String {
    ">".into()
}

impl Replay {
    /// Build a replay from the verb line: an optional subject, then an optional
    /// `seq start..end` or `time start..end` range. The subject is a target, so it sits
    /// beside the stream rather than in a body.
    pub fn parse<'a>(parts: &mut impl Iterator<Item = &'a str>) -> Result<Self> {
        let subject = parts.next().unwrap_or(">");
        crate::core_subscription::validate_subject(subject)?;
        let mut replay = Self {
            subject: subject.into(),
            start_sequence: None,
            end_sequence: None,
            start_time: None,
        };
        if let Some(kind) = parts.next() {
            let span = parts
                .next()
                .ok_or_else(|| anyhow!("Enter a range as {kind} start..end"))?;
            ensure!(
                parts.next().is_none(),
                "Enter one range on the first line: seq or time start..end"
            );
            let (start, end) = span
                .split_once("..")
                .ok_or_else(|| anyhow!("Enter a range as {kind} start..end"))?;
            // A time range still ends at a sequence: the start resolves to one, and
            // there is no per-message time filter.
            replay.end_sequence = match end {
                "" => None,
                end => Some(
                    end.parse()
                        .map_err(|_| anyhow!("Invalid NATS end sequence {end}"))?,
                ),
            };
            match kind {
                "seq" => {
                    replay.start_sequence = match start {
                        "" => None,
                        start => Some(
                            start
                                .parse()
                                .map_err(|_| anyhow!("Invalid NATS start sequence {start}"))?,
                        ),
                    }
                }
                "time" => {
                    ensure!(!start.is_empty(), "Enter a start time as time start..end");
                    async_nats::datetime::parse_rfc3339(start)
                        .map_err(anyhow::Error::from_boxed)?;
                    replay.start_time = Some(start.into());
                }
                other => anyhow::bail!("Unknown NATS range {other}; use seq or time"),
            }
        }
        ensure!(
            replay.start_sequence.is_none_or(|n| n > 0),
            "NATS start_sequence must be positive"
        );
        ensure!(
            replay
                .end_sequence
                .is_none_or(|n| n > replay.start_sequence.unwrap_or(0)),
            "NATS end_sequence must exceed start_sequence"
        );
        Ok(replay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(line: &str) -> Result<Replay> {
        Replay::parse(&mut line.split_whitespace())
    }

    #[test]
    fn the_verb_line_carries_the_subject_and_range() {
        // No subject reads every subject in the stream.
        let all = parse("").unwrap();
        assert_eq!(all.subject, ">");
        assert_eq!((all.start_sequence, all.end_sequence), (None, None));

        let scoped = parse("orders.* seq 1..500").unwrap();
        assert_eq!(scoped.subject, "orders.*");
        assert_eq!(
            (scoped.start_sequence, scoped.end_sequence),
            (Some(1), Some(500))
        );

        // A time range resolves its start to a sequence and may bound the end.
        let timed = parse("orders.* time 2026-01-01T02:00:00+02:00..500").unwrap();
        assert_eq!(
            timed.start_time.as_deref(),
            Some("2026-01-01T02:00:00+02:00")
        );
        assert_eq!(
            (timed.start_sequence, timed.end_sequence),
            (None, Some(500))
        );

        // Either side may be omitted, and so may the range.
        for (line, expected) in [
            ("orders.* seq 1..", (Some(1), None)),
            ("orders.* seq ..500", (None, Some(500))),
            ("orders.* seq ..", (None, None)),
            ("orders.*", (None, None)),
        ] {
            let replay = parse(line).unwrap();
            assert_eq!(
                (replay.start_sequence, replay.end_sequence),
                expected,
                "{line}"
            );
        }

        for line in [
            "a.>.b",                     // invalid subject
            "orders.* seq 0..",          // sequences start at 1
            "orders.* seq 2..2",         // end must exceed start
            "orders.* time yesterday..", // not RFC3339
            "orders.* time ..500",       // a time range needs its start
            "orders.* 1..500",           // range needs its kind
            "orders.* seq 1",            // not a range
            "orders.* seq 1..500 extra", // one range per line
            "orders.* offsets 1..500",   // Kafka spelling, not NATS's
            "orders.* seq a..b",
        ] {
            assert!(parse(line).is_err(), "{line}");
        }
    }
}
