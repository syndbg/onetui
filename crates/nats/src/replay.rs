use anyhow::{Result, ensure};
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
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(
            text.len() <= onetui_core::provider::QUERY_BYTES,
            "Query exceeds 16 KiB"
        );
        let replay: Self = serde_json::from_str(text)?;
        crate::core_subscription::validate_subject(&replay.subject)?;
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
        ensure!(
            replay.start_sequence.is_none() || replay.start_time.is_none(),
            "Choose start_sequence or start_time, not both"
        );
        if let Some(time) = &replay.start_time {
            async_nats::datetime::parse_rfc3339(time).map_err(anyhow::Error::from_boxed)?;
        }
        Ok(replay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_replay_bounds_subjects_and_timestamps() {
        for value in [
            r#"{"subject":"a.>.b"}"#,
            r#"{"start_sequence":0}"#,
            r#"{"start_sequence":2,"end_sequence":2}"#,
            r#"{"start_time":"yesterday"}"#,
            r#"{"start_time":"2026-01-01T00:00:00Z","start_sequence":1}"#,
            r#"{"consumer":"app"}"#,
        ] {
            assert!(Replay::parse(value).is_err(), "{value}");
        }
        assert_eq!(Replay::parse("{}").unwrap().subject, ">");
        assert!(
            Replay::parse(r#"{"subject":"orders.*","start_time":"2026-01-01T02:00:00+02:00"}"#)
                .is_ok()
        );
    }
}
