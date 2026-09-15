use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use futures_util::StreamExt;
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Row, Value};
use serde_json::Value as Json;

pub(crate) const SUBJECT: &str = "$SYS.REQ.SERVER.PING.STATSZ";
pub(crate) const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "nats.servers",
    description: "System-account servers observed during a one-second request window; opt-in, not complete membership",
    columns: &[
        "id",
        "name",
        "host",
        "cluster",
        "domain",
        "version",
        "observed_at",
        "details",
    ],
    paging: true,
    actions: &[],
};

#[derive(Default)]
struct Observation {
    rows: BTreeMap<String, Row>,
    received_bytes: usize,
}

impl Observation {
    fn push(&mut self, payload: &[u8]) -> Result<()> {
        ensure!(
            payload.len() <= PAGE_BYTES - self.received_bytes,
            "NATS discovery responses exceed 1 MiB"
        );
        self.received_bytes += payload.len();
        let value: Json = serde_json::from_slice(payload)?;
        ensure!(
            value.get("error").is_none_or(Json::is_null),
            "NATS system API: {}",
            value["error"]
        );
        let server = &value["server"];
        let id = server["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow!("NATS discovery response has no server ID"))?;
        ensure!(
            value["statsz"].is_object(),
            "NATS discovery response has no statsz object"
        );
        let mut cells = Vec::new();
        for field in ["id", "name", "host", "cluster", "domain", "ver", "time"] {
            cells.push(match &server[field] {
                Json::Null => None,
                Json::String(text) => Some(Value::Text(text.clone())),
                _ => return Err(anyhow!("NATS discovery server field {field} is not text")),
            });
        }
        cells.push(Some(Value::Json(value.to_string())));
        self.rows.insert(
            id.to_owned(),
            Row {
                cells,
                target: None,
            },
        );
        ensure!(
            self.rows.len() <= PAGE_SIZE as usize,
            "NATS discovery exceeds 100 observed servers"
        );
        Ok(())
    }

    fn finish(self) -> Result<Page> {
        let page = Page {
            columns: crate::browse::columns(&[
                ("id", "text"), ("name", "text"), ("host", "text"),
                ("cluster", "text"), ("domain", "text"), ("version", "text"),
                ("observed_at", "text"), ("details", "JSON"),
            ]),
            rows: self.rows.into_values().collect(),
            notice: "Servers observed within one second, not complete membership or verified peer health. Refresh rediscovers; returned hosts are never contacted.".into(),
            ..Page::default()
        };
        ensure!(
            page.bytes() <= PAGE_BYTES,
            "NATS discovery page exceeds 1 MiB"
        );
        Ok(page)
    }
}

pub(crate) async fn page(client: &async_nats::Client) -> Result<Page> {
    let inbox = client.new_inbox();
    let mut replies = client.subscribe(inbox.clone()).await?;
    client.flush().await?;
    client
        .publish_with_reply(SUBJECT, inbox, "{}".into())
        .await?;
    client.flush().await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut observation = Observation::default();
    loop {
        let message = match tokio::time::timeout_at(deadline, replies.next()).await {
            Err(_) => break,
            Ok(Some(message)) => message,
            Ok(None) => return Err(anyhow!("NATS discovery reply subscription closed")),
        };
        if let Some(status) = message.status {
            return Err(anyhow!(
                "NATS {status}: {}",
                message.description.as_deref().unwrap_or("")
            ));
        }
        observation.push(&message.payload)?;
    }
    replies.unsubscribe().await?;
    client.flush().await?;
    observation.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn observation_preserves_full_json_deduplicates_and_sorts() {
        let mut observation = Observation::default();
        for (id, name) in [("b", "first"), ("a", "other"), ("b", "latest")] {
            observation.push(&serde_json::to_vec(&json!({
                "server": {"id": id, "name": name, "host": "not-contacted.invalid", "cluster": "demo", "ver": "2", "time": "now"},
                "statsz": {"connections": 12}, "future": [true]
            })).unwrap()).unwrap();
        }
        let page = observation.finish().unwrap();
        assert_eq!(page.rows.len(), 2);
        assert_eq!(page.rows[0].cells[0], Some("a".into()));
        assert_eq!(page.rows[1].cells[1], Some("latest".into()));
        assert_eq!(page.rows[0].cells[4], None);
        let Some(Value::Json(raw)) = &page.rows[0].cells[7] else {
            panic!("JSON missing")
        };
        assert_eq!(
            serde_json::from_str::<Json>(raw).unwrap()["future"],
            json!([true])
        );
        assert!(page.notice.contains("not complete membership"));
        assert!(!page.next);
    }

    #[test]
    fn errors_invalid_shapes_and_observation_limits_fail_explicitly() {
        for invalid in [
            b"not JSON".as_slice(),
            b"{}",
            br#"{"server":{"id":"a"}}"#,
            br#"{"server":{"id":12},"statsz":{}}"#,
        ] {
            assert!(Observation::default().push(invalid).is_err());
        }
        let error = Observation::default()
            .push(br#"{"error":{"code":403,"description":"permission denied"}}"#)
            .unwrap_err();
        assert!(error.to_string().contains("permission denied"));
        assert!(
            Observation::default()
                .push(&vec![b' '; PAGE_BYTES + 1])
                .is_err()
        );
        let mut observation = Observation::default();
        for id in 0..100 {
            observation
                .push(
                    &serde_json::to_vec(&json!({"server":{"id":id.to_string()},"statsz":{}}))
                        .unwrap(),
                )
                .unwrap();
        }
        assert!(
            observation
                .push(br#"{"server":{"id":"overflow"},"statsz":{}}"#)
                .is_err()
        );
        assert!(
            Observation::default()
                .finish()
                .unwrap()
                .notice
                .contains("not complete membership")
        );
    }
}
