use anyhow::{Result, anyhow, ensure};
use futures_util::{FutureExt, StreamExt};
use onetui_core::provider::PageRequest;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Row, Value};

pub(crate) fn validate_subject(subject: &str) -> Result<()> {
    ensure!(
        !subject.is_empty()
            && subject.len() <= 1024
            && !subject.chars().any(|c| c.is_whitespace() || c.is_control()),
        "NATS subject must be 1..1024 bytes without whitespace or controls"
    );
    let tokens: Vec<_> = subject.split('.').collect();
    for (index, token) in tokens.iter().enumerate() {
        ensure!(
            !token.is_empty()
                && (*token == "*"
                    || (*token == ">" && index + 1 == tokens.len())
                    || !token.contains(['*', '>'])),
            "NATS wildcard must be a whole token; > must be last"
        );
    }
    Ok(())
}

pub(crate) fn empty() -> Page {
    Page {
        columns: [
            ("subject", "text"), ("reply", "text (never answered)"), ("data", "bytes"),
            ("headers", "JSON (SDK header values, not original wire bytes)"),
            ("status", "integer"), ("description", "text"),
        ].into_iter().map(|(name, datatype)| Column { name: name.into(), datatype: datatype.into() }).collect(),
        notice: "Core NATS: press f to subscribe. At-most-once, no history; stopped/disconnected messages are lost.".into(),
        ..Page::default()
    }
}

pub(crate) struct Subscription {
    subscriber: async_nats::Subscriber,
    subject: String,
    identity: u64,
    batch: u64,
    pending: Option<async_nats::Message>,
}

impl Subscription {
    pub async fn start(
        client: &async_nats::Client,
        subject: String,
        identity: u64,
    ) -> Result<Self> {
        validate_subject(&subject)?;
        let subscriber = client.subscribe(subject.clone()).await?;
        // Send SUB before exposing the live cursor. NATS does not acknowledge subscriptions.
        client.flush().await?;
        Ok(Self {
            subscriber,
            subject,
            identity,
            batch: 0,
            pending: None,
        })
    }

    fn cursor(&self) -> String {
        format!("{}:{}", self.identity, self.batch)
    }

    pub async fn page(
        &mut self,
        client: &async_nats::Client,
        request: &PageRequest,
    ) -> Result<Page> {
        ensure!(
            request.resource.path == [self.subject.clone()],
            "NATS subscription subject changed"
        );
        if let Some(cursor) = &request.continuation {
            ensure!(
                *cursor == self.cursor(),
                "NATS live cursor is stale; restart following"
            );
        }
        client.flush().await?;
        let mut page = empty();
        if request.continuation.is_some() {
            while page.rows.len() < PAGE_SIZE as usize {
                let message = match self.pending.take() {
                    Some(message) => message,
                    None => match self.subscriber.next().now_or_never() {
                        Some(Some(message)) => message,
                        Some(None) => {
                            return Err(anyhow!(
                                "NATS subscription closed; restart following; messages may be lost"
                            ));
                        }
                        None => break,
                    },
                };
                ensure!(
                    message.length <= PAGE_BYTES,
                    "NATS Core message exceeds 1 MiB; following stopped; message lost"
                );
                let headers = message.headers.as_ref().map(|headers| {
                    Value::Json(serde_json::Value::Array(headers.iter().map(|(name, values)| {
                        serde_json::json!({"name": name.to_string(), "values": values.iter().map(|v| v.as_str()).collect::<Vec<_>>()})
                    }).collect()).to_string())
                });
                page.rows.push(Row {
                    cells: vec![
                        Some(message.subject.to_string().into()),
                        message.reply.as_ref().map(|s| s.to_string().into()),
                        Some(Value::Bytes(message.payload.to_vec())),
                        headers,
                        message.status.map(|s| s.as_u16().to_string().into()),
                        message.description.clone().map(Value::Text),
                    ],
                    target: None,
                });
                if page.bytes() > (PAGE_BYTES - 8192) / 2 {
                    page.rows.pop();
                    ensure!(
                        !page.rows.is_empty(),
                        "NATS Core message exceeds display budget; following stopped; message lost"
                    );
                    self.pending = Some(message);
                    break;
                }
            }
        }
        self.batch = self
            .batch
            .checked_add(1)
            .ok_or_else(|| anyhow!("NATS live batch overflow"))?;
        page.continuation = Some(self.cursor());
        page.notice = "Core NATS at-most-once; no replay. Stop unsubscribes. Queue overflow or disconnect stops following; lost messages cannot be recovered.".into();
        Ok(page)
    }

    pub async fn stop(mut self, client: &async_nats::Client) -> Result<()> {
        self.subscriber.unsubscribe().await?;
        client.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn subjects_require_explicit_valid_wildcards() {
        for subject in ["demo.live", "demo.*", "demo.>", ">", "orders.東京"] {
            validate_subject(subject).unwrap();
        }
        for subject in ["", ".a", "a.", "a..b", "a*", "a.>.b", "a.**", "a\nb", "a b"] {
            assert!(validate_subject(subject).is_err(), "{subject:?}");
        }
        assert!(validate_subject(&"a".repeat(1025)).is_err());
    }
}
