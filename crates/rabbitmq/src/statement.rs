use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::{Resource, Row};
use reqwest::Method;

/// What a submitted statement asks the management API to do. Every variant but `Raw`
/// names its vhost as a literal, so the default vhost is typed as `/` rather than `%2F`.
pub enum Statement<'a> {
    /// Publish through an exchange. The body is the payload, taken literally.
    Publish {
        vhost: &'a str,
        exchange: &'a str,
        routing_key: &'a str,
        payload: Vec<u8>,
    },
    /// Destructive read from a queue.
    Get {
        vhost: &'a str,
        queue: &'a str,
        count: u32,
        requeue: bool,
    },
    /// Create or replace an object from a JSON body. `collection` is the management
    /// collection word as typed, pluralised but never checked against a list: RabbitMQ
    /// decides what it supports, so a new object kind needs no change here.
    Declare {
        collection: &'a str,
        vhost: &'a str,
        name: &'a str,
        body: &'a str,
    },
    Delete {
        collection: &'a str,
        vhost: &'a str,
        name: &'a str,
    },
    Purge {
        vhost: &'a str,
        queue: &'a str,
    },
    /// Escape hatch for endpoints the verbs above do not model: users, permissions, node
    /// operations. Paths are native, so a literal `/` inside a name stays `%2F` here.
    Raw {
        method: Method,
        path: &'a str,
        body: &'a str,
    },
}

/// The management collection a DECLARE or DELETE addresses, from the singular word the
/// user typed. Only `vhost` is special: it has no enclosing vhost, so it takes one name.
/// Every other word is pluralised and sent; an unsupported one is RabbitMQ's 404 to give.
fn collection_of(word: &str) -> String {
    match word {
        // Policies and the few other -y kinds pluralise to -ies, not -ys.
        word if word.ends_with('y') => format!("{}ies", &word[..word.len() - 1]),
        word if word.ends_with('s') => word.to_owned(),
        word => format!("{word}s"),
    }
}

/// A vhost is addressed by name alone; every other kind sits inside a vhost.
fn scoped(word: &str) -> bool {
    word != "vhost"
}

/// Header line that selects how the payload text is decoded. Consumed here and never
/// sent, matching the NATS editor so one habit works across both message providers.
const ENCODING: &str = "onetui-encoding";

/// A destructive read defaults to a single message and puts it back, which is the
/// non-committal choice for someone inspecting a queue. Any explicit count is sent as
/// given; the management API rejects one it will not serve.
const GET_COUNT: u32 = 1;

pub fn parse(text: &str) -> Result<Statement<'_>> {
    let (line, rest) = text.split_once('\n').unwrap_or((text, ""));
    let mut parts = line.split_whitespace();
    let verb = parts.next().ok_or_else(|| {
        anyhow!("Enter PUBLISH, GET, DECLARE, DELETE, PURGE or RAW on the first line")
    })?;
    match verb {
        "PUBLISH" => {
            let vhost = target(&mut parts, "PUBLISH vhost exchange routing_key")?;
            let exchange = target(&mut parts, "PUBLISH vhost exchange routing_key")?;
            // An empty routing key is meaningful on a fanout exchange, so it is required
            // positionally but may be given as the empty-string marker "".
            let routing_key = target(&mut parts, "PUBLISH vhost exchange routing_key")?;
            ensure!(
                parts.next().is_none(),
                "Enter PUBLISH vhost exchange routing_key on the first line"
            );
            let (headers, remainder) = headers(rest)?;
            let payload = decode(body(remainder)?, headers)?;
            Ok(Statement::Publish {
                vhost,
                exchange,
                routing_key: match routing_key {
                    "\"\"" => "",
                    key => key,
                },
                payload,
            })
        }
        "GET" => {
            let vhost = target(&mut parts, "GET vhost queue [count] [requeue|ack]")?;
            let queue = target(&mut parts, "GET vhost queue [count] [requeue|ack]")?;
            let mut count = GET_COUNT;
            let mut requeue = true;
            for part in parts {
                match part {
                    "requeue" => requeue = true,
                    "ack" => requeue = false,
                    digits => {
                        count = digits.parse().map_err(|_| {
                            anyhow!("Enter GET vhost queue [count] [requeue|ack]; {digits} is not a count")
                        })?;
                    }
                }
            }
            ensure!(
                body(rest)?.trim().is_empty(),
                "GET takes no body; put the count and acknowledgement mode on the first line"
            );
            Ok(Statement::Get {
                vhost,
                queue,
                count,
                requeue,
            })
        }
        "DECLARE" => {
            let word = target(&mut parts, "DECLARE object vhost name")?;
            let (vhost, name) = object(&mut parts, word, "DECLARE")?;
            Ok(Statement::Declare {
                collection: word,
                vhost,
                name,
                // The server validates the definition; an empty body means "defaults".
                body: body(rest)?,
            })
        }
        "DELETE" => {
            let word = target(&mut parts, "DELETE object vhost name")?;
            let (vhost, name) = object(&mut parts, word, "DELETE")?;
            ensure!(
                body(rest)?.trim().is_empty(),
                "DELETE takes no body; name the object on the first line"
            );
            Ok(Statement::Delete {
                collection: word,
                vhost,
                name,
            })
        }
        "PURGE" => {
            let vhost = target(&mut parts, "PURGE vhost queue")?;
            let queue = target(&mut parts, "PURGE vhost queue")?;
            ensure!(
                parts.next().is_none(),
                "Enter PURGE vhost queue on the first line"
            );
            ensure!(
                body(rest)?.trim().is_empty(),
                "PURGE takes no body; name the queue on the first line"
            );
            Ok(Statement::Purge { vhost, queue })
        }
        "RAW" => {
            let method = target(&mut parts, "RAW METHOD /path")?;
            let path = target(&mut parts, "RAW METHOD /path")?;
            ensure!(
                parts.next().is_none(),
                "Enter RAW METHOD /path on the first line"
            );
            let method = Method::from_bytes(method.as_bytes())
                .map_err(|_| anyhow!("Invalid HTTP method {method}"))?;
            ensure!(
                path.starts_with('/') && !path.contains("//"),
                "RAW paths start with / and have no empty segment; encode a literal / in a name as %2F"
            );
            Ok(Statement::Raw {
                method,
                path,
                body: body(rest)?,
            })
        }
        other => Err(anyhow!(
            "Unknown RabbitMQ verb {other}; use PUBLISH, GET, DECLARE, DELETE, PURGE or RAW"
        )),
    }
}

/// One whitespace-delimited word from the verb line.
fn target<'a>(parts: &mut std::str::SplitWhitespace<'a>, usage: &'static str) -> Result<&'a str> {
    parts.next().ok_or_else(|| anyhow!("Enter {usage}"))
}

/// A vhost and name pair, collapsing to a bare name for an unscoped kind.
fn object<'a>(
    parts: &mut std::str::SplitWhitespace<'a>,
    word: &str,
    verb: &'static str,
) -> Result<(&'a str, &'a str)> {
    let usage = if scoped(word) { "vhost name" } else { "name" };
    let first = parts
        .next()
        .ok_or_else(|| anyhow!("Enter {verb} {word} {usage}"))?;
    let pair = if scoped(word) {
        let name = parts
            .next()
            .ok_or_else(|| anyhow!("Enter {verb} {word} {usage}"))?;
        (first, name)
    } else {
        // A vhost has no enclosing vhost, so the single word is its name.
        ("", first)
    };
    ensure!(
        parts.next().is_none(),
        "Enter {verb} {word} {usage} on the first line"
    );
    Ok(pair)
}

/// Header lines sit between the verb line and the blank line. Only the encoding header
/// is understood; anything else is a mistake worth reporting rather than sending blind.
fn headers(rest: &str) -> Result<(Option<&str>, &str)> {
    let mut encoding = None;
    let mut remainder = rest;
    while let Some((line, next)) = remainder.split_once('\n')
        && !line.is_empty()
    {
        let (name, value) = line.split_once(':').ok_or_else(|| {
            anyhow!("Header lines are Name: value, or leave a blank line before the payload")
        })?;
        ensure!(
            name.trim().eq_ignore_ascii_case(ENCODING),
            "Unknown header {}; only {ENCODING} is understood",
            name.trim()
        );
        ensure!(encoding.is_none(), "Repeated {ENCODING} header");
        encoding = Some(value.trim());
        remainder = next;
    }
    Ok((encoding, remainder))
}

/// The body is whatever follows the blank line, taken literally.
fn body(rest: &str) -> Result<&str> {
    if rest.is_empty() {
        return Ok("");
    }
    rest.strip_prefix('\n')
        .ok_or_else(|| anyhow!("Leave a blank line before the body"))
}

/// Absent encoding sends the text as UTF-8, which is what most publishes want.
fn decode(payload: &str, encoding: Option<&str>) -> Result<Vec<u8>> {
    let Some(encoding) = encoding else {
        return Ok(payload.as_bytes().to_vec());
    };
    // Whitespace lets a long encoded payload wrap across lines.
    let compact: String = payload.split_whitespace().collect();
    match encoding.to_ascii_lowercase().as_str() {
        "utf-8" | "utf8" | "none" => Ok(payload.as_bytes().to_vec()),
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(&compact)
                .map_err(|error| anyhow!("Invalid base64 payload: {error}"))
        }
        "hex" => {
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
        other => Err(anyhow!(
            "Unknown {ENCODING} {other}; use base64, hex or utf-8"
        )),
    }
}

/// Prefill the editor from whatever the current view has open. The vhost is a literal
/// here, which is the point: the default vhost reads as `/`, not `%2F`.
pub fn watermark(resource: &Resource, row: Option<&Row>) -> String {
    let vhost = resource.path.first().map(String::as_str).unwrap_or("/");
    let name = row
        .and_then(|row| row.cells.first())
        .and_then(Option::as_ref)
        .and_then(onetui_core::Value::text);
    match resource.id {
        "rabbitmq.queues" => {
            let queue = name.unwrap_or("queue");
            format!("GET {vhost} {queue} 1 requeue")
        }
        "rabbitmq.exchanges" => {
            let exchange = name.unwrap_or("amq.default");
            format!("PUBLISH {vhost} {exchange} demo\n\nhello")
        }
        _ => format!("PUBLISH {vhost} amq.default demo\n\nhello"),
    }
}

/// The management request a statement describes: where it goes, and what it carries.
/// Path segments are pushed as literals, so `url` percent-encodes a vhost named `/`.
pub struct Outbound {
    pub method: Method,
    pub url: url::Url,
    pub body: Option<String>,
}

impl Statement<'_> {
    /// A read returns a page; everything else reports a write outcome.
    pub(crate) fn reads(&self) -> bool {
        match self {
            Self::Get { .. } => true,
            Self::Raw { method, .. } => matches!(*method, Method::GET | Method::HEAD),
            Self::Publish { .. }
            | Self::Declare { .. }
            | Self::Delete { .. }
            | Self::Purge { .. } => false,
        }
    }

    /// What the statement did, for a write summary. Present tense, naming the target.
    pub(crate) fn describes(&self) -> String {
        match self {
            Self::Publish {
                vhost,
                exchange,
                routing_key,
                ..
            } => {
                format!("Published to {exchange} on {vhost} with routing key {routing_key}")
            }
            Self::Declare {
                collection,
                vhost,
                name,
                ..
            } if scoped(collection) => {
                format!("Declared {collection} {name} on {vhost}")
            }
            Self::Declare {
                collection, name, ..
            } => format!("Declared {collection} {name}"),
            Self::Delete {
                collection,
                vhost,
                name,
            } if scoped(collection) => {
                format!("Deleted {collection} {name} on {vhost}")
            }
            Self::Delete {
                collection, name, ..
            } => format!("Deleted {collection} {name}"),
            Self::Purge { vhost, queue } => format!("Purged {queue} on {vhost}"),
            Self::Get { vhost, queue, .. } => format!("Read {queue} on {vhost}"),
            Self::Raw { method, path, .. } => format!("{method} {path}"),
        }
    }

    /// The statement as the user wrote it, for rejected or unknown outcomes. `describes`
    /// is past tense and only fits a request that succeeded.
    pub(crate) fn request(&self) -> String {
        match self {
            Self::Publish {
                vhost,
                exchange,
                routing_key,
                ..
            } => format!("PUBLISH to {exchange} on {vhost} with routing key {routing_key}"),
            Self::Declare {
                collection,
                vhost,
                name,
                ..
            } if scoped(collection) => format!("DECLARE {collection} {name} on {vhost}"),
            Self::Declare {
                collection, name, ..
            } => format!("DECLARE {collection} {name}"),
            Self::Delete {
                collection,
                vhost,
                name,
            } if scoped(collection) => format!("DELETE {collection} {name} on {vhost}"),
            Self::Delete {
                collection, name, ..
            } => format!("DELETE {collection} {name}"),
            Self::Purge { vhost, queue } => format!("PURGE {queue} on {vhost}"),
            Self::Get { vhost, queue, .. } => format!("GET {queue} on {vhost}"),
            Self::Raw { method, path, .. } => format!("RAW {method} {path}"),
        }
    }

    pub(crate) fn outbound(&self, base: &str) -> Result<Outbound> {
        let mut url = crate::config::endpoint(base)?;
        match self {
            Self::Publish {
                vhost,
                exchange,
                routing_key,
                payload,
            } => {
                segments(&mut url, ["exchanges", vhost, exchange, "publish"])?;
                // The management API takes the payload as JSON, base64 for any bytes.
                let body = serde_json::json!({
                    "properties": {},
                    "routing_key": routing_key,
                    "payload": base64::engine::general_purpose::STANDARD.encode(payload),
                    "payload_encoding": "base64",
                });
                Ok(Outbound {
                    method: Method::POST,
                    url,
                    body: Some(body.to_string()),
                })
            }
            Self::Get {
                vhost,
                queue,
                count,
                requeue,
            } => {
                segments(&mut url, ["queues", vhost, queue, "get"])?;
                let body = serde_json::json!({
                    "count": count,
                    "ackmode": if *requeue { "ack_requeue_true" } else { "ack_requeue_false" },
                    // Base64 keeps a non-UTF-8 payload intact on the way back.
                    "encoding": "base64",
                });
                Ok(Outbound {
                    method: Method::POST,
                    url,
                    body: Some(body.to_string()),
                })
            }
            Self::Declare {
                collection,
                vhost,
                name,
                body,
            } => {
                let target = collection_of(collection);
                if scoped(collection) {
                    segments(&mut url, [target.as_str(), vhost, name])?;
                } else {
                    segments(&mut url, [target.as_str(), name])?;
                }
                Ok(Outbound {
                    method: Method::PUT,
                    url,
                    // An empty body means defaults; the server needs a JSON object.
                    body: Some(if body.trim().is_empty() {
                        "{}".into()
                    } else {
                        (*body).into()
                    }),
                })
            }
            Self::Delete {
                collection,
                vhost,
                name,
            } => {
                let target = collection_of(collection);
                if scoped(collection) {
                    segments(&mut url, [target.as_str(), vhost, name])?;
                } else {
                    segments(&mut url, [target.as_str(), name])?;
                }
                Ok(Outbound {
                    method: Method::DELETE,
                    url,
                    body: None,
                })
            }
            Self::Purge { vhost, queue } => {
                segments(&mut url, ["queues", vhost, queue, "contents"])?;
                Ok(Outbound {
                    method: Method::DELETE,
                    url,
                    body: None,
                })
            }
            Self::Raw { method, path, body } => {
                let target = url.join(path)?;
                ensure!(
                    target.origin() == url.origin()
                        && target.fragment().is_none()
                        && target.username().is_empty()
                        && target.password().is_none(),
                    "RAW paths must stay on the configured endpoint"
                );
                Ok(Outbound {
                    method: method.clone(),
                    url: target,
                    body: (!body.is_empty()).then(|| (*body).to_owned()),
                })
            }
        }
    }
}

/// Push `/api` and the given literal segments, percent-encoding each one.
fn segments<'a>(url: &mut url::Url, parts: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut path = url
        .path_segments_mut()
        .map_err(|_| anyhow!("Invalid RabbitMQ URL"))?;
    path.clear().push("api");
    for part in parts {
        ensure!(
            !part.is_empty() && part.len() <= 4096 && !part.chars().any(char::is_control),
            "RabbitMQ names must be 1..4096 bytes without control characters"
        );
        path.push(part);
    }
    Ok(())
}

/// The status and bounded body of one management response.
pub struct Response {
    pub status: reqwest::StatusCode,
    pub body: Vec<u8>,
}

pub async fn send(pending: reqwest::RequestBuilder) -> Result<Response> {
    let mut response = pending
        .send()
        .await
        .map_err(|error| anyhow!(error.without_url()))?;
    let status = response.status();
    // Leave room for the row framing the body is displayed in.
    let limit = onetui_core::PAGE_BYTES - 1024;
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= limit as u64),
        "RabbitMQ HTTP {status}: response exceeds the display limit"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| anyhow!(error.without_url()))?
    {
        ensure!(
            chunk.len() <= limit - body.len(),
            "RabbitMQ HTTP {status}: response exceeds the display limit"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(Response { status, body })
}

/// A read renders the response as a page; a write reports an outcome. The native status
/// decides applied or rejected, so a permission error stays a rejection, not an error.
pub fn outcome(
    statement: &Statement<'_>,
    response: Response,
) -> Result<onetui_core::provider::QueryExecution> {
    use onetui_core::provider::{QueryExecution, WriteOutcome, WriteResult};
    let Response { status, body } = response;
    if statement.reads() {
        // A RAW read shows whatever came back, an error status included: inspecting a 404
        // or a native error body is the reason RAW exists. A GET promises decoded
        // messages, so a failed status cannot be rendered and stays an error.
        ensure!(
            status.is_success() || matches!(statement, Statement::Raw { .. }),
            "RabbitMQ HTTP {status}: {}",
            String::from_utf8_lossy(&body)
        );
        return Ok(QueryExecution::Page(messages(statement, status, &body)?));
    }
    let detail = String::from_utf8_lossy(&body);
    let detail = detail.trim();
    let summary = if status.is_success() {
        match statement {
            // A publish the broker accepted but routed nowhere is a real outcome worth
            // naming: the request succeeded and the message was still dropped.
            Statement::Publish { .. }
                if serde_json::from_slice::<serde_json::Value>(&body)
                    .is_ok_and(|value| value["routed"] == false) =>
            {
                format!(
                    "{}, but no queue matched the routing key",
                    statement.describes()
                )
            }
            _ => format!("{} (HTTP {status})", statement.describes()),
        }
    } else if detail.is_empty() {
        format!("RabbitMQ rejected {}: HTTP {status}", statement.request())
    } else {
        format!(
            "RabbitMQ rejected {}: HTTP {status}: {detail}",
            statement.request()
        )
    };
    Ok(QueryExecution::Write(WriteResult {
        outcome: if status.is_success() {
            WriteOutcome::Applied
        } else {
            WriteOutcome::Rejected
        },
        summary,
    }))
}

/// Render a read. GET returns an array of messages with a base64 payload, which is
/// decoded into bytes so binary shows as bytes rather than an encoded string. A RAW read
/// has no known shape, so its status and body are displayed as-is, which is how a native
/// error stays inspectable.
fn messages(
    statement: &Statement<'_>,
    status: reqwest::StatusCode,
    body: &[u8],
) -> Result<onetui_core::Page> {
    use onetui_core::{Column, Page, Row, Value};
    let Statement::Get { .. } = statement else {
        return Ok(Page {
            rows: vec![Row {
                cells: vec![
                    Some(Value::Text(status.to_string())),
                    Some(Value::Bytes(body.to_vec())),
                ],
                target: None,
            }],
            columns: vec![
                Column {
                    name: "status".into(),
                    datatype: "text".into(),
                },
                Column {
                    name: "body".into(),
                    datatype: "bytes".into(),
                },
            ],
            ..Page::default()
        });
    };
    let values: Vec<serde_json::Value> = serde_json::from_slice(body)?;
    let columns = [
        "routing_key",
        "exchange",
        "redelivered",
        "properties",
        "payload",
    ];
    let rows = values
        .into_iter()
        .map(|value| {
            let cells = columns
                .iter()
                .map(|name| match (*name, &value[*name]) {
                    ("payload", serde_json::Value::String(text)) => {
                        // The request asked for base64 so any bytes survive the trip.
                        base64::engine::general_purpose::STANDARD
                            .decode(text)
                            .map(|bytes| Some(Value::Bytes(bytes)))
                            .unwrap_or_else(|_| Some(Value::Text(text.clone())))
                    }
                    (_, serde_json::Value::Null) => None,
                    (_, serde_json::Value::String(text)) => Some(Value::Text(text.clone())),
                    (_, cell) => Some(Value::Json(cell.to_string())),
                })
                .collect();
            Row {
                cells,
                target: None,
            }
        })
        .collect();
    let page = Page {
        rows,
        columns: columns
            .iter()
            .map(|name| Column {
                name: (*name).into(),
                datatype: "JSON or text".into(),
            })
            .collect(),
        notice: "Destructive read. Messages acknowledged with ack are gone; requeued messages return to the queue in a changed position.".into(),
        ..Page::default()
    };
    ensure!(
        page.bytes() <= onetui_core::PAGE_BYTES,
        "RabbitMQ page exceeds 1 MiB"
    );
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verbs_name_their_vhost_literally_and_bodies_are_strict() {
        let Statement::Publish {
            vhost,
            exchange,
            routing_key,
            payload,
        } = parse("PUBLISH / amq.default demo\n\nhello").unwrap()
        else {
            panic!("expected publish")
        };
        // The default vhost is typed as "/" and never as %2F.
        assert_eq!((vhost, exchange, routing_key), ("/", "amq.default", "demo"));
        assert_eq!(payload, b"hello");

        // An empty routing key is meaningful on a fanout exchange.
        let Statement::Publish { routing_key, .. } = parse("PUBLISH / logs \"\"\n\nhello").unwrap()
        else {
            panic!("expected publish")
        };
        assert!(routing_key.is_empty());

        let Statement::Get {
            vhost,
            queue,
            count,
            requeue,
        } = parse("GET / demo").unwrap()
        else {
            panic!("expected get")
        };
        // A bare GET reads one message and puts it back.
        assert_eq!((vhost, queue, count, requeue), ("/", "demo", 1, true));

        let Statement::Get { count, requeue, .. } = parse("GET / demo 10 ack").unwrap() else {
            panic!("expected get")
        };
        assert_eq!((count, requeue), (10, false));

        let Statement::Declare {
            collection,
            vhost,
            name,
            body,
        } = parse("DECLARE queue / demo\n\n{\"durable\":true}").unwrap()
        else {
            panic!("expected declare")
        };
        assert_eq!(
            (collection, vhost, name, body),
            ("queue", "/", "demo", "{\"durable\":true}")
        );

        // A vhost has no enclosing vhost, so it takes one name.
        let Statement::Declare {
            collection,
            vhost,
            name,
            ..
        } = parse("DECLARE vhost staging").unwrap()
        else {
            panic!("expected declare")
        };
        assert_eq!((collection, vhost, name), ("vhost", "", "staging"));

        let Statement::Delete {
            collection,
            vhost,
            name,
        } = parse("DELETE policy / ha").unwrap()
        else {
            panic!("expected delete")
        };
        assert_eq!((collection, vhost, name), ("policy", "/", "ha"));

        // An object kind OneTUI does not know is sent for RabbitMQ to accept or refuse,
        // rather than rejected here against a list that would age.
        let Statement::Declare { collection, .. } = parse("DECLARE shovel / move").unwrap() else {
            panic!("expected declare")
        };
        assert_eq!(collection, "shovel");
        assert_eq!(collection_of("shovel"), "shovels");
        assert_eq!(collection_of("policy"), "policies");
        // An already-plural word is left alone.
        assert_eq!(collection_of("permissions"), "permissions");

        let Statement::Purge { vhost, queue } = parse("PURGE / demo").unwrap() else {
            panic!("expected purge")
        };
        assert_eq!((vhost, queue), ("/", "demo"));

        for text in [
            "",
            "PUBLISH",
            "PUBLISH /",
            "PUBLISH / amq.default",
            "PUBLISH / amq.default demo extra",
            "PUBLISH / amq.default demo\npayload",
            "GET",
            "GET / demo notacount",
            "GET / demo\n\n{}",
            "DECLARE",
            "DECLARE queue /",
            "DECLARE queue / demo extra",
            "DECLARE vhost staging extra",
            "DELETE queue / demo\n\n{}",
            "PURGE / demo extra",
            "PURGE / demo\n\n{}",
            "FLUSH / demo",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn raw_reaches_unmodelled_endpoints_and_rejects_empty_segments() {
        let Statement::Raw { method, path, body } =
            parse("RAW PUT /api/users/alice\n\n{\"tags\":\"monitoring\"}").unwrap()
        else {
            panic!("expected raw")
        };
        assert_eq!(
            (method, path, body),
            (Method::PUT, "/api/users/alice", "{\"tags\":\"monitoring\"}")
        );

        // A literal / inside a name still needs encoding on the raw path, and an empty
        // segment is the mistake that invites, so it is named rather than silently sent.
        assert!(parse("RAW GET /api/queues/%2F/demo").is_ok());
        for text in [
            "RAW",
            "RAW GET",
            "RAW GET api/overview",
            "RAW GET /api/queues///demo",
            "RAW GET //other.example/",
            "RAW BAD\u{7f}METHOD /api/overview",
            "RAW GET /api/overview extra",
            "RAW POST /api/queues\n{}",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn encoding_header_is_consumed_and_other_headers_are_refused() {
        for (header, expected) in [
            ("Onetui-Encoding: base64\n", b"hello".to_vec()),
            ("onetui-encoding: hex\n", b"hello".to_vec()),
            ("Onetui-Encoding: utf-8\n", b"hello".to_vec()),
            ("", b"hello".to_vec()),
        ] {
            let payload_text = match header {
                "Onetui-Encoding: base64\n" => "aGVsbG8=",
                "onetui-encoding: hex\n" => "68656c6c6f",
                _ => "hello",
            };
            let source = format!("PUBLISH / amq.default demo\n{header}\n{payload_text}");
            let Statement::Publish { payload, .. } = parse(&source).unwrap() else {
                panic!("expected publish")
            };
            assert_eq!(payload, expected, "{source}");
        }

        // Binary that is not valid UTF-8 is exactly what the encoding header is for.
        let Statement::Publish { payload, .. } =
            parse("PUBLISH / amq.default demo\nOnetui-Encoding: hex\n\nff00").unwrap()
        else {
            panic!("expected publish")
        };
        assert_eq!(payload, vec![255, 0]);

        // An empty payload stays empty rather than becoming a missing body.
        let Statement::Publish { payload, .. } = parse("PUBLISH / amq.default demo\n\n").unwrap()
        else {
            panic!("expected publish")
        };
        assert!(payload.is_empty());

        for text in [
            "PUBLISH / amq.default demo\nnot-a-header\n\nhi",
            "PUBLISH / amq.default demo\nContent-Type: application/json\n\nhi",
            "PUBLISH / amq.default demo\nOnetui-Encoding: base64\n\n!!!!",
            "PUBLISH / amq.default demo\nOnetui-Encoding: hex\n\nabc",
            "PUBLISH / amq.default demo\nOnetui-Encoding: rot13\n\nhi",
            "PUBLISH / amq.default demo\nOnetui-Encoding: hex\nOnetui-Encoding: base64\n\n00",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
    }

    #[test]
    fn watermark_uses_the_open_vhost_and_selected_row() {
        let row = Row {
            cells: vec![Some("orders".into())],
            target: None,
        };
        assert_eq!(
            watermark(
                &Resource::new("rabbitmq.queues", vec!["/".into()]),
                Some(&row)
            ),
            "GET / orders 1 requeue"
        );
        assert_eq!(
            watermark(
                &Resource::new("rabbitmq.exchanges", vec!["staging".into()]),
                None
            ),
            "PUBLISH staging amq.default demo\n\nhello"
        );
        // With nothing open the default vhost still reads as "/".
        assert_eq!(
            watermark(&Resource::new("rabbitmq.query", vec![]), None),
            "PUBLISH / amq.default demo\n\nhello"
        );
    }

    #[test]
    fn failed_outcomes_name_the_request_in_its_verb_form() {
        let declare = parse("DECLARE queue / demo").unwrap();
        assert_eq!(declare.request(), "DECLARE queue demo on /");
        let publish = parse("PUBLISH / amq.default demo\n\nhello").unwrap();
        assert_eq!(
            publish.request(),
            "PUBLISH to amq.default on / with routing key demo"
        );
    }
}
