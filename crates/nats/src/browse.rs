use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::catalog::{Action, ActionSource, ResourceAction, ResourceDescriptor};
use onetui_core::provider::PageRequest;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

pub(crate) const RESOURCES: &[&ResourceDescriptor] = &[
    &ResourceDescriptor {
        id: "nats.streams",
        description: "JetStream streams",
        columns: &["name", "subjects", "messages", "bytes", "consumers"],
        paging: true,
        actions: &[ResourceAction {
            id: Action::Columns,
            target: "nats.stream_info",
            source: ActionSource::SelectedTarget,
        }],
    },
    &ResourceDescriptor {
        id: "nats.messages",
        description: "Retained stream messages, without consumers or acknowledgements",
        columns: &["sequence", "subject", "time", "data", "headers"],
        paging: true,
        actions: &[ResourceAction {
            id: Action::Columns,
            target: "nats.stream_info",
            source: ActionSource::Current,
        }],
    },
    &ResourceDescriptor {
        id: "nats.stream_info",
        description: "Stream configuration and state",
        columns: &["info"],
        paging: true,
        actions: &[],
    },
];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    session: u64,
    resource: String,
    path: Vec<String>,
    live: bool,
    next: u64,
    end: u64,
    created: String,
}

fn number(value: &Json, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| anyhow!("NATS response missing integer {key}"))
}
fn string<'a>(value: &'a Json, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| anyhow!("NATS response missing string {key}"))
}
fn columns(names: &[(&str, &str)]) -> Vec<Column> {
    names
        .iter()
        .map(|(name, datatype)| Column {
            name: (*name).into(),
            datatype: (*datatype).into(),
        })
        .collect()
}
pub(crate) fn validate(request: &PageRequest, live: bool) -> Result<()> {
    let resource = &request.resource;
    ensure!(
        RESOURCES.iter().any(|r| r.id == resource.id),
        "Unknown NATS resource"
    );
    ensure!(
        !live || resource.id == "nats.messages",
        "NATS following requires a stream message view"
    );
    ensure!(
        resource.path.len() == usize::from(resource.id != "nats.streams"),
        "Invalid NATS resource path"
    );
    for name in &resource.path {
        ensure!(
            !name.is_empty()
                && name.len() <= 255
                && !name
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || ".*/\\>".contains(c)),
            "Invalid NATS stream name"
        );
    }
    ensure!(
        request
            .continuation
            .as_ref()
            .is_none_or(|c| c.len() <= 4096),
        "NATS continuation exceeds 4096 bytes"
    );
    Ok(())
}

pub(crate) async fn request(
    client: &async_nats::Client,
    subject: String,
    body: Json,
) -> Result<Json> {
    let response = client
        .request(subject, serde_json::to_vec(&body)?.into())
        .await?;
    ensure!(
        response.payload.len() <= PAGE_BYTES,
        "NATS response exceeds 1 MiB"
    );
    Ok(serde_json::from_slice(&response.payload)?)
}
fn success(value: Json) -> Result<Json> {
    if let Some(error) = value.get("error") {
        anyhow::bail!("{error}");
    }
    Ok(value)
}
pub(crate) async fn api(client: &async_nats::Client, operation: &str, body: Json) -> Result<Json> {
    success(request(client, format!("$JS.API.{operation}"), body).await?)
}

pub(crate) async fn page(
    client: &async_nats::Client,
    session: u64,
    request: PageRequest,
    live: bool,
) -> Result<Page> {
    validate(&request, live)?;
    let resource = &request.resource;
    let prior = request
        .continuation
        .as_ref()
        .map(|value| serde_json::from_str::<Cursor>(value))
        .transpose()?;
    if let Some(c) = &prior {
        ensure!(
            c.session == session
                && c.resource == resource.id
                && c.path == resource.path
                && c.live == live,
            "NATS continuation belongs to another session, resource or mode"
        );
    }
    let mut cursor = prior.unwrap_or(Cursor {
        session,
        resource: resource.id.into(),
        path: resource.path.clone(),
        live,
        next: 0,
        end: 0,
        created: String::new(),
    });
    let mut page = Page::default();
    if resource.id == "nats.streams" {
        let info = api(client, "STREAM.LIST", json!({"offset": cursor.next})).await?;
        let streams = info["streams"].as_array().map(Vec::as_slice).unwrap_or(&[]);
        let total = number(&info, "total")?;
        page.columns = columns(&[
            ("name", "text"),
            ("subjects", "json"),
            ("messages", "integer"),
            ("bytes", "integer"),
            ("consumers", "integer"),
        ]);
        // The server chooses its own list batch size; retain only our page and advance by that count.
        for stream in streams.iter().take(PAGE_SIZE as usize) {
            let name = string(&stream["config"], "name")?;
            let target = Resource::new("nats.messages", vec![name.into()]);
            validate(
                &PageRequest {
                    resource: target.clone(),
                    continuation: None,
                },
                false,
            )?;
            page.rows.push(Row {
                cells: vec![
                    Some(name.into()),
                    Some(Value::Json(stream["config"]["subjects"].to_string())),
                    Some(number(&stream["state"], "messages")?.to_string().into()),
                    Some(number(&stream["state"], "bytes")?.to_string().into()),
                    Some(
                        number(&stream["state"], "consumer_count")?
                            .to_string()
                            .into(),
                    ),
                ],
                target: Some(target),
            });
        }
        cursor.next = cursor
            .next
            .checked_add(page.rows.len() as u64)
            .ok_or_else(|| anyhow!("NATS list offset overflow"))?;
        page.next = cursor.next < total;
        ensure!(
            !page.next || !streams.is_empty(),
            "NATS list made no progress"
        );
        page.notice = "Server offset listing; concurrent changes may shift entries".into();
    } else {
        let stream = &resource.path[0];
        let info = api(client, &format!("STREAM.INFO.{stream}"), json!({})).await?;
        if resource.id == "nats.stream_info" {
            ensure!(
                request.continuation.is_none(),
                "NATS stream info has one page"
            );
            page.columns = columns(&[("info", "json")]);
            page.rows.push(Row {
                cells: vec![Some(Value::Json(info.to_string()))],
                target: None,
            });
        } else {
            let first = number(&info["state"], "first_seq")?.max(1);
            let end = number(&info["state"], "last_seq")?
                .checked_add(1)
                .ok_or_else(|| anyhow!("NATS sequence overflow"))?;
            let created = string(&info, "created")?;
            if request.continuation.is_none() {
                cursor.created = created.into();
                cursor.end = end;
                cursor.next = if live { end } else { first.min(end) };
            } else {
                ensure!(
                    cursor.created == created
                        && cursor.next >= first
                        && cursor.next <= end
                        && cursor.end <= end,
                    "NATS stream changed or retained sequence is unavailable; refresh or restart following"
                );
                if live {
                    cursor.end = end;
                }
            }
            page.columns = columns(&[
                ("sequence", "integer"),
                ("subject", "text"),
                ("time", "RFC3339"),
                ("data", "bytes"),
                ("headers", "bytes (NATS header block)"),
            ]);
            while cursor.next < cursor.end && page.rows.len() < PAGE_SIZE as usize {
                let response = request_message(client, stream, cursor.next).await?;
                let Some(message) = response else {
                    cursor.next = cursor.end;
                    break;
                };
                let seq = number(&message, "seq")?;
                ensure!(seq >= cursor.next, "NATS message sequence did not advance");
                if seq >= cursor.end {
                    cursor.next = cursor.end;
                    break;
                }
                let decode = |name| -> Result<Option<Value>> {
                    message
                        .get(name)
                        .map(|v| -> Result<Value> {
                            Ok(Value::Bytes(
                                base64::engine::general_purpose::STANDARD.decode(
                                    v.as_str()
                                        .ok_or_else(|| anyhow!("Invalid NATS base64 field"))?,
                                )?,
                            ))
                        })
                        .transpose()
                };
                let row = Row {
                    cells: vec![
                        Some(seq.to_string().into()),
                        Some(string(&message, "subject")?.into()),
                        Some(string(&message, "time")?.into()),
                        Some(decode("data")?.unwrap_or(Value::Bytes(vec![]))),
                        decode("hdrs")?,
                    ],
                    target: None,
                };
                page.rows.push(row);
                // Leave room for the cursor/notice and the TUI's hexadecimal byte projection.
                if page.bytes() > (PAGE_BYTES - 8192) / 2 {
                    page.rows.pop();
                    ensure!(
                        !page.rows.is_empty(),
                        "NATS message exceeds the bounded page/projection budget"
                    );
                    break;
                }
                cursor.next = seq + 1;
            }
            page.next = cursor.next < cursor.end;
            page.notice = format!(
                "Stream sequences [{first}, {}); no snapshot, consumers or acknowledgements",
                cursor.end
            );
        }
    }
    if page.next || live {
        page.continuation = Some(serde_json::to_string(&cursor)?);
    }
    ensure!(page.bytes() <= PAGE_BYTES, "NATS page exceeds 1 MiB");
    Ok(page)
}

async fn request_message(
    client: &async_nats::Client,
    stream: &str,
    sequence: u64,
) -> Result<Option<Json>> {
    let value = request(
        client,
        format!("$JS.API.STREAM.MSG.GET.{stream}"),
        json!({"seq": sequence, "next_by_subj": ">"}),
    )
    .await?;
    if value["error"]["err_code"].as_u64() == Some(10037) {
        return Ok(None);
    }
    let value = success(value)?;
    Ok(Some(
        value
            .get("message")
            .ok_or_else(|| anyhow!("NATS response missing message"))?
            .clone(),
    ))
}
