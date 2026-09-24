use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::catalog::{Action, ActionSource, ResourceAction, ResourceDescriptor};
use onetui_core::provider::PageRequest;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

pub(crate) const RESOURCES: &[&ResourceDescriptor] = &[
    &crate::discovery::RESOURCE,
    &ResourceDescriptor {
        id: "nats.query",
        description: "Subject-filtered sequence/time replay without consumers",
        columns: &["sequence", "subject", "time", "data", "headers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.consumer_streams",
        description: "Streams with consumer inspection",
        columns: &["name", "subjects", "messages", "bytes", "consumers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.consumers",
        description: "Consumer delivery and acknowledgement state; never pulled",
        columns: &[
            "name",
            "pending",
            "ack_pending",
            "redelivered",
            "delivered",
            "ack_floor",
        ],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.consumer_info",
        description: "Complete consumer configuration and state",
        columns: &["info"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.kv_buckets",
        description: "JetStream KV buckets",
        columns: &["name", "subjects", "messages", "bytes", "consumers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.kv_keys",
        description: "Retained KV keys, including deletion markers",
        columns: &["key", "revisions"],
        paging: true,
        actions: &[ResourceAction {
            id: Action::Columns,
            target: "nats.kv_value",
            source: ActionSource::SelectedTarget,
        }],
    },
    &ResourceDescriptor {
        id: "nats.kv_history",
        description: "Retained KV revisions; f watches future revisions",
        columns: &["sequence", "subject", "time", "data", "headers"],
        paging: true,
        actions: &[ResourceAction {
            id: Action::Columns,
            target: "nats.kv_value",
            source: ActionSource::Current,
        }],
    },
    &ResourceDescriptor {
        id: "nats.kv_value",
        description: "Latest KV entry including deletion/purge headers",
        columns: &["sequence", "subject", "time", "data", "headers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.object_buckets",
        description: "JetStream object buckets",
        columns: &["name", "subjects", "messages", "bytes", "consumers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.objects",
        description: "Retained object metadata names",
        columns: &["name", "revisions"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.object",
        description: "Object metadata or bounded content",
        columns: &["view"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.object_info",
        description: "Object metadata, digest and links",
        columns: &["info"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.object_chunks",
        description: "Lazy content chunks from a pinned object version",
        columns: &["sequence", "subject", "time", "data", "headers"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.resources",
        description: "NATS resources",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.subjects",
        description: "Explicitly configured Core subjects",
        columns: &["subject"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "nats.core_messages",
        description: "Live Core subscription; no history or acknowledgements",
        columns: &[
            "subject",
            "reply",
            "data",
            "headers",
            "status",
            "description",
        ],
        paging: true,
        actions: &[],
    },
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
pub(crate) struct Cursor {
    pub session: u64,
    pub resource: String,
    pub path: Vec<String>,
    pub live: bool,
    pub next: u64,
    pub end: u64,
    pub created: String,
    pub read_rows: u64,
    pub read_bytes: u64,
}

pub(crate) fn number(value: &Json, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| anyhow!("NATS response missing integer {key}"))
}
pub(crate) fn string<'a>(value: &'a Json, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .ok_or_else(|| anyhow!("NATS response missing string {key}"))
}
pub(crate) fn columns(names: &[(&str, &str)]) -> Vec<Column> {
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
        !live
            || matches!(
                resource.id,
                "nats.messages" | "nats.core_messages" | "nats.kv_history"
            ),
        "NATS following requires a stream message view"
    );
    ensure!(
        resource.path.len()
            == match resource.id {
                "nats.streams"
                | "nats.resources"
                | "nats.subjects"
                | "nats.servers"
                | "nats.consumer_streams"
                | "nats.kv_buckets"
                | "nats.object_buckets" => 0,
                "nats.consumer_info" | "nats.kv_history" | "nats.kv_value" | "nats.object"
                | "nats.object_info" | "nats.object_chunks" => 2,
                _ => 1,
            },
        "Invalid NATS resource path"
    );
    for (index, name) in resource.path.iter().enumerate() {
        if index == 1
            && matches!(
                resource.id,
                "nats.object" | "nats.object_info" | "nats.object_chunks"
            )
        {
            ensure!(
                !name.is_empty() && name.len() <= 1024,
                "Invalid NATS object name"
            );
            continue;
        }
        if index == 1 && matches!(resource.id, "nats.kv_history" | "nats.kv_value") {
            ensure!(
                !name.is_empty()
                    && name.len() <= 1024
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-/_=.".contains(&b)),
                "Invalid NATS KV key"
            );
            continue;
        }
        if resource.id == "nats.core_messages" {
            crate::core_subscription::validate_subject(name)?;
            continue;
        }
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

pub(crate) fn local_page(
    config: &crate::config::Config,
    request: &PageRequest,
) -> Result<Option<Page>> {
    let mut page = Page::default();
    match request.resource.id {
        "nats.resources" => {
            page.columns = columns(&[("resource", "text"), ("description", "text")]);
            if config.jetstream {
                page.rows.push(Row {
                    cells: vec![
                        Some("Streams".into()),
                        Some("JetStream retained messages".into()),
                    ],
                    target: Some(Resource::new("nats.streams", vec![])),
                });
                for (name, target) in [
                    ("Consumers", "nats.consumer_streams"),
                    ("KV", "nats.kv_buckets"),
                    ("Objects", "nats.object_buckets"),
                ] {
                    page.rows.push(Row {
                        cells: vec![Some(name.into()), Some("JetStream inspection".into())],
                        target: Some(Resource::new(target, vec![])),
                    });
                }
            }
            page.rows.push(Row {
                cells: vec![
                    Some("Subjects".into()),
                    Some("Core subscriptions from configured subjects".into()),
                ],
                target: Some(Resource::new("nats.subjects", vec![])),
            });
            if config.system_discovery {
                page.rows.push(Row {
                    cells: vec![
                        Some("Servers".into()),
                        Some("Observed system-account responders".into()),
                    ],
                    target: Some(Resource::new("nats.servers", vec![])),
                });
            }
        }
        "nats.subjects" => {
            page.columns = columns(&[("subject", "text")]);
            page.rows = config
                .subjects
                .iter()
                .map(|subject| Row {
                    cells: vec![Some(subject.clone().into())],
                    target: Some(Resource::new("nats.core_messages", vec![subject.clone()])),
                })
                .collect();
            page.notice = "Configure subjects in onetui.toml; opening does not subscribe. Press f in the message view.".into();
        }
        "nats.core_messages" => {
            ensure!(
                config.subjects.contains(&request.resource.path[0]),
                "NATS subject is not configured"
            );
            page = crate::core_subscription::empty();
        }
        _ => return Ok(None),
    }
    ensure!(
        request.continuation.is_none(),
        "NATS local view has one page"
    );
    Ok(Some(page))
}

pub(crate) struct Api<'a> {
    pub client: &'a async_nats::Client,
    pub prefix: &'a str,
}

impl Api<'_> {
    pub async fn raw(&self, operation: &str, body: Json) -> Result<Json> {
        let response = self
            .client
            .request(
                format!("{}.{operation}", self.prefix),
                serde_json::to_vec(&body)?.into(),
            )
            .await?;
        ensure!(
            response.payload.len() <= PAGE_BYTES,
            "NATS response exceeds 1 MiB"
        );
        Ok(serde_json::from_slice(&response.payload)?)
    }
    pub async fn call(&self, operation: &str, body: Json) -> Result<Json> {
        success(self.raw(operation, body).await?)
    }
    pub async fn message(&self, stream: &str, body: Json) -> Result<Option<Json>> {
        let value = self.raw(&format!("STREAM.MSG.GET.{stream}"), body).await?;
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
}
fn success(value: Json) -> Result<Json> {
    if let Some(error) = value.get("error") {
        anyhow::bail!("{error}");
    }
    Ok(value)
}
pub(crate) async fn page(
    api: &Api<'_>,
    session: u64,
    request: PageRequest,
    live: bool,
    replay: Option<&crate::replay::Replay>,
) -> Result<Page> {
    validate(&request, live)?;
    let resource = &request.resource;
    ensure!(
        (resource.id == "nats.query") == replay.is_some(),
        "NATS replay requires query_page"
    );
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
        read_rows: 0,
        read_bytes: 0,
    });
    let mut page = Page::default();
    if let Some(metadata) =
        crate::metadata::page(api, resource, &mut cursor, request.continuation.is_some()).await?
    {
        page = metadata;
    } else if matches!(
        resource.id,
        "nats.streams" | "nats.consumer_streams" | "nats.kv_buckets" | "nats.object_buckets"
    ) {
        let info = api
            .call("STREAM.LIST", json!({"offset": cursor.next}))
            .await?;
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
        let consumed = streams.len().min(PAGE_SIZE as usize);
        for stream in streams.iter().take(consumed) {
            let name = string(&stream["config"], "name")?;
            let (name, target) = match resource.id {
                "nats.kv_buckets" => match name.strip_prefix("KV_") {
                    Some(bucket) => (bucket, "nats.kv_keys"),
                    None => continue,
                },
                "nats.object_buckets" => match name.strip_prefix("OBJ_") {
                    Some(bucket) => (bucket, "nats.objects"),
                    None => continue,
                },
                "nats.consumer_streams" => (name, "nats.consumers"),
                _ => (name, "nats.messages"),
            };
            let target = Resource::new(target, vec![name.into()]);
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
            .checked_add(consumed as u64)
            .ok_or_else(|| anyhow!("NATS list offset overflow"))?;
        page.next = cursor.next < total;
        ensure!(
            !page.next || !streams.is_empty(),
            "NATS list made no progress"
        );
        page.notice = "Server offset listing; concurrent changes may shift entries".into();
    } else {
        let mut scope = crate::metadata::message_scope(api, resource).await?;
        if let Some(replay) = replay {
            scope.subject = replay.subject.clone();
            scope.version = serde_json::to_string(replay)?;
        }
        let stream = &scope.stream;
        let info = api
            .call(&format!("STREAM.INFO.{stream}"), json!({}))
            .await?;
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
            let created = format!("{}:{}", string(&info, "created")?, scope.version);
            if request.continuation.is_none() {
                cursor.created = created;
                cursor.end = end;
                cursor.next = if live { end } else { first.min(end) };
                if let Some(replay) = replay {
                    if let Some(start) = replay.start_sequence {
                        ensure!(
                            start >= first && start <= end,
                            "NATS start_sequence outside retained stream range"
                        );
                        cursor.next = start;
                    }
                    cursor.end = replay.end_sequence.unwrap_or(end).min(end).max(cursor.next);
                }
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
            let start_time = replay
                .and_then(|r| r.start_time.as_deref())
                .map(async_nats::datetime::parse_rfc3339)
                .transpose()
                .map_err(anyhow::Error::from_boxed)?;
            let mut scanned = 0;
            while cursor.next < cursor.end && scanned < PAGE_SIZE {
                let response = api
                    .message(
                        stream,
                        json!({"seq": cursor.next, "next_by_subj": scope.subject}),
                    )
                    .await?;
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
                scanned += 1;
                if let Some(start) = start_time
                    && async_nats::datetime::parse_rfc3339(string(&message, "time")?)
                        .map_err(anyhow::Error::from_boxed)?
                        < start
                {
                    cursor.next = seq + 1;
                    continue;
                }
                let row = message_row(&message)?;
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
                cursor.read_rows = cursor
                    .read_rows
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("NATS row count overflow"))?;
                cursor.read_bytes = cursor
                    .read_bytes
                    .checked_add(
                        page.rows.last().unwrap().cells[3]
                            .as_ref()
                            .unwrap()
                            .bytes()
                            .len() as u64,
                    )
                    .ok_or_else(|| anyhow!("NATS byte count overflow"))?;
            }
            page.next = cursor.next < cursor.end;
            if !page.next
                && let Some((chunks, bytes)) = scope.object_size
            {
                ensure!(
                    cursor.read_rows == chunks && cursor.read_bytes == bytes,
                    "NATS object chunks changed or are missing; refresh"
                );
            }
            page.notice = format!(
                "Stream sequences [{first}, {}); no snapshot, consumers or acknowledgements",
                cursor.end
            );
            if resource.id == "nats.object_chunks" {
                page.notice.push_str("; object version pinned; chunks are separate byte values, whole-object digest not verified");
            }
            if let Some(replay) = replay {
                page.notice.push_str(&format!("; subject {}; scanned {scanned}; time filtering scans at most 100 matching messages per page", replay.subject));
            }
        }
    }
    if page.next || live {
        let token = serde_json::to_string(&cursor)?;
        ensure!(token.len() <= 4096, "NATS continuation exceeds 4096 bytes");
        page.continuation = Some(token);
    }
    ensure!(page.bytes() <= PAGE_BYTES, "NATS page exceeds 1 MiB");
    Ok(page)
}

pub(crate) fn message_row(message: &Json) -> Result<Row> {
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
    Ok(Row {
        cells: vec![
            Some(number(message, "seq")?.to_string().into()),
            Some(string(message, "subject")?.into()),
            Some(string(message, "time")?.into()),
            Some(decode("data")?.unwrap_or(Value::Bytes(vec![]))),
            decode("hdrs")?,
        ],
        target: None,
    })
}
