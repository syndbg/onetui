use anyhow::{Result, anyhow, ensure};
use base64::Engine;
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use serde_json::{Value as Json, json};

use crate::browse::{Api, Cursor, columns, message_row, number, string};

fn json_page(value: Json) -> Page {
    Page {
        columns: columns(&[("info", "json")]),
        rows: vec![Row {
            cells: vec![Some(Value::Json(value.to_string()))],
            target: None,
        }],
        ..Page::default()
    }
}

async fn object_info(api: &Api<'_>, resource: &Resource) -> Result<Json> {
    let bucket = &resource.path[0];
    let name = &resource.path[1];
    let subject = format!(
        "$O.{bucket}.M.{}",
        base64::engine::general_purpose::URL_SAFE.encode(name)
    );
    let message = api
        .message(&format!("OBJ_{bucket}"), json!({"last_by_subj": subject}))
        .await?
        .ok_or_else(|| anyhow!("NATS object metadata is no longer retained"))?;
    ensure!(
        string(&message, "subject")? == subject,
        "NATS returned another object's metadata"
    );
    let bytes = base64::engine::general_purpose::STANDARD.decode(string(&message, "data")?)?;
    let info: Json = serde_json::from_slice(&bytes)?;
    ensure!(
        info["name"] == *name && info["bucket"] == *bucket,
        "NATS object metadata identity mismatch"
    );
    Ok(info)
}

pub(crate) struct MessageScope {
    pub stream: String,
    pub subject: String,
    pub version: String,
    pub object_size: Option<(u64, u64)>,
}

pub(crate) async fn message_scope(api: &Api<'_>, resource: &Resource) -> Result<MessageScope> {
    match resource.id {
        "nats.kv_history" => Ok(MessageScope {
            stream: format!("KV_{}", resource.path[0]),
            subject: format!("$KV.{}.{}", resource.path[0], resource.path[1]),
            version: String::new(),
            object_size: None,
        }),
        "nats.object_chunks" => {
            let info = object_info(api, resource).await?;
            ensure!(info["deleted"] != true, "NATS object is deleted");
            ensure!(
                info["options"]["link"].is_null(),
                "NATS object is a link; inspect its metadata and open the target explicitly"
            );
            let nuid = string(&info, "nuid")?;
            ensure!(
                !nuid.is_empty()
                    && nuid.len() <= 255
                    && nuid.bytes().all(|b| b.is_ascii_alphanumeric()),
                "Invalid NATS object chunk identity"
            );
            let version = serde_json::to_string(&(
                nuid,
                number(&info, "size")?,
                number(&info, "chunks")?,
                &info["digest"],
            ))?;
            Ok(MessageScope {
                stream: format!("OBJ_{}", resource.path[0]),
                subject: format!("$O.{}.C.{nuid}", resource.path[0]),
                version,
                object_size: Some((number(&info, "chunks")?, number(&info, "size")?)),
            })
        }
        _ => Ok(MessageScope {
            stream: resource.path[0].clone(),
            subject: ">".into(),
            version: String::new(),
            object_size: None,
        }),
    }
}

pub(crate) async fn page(
    api: &Api<'_>,
    resource: &Resource,
    cursor: &mut Cursor,
    continued: bool,
) -> Result<Option<Page>> {
    if !matches!(
        resource.id,
        "nats.consumers"
            | "nats.consumer_info"
            | "nats.kv_keys"
            | "nats.kv_value"
            | "nats.objects"
            | "nats.object"
            | "nats.object_info"
    ) {
        return Ok(None);
    }
    if matches!(
        resource.id,
        "nats.consumer_info" | "nats.kv_value" | "nats.object" | "nats.object_info"
    ) {
        ensure!(!continued, "NATS detail has one page");
    }
    let mut page = match resource.id {
        "nats.consumers" => {
            let stream = &resource.path[0];
            let info = api
                .call(
                    &format!("CONSUMER.LIST.{stream}"),
                    json!({"offset": cursor.next}),
                )
                .await?;
            let consumers = info["consumers"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let mut page = Page {
                columns: columns(&[
                    ("name", "text"),
                    ("pending", "integer"),
                    ("ack_pending", "integer"),
                    ("redelivered", "integer"),
                    ("delivered", "json"),
                    ("ack_floor", "json"),
                ]),
                ..Page::default()
            };
            for consumer in consumers.iter().take(PAGE_SIZE as usize) {
                let name = string(consumer, "name")?;
                let target = Resource::new("nats.consumer_info", vec![stream.clone(), name.into()]);
                crate::browse::validate(
                    &onetui_core::provider::PageRequest {
                        resource: target.clone(),
                        continuation: None,
                    },
                    false,
                )?;
                page.rows.push(Row {
                    cells: vec![
                        Some(name.into()),
                        Some(number(consumer, "num_pending")?.to_string().into()),
                        Some(number(consumer, "num_ack_pending")?.to_string().into()),
                        Some(number(consumer, "num_redelivered")?.to_string().into()),
                        Some(Value::Json(consumer["delivered"].to_string())),
                        Some(Value::Json(consumer["ack_floor"].to_string())),
                    ],
                    target: Some(target),
                });
            }
            cursor.next = cursor
                .next
                .checked_add(page.rows.len() as u64)
                .ok_or_else(|| anyhow!("NATS list offset overflow"))?;
            page.next = cursor.next < number(&info, "total")?;
            ensure!(
                !page.next || !page.rows.is_empty(),
                "NATS consumer listing made no progress"
            );
            page
        }
        "nats.consumer_info" => json_page(
            api.call(
                &format!("CONSUMER.INFO.{}.{}", resource.path[0], resource.path[1]),
                json!({}),
            )
            .await?,
        ),
        "nats.kv_keys" | "nats.objects" => {
            let bucket = &resource.path[0];
            let kv = resource.id == "nats.kv_keys";
            let stream = format!("{}_{bucket}", if kv { "KV" } else { "OBJ" });
            let prefix = if kv {
                format!("$KV.{bucket}.")
            } else {
                format!("$O.{bucket}.M.")
            };
            let info = api
                .call(
                    &format!("STREAM.INFO.{stream}"),
                    json!({"offset": cursor.next, "subjects_filter": format!("{prefix}>")}),
                )
                .await?;
            let created = string(&info, "created")?;
            ensure!(
                !continued || cursor.created == created,
                "NATS bucket was replaced; refresh"
            );
            cursor.created = created.into();
            let subjects = info["state"]["subjects"].as_object();
            let total = info["total"]
                .as_u64()
                .unwrap_or_else(|| subjects.map_or(0, |s| s.len() as u64));
            let mut page = Page {
                columns: columns(&[
                    (if kv { "key" } else { "name" }, "text"),
                    ("revisions", "integer"),
                ]),
                ..Page::default()
            };
            if let Some(subjects) = subjects {
                for (subject, count) in subjects.iter().take(PAGE_SIZE as usize) {
                    let encoded = subject
                        .strip_prefix(&prefix)
                        .ok_or_else(|| anyhow!("NATS returned an unexpected bucket subject"))?;
                    let name = if kv {
                        encoded.into()
                    } else {
                        String::from_utf8(
                            base64::engine::general_purpose::URL_SAFE.decode(encoded)?,
                        )?
                    };
                    let target = Resource::new(
                        if kv { "nats.kv_history" } else { "nats.object" },
                        vec![bucket.clone(), name],
                    );
                    crate::browse::validate(
                        &onetui_core::provider::PageRequest {
                            resource: target.clone(),
                            continuation: None,
                        },
                        false,
                    )?;
                    page.rows.push(Row {
                        cells: vec![
                            Some(target.path[1].clone().into()),
                            Some(
                                count
                                    .as_u64()
                                    .ok_or_else(|| anyhow!("NATS subject count is invalid"))?
                                    .to_string()
                                    .into(),
                            ),
                        ],
                        target: Some(target),
                    });
                }
            }
            cursor.next = cursor
                .next
                .checked_add(page.rows.len() as u64)
                .ok_or_else(|| anyhow!("NATS list offset overflow"))?;
            page.next = cursor.next < total;
            ensure!(
                !page.next || !page.rows.is_empty(),
                "NATS bucket listing made no progress"
            );
            page
        }
        "nats.kv_value" => {
            let message = api.message(&format!("KV_{}", resource.path[0]), json!({"last_by_subj": format!("$KV.{}.{}", resource.path[0], resource.path[1])})).await?
                .ok_or_else(|| anyhow!("NATS key is no longer retained"))?;
            Page {
                columns: columns(&[
                    ("sequence", "integer"),
                    ("subject", "text"),
                    ("time", "RFC3339"),
                    ("data", "bytes"),
                    ("headers", "bytes (KV-Operation / deletion markers)"),
                ]),
                rows: vec![message_row(&message)?],
                ..Page::default()
            }
        }
        "nats.object" => Page {
            columns: columns(&[("view", "text")]),
            rows: [
                ("Metadata", "nats.object_info"),
                ("Contents", "nats.object_chunks"),
            ]
            .into_iter()
            .map(|(label, target)| Row {
                cells: vec![Some(label.into())],
                target: Some(Resource::new(target, resource.path.clone())),
            })
            .collect(),
            ..Page::default()
        },
        "nats.object_info" => json_page(object_info(api, resource).await?),
        _ => unreachable!(),
    };
    page.notice = "Metadata; independent reads, no snapshot".into();
    ensure!(
        page.bytes() <= (PAGE_BYTES - 8192) / 2,
        "NATS metadata exceeds display budget"
    );
    Ok(Some(page))
}
