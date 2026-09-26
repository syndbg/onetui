use anyhow::{Result, anyhow, bail, ensure};
use aws_sdk_dynamodbstreams::{Client, error::DisplayErrorContext, types::ShardIteratorType};
use onetui_core::{Column, PAGE_BYTES, Page, Resource, Row, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use crate::response::Capture;

macro_rules! send {
    ($request:expr) => {{
        let capture = Capture::default();
        let result = $request
            .customize()
            .interceptor(capture.clone())
            .send()
            .await
            .map(|_| ())
            .map_err(|e| anyhow!("{}", DisplayErrorContext(e)));
        capture.finish(result)
    }};
}

pub(crate) fn is_resource(id: &str) -> bool {
    matches!(
        id,
        "dynamodb.streams"
            | "dynamodb.table_streams"
            | "dynamodb.stream_info"
            | "dynamodb.shards"
            | "dynamodb.shard_details"
            | "dynamodb.records"
    )
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "position", deny_unknown_fields)]
enum Cursor {
    At {
        sequence: String,
        iterator: Option<String>,
    },
    After {
        sequence: String,
        iterator: Option<String>,
    },
    Unanchored {
        iterator: String,
    },
    Closed,
}

pub(crate) fn closed(token: &Json) -> bool {
    token["position"] == "Closed"
}

fn bounded_string(value: &Json, field: &str, max: usize) -> Result<String> {
    let text = value
        .as_str()
        .ok_or_else(|| anyhow!("DynamoDB Streams missing {field}"))?;
    ensure!(
        !text.is_empty() && text.len() <= max && !text.chars().any(char::is_control),
        "Invalid DynamoDB Streams {field}"
    );
    Ok(text.into())
}

fn sequence(value: &Json) -> Result<String> {
    let text = bounded_string(value, "sequence", 40)?;
    ensure!(
        text.bytes().all(|c| c.is_ascii_digit()),
        "Invalid DynamoDB Streams sequence"
    );
    Ok(text)
}

fn column(name: &str, datatype: &str) -> Column {
    Column {
        name: name.into(),
        datatype: datatype.into(),
    }
}

pub(crate) async fn read(
    client: &Client,
    resource: &Resource,
    position: Option<&Json>,
    follow: bool,
) -> Result<(Page, Option<Json>)> {
    if resource.id == "dynamodb.records" {
        return records(client, resource, position, follow, None, 100).await;
    }
    let token = position
        .map(|v| bounded_string(v, "metadata bookmark", 2048))
        .transpose()?;
    let mut page = Page {
        columns: vec![column("description", "JSON")],
        notice: "Independent Streams metadata reads; refresh to discover new shards".into(),
        ..Page::default()
    };
    let shards = matches!(resource.id, "dynamodb.shards" | "dynamodb.shard_details");
    if shards {
        page.columns = ["shard_id", "parent", "start_sequence", "end_sequence"]
            .map(|name| column(name, "text"))
            .into();
        if resource.id == "dynamodb.shard_details" {
            page.columns.push(column("description", "JSON"));
        }
    }
    let (body, array, key, target, parent) =
        if matches!(resource.id, "dynamodb.streams" | "dynamodb.table_streams") {
            let body = send!(
                client
                    .list_streams()
                    .limit(100)
                    .set_table_name(resource.path.first().cloned())
                    .set_exclusive_start_stream_arn(token)
            )?;
            (body, "Streams", "StreamArn", "dynamodb.stream", vec![])
        } else {
            let body = send!(
                client
                    .describe_stream()
                    .stream_arn(&resource.path[0])
                    .limit(100)
                    .set_exclusive_start_shard_id(token)
            )?;
            let body = body
                .get("StreamDescription")
                .cloned()
                .ok_or_else(|| anyhow!("DynamoDB Streams missing StreamDescription"))?;
            if resource.id == "dynamodb.stream_info" {
                let next = body.get("LastEvaluatedShardId").cloned();
                page.rows.push(Row {
                    cells: vec![Some(Value::Json(body.to_string()))],
                    target: None,
                });
                return Ok((page, next));
            }
            (
                body,
                "Shards",
                "ShardId",
                "dynamodb.records",
                resource.path.clone(),
            )
        };
    if let Some(values) = body.get(array) {
        let values = values
            .as_array()
            .ok_or_else(|| anyhow!("DynamoDB Streams {array} must be an array"))?;
        ensure!(
            values.len() <= 100,
            "DynamoDB Streams metadata exceeds 100 rows"
        );
        for value in values {
            let name = bounded_string(&value[key], key, 2048)?;
            let mut path = parent.clone();
            path.push(name.clone());
            let mut cells = if shards {
                vec![
                    Some(name.into()),
                    value["ParentShardId"].as_str().map(Value::from),
                    value["SequenceNumberRange"]["StartingSequenceNumber"]
                        .as_str()
                        .map(Value::from),
                    value["SequenceNumberRange"]["EndingSequenceNumber"]
                        .as_str()
                        .map(Value::from),
                ]
            } else {
                vec![Some(Value::Json(value.to_string()))]
            };
            let target = if resource.id == "dynamodb.shard_details" {
                cells.push(Some(Value::Json(value.to_string())));
                None
            } else {
                Some(Resource::new(target, path))
            };
            page.rows.push(Row { cells, target });
        }
    }
    let next = body
        .get(if array == "Streams" {
            "LastEvaluatedStreamArn"
        } else {
            "LastEvaluatedShardId"
        })
        .cloned();
    Ok((page, next))
}

async fn iterator(
    client: &Client,
    resource: &Resource,
    kind: ShardIteratorType,
    sequence: Option<&str>,
) -> Result<String> {
    let body = send!(
        client
            .get_shard_iterator()
            .stream_arn(&resource.path[0])
            .shard_id(&resource.path[1])
            .shard_iterator_type(kind)
            .set_sequence_number(sequence.map(str::to_owned))
    )?;
    bounded_string(&body["ShardIterator"], "shard iterator", 2048)
}

async fn batch(
    client: &Client,
    iterator: &str,
    limit: i32,
) -> (Result<Json>, bool, Option<String>) {
    let capture = Capture::default();
    let result = client
        .get_records()
        .shard_iterator(iterator)
        .limit(limit)
        .customize()
        .interceptor(capture.clone())
        .send()
        .await;
    let expired = result
        .as_ref()
        .err()
        .and_then(|e| e.as_service_error())
        .is_some_and(|e| e.is_expired_iterator_exception());
    let model_error = result
        .as_ref()
        .err()
        .filter(|e| e.raw_response().is_some_and(|r| r.status().is_success()))
        .and_then(|e| e.as_service_error())
        .map(|e| {
            std::error::Error::source(e).map_or_else(|| e.to_string(), |source| source.to_string())
        });
    (
        capture.finish_json(
            result
                .map(|_| ())
                .map_err(|e| anyhow!("{}", DisplayErrorContext(e))),
            model_error.is_some(),
        ),
        expired,
        model_error,
    )
}

async fn records(
    client: &Client,
    resource: &Resource,
    position: Option<&Json>,
    follow: bool,
    replay: Option<(&str, bool)>,
    limit: i32,
) -> Result<(Page, Option<Json>)> {
    let cursor: Option<Cursor> = position.cloned().map(serde_json::from_value).transpose()?;
    let cursor = cursor.or_else(|| {
        replay.map(|(sequence, after)| {
            if after {
                Cursor::After {
                    sequence: sequence.into(),
                    iterator: None,
                }
            } else {
                Cursor::At {
                    sequence: sequence.into(),
                    iterator: None,
                }
            }
        })
    });
    let (mut after, mut at, existing) = match cursor {
        Some(Cursor::Closed) => {
            bail!("DynamoDB Streams shard is closed; select a child shard from the shard list")
        }
        Some(Cursor::After {
            sequence: s,
            iterator,
        }) => {
            sequence(&json!(s))?;
            (Some(s), None, iterator)
        }
        Some(Cursor::At {
            sequence: s,
            iterator,
        }) => {
            sequence(&json!(s))?;
            (None, Some(s), iterator)
        }
        Some(Cursor::Unanchored { iterator }) => (None, None, Some(iterator)),
        None => (None, None, None),
    };
    let kind = if after.is_some() {
        ShardIteratorType::AfterSequenceNumber
    } else if at.is_some() {
        ShardIteratorType::AtSequenceNumber
    } else if follow {
        ShardIteratorType::Latest
    } else {
        ShardIteratorType::TrimHorizon
    };
    let token = match existing {
        Some(value) => bounded_string(&json!(value), "shard iterator", 2048)?,
        None => {
            iterator(
                client,
                resource,
                kind.clone(),
                after.as_deref().or(at.as_deref()),
            )
            .await?
        }
    };
    let (mut result, expired, mut model_error) = batch(client, &token, limit).await;
    if expired && replay.is_none() && (after.is_some() || at.is_some()) {
        // Preserve inclusive replay until the first record is actually retained.
        let renewed = iterator(client, resource, kind, after.as_deref().or(at.as_deref())).await?;
        (result, _, model_error) = batch(client, &renewed, limit).await;
    }
    let body = result?;
    let values = body
        .get("Records")
        .map(|v| {
            v.as_array()
                .ok_or_else(|| anyhow!("DynamoDB Streams Records must be an array"))
        })
        .transpose()?
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    ensure!(
        values.len() <= limit as usize,
        "DynamoDB Streams response exceeds requested record limit"
    );
    let mut page = Page {
        columns: vec![column("sequence", "decimal string"), column("event", "text"), column("record", "JSON (original typed images)")],
        notice: "Independent shard reads; no offset commits. Records retain keys, old/new images and native attribute tags".into(),
        ..Page::default()
    };
    if let Some(error) = model_error {
        page.notice.push_str(&format!(
            " | SDK decode error: {error}; showing original JSON"
        ));
    }
    let mut cut = false;
    for value in values {
        let next_sequence = sequence(&value["dynamodb"]["SequenceNumber"])?;
        if let Some(previous) = &after {
            ensure!(
                decimal_order(&next_sequence, previous).is_gt(),
                "DynamoDB Streams returned a non-increasing sequence"
            );
        }
        if let Some(start) = &at {
            ensure!(
                !decimal_order(&next_sequence, start).is_lt(),
                "DynamoDB Streams returned a sequence before the requested start"
            );
        }
        page.rows.push(Row {
            cells: vec![
                Some(next_sequence.clone().into()),
                value
                    .get("eventName")
                    .and_then(Json::as_str)
                    .map(|s| s.to_owned().into()),
                Some(Value::Json(value.to_string())),
            ],
            target: None,
        });
        if page.bytes() > PAGE_BYTES {
            page.rows.pop();
            ensure!(
                !page.rows.is_empty(),
                "DynamoDB Streams record exceeds the 1 MiB display limit"
            );
            cut = true;
            break;
        }
        after = Some(next_sequence);
        at = None;
    }
    let next = body
        .get("NextShardIterator")
        .filter(|v| !v.is_null())
        .map(|v| bounded_string(v, "next shard iterator", 2048))
        .transpose()?;
    let cursor = if cut {
        Cursor::After {
            sequence: after.unwrap(),
            iterator: None,
        }
    } else if let Some(iterator) = next {
        match after {
            Some(sequence) => Cursor::After {
                sequence,
                iterator: Some(iterator),
            },
            None => match at {
                Some(sequence) => Cursor::At {
                    sequence,
                    iterator: Some(iterator),
                },
                None => Cursor::Unanchored { iterator },
            },
        }
    } else {
        page.notice.push_str(" | Shard closed");
        Cursor::Closed
    };
    Ok((page, Some(serde_json::to_value(cursor)?)))
}

fn decimal_order(left: &str, right: &str) -> std::cmp::Ordering {
    let left = left.trim_start_matches('0');
    let right = right.trim_start_matches('0');
    (left.len(), left).cmp(&(right.len(), right))
}

pub(crate) async fn replay(
    client: &Client,
    arn: &str,
    shard: &str,
    sequence: &str,
    after: bool,
    limit: i32,
    position: Option<&Json>,
) -> Result<(Page, Option<Json>)> {
    let resource = Resource::new("dynamodb.records", vec![arn.into(), shard.into()]);
    records(
        client,
        &resource,
        position,
        false,
        Some((sequence, after)),
        limit,
    )
    .await
}
