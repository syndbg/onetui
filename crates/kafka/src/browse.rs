use anyhow::{Result, anyhow, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::PageRequest;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use rdkafka::Message;
use rdkafka::message::Headers;
use rdkafka::metadata::Metadata;
use serde::{Deserialize, Serialize};

pub static RESOURCES: &[&ResourceDescriptor] = &[
    &ResourceDescriptor {
        id: "kafka.topics",
        description: "Topic metadata; Enter opens partitions without joining a consumer group",
        columns: &["name", "partitions"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.partitions",
        description: "Partition metadata; Enter reads committed records from the earliest available offset",
        columns: &["partition", "leader", "replicas", "in_sync_replicas"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.records",
        description: "Read-committed partition records; explicit offsets, no commits or group subscription",
        columns: &["offset", "timestamp_ms", "key", "value", "headers"],
        paging: true,
        actions: &[],
    },
];

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Position {
    pub executor: u64,
    pub resource: String,
    pub path: Vec<String>,
    pub offset: i64,
    pub end: Option<i64>,
}

pub(crate) fn validate(request: &PageRequest, identity: u64) -> Result<Option<Position>> {
    let valid = match (request.resource.id, request.resource.path.as_slice()) {
        ("kafka.topics", []) => true,
        ("kafka.partitions", [topic]) => valid_topic(topic),
        ("kafka.records", [topic, partition]) => {
            valid_topic(topic)
                && partition.len() <= 10
                && partition.parse::<i32>().is_ok_and(|n| n >= 0)
        }
        _ => false,
    };
    ensure!(valid, "Unsupported Kafka resource or path");
    let Some(token) = &request.continuation else {
        return Ok(None);
    };
    ensure!(token.len() <= 4096, "Invalid Kafka continuation");
    let position: Position =
        serde_json::from_str(token).map_err(|_| anyhow!("Invalid Kafka continuation"))?;
    ensure!(
        position.executor == identity
            && position.resource == request.resource.id
            && position.path == request.resource.path,
        "Kafka continuation belongs to another session or resource; refresh"
    );
    ensure!(position.offset >= 0, "Invalid Kafka continuation offset");
    ensure!(
        if request.resource.id == "kafka.records" {
            position.end.is_some_and(|end| end >= position.offset)
        } else {
            position.end.is_none()
        },
        "Invalid Kafka continuation window"
    );
    Ok(Some(position))
}

fn valid_topic(topic: &str) -> bool {
    !matches!(topic, "" | "." | "..")
        && topic.len() <= 249
        && topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub(crate) fn page(resource: &Resource, notice: &str) -> Page {
    let descriptor = RESOURCES
        .iter()
        .find(|d| d.id == resource.id)
        .expect("validated resource");
    Page {
        columns: descriptor
            .columns
            .iter()
            .map(|name| Column {
                name: (*name).into(),
                datatype: match *name {
                    "key" | "value" => "bytes",
                    "headers" => "JSON (ordered header names and nullable byte arrays)",
                    "offset" | "timestamp_ms" | "partition" | "leader" | "partitions" => "integer",
                    _ => "text",
                }
                .into(),
            })
            .collect(),
        notice: notice.into(),
        ..Page::default()
    }
}

pub(crate) fn finish(
    mut page: Page,
    resource: &Resource,
    identity: u64,
    next: Option<(i64, Option<i64>)>,
) -> Result<Page> {
    if let Some((offset, end)) = next {
        page.continuation = Some(serde_json::to_string(&Position {
            executor: identity,
            resource: resource.id.into(),
            path: resource.path.clone(),
            offset,
            end,
        })?);
        page.next = true;
    }
    ensure!(
        page.rows.len() <= PAGE_SIZE as usize && page.bytes() <= PAGE_BYTES,
        "Kafka page exceeds 100 items or 1 MiB; current page and bookmark retained"
    );
    Ok(page)
}

pub(crate) fn metadata(
    resource: &Resource,
    metadata: &Metadata,
    offset: i64,
    identity: u64,
) -> Result<Page> {
    let mut page = page(
        resource,
        "Metadata re-read per page; no snapshot. No group subscription or offset commits.",
    );
    let offset = usize::try_from(offset)?;
    let total;
    if resource.id == "kafka.topics" {
        let mut topics: Vec<_> = metadata.topics().iter().collect();
        topics.sort_by_key(|t| t.name());
        total = topics.len();
        for topic in topics.into_iter().skip(offset).take(PAGE_SIZE as usize) {
            ensure!(
                topic.error().is_none(),
                "Kafka topic metadata: {:?}",
                topic.error()
            );
            ensure!(
                valid_topic(topic.name()),
                "Kafka returned an invalid topic name"
            );
            page.rows.push(Row {
                cells: vec![
                    Some(topic.name().into()),
                    Some(topic.partitions().len().to_string().into()),
                ],
                target: Some(Resource::new("kafka.partitions", vec![topic.name().into()])),
            });
        }
    } else {
        let topic = metadata
            .topics()
            .iter()
            .find(|t| t.name() == resource.path[0])
            .ok_or_else(|| anyhow!("Kafka topic missing from metadata; refresh its parent"))?;
        ensure!(
            topic.error().is_none(),
            "Kafka topic metadata: {:?}",
            topic.error()
        );
        let mut partitions: Vec<_> = topic.partitions().iter().collect();
        partitions.sort_by_key(|p| p.id());
        total = partitions.len();
        for partition in partitions.into_iter().skip(offset).take(PAGE_SIZE as usize) {
            ensure!(
                partition.error().is_none(),
                "Kafka partition metadata: {:?}",
                partition.error()
            );
            page.rows.push(Row {
                cells: [
                    partition.id().to_string(),
                    partition.leader().to_string(),
                    format!("{:?}", partition.replicas()),
                    format!("{:?}", partition.isr()),
                ]
                .into_iter()
                .map(|s| Some(s.into()))
                .collect(),
                target: Some(Resource::new(
                    "kafka.records",
                    vec![resource.path[0].clone(), partition.id().to_string()],
                )),
            });
        }
    }
    ensure!(
        offset <= total,
        "Kafka metadata changed; refresh its parent"
    );
    let next = offset + page.rows.len();
    finish(
        page,
        resource,
        identity,
        (next < total).then_some((next as i64, None)),
    )
}

pub(crate) fn record(message: &impl Message) -> Result<Row> {
    let header_bytes = message.headers().map_or(0, |headers| {
        headers
            .iter()
            .map(|header| header.key.len() + header.value.map_or(0, <[u8]>::len))
            .sum::<usize>()
    });
    ensure!(
        message
            .key()
            .map_or(0, <[u8]>::len)
            .saturating_add(message.payload().map_or(0, <[u8]>::len))
            .saturating_add(header_bytes)
            <= PAGE_BYTES,
        "Kafka record exceeds 1 MiB; current page and bookmark retained"
    );
    let headers: Vec<_> = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| serde_json::json!({"name": header.key, "value": header.value}))
                .collect()
        })
        .unwrap_or_default();
    let row = Row {
        cells: vec![
            Some(message.offset().to_string().into()),
            message
                .timestamp()
                .to_millis()
                .map(|n| n.to_string().into()),
            message.key().map(|bytes| Value::Bytes(bytes.to_vec())),
            message.payload().map(|bytes| Value::Bytes(bytes.to_vec())),
            Some(Value::Json(serde_json::to_string(&headers)?)),
        ],
        target: None,
    };
    ensure!(
        row.cells
            .iter()
            .flatten()
            .map(|v| v.bytes().len())
            .sum::<usize>()
            <= PAGE_BYTES,
        "Kafka displayed record exceeds 1 MiB; current page and bookmark retained"
    );
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::{Header, OwnedHeaders, OwnedMessage, Timestamp};

    #[test]
    fn bytes_tombstones_and_duplicate_headers_survive_conversion() {
        let message = OwnedMessage::new(
            None,
            Some(vec![0, 255]),
            "test".into(),
            Timestamp::NotAvailable,
            0,
            i64::MAX - 1,
            Some(
                OwnedHeaders::new()
                    .insert(Header {
                        key: "same",
                        value: Some(&[255u8, 0][..]),
                    })
                    .insert(Header::<&[u8]> {
                        key: "same",
                        value: None,
                    }),
            ),
        );
        let row = record(&message).unwrap();
        assert_eq!(
            row.cells[0].as_ref().unwrap().text(),
            Some((i64::MAX - 1).to_string().as_str())
        );
        assert_eq!(row.cells[2], Some(Value::Bytes(vec![0, 255])));
        assert_eq!(row.cells[3], None);
        let headers: serde_json::Value =
            serde_json::from_str(row.cells[4].as_ref().unwrap().text().unwrap()).unwrap();
        assert_eq!(headers[0]["name"], headers[1]["name"]);
        assert_eq!(headers[0]["value"], serde_json::json!([255, 0]));
        assert!(headers[1]["value"].is_null());
        let empty = OwnedMessage::new(
            Some(vec![]),
            None,
            "test".into(),
            Timestamp::CreateTime(0),
            0,
            0,
            None,
        );
        assert_eq!(record(&empty).unwrap().cells[3], Some(Value::Bytes(vec![])));
    }

    #[test]
    fn bookmarks_are_scoped_and_sizes_are_enforced() {
        let resource = Resource::new("kafka.records", vec!["test".into(), "0".into()]);
        let page = finish(page(&resource, ""), &resource, 7, Some((100, Some(250)))).unwrap();
        let request = PageRequest {
            resource: resource.clone(),
            continuation: page.continuation,
        };
        assert_eq!(validate(&request, 7).unwrap().unwrap().offset, 100);
        assert!(validate(&request, 8).is_err());
        let huge = OwnedMessage::new(
            Some(vec![0; PAGE_BYTES + 1]),
            None,
            "test".into(),
            Timestamp::NotAvailable,
            0,
            0,
            None,
        );
        assert!(record(&huge).is_err());
        assert!(
            validate(
                &PageRequest {
                    resource: Resource::new("kafka.records", vec!["test".into(), "-1".into()]),
                    continuation: None
                },
                7
            )
            .is_err()
        );
    }
}
