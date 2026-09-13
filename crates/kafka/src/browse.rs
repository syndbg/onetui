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
        id: "kafka.topic",
        description: "Choose all records, individual partitions or read-only topic configuration",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.group",
        description: "Choose members or committed offsets without joining the group",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.topic_config",
        description: "Read-only topic settings; sensitive values are withheld",
        columns: &[
            "name",
            "value",
            "source",
            "is_default",
            "is_read_only",
            "is_sensitive",
            "synonyms",
        ],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.broker_config",
        description: "Read-only broker settings; sensitive values are withheld",
        columns: &[
            "name",
            "value",
            "source",
            "is_default",
            "is_read_only",
            "is_sensitive",
            "synonyms",
        ],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.offsets",
        description: "Group committed offsets and read-committed offset distance; not message counts",
        columns: &[
            "topic",
            "partition",
            "committed",
            "low",
            "stable_end",
            "lag",
            "status",
        ],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.resources",
        description: "Choose topics, brokers or consumer groups; no broker mutation",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.brokers",
        description: "Broker IDs and advertised addresses from cluster metadata",
        columns: &["id", "host", "port"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.groups",
        description: "Consumer group state and protocol; Enter chooses members or offsets",
        columns: &["name", "state", "protocol_type", "protocol", "members"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.members",
        description: "Consumer group members with unmodified metadata and assignment bytes",
        columns: &["id", "client_id", "client_host", "metadata", "assignment"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "kafka.topics",
        description: "Topic metadata; Enter chooses partitions or configuration",
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
        description: "Read-committed topic or partition records; topic-wide rows include partition IDs; no commits or subscription",
        columns: &[
            "partition",
            "offset",
            "timestamp_ms",
            "key",
            "value",
            "headers",
        ],
        paging: true,
        actions: &[],
    },
    &crate::query::RESOURCE,
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

pub(crate) fn validate(
    request: &PageRequest,
    identity: u64,
    following: bool,
) -> Result<Option<Position>> {
    if request.resource.id == "kafka.records" && request.resource.path.len() == 1 {
        return crate::topic::validate(request, identity, following).map(|_| None);
    }
    ensure!(
        !following || request.resource.id == "kafka.records",
        "Live following requires a Kafka partition record view"
    );
    let valid = match (request.resource.id, request.resource.path.as_slice()) {
        ("kafka.resources" | "kafka.topics" | "kafka.brokers" | "kafka.groups", []) => true,
        ("kafka.group" | "kafka.members" | "kafka.offsets", [group]) => valid_group(group),
        ("kafka.topic" | "kafka.topic_config" | "kafka.partitions", [topic]) => valid_topic(topic),
        ("kafka.broker_config", [broker]) => {
            broker.len() <= 10 && broker.parse::<i32>().is_ok_and(|n| n >= 0)
        }
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
    let token = if following {
        token
            .strip_prefix("follow:")
            .ok_or_else(|| anyhow!("Invalid Kafka follow continuation"))?
    } else {
        token.as_str()
    };
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

pub(crate) fn valid_topic(topic: &str) -> bool {
    !matches!(topic, "" | "." | "..")
        && topic.len() <= 249
        && topic
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub(crate) fn valid_group(group: &str) -> bool {
    !group.is_empty() && group.len() <= 1024 && !group.chars().any(char::is_control)
}

pub(crate) fn resources(resource: &Resource, offset: i64, identity: u64) -> Result<Page> {
    let mut page = page(
        resource,
        "Choose a Kafka resource. All operations are read-only.",
    );
    let rows: &[(&str, &str)] = match resource.id {
        "kafka.topic" => &[
            ("kafka.partitions", "Partitions and record replay/following"),
            ("kafka.topic_config", "Read-only topic configuration"),
            (
                "kafka.records",
                "All partitions: bounded browsing and following",
            ),
        ],
        "kafka.group" => &[
            ("kafka.members", "Member metadata and assignment bytes"),
            ("kafka.offsets", "Committed offsets and read-committed lag"),
        ],
        _ => &[
            (
                "kafka.topics",
                "Topic partitions and record replay/following",
            ),
            ("kafka.brokers", "Broker IDs and advertised addresses"),
            ("kafka.groups", "Consumer group state, members and offsets"),
        ],
    };
    ensure!(offset <= rows.len() as i64, "Invalid Kafka resource offset");
    for &(id, description) in rows.iter().skip(usize::try_from(offset)?) {
        page.rows.push(Row {
            cells: vec![Some(id.into()), Some(description.into())],
            target: Some(Resource::new(id, resource.path.clone())),
        });
    }
    finish(page, resource, identity, None, false)
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
            .filter(|name| {
                !(resource.id == "kafka.records"
                    && resource.path.len() == 2
                    && **name == "partition")
            })
            .map(|name| Column {
                name: (*name).into(),
                datatype: match *name {
                    "value" if resource.id.ends_with("_config") => "text",
                    "is_default" | "is_read_only" | "is_sensitive" => "boolean",
                    "key" | "value" | "metadata" | "assignment" => "bytes",
                    "headers" => "JSON (ordered header names and nullable byte arrays)",
                    "synonyms" => "JSON (configuration precedence order)",
                    "offset" | "timestamp_ms" | "partition" | "leader" | "partitions" | "port"
                    | "members" | "committed" | "low" | "stable_end" | "lag" => "integer",
                    "id" if resource.id == "kafka.brokers" => "integer",
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
    following: bool,
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
        if following {
            page.continuation = page.continuation.map(|token| format!("follow:{token}"));
        }
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
    if resource.id == "kafka.brokers" {
        let mut brokers: Vec<_> = metadata.brokers().iter().collect();
        brokers.sort_by_key(|broker| broker.id());
        total = brokers.len();
        for broker in brokers.into_iter().skip(offset).take(PAGE_SIZE as usize) {
            page.rows.push(Row {
                cells: vec![
                    Some(broker.id().to_string().into()),
                    Some(broker.host().into()),
                    Some(broker.port().to_string().into()),
                ],
                target: Some(Resource::new(
                    "kafka.broker_config",
                    vec![broker.id().to_string()],
                )),
            });
        }
    } else if resource.id == "kafka.topics" {
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
                target: Some(Resource::new("kafka.topic", vec![topic.name().into()])),
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
        false,
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
    fn resource_menu_and_group_paths_are_bounded() {
        let resource = Resource::new("kafka.resources", vec![]);
        let menu = resources(&resource, 0, 1).unwrap();
        assert_eq!(menu.rows.len(), 3);
        assert!(!menu.next);
        assert!(resources(&resource, 4, 1).is_err());
        for row in menu.rows {
            let target = row.target.unwrap();
            assert!(RESOURCES.iter().any(|d| d.id == target.id));
            assert!(
                validate(
                    &PageRequest {
                        resource: target,
                        continuation: None
                    },
                    1,
                    false
                )
                .is_ok()
            );
        }
        for group in ["", "bad\nname", &"g".repeat(1025)] {
            assert!(
                validate(
                    &PageRequest {
                        resource: Resource::new("kafka.members", vec![group.into()]),
                        continuation: None
                    },
                    1,
                    false
                )
                .is_err()
            );
        }
        assert!(
            validate(
                &PageRequest {
                    resource: Resource::new("kafka.members", vec!["София".into()]),
                    continuation: None
                },
                1,
                false
            )
            .is_ok()
        );
    }

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
        let page = finish(
            page(&resource, ""),
            &resource,
            7,
            Some((100, Some(250))),
            false,
        )
        .unwrap();
        let request = PageRequest {
            resource: resource.clone(),
            continuation: page.continuation,
        };
        assert_eq!(validate(&request, 7, false).unwrap().unwrap().offset, 100);
        assert!(validate(&request, 8, false).is_err());
        assert!(validate(&request, 7, true).is_err());
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
                7,
                false
            )
            .is_err()
        );
    }
}
