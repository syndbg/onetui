use anyhow::{Result, anyhow, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

const fn descriptor(
    id: &'static str,
    description: &'static str,
    columns: &'static [&'static str],
) -> ResourceDescriptor {
    ResourceDescriptor {
        id,
        description,
        columns,
        paging: true,
        actions: &[],
    }
}

pub(crate) const RESOURCES: &[&ResourceDescriptor] = &[
    &descriptor(
        "rabbitmq.resources",
        "RabbitMQ management resources",
        &["resource", "description"],
    ),
    &descriptor(
        "rabbitmq.overview",
        "Overview",
        &[
            "cluster_name",
            "rabbitmq_version",
            "object_totals",
            "queue_totals",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.nodes",
        "Nodes",
        &[
            "name",
            "running",
            "type",
            "mem_used",
            "mem_limit",
            "disk_free",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.vhosts",
        "Virtual hosts",
        &["name", "description", "messages", "details"],
    ),
    &descriptor(
        "rabbitmq.vhost",
        "Virtual host resources",
        &["resource", "description"],
    ),
    &descriptor(
        "rabbitmq.queues",
        "Queues",
        &[
            "name",
            "vhost",
            "type",
            "state",
            "messages_ready",
            "messages_unacknowledged",
            "consumers",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.exchanges",
        "Exchanges",
        &["name", "vhost", "type", "durable", "internal", "details"],
    ),
    &descriptor(
        "rabbitmq.bindings",
        "Bindings",
        &[
            "source",
            "vhost",
            "destination",
            "destination_type",
            "routing_key",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.connections",
        "Connections",
        &[
            "name", "vhost", "user", "state", "channels", "recv_oct", "send_oct", "details",
        ],
    ),
    &descriptor(
        "rabbitmq.channels",
        "Channels",
        &[
            "name",
            "vhost",
            "user",
            "consumer_count",
            "messages_unacknowledged",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.consumers",
        "Consumers",
        &[
            "consumer_tag",
            "queue",
            "ack_required",
            "prefetch_count",
            "active",
            "details",
        ],
    ),
    &descriptor(
        "rabbitmq.policies",
        "Policies",
        &[
            "name",
            "vhost",
            "pattern",
            "apply-to",
            "priority",
            "definition",
            "details",
        ],
    ),
];

pub(crate) fn descriptor_for(id: &str) -> Result<&'static ResourceDescriptor> {
    RESOURCES
        .iter()
        .copied()
        .find(|r| r.id == id)
        .ok_or_else(|| anyhow!("Unknown RabbitMQ resource"))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    owner: u64,
    resource: String,
    path: Vec<String>,
    page: u32,
}

pub(crate) fn position(resource: &Resource, continuation: Option<&str>, owner: u64) -> Result<u32> {
    descriptor_for(resource.id)?;
    let depth = resource.path.len();
    let valid = match resource.id {
        "rabbitmq.resources" | "rabbitmq.overview" | "rabbitmq.nodes" | "rabbitmq.vhosts" => {
            depth == 0
        }
        "rabbitmq.vhost" => depth == 1,
        _ => depth <= 1,
    };
    ensure!(
        valid
            && resource
                .path
                .iter()
                .all(|p| !p.is_empty() && p.len() <= 4096 && !matches!(p.as_str(), "." | "..")),
        "Invalid RabbitMQ resource path"
    );
    let Some(raw) = continuation else {
        return Ok(1);
    };
    ensure!(
        !matches!(
            resource.id,
            "rabbitmq.resources" | "rabbitmq.vhost" | "rabbitmq.overview"
        ),
        "RabbitMQ resource has no continuation"
    );
    ensure!(raw.len() <= 16 * 1024, "RabbitMQ continuation is too large");
    let position: Position = serde_json::from_str(raw)?;
    ensure!(
        position.owner == owner
            && position.resource == resource.id
            && position.path == resource.path
            && position.page >= 2,
        "RabbitMQ continuation belongs to another session or resource"
    );
    Ok(position.page)
}

pub(crate) fn menu(resource: &Resource) -> Option<Page> {
    let start = match resource.id {
        "rabbitmq.resources" => 1,
        "rabbitmq.vhost" => 5,
        _ => return None,
    };
    Some(Page {
        columns: columns(descriptor_for(resource.id).expect("menu descriptor")),
        rows: RESOURCES
            .iter()
            .skip(start)
            .filter(|r| r.id != "rabbitmq.vhost")
            .map(|r| Row {
                cells: vec![Some(r.description.into()), Some(r.id.into())],
                target: Some(Resource::new(r.id, resource.path.clone())),
            })
            .collect(),
        ..Page::default()
    })
}

pub(crate) fn request_url(base: &str, resource: &Resource, page: u32) -> Result<url::Url> {
    let mut url = crate::config::endpoint(base)?;
    let kind = resource
        .id
        .strip_prefix("rabbitmq.")
        .ok_or_else(|| anyhow!("Invalid RabbitMQ resource"))?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("Invalid RabbitMQ URL"))?;
        path.clear().push("api");
        match (kind, resource.path.first()) {
            ("connections" | "channels", Some(vhost)) => {
                path.push("vhosts").push(vhost).push(kind);
            }
            ("queues", None) => {
                path.push("queues").push("detailed");
            }
            (_, vhost) => {
                path.push(kind);
                if let Some(vhost) = vhost {
                    path.push(vhost);
                }
            }
        }
    }
    // Vhost pagination crashes RabbitMQ 4.3.5. Nodes and policies also use bounded arrays.
    if matches!(
        kind,
        "queues" | "exchanges" | "bindings" | "connections" | "channels" | "consumers"
    ) {
        url.query_pairs_mut()
            .append_pair("pagination", "true")
            .append_pair("page", &page.to_string())
            .append_pair("page_size", &PAGE_SIZE.to_string());
    }
    Ok(url)
}

fn columns(descriptor: &ResourceDescriptor) -> Vec<Column> {
    descriptor
        .columns
        .iter()
        .map(|name| Column {
            name: (*name).into(),
            datatype: "JSON or text".into(),
        })
        .collect()
}

pub(crate) fn page(resource: &Resource, value: Json, number: u32, owner: u64) -> Result<Page> {
    let descriptor = descriptor_for(resource.id)?;
    let (values, next) = if resource.id == "rabbitmq.overview" {
        ensure!(value.is_object(), "RabbitMQ overview is not an object");
        (vec![value], false)
    } else if let Some(values) = value.as_array() {
        let offset = (number as usize - 1)
            .checked_mul(PAGE_SIZE as usize)
            .ok_or_else(|| anyhow!("RabbitMQ page offset overflow"))?;
        (
            values
                .iter()
                .skip(offset)
                .take(PAGE_SIZE as usize)
                .cloned()
                .collect(),
            values.len().saturating_sub(offset) > PAGE_SIZE as usize,
        )
    } else {
        let values = value["items"]
            .as_array()
            .ok_or_else(|| anyhow!("RabbitMQ list has no items array"))?;
        let total = value["page_count"]
            .as_u64()
            .ok_or_else(|| anyhow!("RabbitMQ list has no page_count"))?;
        ensure!(
            value["page"].as_u64() == Some(number as u64) && values.len() <= PAGE_SIZE as usize,
            "Invalid RabbitMQ page envelope"
        );
        ensure!(
            total >= number as u64 || (total == 0 && number == 1 && values.is_empty()),
            "RabbitMQ page no longer exists; refresh the list"
        );
        (values.clone(), (number as u64) < total)
    };
    ensure!(
        values.iter().all(Json::is_object),
        "RabbitMQ list contains a non-object item"
    );
    let rows = values
        .into_iter()
        .map(|value| {
            let target = if resource.id == "rabbitmq.vhosts" {
                let name = value["name"]
                    .as_str()
                    .ok_or_else(|| anyhow!("RabbitMQ virtual host has no name"))?;
                Some(Resource::new("rabbitmq.vhost", vec![name.into()]))
            } else {
                None
            };
            let cells = descriptor
                .columns
                .iter()
                .map(|name| {
                    let cell = if *name == "details" {
                        &value
                    } else {
                        &value[*name]
                    };
                    match cell {
                        Json::Null => None,
                        Json::String(text) => Some(Value::Text(text.clone())),
                        _ => Some(Value::Json(cell.to_string())),
                    }
                })
                .collect();
            Ok(Row { cells, target })
        })
        .collect::<Result<Vec<_>>>()?;
    let continuation = if next {
        Some(serde_json::to_string(&Position {
            owner,
            resource: resource.id.into(),
            path: resource.path.clone(),
            page: number
                .checked_add(1)
                .ok_or_else(|| anyhow!("RabbitMQ page overflow"))?,
        })?)
    } else {
        None
    };
    let page = Page {
        rows, columns: columns(descriptor), next, continuation,
        notice: "Management metadata and metrics only. Independent reads, not a snapshot. Enter shows all returned fields in details.".into(),
    };
    ensure!(page.bytes() <= PAGE_BYTES, "RabbitMQ page exceeds 1 MiB");
    Ok(page)
}
