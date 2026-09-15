use anyhow::{Result, anyhow, bail, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};
use serde_json::{Value as Json, json};

pub(crate) const ROOT: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.resources",
    description: "Collections and read-only cluster topology",
    columns: &["resource", "description"],
    paging: true,
    actions: &[],
};
pub(crate) const CLUSTER: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.cluster",
    description: "Cluster status and local consensus state; requires rest_url",
    columns: &[
        "status",
        "peer_id",
        "leader",
        "role",
        "term",
        "commit",
        "pending_operations",
        "details",
    ],
    paging: true,
    actions: &[],
};
pub(crate) const PEERS: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.peers",
    description: "Peers reported by the configured REST node; addresses are never contacted",
    columns: &["peer_id", "uri", "is_self", "is_leader", "details"],
    paging: true,
    actions: &[],
};
pub(crate) const SHARDS: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.shards",
    description: "Local and remote shard replicas reported by the REST node",
    columns: &[
        "shard_id",
        "peer_id",
        "location",
        "state",
        "points_count",
        "shard_key",
        "details",
    ],
    paging: true,
    actions: &[],
};
pub(crate) const TRANSFERS: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.transfers",
    description: "Active shard transfers; observation only",
    columns: &[
        "shard_id",
        "from",
        "to",
        "sync",
        "method",
        "to_shard_id",
        "comment",
        "details",
    ],
    paging: true,
    actions: &[],
};
pub(crate) const COLLECTION_CLUSTER: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.collection_cluster",
    description: "Complete collection topology, including resharding operations",
    columns: &["details"],
    paging: true,
    actions: &[],
};

pub(crate) fn is_resource(id: &str) -> bool {
    matches!(
        id,
        "qdrant.cluster"
            | "qdrant.peers"
            | "qdrant.shards"
            | "qdrant.transfers"
            | "qdrant.collection_cluster"
    )
}

pub(crate) async fn read(
    client: &reqwest::Client,
    base: &str,
    key: Option<&str>,
    resource: &Resource,
) -> Result<Json> {
    let mut url = crate::qdrant_url(base)?;
    {
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow!("Invalid Qdrant REST URL"))?;
        path.clear();
        if let Some(collection) = resource.path.first() {
            path.push("collections").push(collection);
        }
        path.push("cluster");
    }
    let mut request = client.get(url);
    if let Some(key) = key {
        let mut header = reqwest::header::HeaderValue::from_str(key)?;
        header.set_sensitive(true);
        request = request.header("api-key", header);
    }
    let mut response = request.send().await?;
    let status = response.status();
    ensure!(
        response
            .content_length()
            .is_none_or(|n| n <= PAGE_BYTES as u64),
        "Qdrant HTTP {status}: response exceeds the 1 MiB limit"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            chunk.len() <= PAGE_BYTES - body.len(),
            "Qdrant HTTP {status}: response exceeds the 1 MiB limit"
        );
        body.extend_from_slice(&chunk);
    }
    if !status.is_success() {
        bail!("Qdrant HTTP {status}: {}", String::from_utf8_lossy(&body));
    }
    let envelope: Json = serde_json::from_slice(&body)?;
    ensure!(
        envelope["status"] == "ok",
        "Qdrant HTTP {status}: {}",
        String::from_utf8_lossy(&body)
    );
    ensure!(
        envelope["result"].is_object(),
        "Qdrant topology response has no result object"
    );
    Ok(envelope["result"].clone())
}

fn cell(value: &Json) -> Option<Value> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s.clone().into()),
        _ => Some(Value::Json(value.to_string())),
    }
}

fn row(values: &[&Json]) -> Row {
    Row {
        cells: values.iter().map(|v| cell(v)).collect(),
        target: None,
    }
}

fn array<'a>(value: &'a Json, key: &str) -> Result<&'a Vec<Json>> {
    let array = value[key]
        .as_array()
        .ok_or_else(|| anyhow!("Qdrant topology response is missing {key}"))?;
    ensure!(
        array.iter().all(Json::is_object),
        "Qdrant topology {key} contains a non-object entry"
    );
    Ok(array)
}

pub(crate) fn page(resource: &Resource, value: Json, offset: usize, executor: u64) -> Result<Page> {
    let mut notice =
        "Topology observed by the REST node; re-read per page, not a snapshot".to_owned();
    let rows = match resource.id {
        "qdrant.cluster" | "qdrant.peers" => {
            let status = value["status"]
                .as_str()
                .ok_or_else(|| anyhow!("Qdrant cluster response is missing status"))?;
            ensure!(
                matches!(status, "enabled" | "disabled"),
                "Unknown Qdrant cluster status: {status}"
            );
            if status == "disabled" {
                notice = "Distributed mode is disabled on the REST node".into();
            }
            if resource.id == "qdrant.cluster" {
                let raft = &value["raft_info"];
                vec![row(&[
                    &value["status"],
                    &value["peer_id"],
                    &raft["leader"],
                    &raft["role"],
                    &raft["term"],
                    &raft["commit"],
                    &raft["pending_operations"],
                    &value,
                ])]
            } else if status == "disabled" {
                Vec::new()
            } else {
                let peers = value["peers"]
                    .as_object()
                    .ok_or_else(|| anyhow!("Qdrant cluster response is missing peers"))?;
                let mut peers = peers
                    .iter()
                    .map(|(id, info)| Ok((id.parse::<u64>()?, info)))
                    .collect::<Result<Vec<_>>>()?;
                peers.sort_by_key(|(id, _)| *id);
                peers
                    .into_iter()
                    .skip(offset)
                    .take(PAGE_SIZE as usize + 1)
                    .map(|(id, info)| {
                        row(&[
                            &json!(id),
                            &info["uri"],
                            &json!(value["peer_id"].as_u64().map(|peer| peer == id)),
                            &json!(value["raft_info"]["leader"].as_u64().map(|peer| peer == id)),
                            info,
                        ])
                    })
                    .collect()
            }
        }
        "qdrant.shards" => {
            ensure!(
                value["peer_id"].is_u64(),
                "Qdrant collection topology is missing peer_id"
            );
            let mut shards = Vec::new();
            for (key, location) in [("local_shards", "local"), ("remote_shards", "remote")] {
                for shard in array(&value, key)? {
                    let peer = if location == "local" {
                        &value["peer_id"]
                    } else {
                        &shard["peer_id"]
                    };
                    ensure!(
                        shard["shard_id"].is_u64() && peer.is_u64(),
                        "Qdrant shard is missing its ID or peer ID"
                    );
                    shards.push((shard, peer, location));
                }
            }
            shards.sort_by_key(|(shard, peer, location)| {
                (shard["shard_id"].as_u64(), peer.as_u64(), *location)
            });
            shards
                .into_iter()
                .skip(offset)
                .take(PAGE_SIZE as usize + 1)
                .map(|(s, p, location)| {
                    row(&[
                        &s["shard_id"],
                        p,
                        &json!(location),
                        &s["state"],
                        &s["points_count"],
                        &s["shard_key"],
                        s,
                    ])
                })
                .collect()
        }
        "qdrant.transfers" => {
            let mut transfers = array(&value, "shard_transfers")?.iter().collect::<Vec<_>>();
            transfers
                .sort_by_key(|s| (s["shard_id"].as_u64(), s["from"].as_u64(), s["to"].as_u64()));
            transfers
                .into_iter()
                .skip(offset)
                .take(PAGE_SIZE as usize + 1)
                .map(|s| {
                    row(&[
                        &s["shard_id"],
                        &s["from"],
                        &s["to"],
                        &s["sync"],
                        &s["method"],
                        &s["to_shard_id"],
                        &s["comment"],
                        s,
                    ])
                })
                .collect()
        }
        "qdrant.collection_cluster" => vec![row(&[&value])],
        _ => bail!("Unsupported Qdrant topology resource"),
    };
    let end = offset.saturating_add(PAGE_SIZE as usize);
    let next = rows.len() > PAGE_SIZE as usize;
    let mut page = crate::browse::page(
        resource,
        rows.into_iter().take(PAGE_SIZE as usize).collect(),
        &notice,
    );
    page.next = next;
    for column in &mut page.columns {
        column.datatype = match column.name.as_str() {
            "details" => "JSON",
            "is_self" | "is_leader" | "sync" => "boolean",
            "status" | "uri" | "role" | "location" | "state" | "method" | "comment" => "text",
            "shard_key" => "string or integer",
            _ => "unsigned integer",
        }
        .into();
    }
    if next {
        page.continuation = Some(crate::browse::continuation(
            executor,
            resource,
            crate::browse::Offset::Topology(end),
        )?);
    }
    crate::browse::bounded(page)
}
