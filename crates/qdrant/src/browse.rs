use anyhow::{Result, anyhow, bail, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::PageRequest;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row};
use qdrant_client::qdrant::{
    CollectionInfo, PointId, RetrievedPoint, VectorOutput, point_id::PointIdOptions,
    vector_output::Vector, vectors_output::VectorsOptions,
};
use serde::{Deserialize, Serialize};

pub static RESOURCES: &[&ResourceDescriptor] = &[
    &crate::topology::ROOT,
    &crate::topology::CLUSTER,
    &crate::topology::PEERS,
    &crate::topology::SHARDS,
    &crate::topology::TRANSFERS,
    &crate::topology::COLLECTION_CLUSTER,
    &ResourceDescriptor {
        id: "qdrant.collections",
        description: "Collection names; Enter opens browsing and topology choices",
        columns: &["name"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.collection",
        description: "Choose points, metadata or topology; no read until opened",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.metadata",
        description: "Collection status and approximate counts; not an exact count query",
        columns: &["field", "value"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.points",
        description: "ID-ordered Scroll; payload and vectors are not requested",
        columns: &["id", "type"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.point",
        description: "Choose payload or vectors; each is fetched separately when opened",
        columns: &["resource", "description"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.payload",
        description: "Selected point payload as JSON; arbitrary keys, not known offline",
        columns: &["payload"],
        paging: true,
        actions: &[],
    },
    &ResourceDescriptor {
        id: "qdrant.vectors",
        description: "Selected point's named/unnamed dense, sparse and multivectors; full values within the limit",
        columns: &["name", "type", "shape", "values"],
        paging: true,
        actions: &[],
    },
    &crate::query::RESOURCE,
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Id {
    Num(u64),
    Uuid(String),
}

impl Id {
    fn from_native(id: PointId) -> Result<Self> {
        match id.point_id_options {
            Some(PointIdOptions::Num(n)) => Ok(Self::Num(n)),
            Some(PointIdOptions::Uuid(s)) => {
                ensure!(valid_uuid(&s), "Qdrant returned an invalid UUID");
                Ok(Self::Uuid(s))
            }
            None => bail!("Qdrant returned a point without an ID"),
        }
    }

    pub(crate) fn native(&self) -> PointId {
        match self {
            Self::Num(n) => (*n).into(),
            Self::Uuid(s) => s.clone().into(),
        }
    }

    fn text(&self) -> String {
        match self {
            Self::Num(n) => n.to_string(),
            Self::Uuid(s) => s.clone(),
        }
    }

    fn parse(value: &str) -> Result<Self> {
        if let Ok(n) = value.parse::<u64>() {
            return Ok(Self::Num(n));
        }
        ensure!(valid_uuid(value), "Invalid Qdrant point ID");
        Ok(Self::Uuid(value.into()))
    }
}

fn valid_uuid(s: &str) -> bool {
    s.len() == 36
        && s.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Offset {
    Collections(usize),
    Topology(usize),
    Point(Id),
}

#[derive(Serialize, Deserialize)]
struct Position {
    executor: u64,
    resource: String,
    path: Vec<String>,
    offset: Offset,
}

pub(crate) fn validate(request: &PageRequest, executor: u64) -> Result<Option<Offset>> {
    let valid = match (request.resource.id, request.resource.path.as_slice()) {
        ("qdrant.resources" | "qdrant.collections" | "qdrant.cluster" | "qdrant.peers", []) => true,
        ("qdrant.shards" | "qdrant.transfers" | "qdrant.collection_cluster", [name]) => {
            !name.is_empty() && !matches!(name.as_str(), "." | "..")
        }
        ("qdrant.collection" | "qdrant.metadata" | "qdrant.points", [name]) => !name.is_empty(),
        ("qdrant.point" | "qdrant.payload" | "qdrant.vectors", [name, id]) => {
            !name.is_empty() && Id::parse(id).is_ok()
        }
        _ => false,
    };
    ensure!(valid, "Unsupported Qdrant resource or path");
    ensure!(
        request.resource.path.iter().map(String::len).sum::<usize>() <= PAGE_BYTES,
        "Qdrant resource path exceeds the 1 MiB limit"
    );
    let Some(token) = &request.continuation else {
        return Ok(None);
    };
    ensure!(token.len() <= PAGE_BYTES, "Invalid Qdrant continuation");
    let position: Position =
        serde_json::from_str(token).map_err(|_| anyhow!("Invalid Qdrant continuation"))?;
    ensure!(
        position.executor == executor
            && position.resource == request.resource.id
            && position.path == request.resource.path,
        "Qdrant continuation belongs to another session or resource; refresh"
    );
    ensure!(
        matches!(
            (&position.offset, request.resource.id),
            (Offset::Collections(_), "qdrant.collections")
                | (Offset::Point(_), "qdrant.points")
                | (
                    Offset::Topology(_),
                    "qdrant.peers" | "qdrant.shards" | "qdrant.transfers"
                )
        ),
        "Invalid Qdrant continuation kind"
    );
    if let Offset::Point(id) = &position.offset {
        Id::from_native(id.native())?;
    }
    Ok(Some(position.offset))
}

pub(crate) fn continuation(executor: u64, resource: &Resource, offset: Offset) -> Result<String> {
    Ok(serde_json::to_string(&Position {
        executor,
        resource: resource.id.into(),
        path: resource.path.clone(),
        offset,
    })?)
}

fn row(cells: impl IntoIterator<Item = String>, target: Option<Resource>) -> Row {
    Row {
        cells: cells.into_iter().map(|s| Some(s.into())).collect(),
        target,
    }
}

pub(crate) fn page(resource: &Resource, rows: Vec<Row>, notice: &str) -> Page {
    let descriptor = RESOURCES
        .iter()
        .find(|d| d.id == resource.id)
        .expect("validated resource");
    Page {
        rows,
        columns: descriptor
            .columns
            .iter()
            .map(|name| Column {
                name: (*name).into(),
                datatype: "text".into(),
            })
            .collect(),
        notice: notice.into(),
        ..Page::default()
    }
}

pub(crate) fn bounded(page: Page) -> Result<Page> {
    ensure!(
        page.rows.len() <= PAGE_SIZE as usize,
        "Qdrant page exceeds the 100-row limit; current page retained"
    );
    ensure!(
        page.bytes() <= PAGE_BYTES,
        "Qdrant page exceeds the 1 MiB display limit; current page retained"
    );
    Ok(page)
}

pub(crate) fn menu(resource: &Resource) -> Option<Page> {
    let choices: &[(&str, &'static str, &str)] = match resource.id {
        "qdrant.resources" => &[
            (
                "collections",
                "qdrant.collections",
                "Browse collections through gRPC",
            ),
            (
                "cluster",
                "qdrant.cluster",
                "Cluster status and consensus; requires rest_url",
            ),
            (
                "peers",
                "qdrant.peers",
                "Peer IDs and addresses; requires rest_url",
            ),
        ],
        "qdrant.collection" => &[
            (
                "points",
                "qdrant.points",
                "Browse point IDs without payload or vectors",
            ),
            (
                "metadata",
                "qdrant.metadata",
                "Read collection status and approximate counts",
            ),
            (
                "shards",
                "qdrant.shards",
                "Local/remote shard placement; requires rest_url",
            ),
            (
                "transfers",
                "qdrant.transfers",
                "Active shard transfers; requires rest_url",
            ),
            (
                "cluster details",
                "qdrant.collection_cluster",
                "Full collection topology, including resharding; requires rest_url",
            ),
        ],
        "qdrant.point" => &[
            ("payload", "qdrant.payload", "Read payload only"),
            ("vectors", "qdrant.vectors", "Read vectors only"),
        ],
        _ => return None,
    };
    Some(page(
        resource,
        choices
            .iter()
            .map(|&(label, target, description)| {
                row(
                    [label.into(), description.into()],
                    Some(Resource::new(target, resource.path.clone())),
                )
            })
            .collect(),
        "Enter opens selected resource; nothing fetched by this menu",
    ))
}

pub(crate) fn collections(
    resource: &Resource,
    mut names: Vec<String>,
    offset: usize,
    executor: u64,
) -> Result<Page> {
    // Qdrant List has no server-side pagination; the decode cap bounds each re-read.
    names.sort();
    let end = offset.saturating_add(PAGE_SIZE as usize);
    let next = end < names.len();
    let rows = names
        .into_iter()
        .skip(offset)
        .take(PAGE_SIZE as usize)
        .map(|name| {
            row(
                [name.clone()],
                Some(Resource::new("qdrant.collection", vec![name])),
            )
        })
        .collect();
    let mut page = page(
        resource,
        rows,
        "Collection list re-read per page (1 MiB RPC cap); concurrent changes can shift pages",
    );
    page.next = next;
    if next {
        page.continuation = Some(continuation(executor, resource, Offset::Collections(end))?);
    }
    bounded(page)
}

pub(crate) fn points(
    resource: &Resource,
    records: Vec<RetrievedPoint>,
    next: Option<PointId>,
    executor: u64,
) -> Result<Page> {
    ensure!(
        records.len() <= PAGE_SIZE as usize,
        "Qdrant returned more than 100 points"
    );
    let mut rows = Vec::with_capacity(records.len());
    for record in records {
        let id = Id::from_native(
            record
                .id
                .ok_or_else(|| anyhow!("Qdrant returned a point without an ID"))?,
        )?;
        let kind = match id {
            Id::Num(_) => "numeric",
            Id::Uuid(_) => "uuid",
        };
        rows.push(row(
            [id.text(), kind.into()],
            Some(Resource::new(
                "qdrant.point",
                vec![resource.path[0].clone(), id.text()],
            )),
        ));
    }
    let mut page = page(
        resource,
        rows,
        "ID-ordered Scroll; payload/vectors not fetched; independent reads, not a snapshot",
    );
    if let Some(next) = next {
        page.continuation = Some(continuation(
            executor,
            resource,
            Offset::Point(Id::from_native(next)?),
        )?);
        page.next = true;
    }
    bounded(page)
}

pub(crate) fn metadata(resource: &Resource, info: CollectionInfo) -> Result<Page> {
    let status = qdrant_client::qdrant::CollectionStatus::try_from(info.status)
        .map(|s| s.as_str_name().to_owned())
        .unwrap_or_else(|_| format!("unknown ({})", info.status));
    let rows = vec![
        row(["status".into(), status], None),
        row(
            ["segments_count".into(), info.segments_count.to_string()],
            None,
        ),
        Row {
            cells: vec![
                Some("points_count (approximate)".into()),
                info.points_count.map(|n| n.to_string().into()),
            ],
            target: None,
        },
        Row {
            cells: vec![
                Some("indexed_vectors_count (approximate)".into()),
                info.indexed_vectors_count.map(|n| n.to_string().into()),
            ],
            target: None,
        },
    ];
    bounded(page(
        resource,
        rows,
        "Status and approximate counts only; no exact count query",
    ))
}

pub(crate) fn point_id(resource: &Resource) -> Result<PointId> {
    Ok(Id::parse(&resource.path[1])?.native())
}

pub(crate) fn detail(resource: &Resource, mut records: Vec<RetrievedPoint>) -> Result<Page> {
    ensure!(
        !records.is_empty(),
        "Qdrant point disappeared; refresh the points page"
    );
    ensure!(
        records.len() == 1,
        "Qdrant returned an unexpected point count"
    );
    let record = records.pop().expect("one point");
    ensure!(
        record.id == Some(point_id(resource)?),
        "Qdrant returned an unexpected point ID"
    );
    let mut rows = if resource.id == "qdrant.payload" {
        let payload: serde_json::Value = qdrant_client::Payload::from(record.payload).into();
        vec![row([serde_json::to_string(&payload)?], None)]
    } else {
        let vectors = record.vectors.and_then(|v| v.vectors_options);
        let mut named: Vec<_> = match vectors {
            None => vec![],
            Some(VectorsOptions::Vector(v)) => vec![(String::new(), v)],
            Some(VectorsOptions::Vectors(v)) => v.vectors.into_iter().collect(),
        };
        named.sort_by(|a, b| a.0.cmp(&b.0));
        ensure!(
            named.len() <= PAGE_SIZE as usize,
            "Qdrant point has more than 100 vectors; detail rejected"
        );
        named
            .into_iter()
            .map(|(name, vector)| vector_row(name, vector))
            .collect::<Result<Vec<_>>>()?
    };
    for row in &mut rows {
        let index = if resource.id == "qdrant.payload" {
            0
        } else {
            3
        };
        if let Some(onetui_core::Value::Text(text)) = row.cells[index].take() {
            row.cells[index] = Some(onetui_core::Value::Json(text));
        }
    }
    bounded(page(
        resource,
        rows,
        "Selected point only; full values within 1 MiB; Enter inspects cached fields",
    ))
}

fn vector_row(name: String, vector: VectorOutput) -> Result<Row> {
    // The SDK's legacy multivector conversion divides/chunks without validation.
    #[allow(deprecated)]
    if vector.vector.is_none()
        && vector.indices.is_none()
        && let Some(count) = vector.vectors_count
    {
        ensure!(
            count > 0
                && !vector.data.is_empty()
                && vector.data.len().is_multiple_of(count as usize),
            "Unsupported malformed Qdrant multivector"
        );
    }
    let (kind, shape, values) = match vector.into_vector() {
        Vector::Dense(v) => {
            ensure!(
                v.data.iter().all(|n| n.is_finite()),
                "Unsupported non-finite Qdrant vector value"
            );
            (
                "dense",
                v.data.len().to_string(),
                serde_json::to_string(&v.data)?,
            )
        }
        Vector::Sparse(v) => {
            ensure!(
                v.indices.len() == v.values.len() && v.values.iter().all(|n| n.is_finite()),
                "Unsupported malformed Qdrant sparse vector"
            );
            (
                "sparse",
                format!("{} nonzero entries", v.indices.len()),
                serde_json::to_string(
                    &serde_json::json!({"indices": v.indices, "values": v.values}),
                )?,
            )
        }
        Vector::MultiDense(v) => {
            let dimensions = v.vectors.first().map_or(0, |v| v.data.len());
            ensure!(
                v.vectors
                    .iter()
                    .all(|v| v.data.len() == dimensions && v.data.iter().all(|n| n.is_finite())),
                "Unsupported malformed Qdrant multivector"
            );
            let shape = format!("{} x {dimensions}", v.vectors.len());
            (
                "multivector",
                shape,
                serde_json::to_string(&v.vectors.into_iter().map(|v| v.data).collect::<Vec<_>>())?,
            )
        }
    };
    Ok(row(
        [
            if name.is_empty() {
                "(unnamed)".into()
            } else {
                name
            },
            kind.into(),
            shape,
            values,
        ],
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paging_preserves_ids_scope_and_terminal_safety() {
        let resource = Resource::new("qdrant.points", vec!["collection\x1b".into()]);
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let page = points(
            &resource,
            vec![RetrievedPoint {
                id: Some(u64::MAX.into()),
                ..Default::default()
            }],
            Some(uuid.into()),
            7,
        )
        .unwrap();
        assert_eq!(
            page.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("18446744073709551615")
        );
        assert_eq!(
            page.rows[0].target.as_ref().unwrap().path,
            ["collection\x1b", "18446744073709551615"]
        );
        let mut request = PageRequest {
            resource: resource.clone(),
            continuation: page.continuation,
        };
        assert!(
            matches!(validate(&request, 7).unwrap(), Some(Offset::Point(Id::Uuid(id))) if id == uuid)
        );
        assert!(validate(&request, 8).is_err());
        request.resource.path[0] = "other".into();
        assert!(validate(&request, 7).is_err());
        request.continuation = None;
        assert!(validate(&request, 7).unwrap().is_none());
        let collection = Resource::new("qdrant.collections", vec![]);
        let page = collections(&collection, vec!["bad\x1b\u{202e}name".into()], 0, 7).unwrap();
        assert!(
            page.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains('\x1b')
        );
        assert!(points(&resource, vec![RetrievedPoint::default()], None, 7).is_err());
        assert!(Id::parse("not-a-uuid").is_err());
        assert!(Id::parse("18446744073709551616").is_err());
    }

    #[test]
    fn collection_pages_and_detail_limits_reject_without_truncating() {
        let resource = Resource::new("qdrant.collections", vec![]);
        let names: Vec<_> = (0..101).rev().map(|i| format!("c{i:03}")).collect();
        let first = collections(&resource, names.clone(), 0, 1).unwrap();
        assert_eq!(first.rows.len(), 100);
        assert_eq!(
            first.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("c000")
        );
        let request = PageRequest {
            resource: resource.clone(),
            continuation: first.continuation,
        };
        let Some(Offset::Collections(offset)) = validate(&request, 1).unwrap() else {
            panic!("collection offset")
        };
        let last = collections(&resource, names, offset, 1).unwrap();
        assert_eq!(
            last.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("c100")
        );
        assert!(!last.next);
        assert!(collections(&resource, vec!["\x1b".repeat(PAGE_BYTES / 2)], 0, 1).is_err());
        let payload = Resource::new("qdrant.payload", vec!["c".into(), "1".into()]);
        assert!(
            detail(&payload, vec![])
                .unwrap_err()
                .to_string()
                .contains("disappeared")
        );
        let record = RetrievedPoint {
            id: Some(1_u64.into()),
            ..Default::default()
        };
        assert_eq!(
            detail(&payload, vec![record.clone()]).unwrap().rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("{}")
        );
        let mut large = record;
        large
            .payload
            .insert("large".into(), "x".repeat(PAGE_BYTES).into());
        assert!(
            detail(&payload, vec![large])
                .unwrap_err()
                .to_string()
                .contains("1 MiB")
        );
    }

    #[test]
    #[allow(deprecated)]
    fn malformed_legacy_vectors_cannot_panic_or_be_silently_changed() {
        let malformed = VectorOutput {
            data: vec![1.0],
            vectors_count: Some(0),
            ..Default::default()
        };
        assert!(vector_row("".into(), malformed).is_err());
        let nonfinite = VectorOutput {
            data: vec![f32::NAN],
            ..Default::default()
        };
        assert!(vector_row("".into(), nonfinite).is_err());
        let legacy = VectorOutput {
            data: vec![1.0, 2.0, 3.0, 4.0],
            vectors_count: Some(2),
            ..Default::default()
        };
        let row = vector_row("legacy".into(), legacy).unwrap();
        assert_eq!(
            row.cells[2].as_ref().and_then(onetui_core::Value::text),
            Some("2 x 2")
        );
        assert_eq!(
            row.cells[3].as_ref().and_then(onetui_core::Value::text),
            Some("[[1.0,2.0],[3.0,4.0]]")
        );
    }
}
