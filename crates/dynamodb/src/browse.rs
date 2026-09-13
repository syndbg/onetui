use anyhow::{Result, anyhow, ensure};
use onetui_core::{Column, PAGE_BYTES, Page, Resource, Row, Value};
use onetui_core::{catalog::ResourceDescriptor, provider::PageRequest};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

macro_rules! resources {
    ($(($id:literal,$description:literal)),* $(,)?)=>{pub(crate) static RESOURCES:&[&ResourceDescriptor]=&[$(&ResourceDescriptor{id:$id,description:$description,columns:&[],paging:true,actions:&[]}),*];};
}
resources![
    (
        "dynamodb.resources",
        "Choose tables or account metadata; no item scan"
    ),
    ("dynamodb.tables", "Table names"),
    ("dynamodb.streams", "Stream inventory"),
    ("dynamodb.table_streams", "Streams for the selected table"),
    ("dynamodb.stream", "Choose stream details or shards"),
    (
        "dynamodb.stream_info",
        "Stream description, with bounded shard pages"
    ),
    (
        "dynamodb.shards",
        "Shard descriptions and parent relationships"
    ),
    (
        "dynamodb.records",
        "Bounded shard records; original typed images retained"
    ),
    ("dynamodb.table", "Choose item reads or table metadata"),
    (
        "dynamodb.items",
        "Explicit bounded Scan; independent reads, not a snapshot"
    ),
    (
        "dynamodb.query",
        "Read-only native Query, Scan, GetItem or Streams GetRecords JSON"
    ),
    ("dynamodb.table_info", "Complete DescribeTable response"),
    (
        "dynamodb.indexes",
        "Local and global secondary index descriptions"
    ),
    ("dynamodb.replicas", "Global-table replica descriptions"),
    ("dynamodb.ttl", "Time-to-live settings"),
    ("dynamodb.backups_status", "Continuous-backup settings"),
    ("dynamodb.insights", "Table contributor-insight settings"),
    ("dynamodb.kinesis", "Kinesis streaming destinations"),
    ("dynamodb.replica_scaling", "Replica auto-scaling settings"),
    ("dynamodb.tags", "Table tags"),
    ("dynamodb.policy", "Table resource policy"),
    ("dynamodb.backups", "Existing backup inventory"),
    ("dynamodb.backup", "Backup description"),
    (
        "dynamodb.exports",
        "Existing export jobs; does not start exports"
    ),
    ("dynamodb.export", "Export description"),
    (
        "dynamodb.imports",
        "Existing import jobs; does not start imports"
    ),
    ("dynamodb.import", "Import description"),
    ("dynamodb.global_tables", "Legacy global-table inventory"),
    ("dynamodb.global_table", "Legacy global-table description"),
    ("dynamodb.global_settings", "Legacy global-table settings"),
    ("dynamodb.account_insights", "Contributor-insight inventory"),
    ("dynamodb.limits", "Account provisioned-capacity limits"),
    (
        "dynamodb.endpoints",
        "Service endpoints; never connects to returned endpoints"
    ),
];

pub(crate) fn depth(id: &str) -> usize {
    match id {
        "dynamodb.resources"
        | "dynamodb.streams"
        | "dynamodb.tables"
        | "dynamodb.backups"
        | "dynamodb.exports"
        | "dynamodb.imports"
        | "dynamodb.global_tables"
        | "dynamodb.account_insights"
        | "dynamodb.limits"
        | "dynamodb.endpoints" => 0,
        "dynamodb.records" => 2,
        _ => 1,
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bookmark {
    session: u64,
    resource: String,
    path: Vec<String>,
    query: String,
    token: Json,
}

pub(crate) fn position(request: &PageRequest, session: u64, query: &str) -> Result<Option<Json>> {
    ensure!(
        RESOURCES.iter().any(|r| r.id == request.resource.id),
        "Unknown DynamoDB resource"
    );
    ensure!(
        request.resource.path.len() == depth(request.resource.id),
        "Invalid DynamoDB resource path"
    );
    ensure!(
        request
            .resource
            .path
            .iter()
            .all(|p| !p.is_empty() && p.len() <= 2048 && !p.chars().any(char::is_control)),
        "Invalid DynamoDB resource name"
    );
    request
        .continuation
        .as_deref()
        .map(|token| {
            ensure!(token.len() <= 65536, "DynamoDB bookmark exceeds 64 KiB");
            let bookmark: Bookmark = serde_json::from_str(token)?;
            ensure!(
                bookmark.session == session
                    && bookmark.resource == request.resource.id
                    && bookmark.path == request.resource.path
                    && bookmark.query == query,
                "DynamoDB bookmark belongs to another connection, resource or query"
            );
            Ok(bookmark.token)
        })
        .transpose()
}

pub(crate) fn continuation(
    page: &mut Page,
    request: &PageRequest,
    session: u64,
    query: &str,
    token: Option<Json>,
) -> Result<()> {
    if let Some(token) = token.filter(|v| {
        !v.is_null() && v.as_object().is_none_or(|o| !o.is_empty()) && v.as_str() != Some("")
    }) {
        let bookmark = serde_json::to_string(&Bookmark {
            session,
            resource: request.resource.id.into(),
            path: request.resource.path.clone(),
            query: query.into(),
            token,
        })?;
        ensure!(bookmark.len() <= 65536, "DynamoDB bookmark exceeds 64 KiB");
        page.continuation = Some(bookmark);
        page.next = true;
    }
    ensure!(
        page.rows.len() <= 100 && page.bytes() <= PAGE_BYTES,
        "DynamoDB page exceeds 100 rows or 1 MiB; reduce the query limit or projection"
    );
    Ok(())
}

fn columns(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|n| Column {
            name: (*n).into(),
            datatype: "text".into(),
        })
        .collect()
}

pub(crate) fn menu(id: &str, path: &[String]) -> Page {
    let choices: &[(&str, &str)] = if id == "dynamodb.resources" {
        &[
            ("dynamodb.tables", "Tables"),
            ("dynamodb.streams", "Streams"),
            ("dynamodb.backups", "Backups"),
            ("dynamodb.exports", "Exports"),
            ("dynamodb.imports", "Imports"),
            ("dynamodb.global_tables", "Legacy global tables"),
            ("dynamodb.account_insights", "Contributor insights"),
            ("dynamodb.limits", "Limits"),
            ("dynamodb.endpoints", "Endpoints"),
        ]
    } else if id == "dynamodb.stream" {
        &[
            ("dynamodb.shards", "Shards"),
            ("dynamodb.stream_info", "Stream details"),
        ]
    } else {
        &[
            ("dynamodb.items", "Scan items"),
            ("dynamodb.table_streams", "Streams"),
            ("dynamodb.table_info", "Table details"),
            ("dynamodb.indexes", "Indexes"),
            ("dynamodb.replicas", "Replicas"),
            ("dynamodb.ttl", "TTL"),
            ("dynamodb.backups_status", "Continuous backups"),
            ("dynamodb.insights", "Contributor insights"),
            ("dynamodb.kinesis", "Kinesis destinations"),
            ("dynamodb.replica_scaling", "Replica scaling"),
            ("dynamodb.tags", "Tags"),
            ("dynamodb.policy", "Resource policy"),
            ("dynamodb.global_table", "Legacy global-table details"),
            ("dynamodb.global_settings", "Legacy global-table settings"),
        ]
    };
    Page {
        columns: columns(&["resource", "description"]),
        rows: choices
            .iter()
            .map(|(id, label)| Row {
                cells: vec![
                    Some((*label).into()),
                    Some(
                        RESOURCES
                            .iter()
                            .find(|r| r.id == *id)
                            .unwrap()
                            .description
                            .into(),
                    ),
                ],
                target: Some(Resource::new(id, path.to_vec())),
            })
            .collect(),
        ..Page::default()
    }
}

pub(crate) fn metadata(id: &str, mut body: Json) -> Result<(Page, Option<Json>)> {
    let mut page = Page::default();
    let (array, next, target, key) = match id {
        "dynamodb.tables" => (
            "TableNames",
            "LastEvaluatedTableName",
            Some("dynamodb.table"),
            None,
        ),
        "dynamodb.backups" => (
            "BackupSummaries",
            "LastEvaluatedBackupArn",
            Some("dynamodb.backup"),
            Some("BackupArn"),
        ),
        "dynamodb.exports" => (
            "ExportSummaries",
            "NextToken",
            Some("dynamodb.export"),
            Some("ExportArn"),
        ),
        "dynamodb.imports" => (
            "ImportSummaryList",
            "NextToken",
            Some("dynamodb.import"),
            Some("ImportArn"),
        ),
        "dynamodb.global_tables" => (
            "GlobalTables",
            "LastEvaluatedGlobalTableName",
            Some("dynamodb.table"),
            Some("GlobalTableName"),
        ),
        "dynamodb.account_insights" => ("ContributorInsightsSummaries", "NextToken", None, None),
        "dynamodb.tags" => ("Tags", "NextToken", None, None),
        _ => {
            if id == "dynamodb.indexes" {
                body = json!({"GlobalSecondaryIndexes":body["Table"].get("GlobalSecondaryIndexes").cloned().unwrap_or(json!([])),"LocalSecondaryIndexes":body["Table"].get("LocalSecondaryIndexes").cloned().unwrap_or(json!([]))});
            }
            if id == "dynamodb.replicas" {
                body = body["Table"].get("Replicas").cloned().unwrap_or(json!([]));
            }
            page.columns = columns(&["response"]);
            page.columns[0].datatype = "JSON".into();
            page.rows.push(Row {
                cells: vec![Some(Value::Json(serde_json::to_string(&body)?))],
                target: None,
            });
            return Ok((page, None));
        }
    };
    page.columns = columns(&[if id == "dynamodb.tables" {
        "name"
    } else {
        "description"
    }]);
    let values = body
        .get(array)
        .map(|value| {
            value
                .as_array()
                .map(Vec::as_slice)
                .ok_or_else(|| anyhow!("DynamoDB {array} is not an array"))
        })
        .transpose()?
        .unwrap_or(&[]);
    for value in values {
        let destination = target
            .map(|id| {
                let name = key
                    .map_or(value, |key| &value[key])
                    .as_str()
                    .ok_or_else(|| anyhow!("DynamoDB response is missing resource identity"))?;
                Ok::<_, anyhow::Error>(Resource::new(id, vec![name.into()]))
            })
            .transpose()?;
        page.rows.push(Row {
            cells: vec![Some(
                value
                    .as_str()
                    .map(|s| Value::from(s.to_owned()))
                    .unwrap_or_else(|| Value::Json(value.to_string())),
            )],
            target: destination,
        });
    }
    page.notice = "Independent metadata reads; listings may change while paging".into();
    Ok((page, body.get(next).cloned()))
}

pub(crate) fn items(body: Json) -> Result<(Page, Option<Json>)> {
    let values = if let Some(items) = body.get("Items") {
        items
            .as_array()
            .ok_or_else(|| anyhow!("DynamoDB Items must be an array"))?
            .clone()
    } else {
        body.get("Item")
            .filter(|v| v.as_object().is_some_and(|o| !o.is_empty()))
            .cloned()
            .into_iter()
            .collect()
    };
    let mut names = std::collections::BTreeSet::new();
    for value in &values {
        names.extend(
            value
                .as_object()
                .ok_or_else(|| anyhow!("DynamoDB item is not an object"))?
                .keys()
                .cloned(),
        );
    }
    ensure!(
        names.len() <= 1024,
        "DynamoDB page exceeds 1024 distinct attribute names; use projection_expression"
    );
    let page = Page {
        columns: names
            .iter()
            .map(|name| Column {
                name: name.clone(),
                datatype: "DynamoDB AttributeValue (tagged JSON)".into(),
            })
            .collect(),
        rows: values
            .iter()
            .map(|item| Row {
                cells: names
                    .iter()
                    .map(|name| item.get(name).map(|v| Value::Json(v.to_string())))
                    .collect(),
                target: None,
            })
            .collect(),
        notice: format!(
            "{} | Independent reads, not a snapshot; attribute tags and decimal strings retained",
            json!({"Count":body.get("Count"),"ScannedCount":body.get("ScannedCount"),"ConsumedCapacity":body.get("ConsumedCapacity")})
        ),
        ..Page::default()
    };
    Ok((page, body.get("LastEvaluatedKey").cloned()))
}
