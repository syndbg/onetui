use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, anyhow, bail, ensure};
use tokio::sync::oneshot;
use tokio_postgres::{Client, types::ToSql};

use crate::check;

use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, display};

pub(crate) fn pg_error(error: tokio_postgres::Error) -> anyhow::Error {
    match error.code().map(|code| code.code()) {
        Some("42501") => anyhow!("PostgreSQL access denied"),
        Some("57014") => anyhow!("PostgreSQL request timed out or was cancelled"),
        Some(code) if code.starts_with("28") => {
            anyhow!("PostgreSQL authentication failed; check credentials")
        }
        _ => {
            anyhow!("PostgreSQL request failed; check permissions, endpoint and TLS trust")
        }
    }
}

pub async fn fetch(
    url: String,
    ca_file: Option<PathBuf>,
    resource: Resource,
    offset: i64,
    continuation: Option<String>,
    deadline: Duration,
    mut cancel: oneshot::Receiver<()>,
) -> Result<Page> {
    let until = tokio::time::Instant::now() + deadline;
    let mut config = check::postgres_config(&url, deadline)?;
    config.application_name("onetui-browse");
    let tls = check::postgres_tls(&config, ca_file.as_deref())?;
    let (client, driver) = tokio::select! {
        result = tokio::time::timeout_at(until, config.connect(tls.clone())) => {
            result.map_err(|_| anyhow!("PostgreSQL connection timed out"))?.map_err(pg_error)?
        }
        _ = &mut cancel => bail!("Request cancelled; connection discarded"),
    };
    // Keep client and driver scoped to this request; neither may survive it.
    tokio::pin!(driver);
    let query = async {
        if resource.id == "postgres.rows" {
            crate::rows::fetch(&client, &resource, offset, continuation.as_deref()).await
        } else {
            metadata(&client, &resource, offset).await
        }
    };
    tokio::pin!(query);
    tokio::select! {
        result = &mut query => return result,
        _ = &mut driver => bail!("PostgreSQL connection closed during read"),
        _ = tokio::time::sleep_until(until) => {},
        _ = &mut cancel => {},
    }
    // Cleanup has its own short deadline. Never reuse a connection with uncertain cleanup.
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            _ = async {
                client.cancel_token().cancel_query(tls).await.map_err(pg_error)?;
                let _ = query.await;
                Ok::<_, anyhow::Error>(())
            } => {},
            _ = &mut driver => {},
        }
    })
    .await;
    bail!("Request cancelled or timed out; connection discarded")
}

async fn metadata(client: &Client, resource: &Resource, offset: i64) -> Result<Page> {
    ensure!(offset >= 0, "invalid metadata page offset");
    let limit = PAGE_SIZE + 1;
    let rows = match (resource.id, resource.path.as_slice()) {
        ("postgres.schemas", []) => client.query(
            "SELECT nspname::text FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege(oid, 'USAGE') ORDER BY nspname, oid LIMIT $1 OFFSET $2", &[&limit, &offset]
        ).await.map_err(pg_error)?,
        ("postgres.relations", [schema]) => {
            let visible = client.query_opt("SELECT pg_catalog.has_schema_privilege(oid, 'USAGE') FROM pg_catalog.pg_namespace WHERE nspname = $1", &[schema]).await.map_err(pg_error)?;
            ensure!(visible.is_some_and(|row| row.get::<_, bool>(0)), "Schema disappeared or USAGE permission was denied; refresh its parent");
            client.query(
                "SELECT c.relname::text, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'foreign table' END, CASE WHEN pg_catalog.has_table_privilege(c.oid, 'SELECT') THEN 'table' WHEN pg_catalog.has_any_column_privilege(c.oid, 'SELECT') THEN 'some columns' ELSE 'denied' END FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f') ORDER BY c.relname, c.oid LIMIT $2 OFFSET $3", &[schema as &(dyn ToSql + Sync), &limit, &offset]
            ).await.map_err(pg_error)?
        }
        ("postgres.columns", [schema, relation]) => {
            let row = client.query_opt(
                "SELECT c.oid, pg_catalog.has_schema_privilege(n.oid, 'USAGE') AND (pg_catalog.has_table_privilege(c.oid, 'SELECT') OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT')) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r','p','v','m','f')", &[schema, relation]
            ).await.map_err(pg_error)?.ok_or_else(|| anyhow!("Relation disappeared; refresh its parent"))?;
            ensure!(row.get::<_, bool>(1), "PostgreSQL metadata access denied; SELECT permission required");
            let oid: u32 = row.get(0);
            client.query(
                "SELECT attname::text, pg_catalog.format_type(atttypid, atttypmod), CASE WHEN attnotnull THEN 'yes' ELSE 'no' END FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum LIMIT $2 OFFSET $3", &[&oid as &(dyn ToSql + Sync), &limit, &offset]
            ).await.map_err(pg_error)?
        }
        _ => bail!("Unsupported PostgreSQL metadata resource or path"),
    };
    let mut page = Page {
        next: rows.len() > PAGE_SIZE as usize,
        rows: Vec::new(),
        ..Page::default()
    };
    let mut bytes = 0;
    for row in rows.into_iter().take(PAGE_SIZE as usize) {
        let name: String = row.try_get(0).map_err(pg_error)?;
        let target = match (resource.id, resource.path.as_slice()) {
            ("postgres.schemas", []) => Some(Resource::new("postgres.relations", vec![name])),
            ("postgres.relations", [schema]) => {
                Some(Resource::new("postgres.rows", vec![schema.clone(), name]))
            }
            _ => None,
        };
        let cells = (0..row.len())
            .map(|i| {
                row.try_get::<_, String>(i)
                    .map(|v| Some(display(&v)))
                    .map_err(pg_error)
            })
            .collect::<Result<Vec<_>>>()?;
        bytes += cells.iter().flatten().map(String::len).sum::<usize>();
        ensure!(
            bytes <= PAGE_BYTES,
            "Metadata page exceeds the 1 MiB display limit; current page retained"
        );
        page.rows.push(Row { cells, target });
    }
    Ok(page)
}
