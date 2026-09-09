use anyhow::{Result, anyhow, bail, ensure};
use tokio_postgres::{Client, types::ToSql};

use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, Value};

pub(crate) fn pg_error(error: tokio_postgres::Error) -> anyhow::Error {
    if let Some(db) = error.as_db_error() {
        let mut message = format!("PostgreSQL [{}] {db}", db.code().code());
        if let Some(context) = db.where_() {
            message.push_str(&format!("\nCONTEXT: {context}"));
        }
        anyhow!(message)
    } else {
        anyhow::Error::new(error).context("PostgreSQL")
    }
}

pub(crate) async fn metadata(client: &Client, resource: &Resource, offset: i64) -> Result<Page> {
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
                    .map(|v| Some(v.into()))
                    .map_err(pg_error)
            })
            .collect::<Result<Vec<_>>>()?;
        bytes += cells
            .iter()
            .flatten()
            .map(|v: &Value| v.bytes().len())
            .sum::<usize>();
        ensure!(
            bytes <= PAGE_BYTES,
            "Metadata page exceeds the 1 MiB display limit; current page retained"
        );
        page.rows.push(Row { cells, target });
    }
    Ok(page)
}
