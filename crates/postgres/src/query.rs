use anyhow::{Result, ensure};
use futures_util::TryStreamExt;
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::{QueryExecution, WriteOutcome, WriteResult};
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Row, Value, display};
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::browse::pg_error;

pub(crate) const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.query",
    description: "SQL results; dynamic columns, independent OFFSET pages",
    columns: &[],
    paging: true,
    actions: &[],
};

pub(crate) async fn fetch(client: &Client, sql: &str, offset: i64) -> Result<Page> {
    client
        .batch_execute("BEGIN READ ONLY")
        .await
        .map_err(pg_error)?;
    // Extended-protocol preparation rejects multiple statements without executing them.
    let statement = client.prepare(sql).await.map_err(pg_error)?;
    let columns = statement.columns();
    ensure!(
        !columns.is_empty() && columns.len() <= 256,
        "SQL queries must return 1..256 columns"
    );
    ensure!(
        statement.params().is_empty(),
        "SQL parameters are not supported; use literals"
    );
    let aliases = (0..columns.len())
        .map(|i| format!("c{i}"))
        .collect::<Vec<_>>();
    let projections = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let cast = if c.type_() == &tokio_postgres::types::Type::BYTEA {
                ""
            } else {
                "::pg_catalog.text"
            };
            format!("src.c{i}{cast} AS c{i}")
        })
        .collect::<Vec<_>>();
    let size = aliases
        .iter()
        .map(|c| format!("COALESCE(pg_catalog.octet_length({c}), 0)::bigint"))
        .collect::<Vec<_>>()
        .join(" + ");
    let guarded = aliases
        .iter()
        .map(|c| format!("CASE WHEN NOT oversized THEN {c} END"))
        .collect::<Vec<_>>()
        .join(", ");
    // A derived table admits row queries, not transaction control or utility statements.
    // Newlines prevent a trailing SQL comment from swallowing the wrapper.
    let sql = sql.trim().strip_suffix(';').unwrap_or(sql.trim());
    // ponytail: deep OFFSET pages rerun work; keysets need a declared unique result key.
    let bounded = format!(
        "WITH page AS MATERIALIZED (SELECT {} FROM (\n{sql}\n) AS src({}) LIMIT {} OFFSET {offset}), \
         sized AS (SELECT *, ({size}) > {PAGE_BYTES} AS oversized FROM page) \
         SELECT {guarded}, oversized FROM sized",
        projections.join(", "),
        aliases.join(", "),
        PAGE_SIZE + 1,
    );
    let stream = client
        .query_raw(&bounded, std::iter::empty::<&str>())
        .await
        .map_err(pg_error)?;
    tokio::pin!(stream);
    let mut page = Page {
        columns: columns.iter().map(|c| Column { name: display(c.name()), datatype: display(c.type_().name()) }).collect(),
        notice: "SQL OFFSET pages; use ORDER BY with a unique tie-breaker. Independent reads, not a snapshot; deep pages may be slow".into(),
        ..Page::default()
    };
    while let Some(row) = stream.try_next().await.map_err(pg_error)? {
        if page.rows.len() == PAGE_SIZE as usize {
            page.next = true;
            break;
        }
        ensure!(
            !row.get::<_, bool>(columns.len()),
            "Query row exceeds the 1 MiB server value limit"
        );
        let cells = columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if c.type_() == &tokio_postgres::types::Type::BYTEA {
                    row.try_get::<_, Option<Vec<u8>>>(i)
                        .map(|v| v.map(Value::Bytes))
                } else {
                    row.try_get::<_, Option<String>>(i)
                        .map(|v| v.map(Value::Text))
                }
                .map_err(pg_error)
            })
            .collect::<Result<Vec<_>>>()?;
        page.rows.push(Row {
            cells,
            target: None,
        });
        ensure!(
            page.bytes() <= PAGE_BYTES,
            "Query page exceeds the 1 MiB value limit"
        );
    }
    // SELECT can change session settings or acquire advisory locks. Do not leak these
    // into later browsing; read-only transactions alone are not a SQL sandbox.
    client.batch_execute("ROLLBACK").await.map_err(pg_error)?;
    client
        .batch_execute("DISCARD ALL")
        .await
        .map_err(pg_error)?;
    Ok(page)
}

pub(crate) async fn execute_once(client: &Client, sql: &str) -> Result<QueryExecution> {
    let stream = client.simple_query_raw(sql).await?;
    tokio::pin!(stream);
    let mut page = Page::default();
    let mut returns_rows = false;
    let mut truncated = false;
    let mut affected = 0;
    while let Some(message) = stream.try_next().await? {
        match message {
            SimpleQueryMessage::RowDescription(columns) => {
                returns_rows = true;
                ensure!(columns.len() <= 256, "SQL result has more than 256 columns");
                page.columns = columns
                    .iter()
                    .map(|column| Column {
                        name: display(column.name()),
                        datatype: "text".into(),
                    })
                    .collect();
                ensure!(
                    page.bytes() <= PAGE_BYTES,
                    "SQL result columns exceed 1 MiB"
                );
            }
            SimpleQueryMessage::Row(row) => {
                if truncated || page.rows.len() == PAGE_SIZE as usize {
                    truncated = true;
                    continue;
                }
                let cells = (0..row.len())
                    .map(|index| {
                        row.try_get(index)
                            .map(|value| value.map(|text| Value::Text(display(text))))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let next = Row {
                    cells,
                    target: None,
                };
                page.rows.push(next);
                if page.bytes() > PAGE_BYTES {
                    page.rows.pop();
                    truncated = true;
                }
            }
            SimpleQueryMessage::CommandComplete(rows) => affected = rows,
            _ => {}
        }
    }
    if returns_rows {
        page.notice = if truncated {
            format!(
                "Executed once; showing first {} of {affected} rows",
                page.rows.len()
            )
        } else {
            format!("Executed once; {affected} rows returned")
        };
        Ok(QueryExecution::Page(page))
    } else {
        Ok(QueryExecution::Write(WriteResult {
            outcome: WriteOutcome::Applied,
            summary: format!("PostgreSQL command completed; {affected} rows affected"),
        }))
    }
}
