use anyhow::{Result, anyhow, ensure};
use futures_util::TryStreamExt;
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row, display};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, types::ToSql};

use crate::browse::pg_error;

const FIELD_BYTES: usize = 1024 * 1024;
const MAX_COLUMNS: usize = 256;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Shape {
    relation: u32,
    columns: Vec<(String, u32, i32, u32)>,
    index: Option<u32>,
    keys: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    shape: Shape,
    values: Vec<String>,
    offset: i64,
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(crate) async fn fetch(
    client: &Client,
    resource: &Resource,
    offset: i64,
    continuation: Option<&str>,
) -> Result<Page> {
    let [schema, relation] = resource.path.as_slice() else {
        anyhow::bail!("Invalid PostgreSQL row path");
    };
    ensure!(offset >= 0, "Invalid row offset");
    ensure!(
        (offset == 0) == continuation.is_none(),
        "Missing or unexpected continuation; refresh"
    );
    let cursor: Option<Cursor> = continuation
        .map(|token| {
            ensure!(token.len() <= PAGE_BYTES, "Invalid continuation; refresh");
            serde_json::from_str(token).map_err(|_| anyhow!("Invalid continuation; refresh"))
        })
        .transpose()?;
    let table = format!("{}.{}", quote(schema), quote(relation));
    client
        .batch_execute("BEGIN READ ONLY")
        .await
        .map_err(pg_error)?;
    // Lock the relation before reading its catalog shape; finish the transaction before display.
    client
        .simple_query(&format!("SELECT 1 FROM {table} LIMIT 0"))
        .await
        .map_err(pg_error)?;
    let oid: u32 = client
        .query_one(
            "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r','p','v','m','f')",
            &[schema, relation],
        ).await.map_err(pg_error)?.get(0);
    let attributes = client.query(
        "SELECT attname::text, atttypid, atttypmod, pg_catalog.format_type(atttypid, atttypmod), attnum, attcollation FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped ORDER BY attnum LIMIT 257",
        &[&oid],
    ).await.map_err(pg_error)?;
    ensure!(
        !attributes.is_empty() && attributes.len() <= MAX_COLUMNS,
        "Row browsing supports 1..256 columns; use column metadata instead"
    );
    // Reject keys whose uniqueness or comparison semantics do not cover every returned row.
    let index = client.query_opt(
        "SELECT i.indexrelid, i.indkey::smallint[], i.indnkeyatts FROM pg_catalog.pg_index i \
         JOIN pg_catalog.pg_class c ON c.oid = i.indrelid \
         JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid \
         JOIN pg_catalog.pg_am am ON am.oid = ic.relam \
         WHERE i.indrelid = $1 AND i.indisunique AND i.indisvalid AND i.indisready \
         AND i.indimmediate AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname = 'btree' \
         AND (c.relkind = 'p' OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = c.oid)) \
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) \
             LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
             LEFT JOIN pg_catalog.pg_opclass op ON op.oid = i.indclass[(k.ord - 1)::int] \
             WHERE k.ord <= i.indnkeyatts AND \
             (a.attnum IS NULL OR NOT a.attnotnull OR a.atttypid NOT IN (20,25) \
              OR NOT op.opcdefault OR i.indcollation[(k.ord - 1)::int] <> a.attcollation)) \
         ORDER BY i.indisprimary DESC, i.indexrelid LIMIT 1",
        &[&oid],
    ).await.map_err(pg_error)?;
    let (index_id, keys) = if let Some(index) = index {
        let index_id: u32 = index.get(0);
        let key_count: i16 = index.get(2);
        let numbers: Vec<i16> = index.get(1);
        let keys = numbers
            .iter()
            .take(key_count as usize)
            .map(|number| {
                attributes
                    .iter()
                    .position(|a| a.get::<_, i16>(4) == *number)
                    .ok_or_else(|| anyhow!("Relation changed; refresh"))
            })
            .collect::<Result<Vec<_>>>()?;
        (Some(index_id), keys)
    } else {
        (None, Vec::new())
    };
    let shape = Shape {
        relation: oid,
        columns: attributes
            .iter()
            .map(|a| (a.get(0), a.get(1), a.get(2), a.get(5)))
            .collect(),
        index: index_id,
        keys,
    };
    if let Some(cursor) = &cursor {
        ensure!(
            cursor.shape == shape
                && cursor.offset == offset
                && cursor.values.len() == shape.keys.len(),
            "Relation or paging key changed; refresh"
        );
    }
    let key_names: Vec<_> = shape
        .keys
        .iter()
        .map(|&i| format!("src.{}", quote(&shape.columns[i].0)))
        .collect();
    let mut parameters: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let predicate = if let Some(cursor) = &cursor
        && !shape.keys.is_empty()
    {
        for (&i, value) in shape.keys.iter().zip(&cursor.values) {
            parameters.push(if shape.columns[i].1 == 20 {
                Box::new(
                    value
                        .parse::<i64>()
                        .map_err(|_| anyhow!("Invalid bigint continuation; refresh"))?,
                )
            } else {
                Box::new(value.clone())
            });
        }
        let bindings = (1..=parameters.len())
            .map(|i| format!("{}{i}", '$'))
            .collect::<Vec<_>>();
        format!(
            "WHERE ({}) OPERATOR(pg_catalog.>) ({})",
            key_names.join(", "),
            bindings.join(", ")
        )
    } else {
        String::new()
    };
    let ordering = if key_names.is_empty() {
        String::new()
    } else {
        format!("ORDER BY {}", key_names.join(", "))
    };
    parameters.push(Box::new(PAGE_SIZE + 1));
    let limit = format!("LIMIT {}{}", '$', parameters.len());
    let skip = if shape.keys.is_empty() {
        parameters.push(Box::new(offset));
        format!("OFFSET {}{}", '$', parameters.len())
    } else {
        String::new()
    };
    let mut projections: Vec<_> = shape
        .columns
        .iter()
        .enumerate()
        .map(|(i, column)| format!("src.{}::pg_catalog.text AS c{i}", quote(&column.0)))
        .collect();
    projections.extend(
        key_names
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{name} AS k{i}")),
    );
    let size = (0..shape.columns.len())
        .map(|i| format!("COALESCE(pg_catalog.octet_length(c{i}), 0)::bigint"))
        .collect::<Vec<_>>()
        .join(" + ");
    let guarded = (0..shape.columns.len())
        .map(|i| format!("CASE WHEN NOT oversized THEN c{i} END"))
        .collect::<Vec<_>>()
        .join(", ");
    let outer_order = if shape.keys.is_empty() {
        String::new()
    } else {
        format!(
            "ORDER BY {}",
            (0..shape.keys.len())
                .map(|i| format!("k{i}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    // Materialization covers only LIMIT+1 rows. Native keys preserve numeric/collation ordering.
    // Flag oversized data separately from SQL NULL; omit its text from the response.
    let sql = format!(
        "WITH page AS MATERIALIZED (SELECT {} FROM {table} AS src {predicate} {ordering} {limit} {skip}), \
         sized AS (SELECT *, ({size}) > {FIELD_BYTES} AS oversized FROM page) \
         SELECT {guarded}, oversized FROM sized {outer_order}",
        projections.join(", ")
    );
    let stream = client
        .query_raw(&sql, parameters.iter().map(|p| &**p as &(dyn ToSql + Sync)))
        .await
        .map_err(pg_error)?;
    tokio::pin!(stream);
    let mut page = Page {
        columns: attributes
            .iter()
            .map(|a| Column {
                name: display(a.get::<_, &str>(0)),
                datatype: display(a.get::<_, &str>(3)),
            })
            .collect(),
        notice: if shape.keys.is_empty() {
            "OFFSET: best-effort order, deep pages may be slow; no cross-page snapshot"
        } else {
            "Keyset: unique bigint/text key; no cross-page snapshot"
        }
        .into(),
        ..Page::default()
    };
    let mut last_key = Vec::new();
    while let Some(row) = stream.try_next().await.map_err(pg_error)? {
        if page.rows.len() == PAGE_SIZE as usize {
            page.next = true;
            break;
        }
        ensure!(
            !row.get::<_, bool>(shape.columns.len()),
            "Row exceeds the 1 MiB server text limit; current page retained"
        );
        let raw = (0..shape.columns.len())
            .map(|i| row.try_get::<_, Option<String>>(i).map_err(pg_error))
            .collect::<Result<Vec<_>>>()?;
        last_key = shape
            .keys
            .iter()
            .map(|&i| {
                raw[i]
                    .clone()
                    .ok_or_else(|| anyhow!("Paging key became NULL; refresh"))
            })
            .collect::<Result<Vec<_>>>()?;
        page.rows.push(Row {
            cells: raw.into_iter().map(|v| v.map(|v| display(&v))).collect(),
            target: None,
        });
        ensure!(
            page.bytes() <= PAGE_BYTES,
            "Row page exceeds the 1 MiB display limit; current page retained"
        );
    }
    if page.next {
        page.continuation = Some(serde_json::to_string(&Cursor {
            shape,
            values: last_key,
            offset: offset
                .checked_add(PAGE_SIZE)
                .ok_or_else(|| anyhow!("Row offset exhausted; refresh"))?,
        })?);
    }
    ensure!(
        page.bytes() <= PAGE_BYTES,
        "Row page exceeds the 1 MiB display limit; current page retained"
    );
    client.batch_execute("COMMIT").await.map_err(pg_error)?;
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted_individually() {
        assert_eq!(quote("odd\"'; --"), "\"odd\"\"'; --\"");
        assert_eq!(quote("a.b"), "\"a.b\"");
    }
}
