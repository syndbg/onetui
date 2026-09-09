use anyhow::{Result, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::{PageRequest, QueryRequest};
use onetui_core::{PAGE_BYTES, PAGE_SIZE};
use qdrant_client::qdrant::{Condition, Filter, Range, ScrollPoints, ScrollPointsBuilder};
use serde::{Deserialize, Serialize};

pub(crate) const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.query",
    description: "Filtered Scroll results; Enter opens point payload/vector choices",
    columns: &["id", "type"],
    paging: true,
    actions: &[],
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Scroll {
    filter: Option<Predicate>,
    limit: Option<u32>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Predicate {
    #[serde(default)]
    must: Vec<Field>,
    #[serde(default)]
    should: Vec<Field>,
    #[serde(default)]
    must_not: Vec<Field>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Field {
    key: String,
    r#match: Option<Exact>,
    range: Option<Bounds>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Exact {
    value: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bounds {
    gt: Option<f64>,
    gte: Option<f64>,
    lt: Option<f64>,
    lte: Option<f64>,
}

fn conditions(fields: Vec<Field>) -> Result<Vec<Condition>> {
    fields
        .into_iter()
        .map(|field| {
            ensure!(!field.key.is_empty(), "Filter key is empty");
            match (field.r#match, field.range) {
                (Some(value), None) => match value.value {
                    serde_json::Value::String(s) => Ok(Condition::matches(field.key, s)),
                    serde_json::Value::Bool(b) => Ok(Condition::matches(field.key, b)),
                    serde_json::Value::Number(n) if n.as_i64().is_some() => {
                        Ok(Condition::matches(field.key, n.as_i64().unwrap()))
                    }
                    _ => anyhow::bail!(
                        "match.value must be a string, boolean or signed 64-bit integer"
                    ),
                },
                (None, Some(r)) => {
                    ensure!(
                        [r.gt, r.gte, r.lt, r.lte].iter().any(Option::is_some),
                        "Range needs at least one bound"
                    );
                    Ok(Condition::range(
                        field.key,
                        Range {
                            gt: r.gt,
                            gte: r.gte,
                            lt: r.lt,
                            lte: r.lte,
                        },
                    ))
                }
                _ => anyhow::bail!("Each filter condition needs exactly one of match or range"),
            }
        })
        .collect()
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    query: String,
    inner: String,
}

pub(crate) fn prepare(request: &QueryRequest, executor: u64) -> Result<ScrollPoints> {
    request.validate()?;
    ensure!(
        request.page.resource.id == RESOURCE.id,
        "Invalid Qdrant query resource"
    );
    let input: Scroll = serde_json::from_str(&request.text)?;
    let limit = input.limit.unwrap_or(PAGE_SIZE as u32);
    ensure!(
        (1..=PAGE_SIZE as u32).contains(&limit),
        "Scroll limit must be 1..100"
    );
    let continuation = request
        .page
        .continuation
        .as_deref()
        .map(|token| {
            ensure!(token.len() <= PAGE_BYTES, "Invalid query continuation");
            let position: Position = serde_json::from_str(token)?;
            ensure!(
                position.query == request.text,
                "Continuation belongs to another query; run from the beginning"
            );
            Ok(position.inner)
        })
        .transpose()?;
    let offset = crate::browse::validate(
        &PageRequest {
            resource: request.page.resource.clone(),
            continuation,
        },
        executor,
    )?;
    let filter = input.filter.unwrap_or_default();
    let mut scroll = ScrollPointsBuilder::new(&request.page.resource.path[0])
        .limit(limit)
        .with_payload(false)
        .with_vectors(false)
        .filter(Filter {
            must: conditions(filter.must)?,
            should: conditions(filter.should)?,
            must_not: conditions(filter.must_not)?,
            ..Default::default()
        });
    if let Some(crate::browse::Offset::Point(id)) = offset {
        scroll = scroll.offset(id.native());
    }
    Ok(scroll.build())
}

pub(crate) fn continuation(query: String, inner: String) -> Result<String> {
    Ok(serde_json::to_string(&Position { query, inner })?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::Resource;

    #[test]
    fn scroll_json_is_strict_and_bounded() {
        let request = |text: &str| QueryRequest {
            page: PageRequest {
                resource: Resource::new(RESOURCE.id, vec!["demo".into()]),
                continuation: None,
            },
            text: text.into(),
        };
        let scroll = prepare(&request(r#"{"filter":{"must":[{"key":"active","match":{"value":true}},{"key":"price","range":{"gte":1}}]},"limit":20}"#), 1).unwrap();
        assert_eq!(scroll.limit, Some(20));
        assert_eq!(scroll.filter.unwrap().must.len(), 2);
        for text in [
            r#"{"limit":0}"#,
            r#"{"limit":101}"#,
            r#"{"offset":5}"#,
            r#"{"filter":{"mustt":[]}}"#,
            r#"{"filter":{"must":[{"key":"x","match":{"value":1.5}}]}}"#,
            r#"{"filter":{"must":[{"key":"x","range":{}}]}}"#,
            r#"{"with_payload":true}"#,
        ] {
            assert!(prepare(&request(text), 1).is_err(), "{text}");
        }
        assert!(prepare(&request(&" ".repeat(16385)), 1).is_err());
    }
}
