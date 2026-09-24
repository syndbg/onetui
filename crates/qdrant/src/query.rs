use anyhow::{Result, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::{PageRequest, QueryRequest};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Resource};
use qdrant_client::qdrant::{
    Condition, Filter, PointStruct, Range, ScrollPoints, ScrollPointsBuilder, UpsertPoints,
    UpsertPointsBuilder,
};
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    operation: String,
    points: Vec<WritePoint>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WritePoint {
    id: WriteId,
    vector: Vec<f32>,
    payload: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WriteId {
    Number(u64),
    Uuid(String),
}

impl WriteId {
    fn native(self) -> Result<crate::browse::Id> {
        match self {
            Self::Number(id) => Ok(crate::browse::Id::Num(id)),
            Self::Uuid(id) => crate::browse::Id::parse(&id),
        }
    }
}

fn prepare_write(resource: &Resource, value: serde_json::Value) -> Result<PointStruct> {
    ensure!(
        resource.id == RESOURCE.id && resource.path.len() == 1 && !resource.path[0].is_empty(),
        "Qdrant upsert requires a selected collection"
    );
    let write: Write = serde_json::from_value(value)?;
    ensure!(
        write.operation == "upsert",
        "Unsupported Qdrant write operation"
    );
    ensure!(
        write.points.len() == 1,
        "Qdrant upsert accepts exactly one point"
    );
    let point = write.points.into_iter().next().expect("one point");
    ensure!(!point.vector.is_empty(), "Point vector must not be empty");
    ensure!(
        point.vector.iter().all(|value| value.is_finite()),
        "Point vector values must be finite"
    );
    let id = point.id.native()?;
    let payload: qdrant_client::Payload = match point.payload {
        Some(value @ serde_json::Value::Object(_)) => value.try_into()?,
        Some(_) => anyhow::bail!("Point payload must be a JSON object"),
        None => qdrant_client::Payload::default(),
    };
    Ok(PointStruct::new(id.native(), point.vector, payload))
}

pub(crate) fn prepare_upsert(request: &QueryRequest) -> Result<Option<UpsertPoints>> {
    request.validate()?;
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&request.text) else {
        return Ok(None);
    };
    if !value
        .as_object()
        .is_some_and(|object| object.contains_key("operation"))
    {
        return Ok(None);
    }
    let point = prepare_write(&request.page.resource, value)?;
    Ok(Some(
        UpsertPointsBuilder::new(&request.page.resource.path[0], vec![point])
            .wait(true)
            .into(),
    ))
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

    #[test]
    fn point_upsert_is_strict_and_scoped_to_one_collection() {
        let resource = Resource::new(RESOURCE.id, vec!["demo".into()]);
        let request = |resource: Resource, text: &str| QueryRequest {
            page: PageRequest {
                resource,
                continuation: None,
            },
            text: text.into(),
        };
        let text = r#"{"operation":"upsert","points":[{"id":42,"vector":[0.1,0.2],"payload":{"label":"demo"}}]}"#;
        assert!(
            prepare_upsert(&request(resource.clone(), text))
                .unwrap()
                .is_some()
        );
        assert!(
            prepare_upsert(&request(resource.clone(), r#"{"filter":{"must":[]}}"#))
                .unwrap()
                .is_none()
        );

        for invalid in [
            r#"{"operation":"delete","points":[]}"#,
            r#"{"operation":"upsert","collection":"other","points":[{"id":42,"vector":[1]}]}"#,
            r#"{"operation":"upsert","points":[]}"#,
            r#"{"operation":"upsert","points":[{"id":42,"vector":[]},{"id":43,"vector":[1]}]}"#,
            r#"{"operation":"upsert","points":[{"id":-1,"vector":[1]}]}"#,
            r#"{"operation":"upsert","points":[{"id":"not-a-uuid","vector":[1]}]}"#,
            r#"{"operation":"upsert","points":[{"id":42,"vector":[1],"payload":[]}]}"#,
        ] {
            assert!(
                prepare_upsert(&request(resource.clone(), invalid)).is_err(),
                "{invalid}"
            );
        }
        assert!(prepare_upsert(&request(Resource::new(RESOURCE.id, vec![]), text)).is_err());
        assert!(prepare_upsert(&request(resource, &" ".repeat(16385))).is_err());
    }
}
