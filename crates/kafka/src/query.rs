use anyhow::{Result, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::{PageRequest, QueryRequest};
use onetui_core::{PAGE_BYTES, Page, Resource};
use serde::{Deserialize, Serialize};

pub(crate) const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.query",
    description: "Read-committed partition replay from an offset or timestamp; no commits",
    columns: &["offset", "timestamp_ms", "key", "value", "headers"],
    paging: true,
    actions: &[],
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Replay {
    pub offset: Option<i64>,
    pub timestamp_ms: Option<i64>,
    pub end_offset: Option<i64>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Position {
    query: String,
    inner: String,
}

pub(crate) fn prepare(request: &QueryRequest, identity: u64) -> Result<(Replay, PageRequest)> {
    request.validate()?;
    ensure!(
        request.page.resource.id == RESOURCE.id,
        "Invalid Kafka query resource"
    );
    let input: Replay = serde_json::from_str(&request.text)?;
    ensure!(
        input.offset.is_none() || input.timestamp_ms.is_none(),
        "Choose offset or timestamp_ms, not both"
    );
    ensure!(
        [input.offset, input.timestamp_ms, input.end_offset]
            .into_iter()
            .flatten()
            .all(|n| n >= 0),
        "Kafka replay positions must be nonnegative signed 64-bit integers"
    );
    ensure!(
        input
            .offset
            .zip(input.end_offset)
            .is_none_or(|(start, end)| start <= end),
        "Kafka replay offset exceeds end_offset"
    );
    let continuation = request
        .page
        .continuation
        .as_deref()
        .map(|token| {
            ensure!(
                token.len() <= PAGE_BYTES,
                "Invalid Kafka query continuation"
            );
            let position: Position = serde_json::from_str(token)?;
            ensure!(
                position.query == request.text,
                "Continuation belongs to another query; run from the beginning"
            );
            Ok(position.inner)
        })
        .transpose()?;
    let page = PageRequest {
        resource: Resource::new("kafka.records", request.page.resource.path.clone()),
        continuation,
    };
    crate::browse::validate(&page, identity, false)?;
    Ok((input, page))
}

pub(crate) fn finish(mut page: Page, query: String) -> Result<Page> {
    page.continuation = page
        .continuation
        .map(|inner| serde_json::to_string(&Position { query, inner }))
        .transpose()?;
    ensure!(
        page.bytes() <= PAGE_BYTES,
        "Kafka query page exceeds 1 MiB; current page and bookmark retained"
    );
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(text: &str) -> QueryRequest {
        QueryRequest {
            page: PageRequest {
                resource: Resource::new(RESOURCE.id, vec!["events".into(), "0".into()]),
                continuation: None,
            },
            text: text.into(),
        }
    }

    #[test]
    fn replay_is_strict_and_bookmarks_bind_query_session_and_partition() {
        for text in [
            "{}",
            r#"{"offset":100,"end_offset":250}"#,
            r#"{"timestamp_ms":1700000000000}"#,
        ] {
            assert!(prepare(&request(text), 7).is_ok(), "{text}");
        }
        for text in [
            "",
            "[]",
            r#"{"offset":-1}"#,
            r#"{"timestamp_ms":-1}"#,
            r#"{"end_offset":-1}"#,
            r#"{"offset":2,"end_offset":1}"#,
            r#"{"offset":0,"timestamp_ms":0}"#,
            r#"{"offset":1.5}"#,
            r#"{"offset":9223372036854775808}"#,
            r#"{"limit":100}"#,
        ] {
            assert!(prepare(&request(text), 7).is_err(), "{text}");
        }
        let mut request = request(r#"{"offset":100}"#);
        let (_, inner) = prepare(&request, 7).unwrap();
        let page = crate::browse::finish(
            Page::default(),
            &inner.resource,
            7,
            Some((200, Some(300))),
            false,
        )
        .unwrap();
        request.page.continuation = finish(page, request.text.clone()).unwrap().continuation;
        assert!(prepare(&request, 7).unwrap().1.continuation.is_some());
        assert!(prepare(&request, 8).is_err());
        request.page.resource.path[1] = "1".into();
        assert!(prepare(&request, 7).is_err());
        request.page.resource.path[1] = "0".into();
        request.text = "{}".into();
        assert!(prepare(&request, 7).is_err());
        let full = Page {
            notice: "x".repeat(PAGE_BYTES - 1),
            continuation: Some("0".into()),
            ..Page::default()
        };
        assert_eq!(full.bytes(), PAGE_BYTES);
        assert!(
            finish(full, "{}".into()).is_err(),
            "query wrapper counts toward the retained-page budget"
        );
    }
}
