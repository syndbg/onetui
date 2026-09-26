use anyhow::{Result, ensure};
use onetui_core::catalog::ResourceDescriptor;
use onetui_core::provider::{PageRequest, QueryRequest};
use onetui_core::{PAGE_BYTES, Page, Resource};
use serde::{Deserialize, Serialize};

use crate::statement::{Replay, Statement};

pub(crate) const RESOURCE: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.query",
    description: "Partition records from an offset or timestamp, or a publish delivery report",
    columns: &["offset", "timestamp_ms", "key", "value", "headers"],
    paging: true,
    actions: &[],
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Position {
    query: String,
    inner: String,
}

/// Prefill the editor with a verb line naming whatever the current view has open.
pub(crate) fn watermark(resource: &Resource, _: Option<&onetui_core::Row>) -> String {
    match resource.path.as_slice() {
        [topic, partition, ..] => format!("CONSUME {topic}/{partition}"),
        [topic] => format!("PRODUCE {topic}\n\n{{\"value\":\"\"}}"),
        [] => "PRODUCE topic\n\n{\"value\":\"\"}".into(),
    }
}

/// Resolve a replay submission to the partition read it describes. Produce submissions do
/// not page or browse, so they never reach this path.
pub(crate) fn prepare(request: &QueryRequest, identity: u64) -> Result<(Replay, PageRequest)> {
    request.validate()?;
    ensure!(
        request.page.resource.id == RESOURCE.id,
        "Invalid Kafka query resource"
    );
    let Statement::Replay { target, replay } = crate::statement::parse(&request.text)? else {
        anyhow::bail!("Kafka PRODUCE returns a delivery report; it does not page")
    };
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
    // The verb line names the target, so a replay reads it rather than the editor's
    // resource path, which the TUI truncates to the query's declared depth.
    let page = PageRequest {
        resource: Resource::new(
            "kafka.records",
            vec![
                target.topic,
                target
                    .partition
                    .expect("replay requires a partition")
                    .to_string(),
            ],
        ),
        continuation,
    };
    crate::browse::validate(&page, identity, false)?;
    Ok((replay, page))
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
                resource: Resource::new(RESOURCE.id, vec!["events".into()]),
                continuation: None,
            },
            text: text.into(),
        }
    }

    #[test]
    fn replay_targets_the_verb_line_and_bookmarks_bind_query_session_and_partition() {
        // The verb line names the partition, not the editor's resource path.
        let (_, page) = prepare(&request("CONSUME events/0 offsets 100.."), 7).unwrap();
        assert_eq!(page.resource.path, vec!["events".to_string(), "0".into()]);
        // A produce submission has no page to read.
        assert!(prepare(&request("PRODUCE events\n\n{\"value\":\"a\"}"), 7).is_err());

        let mut request = request("CONSUME events/0 offsets 100..");
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
        assert!(prepare(&request, 8).is_err(), "bookmarks bind the session");
        // A bookmark from another partition or another query text does not carry over.
        let mut moved = request.text.replace("events/0", "events/1");
        std::mem::swap(&mut moved, &mut request.text);
        assert!(prepare(&request, 7).is_err());
        request.text = moved;
        request.text = "CONSUME events/0".into();
        assert!(prepare(&request, 7).is_err());

        let full = Page {
            notice: "x".repeat(PAGE_BYTES - 1),
            continuation: Some("0".into()),
            ..Page::default()
        };
        assert_eq!(full.bytes(), PAGE_BYTES);
        assert!(
            finish(full, "CONSUME events/0".into()).is_err(),
            "query wrapper counts toward the retained-page budget"
        );
    }

    #[test]
    fn watermarks_name_the_open_target() {
        assert_eq!(
            watermark(
                &Resource::new(RESOURCE.id, vec!["events".into(), "2".into()]),
                None
            ),
            "CONSUME events/2"
        );
        assert!(
            watermark(&Resource::new(RESOURCE.id, vec!["events".into()]), None)
                .starts_with("PRODUCE events")
        );
    }
}
