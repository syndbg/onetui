use std::ops::Range;

use anyhow::{Result, anyhow, ensure};
use onetui_core::provider::PageRequest;
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page};
use serde::{Deserialize, Serialize};

pub(crate) const PARTITIONS: usize = 32;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cursor {
    executor: u64,
    topic: String,
    turn: usize,
    partitions: Vec<Partition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Partition {
    id: i32,
    next: i64,
    end: i64,
}

pub(crate) fn validate(
    request: &PageRequest,
    identity: u64,
    following: bool,
) -> Result<Option<Cursor>> {
    ensure!(
        request.resource.id == "kafka.records"
            && request.resource.path.len() == 1
            && crate::browse::valid_topic(&request.resource.path[0]),
        "Unsupported Kafka topic-wide resource or path"
    );
    let Some(token) = &request.continuation else {
        return Ok(None);
    };
    ensure!(token.len() <= 4096, "Invalid Kafka topic continuation");
    let prefix = if following { "follow:topic:" } else { "topic:" };
    let cursor: Cursor = serde_json::from_str(
        token
            .strip_prefix(prefix)
            .ok_or_else(|| anyhow!("Invalid Kafka topic continuation mode"))?,
    )
    .map_err(|_| anyhow!("Invalid Kafka topic continuation"))?;
    ensure!(
        cursor.executor == identity && cursor.topic == request.resource.path[0],
        "Kafka continuation belongs to another session or topic; refresh"
    );
    ensure!(
        cursor.partitions.len() <= PARTITIONS
            && cursor.turn < cursor.partitions.len().max(1)
            && cursor
                .partitions
                .iter()
                .all(|p| p.id >= 0 && p.next >= 0 && p.end >= p.next)
            && cursor.partitions.windows(2).all(|p| p[0].id < p[1].id),
        "Invalid Kafka topic cursor positions"
    );
    Ok(Some(cursor))
}

fn token(cursor: &Cursor, following: bool) -> Result<String> {
    let prefix = if following { "follow:topic:" } else { "topic:" };
    let token = format!("{prefix}{}", serde_json::to_string(cursor)?);
    ensure!(token.len() <= 4096, "Kafka topic cursor exceeds 4 KiB");
    Ok(token)
}

pub(crate) fn page(
    request: &PageRequest,
    identity: u64,
    following: bool,
    byte_limit: usize,
    mut windows: Vec<(i32, i64, i64)>,
    mut read: impl FnMut(i32, Range<i64>, usize) -> Result<(Page, i64)>,
) -> Result<Page> {
    let saved = validate(request, identity, following)?;
    windows.sort_by_key(|w| w.0);
    ensure!(
        windows.len() <= PARTITIONS
            && windows
                .iter()
                .all(|&(id, low, end)| id >= 0 && low >= 0 && end >= low)
            && windows.windows(2).all(|w| w[0].0 < w[1].0),
        "Invalid Kafka topic partition windows"
    );
    let mut cursor = saved.unwrap_or_else(|| Cursor {
        executor: identity,
        topic: request.resource.path[0].clone(),
        turn: 0,
        partitions: windows
            .iter()
            .map(|&(id, low, end)| Partition {
                id,
                next: if following { end } else { low },
                end,
            })
            .collect(),
    });
    ensure!(
        cursor
            .partitions
            .iter()
            .map(|p| p.id)
            .eq(windows.iter().map(|w| w.0)),
        "Kafka partition set changed; refresh or restart following"
    );
    for (partition, &(_, low, stable_end)) in cursor.partitions.iter_mut().zip(&windows) {
        ensure!(
            partition.next >= low && partition.end <= stable_end,
            "Kafka partition {} offsets unavailable or moved backwards; refresh or restart following",
            partition.id
        );
        if following {
            partition.end = stable_end;
        }
    }
    let mut page = crate::browse::page(
        &request.resource,
        &format!(
            "Topic-wide | {} partitions | rotating partition batches, offset order within each partition; no global time order or snapshot; no commits.",
            cursor.partitions.len()
        ),
    );
    // Reserve the longest possible cursor before accepting rows. Otherwise a
    // near-limit value could fit now but overflow when its bookmark is attached.
    let mut upper = cursor.clone();
    upper.turn = upper.partitions.len().saturating_sub(1);
    for p in &mut upper.partitions {
        p.next = p.end;
    }
    page.continuation = Some(token(&upper, following)?);
    let count = cursor.partitions.len();
    let active = cursor.partitions.iter().filter(|p| p.next < p.end).count();
    let quota = (PAGE_SIZE as usize).div_ceil(active.max(1));
    let start = cursor.turn;
    'partitions: for step in 0..count {
        let index = (start + step) % count;
        let p = &mut cursor.partitions[index];
        if p.next == p.end {
            continue;
        }
        let limit = quota.min(PAGE_SIZE as usize - page.rows.len());
        let (batch, next) = read(p.id, p.next..p.end, limit)?;
        ensure!(
            batch.rows.len() <= limit && next >= p.next && next <= p.end,
            "Kafka returned an invalid partition batch"
        );
        for mut row in batch.rows {
            let offset: i64 = row
                .cells
                .first()
                .and_then(Option::as_ref)
                .and_then(|v| v.text())
                .ok_or_else(|| anyhow!("Kafka record has no offset"))?
                .parse()?;
            ensure!(
                offset >= p.next && offset < next,
                "Kafka returned a record outside its partition batch"
            );
            row.cells.insert(0, Some(p.id.to_string().into()));
            page.rows.push(row);
            if page.bytes() > byte_limit {
                page.rows.pop();
                ensure!(
                    !page.rows.is_empty(),
                    "Kafka topic record and cursor exceed 1 MiB; current page retained"
                );
                p.next = offset;
                // Give the first unaccepted record room on the next page.
                cursor.turn = index;
                break 'partitions;
            }
            p.next = offset
                .checked_add(1)
                .ok_or_else(|| anyhow!("Kafka offset overflow"))?;
        }
        p.next = next;
        cursor.turn = (index + 1) % count;
        if page.rows.len() == PAGE_SIZE as usize {
            break;
        }
    }
    page.next = following || cursor.partitions.iter().any(|p| p.next < p.end);
    page.continuation = if page.next {
        Some(token(&cursor, following)?)
    } else {
        None
    };
    ensure!(
        page.rows.len() <= PAGE_SIZE as usize && page.bytes() <= PAGE_BYTES,
        "Kafka topic page exceeds 100 rows or 1 MiB"
    );
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::{Resource, Row};

    fn request(continuation: Option<String>) -> PageRequest {
        PageRequest {
            resource: Resource::new("kafka.records", vec!["demo".into()]),
            continuation,
        }
    }

    fn records(_: i32, window: Range<i64>, limit: usize) -> Result<(Page, i64)> {
        let end = (window.start + limit as i64).min(window.end);
        Ok((
            Page {
                rows: (window.start..end)
                    .map(|n| Row {
                        cells: vec![Some(n.to_string().into())],
                        target: None,
                    })
                    .collect(),
                ..Page::default()
            },
            end,
        ))
    }

    #[test]
    fn rotating_pages_bookmarks_and_cursor_validation() {
        let windows: Vec<_> = (0..32).map(|id| (id, 0, 10)).collect();
        let first = page(
            &request(None),
            7,
            false,
            PAGE_BYTES,
            windows.clone(),
            records,
        )
        .unwrap();
        assert_eq!(first.rows.len(), 100);
        assert_eq!(first.rows[0].cells[0], Some("0".into()));
        let second = page(
            &request(first.continuation.clone()),
            7,
            false,
            PAGE_BYTES,
            windows.clone(),
            records,
        )
        .unwrap();
        assert_eq!(second.rows[0].cells[0], Some("25".into()));
        let replay = page(
            &request(first.continuation.clone()),
            7,
            false,
            PAGE_BYTES,
            windows.clone(),
            records,
        )
        .unwrap();
        assert_eq!(
            second.rows.iter().map(|r| &r.cells).collect::<Vec<_>>(),
            replay.rows.iter().map(|r| &r.cells).collect::<Vec<_>>()
        );
        let mut all = first.rows;
        let mut continuation = first.continuation;
        while continuation.is_some() {
            let next = page(
                &request(continuation),
                7,
                false,
                PAGE_BYTES,
                windows.clone(),
                records,
            )
            .unwrap();
            all.extend(next.rows);
            continuation = next.continuation;
        }
        let keys: std::collections::BTreeSet<_> = all
            .iter()
            .map(|r| {
                (
                    r.cells[0].as_ref().unwrap().text(),
                    r.cells[1].as_ref().unwrap().text(),
                )
            })
            .collect();
        assert_eq!(keys.len(), 320);
        assert_eq!(all.len(), 320);
        let request = request(second.continuation);
        assert!(validate(&request, 8, false).is_err());
        assert!(validate(&request, 7, true).is_err());
        assert!(page(&request, 7, false, PAGE_BYTES, vec![(0, 0, 10)], records).is_err());
        assert!(
            page(
                &request,
                7,
                false,
                PAGE_BYTES,
                (0..33).map(|id| (id, 0, 10)).collect(),
                records
            )
            .is_err()
        );
    }

    #[test]
    fn following_sparse_windows_retention_and_byte_budget() {
        let initial = page(
            &request(None),
            7,
            true,
            PAGE_BYTES,
            vec![(0, 0, 5), (1, 0, 7)],
            |_, _, _| panic!("initial follow only captures ends"),
        )
        .unwrap();
        assert!(initial.rows.is_empty());
        let live = page(
            &request(initial.continuation.clone()),
            7,
            true,
            PAGE_BYTES,
            vec![(0, 0, 8), (1, 0, 9)],
            records,
        )
        .unwrap();
        assert_eq!(live.rows.len(), 5);
        assert!(
            page(
                &request(initial.continuation.clone()),
                7,
                true,
                PAGE_BYTES,
                vec![(0, 6, 8), (1, 0, 9)],
                records
            )
            .is_err()
        );
        assert!(
            page(
                &request(initial.continuation),
                7,
                true,
                PAGE_BYTES,
                vec![(0, 0, 4), (1, 0, 9)],
                records
            )
            .is_err()
        );
        let big = |_: i32, window: Range<i64>, _: usize| {
            Ok((
                Page {
                    rows: vec![Row {
                        cells: vec![
                            Some(window.start.to_string().into()),
                            Some("x".repeat(600_000).into()),
                        ],
                        target: None,
                    }],
                    ..Page::default()
                },
                window.end,
            ))
        };
        let first = page(
            &request(None),
            7,
            false,
            PAGE_BYTES,
            vec![(0, 0, 1), (1, 0, 1)],
            big,
        )
        .unwrap();
        assert_eq!(first.rows.len(), 1);
        let second = page(
            &request(first.continuation),
            7,
            false,
            PAGE_BYTES,
            vec![(0, 0, 1), (1, 0, 1)],
            big,
        )
        .unwrap();
        assert_eq!(second.rows[0].cells[0], Some("1".into()));
        assert!(!second.next);
        let empty = page(&request(None), 7, true, PAGE_BYTES, vec![], records).unwrap();
        assert!(empty.rows.is_empty() && empty.continuation.is_some());
    }
}
