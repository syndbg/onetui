---
status: accepted
date: 2026-09-10
---

# ADR-0007: Follow live records in bounded batches

## Decision

Add a native async `Executor::follow_page(PageRequest, RequestContext)` operation. A provider declares its supported resource in `ProviderDescriptor::follow_resource`; the built-in executor enum dispatches the operation. Core and TUI do not import Kafka types. Providers without following return an unsupported-operation error and do not advertise the action.

Each call returns a finite batch and an opaque continuation, including when no records are available. The TUI runs at most one request at a time and schedules another one second after completion. This reuses the existing worker, request deadlines, cancellation and generation checks without adding a permanent streaming task or an unbounded channel.

Kafka starts at the selected partition's current read-committed stable end. Later calls read forward from the last successful cursor to a newly captured stable end. Follow cursors are separate from historical page bookmarks and belong to one executor, resource and partition. Manual assignment, disabled offset storage/commits and the private session group remain unchanged from [ADR-0006](0006-browse-kafka-with-rust-rdkafka.md). Following never joins an application consumer group.

## Display and lifetime

`f` starts or stops following. Stopping retains the displayed window; starting again captures a new end, rather than claiming to resume records missed while stopped. Navigation, inspection and command entry stop following before changing the view. Refresh returns to historical browsing. The footer distinguishes live and stopped windows from ordinary pages.

Retain at most 100 records and 1 MiB of page data, with the existing separate display-projection budget. Evict the oldest displayed records when needed and show the eviction count. These are local display evictions, not skipped broker offsets. Batches also obey the existing row, byte and native receive limits. There is no whole-topic ordering or process RSS guarantee.

An unavailable offset, backwards-moving log boundary, oversized record or request error stops following and preserves the successful cursor and displayed data. The user explicitly chooses whether to start again. Compaction and transaction markers can produce offset gaps; the connector cannot infer that every numerical gap means a lost message. Native reconnect attempts remain bounded by each request deadline.

## Alternatives

- Repeated historical refresh would replay older data and reset the browsing window. A separate operation keeps historical paging unchanged.
- An endless stream would need another event protocol, buffering and shutdown path. Finite batches fit the existing executor lifetime and cancellation model. The trade-off is polling latency and a maximum catch-up rate of one bounded batch per interval.
- Consumer-group subscriptions would introduce group membership, rebalances and shared positions. Manual partition reads preserve the read-only browser contract.

The traffic simulator is a separate fixture producer, not a write feature in OneTUI. It sends one record every 15 seconds to the fixed local `demo_live` topic.
