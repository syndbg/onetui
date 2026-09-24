---
status: accepted
date: 2026-09-10
---

# ADR-0007: Follow live records in bounded batches

## Decision

Use native async `Executor::follow_page(PageRequest, RequestContext)` and `stop_follow(ShutdownContext)` operations. A provider declares its supported views in `ProviderDescriptor::follow_resources`; the built-in executor enum dispatches the operations. Core and TUI do not import datasource SDK types. Providers without following return an unsupported-operation error and do not advertise the action.

Each call returns a finite batch and an opaque continuation, including when no records are available. The TUI runs at most one request at a time and schedules another one second after completion. This reuses the existing worker, request deadlines, cancellation and generation checks without adding a permanent streaming task or an unbounded channel.

Kafka starts at the current read-committed stable end of the selected partition, or at independent ends for every partition in a selected topic. Later calls read forward from each successful cursor to a newly captured stable end. Follow cursors are separate from historical page bookmarks and belong to one executor, resource and partition set. Topic-wide reads rotate bounded partition batches, preserving offset order within each partition without promising global timestamp order. A changed partition set requires an explicit refresh or restart; it must not silently reset existing positions. Both modes reuse the same native reader and request deadline.

Manual assignment, disabled offset storage/commits and the private session group remain unchanged from [ADR-0006](0006-browse-kafka-with-rust-rdkafka.md). Following never joins an application consumer group.

## Display and lifetime

`f` starts or stops following. Stopping retains the displayed window; starting again captures a new end, rather than claiming to resume records missed while stopped. Navigation, inspection and command entry stop following before changing the view. Refresh returns to historical browsing. The footer distinguishes live and stopped windows from ordinary pages.

Stopping also invokes `stop_follow`, including between batches. This releases a Core NATS subscription while retaining the connection. The worker serializes cleanup before subsequent reads and bounds it by the shutdown deadline; polling providers need no cleanup operation beyond the default no-op.

Retain at most 100 records and 1 MiB of page data, with the existing separate display-projection budget. Evict the oldest displayed records when needed and show the eviction count. These are local display evictions, not skipped broker offsets. Batches also obey the existing row, byte and native receive limits. There is no whole-topic ordering or process RSS guarantee.

An unavailable offset, backwards-moving log boundary, oversized record or request error stops following and preserves the successful cursor and displayed data. The user explicitly chooses whether to start again. Compaction and transaction markers can produce offset gaps; the connector cannot infer that every numerical gap means a lost message. Native reconnect attempts remain bounded by each request deadline.

## Alternatives

- Repeated historical refresh would replay older data and reset the browsing window. A separate operation keeps historical paging unchanged.
- An endless stream would need another event protocol, buffering and shutdown path. Finite batches fit the existing executor lifetime and cancellation model. The trade-off is polling latency and a maximum catch-up rate of one bounded batch per interval.
- Consumer-group subscriptions would introduce group membership, rebalances and shared positions. Manual partition reads preserve independent Kafka browsing.

The traffic simulator is a separate fixture producer, not a write feature in OneTUI. It sends one record every 15 seconds to the fixed local `demo_live` topic.
