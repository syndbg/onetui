---
status: accepted
date: 2026-09-09
---

# ADR-0005: Run native queries through providers

## Decision

Use one query editor and result-viewing flow, with each provider owning its query syntax, validation, execution and continuation. PostgreSQL accepts read-only SQL; Qdrant accepts filtered Scroll JSON scoped to a collection. Do not translate between them or introduce a universal query language.

`e` and `:query` open a compact editor above the retained rows, so editing or a failed request does not hide the data. Ctrl-R or F5 explicitly executes a draft; Enter inserts a newline. Successful execution focuses the result table and keeps the executed query visible above it. Context hints follow the active editor or results. The command bar remains for `:` commands and `/` page-local filtering. Typing a query never sends a datasource request.

[Query usage](../queries.md) documents the supported syntax, examples and current limits.

## Provider and UI boundary

The [core contract](../../crates/core/src/provider.rs) exposes an optional `QueryDescriptor` with the language, example, result resource and required resource-path depth. The same descriptor supplies the UI and offline `onetui schema` catalog. A provider without this capability does not expose the query action.

The worker submits the draft and its paging request through `Executor::query_page`:

```rust
pub struct QueryRequest {
    pub page: PageRequest,
    pub text: String,
}

fn query_page(
    &self,
    request: QueryRequest,
    context: RequestContext,
) -> impl Future<Output = Result<Page>> + Send;
```

The [built-in executor enum](../../src/providers.rs) delegates this operation with native async dispatch, following [ADR-0002](0002-use-static-enum-dispatch-for-built-in-providers.md). Core stays independent of SDKs and Ratatui. The [existing worker](../../crates/tui/src/worker.rs) owns cancellation and deadlines; the TUI rejects stale results and reuses its tables, row inspectors and [value formats](0004-preserve-values-and-select-display-formats.md). Query syntax and SDK conversion stay out of the renderer.

## Execution and lifetime

The [PostgreSQL connector](../../crates/postgres/src/query.rs) prepares one statement in a read-only transaction and requires a row-producing query that fits a derived table. Server-side size guards bound returned values before decoding. After a successful page it rolls back and runs `DISCARD ALL` in a separate request, clearing session settings and advisory locks before reuse. Errors and cancellation retire the transport. SQL parsing and database enforcement replace keyword-based safety checks. Least-privilege credentials remain necessary: read-only transactions do not sandbox functions with external effects.

SQL pages use independent LIMIT/OFFSET reads. An explicit unique ordering makes traversal more predictable, but there is no cross-page snapshot and deep offsets can repeat expensive work. No transaction or cursor stays open while the user reads the screen, preserving [ADR-0001's lifecycle](0001-register-providers-and-own-session-lifecycles.md).

The [Qdrant connector](../../crates/qdrant/src/query.rs) converts a strict JSON subset into native Scroll requests: `must`, `should` and `must_not` field conditions with exact matches or numeric ranges. Unsupported fields are errors rather than silently omitted predicates. Results contain point IDs; payloads and vectors remain lazy reads. OneTUI owns the native offset, so editing a draft cannot silently resume an older query.

Query text is bounded to 16 KiB. Pages retain the existing 100-item ceiling and 1 MiB value budget; Qdrant also caps RPC decoding at 1 MiB. The existing request timeout applies. These are work and retained-data limits, not an RSS guarantee or a bound on database computation before cancellation.

Drafts and query tokens stay in memory, outside configuration and application logs. Database-side query logging remains possible. Each token is bound to its executor, resource and exact draft text. Successful execution of changed text starts fresh results and bookmarks; failed execution preserves the draft and prior data. Returning to browsing restores its retained view. Evicted result pages may be refetched from bookmarks, so revisiting them can show changed data.

## Alternatives and scope

- A shared SQL-like language would need to define semantics across relational rows, point filters and future message backends. Native syntax keeps those differences visible.
- Executing on each keystroke would turn editing into repeated remote work. Live filtering remains appropriate only for cached page text.
- A long-lived SQL cursor would retain transaction or materialization state while the user is idle. Independent pages accept weaker consistency to avoid that lifetime.
- A second query worker or runtime plugin layer would duplicate the existing session, cancellation and dispatch mechanisms. Queries use the selected executor instead.

The initial query operation is read-only. SQL writes, parameters and utility commands, arbitrary Qdrant endpoints, advanced filter forms and vector similarity search are outside this slice. This decision adds no configuration file or setting. Saved drafts and query history would require a separate retention and secret-handling decision.
