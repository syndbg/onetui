---
status: accepted
date: 2026-09-26
---

# ADR-0012: Submit editor queries through one provider operation

## Decision

The TUI sends every editor submission through `Executor::execute_query`. The provider decides how to run it and what result to return. The TUI does not classify a query by datasource or by whether it reads or changes data.

All providers use the same editor, confirmation, history and cancellation flow. Native results remain pages. Providers report a write outcome when the protocol provides that result, or when a possible write loses its response. DynamoDB reports completed client errors as `Rejected` and uncertain PartiQL results as `Unknown`. Cancellation before dispatch remains an ordinary error. A batch response stays intact because individual statements can fail even when the request succeeds.

RabbitMQ and Qdrant share `onetui-http-query` for `METHOD /path`, same-origin checks and bounded raw responses. Each provider owns authentication, TLS and its connection lifecycle. Complete HTTP responses retain their status and body, including native errors, because HTTP success alone does not prove that a publish was routed or an asynchronous operation finished. Editor requests do not follow redirects or retry automatically. The protocol tests cover native bodies, uncertain outcomes and cancellation. RabbitMQ fixture tests cover publishing and consuming through the same execution entry point.

PostgreSQL executes one server-parsed statement once. It bounds the displayed result and does not page or refresh it, since doing so could execute the statement again. Browsing remains a separate operation.

## Context and consequences

The paged SQL path in [ADR-0005](0005-run-native-queries-through-providers.md) reruns SQL for each page. A separate editor key and `execute_once` route made the UI choose PostgreSQL execution semantics. A single provider operation removes that choice and avoids client-side SQL classification. PostgreSQL query results now stop at the display limit; table browsing still pages.
