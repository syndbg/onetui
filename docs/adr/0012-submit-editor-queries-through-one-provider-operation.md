---
status: accepted
date: 2026-09-26
---

# ADR-0012: Submit editor queries through one provider operation

## Decision

The TUI sends every editor submission through `Executor::execute_query`. The provider decides how to run it and what result to return. The TUI does not classify a query by datasource or by whether it reads or changes data.

PostgreSQL executes one server-parsed statement once. It bounds the displayed result and does not page or refresh it, since doing so could execute the statement again. Browsing remains a separate operation.

## Context and consequences

The paged SQL path in [ADR-0005](0005-run-native-queries-through-providers.md) reruns SQL for each page. A separate editor key and `execute_once` route made the UI choose PostgreSQL execution semantics. A single provider operation removes that choice and avoids client-side SQL classification. PostgreSQL query results now stop at the display limit; table browsing still pages.
