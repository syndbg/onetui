---
status: accepted
date: 2026-09-26
---

# ADR-0013: Keep connections after query rejections

## Decision

A datasource rejecting a query does not disconnect OneTUI. The connector keeps a usable client or connection and reports the query error. PostgreSQL rolls back and clears session settings after each completed one-shot or paged SQL query, including a server rejection, then keeps its backend connection. This keeps query state out of later browsing. DynamoDB keeps its SDK client after a completed query failure. NATS keeps its client after a JetStream API rejection. Kafka keeps its native client and connected status after a request error that did not report a connection failure. Qdrant already retains its REST client when an HTTP response reports an error.

Cancellation, deadlines, broken transport, and incomplete protocol cleanup may still require retiring a session. Those cases do not prove that the datasource rejected the query or that a write did not take effect.

## Context

The earlier query and session decisions treated some query errors as connection failures. That made the connection indicator change and forced a reconnect after an ordinary rejected statement. This decision supersedes the connection-retirement behavior described for PostgreSQL editor queries in [ADR-0012](0012-submit-editor-queries-through-one-provider-operation.md). It does not change query confirmation, result limits, or one-shot submission.
