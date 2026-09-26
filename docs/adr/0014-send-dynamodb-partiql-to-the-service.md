---
status: accepted
date: 2026-09-26
---

# ADR-0014: Let DynamoDB parse PartiQL statements

## Decision

Send PartiQL statements from the query editor to DynamoDB without parsing their SQL or restricting them to the selected table. The statement names its target. Keep the JSON request shape, typed parameters, response limits, and native read bookmarks. Show successful responses as returned, including responses with no items.

Disable SDK retries for requests submitted through the query editor, including Streams replay. Browsing and following keep native retries and cursor renewal. The user decides whether to resubmit a failed query. A lost PartiQL response or server failure can leave a write's outcome unknown. A completed client-error response remains a rejection. Query text still uses the shared confirmation and history settings.

This supersedes the local PartiQL `SELECT` and table-scope guard in [ADR-0011](0011-use-native-aws-clients-for-dynamodb-reads.md). The guard could reject valid datasource syntax and prevented native write operations. Database credentials remain the authority for tables and operations.
