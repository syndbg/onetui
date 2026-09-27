---
status: accepted
date: 2026-09-27
---

# ADR-0018: Serve ScyllaDB and Cassandra through one CQL provider

## Decision

Add one provider, `kind = "cql"`, in [crates/cql](../../crates/cql), built on the ScyllaDB Rust driver (`scylla` 1.9 with `rustls-023`). It serves ScyllaDB and Apache Cassandra 3+ through the CQL native protocol.

- **Browse:** keyspaces, tables, column metadata and rows. Metadata comes from `system_schema`, which both databases provide. Rows use `SELECT * FROM "keyspace"."table"`.
- **Paging:** the driver's native paging state, 100 rows per page, stored in the continuation and bound to the executor, resource and statement.
- **Editor:** one CQL statement, sent as written, following [ADR-0005](0005-run-native-queries-through-providers.md). The server prepares it first. Result metadata from the prepare decides the path: rows are a pageable read; no rows means a write that reports applied, rejected or unknown, as in [ADR-0013](0013-keep-connections-after-query-rejections.md).
- **Session:** one lazy driver session per executor. The driver owns per-node pools and reconnects them, so errors do not retire the session; shutdown drops it.

## Why one provider

ScyllaDB implements the Cassandra wire protocol and schema tables. Two providers would duplicate the driver, value decoding and paging for no behavioral difference the client can act on. The name is the protocol, `cql`, because the driver is ScyllaDB's but the surface is not ScyllaDB-specific. The fixtures run both databases against the same tests.

## What the client decides

- **Names:** keyspace, table and column names are quoted with `"` doubled, never checked against an allowlist. Quoted CQL names keep their case and may contain any character, so quoting reaches every name the server has.
- **Metadata filters** are bound values, never interpolated.
- **Rejected or unknown:** only server errors that guarantee nothing was applied are rejections. Coordinator timeouts and replica failures may leave the write on some replicas, so they report an unknown outcome; retrying a counter or list append would apply it twice. A failure while rendering a response (such as the 1 MiB limit) is a plain error, since the statement ran.
- **Read or write** is decided by the server's prepare metadata, not by parsing CQL. Preparing also makes a syntax error a rejection with nothing executed. Lightweight transactions return rows (`[applied]`) and show as a page.
- **Values:** `varint` and `decimal` convert to exact base-10 text from their big-endian bytes; a float would lose digits. `timestamp`, `date` and `time` render in UTC like cqlsh using integer civil-date arithmetic, valid over each type's full range. `blob` stays bytes for the display formats. Collections, tuples and UDTs become JSON; maps become key/value pairs because CQL keys need not be strings.

## Consequences

The driver enables rustls' `aws-lc-rs` backend through its default features. Cargo unifies features, so every crate in the binary sees two crypto providers and rustls can no longer pick a process default. Every rustls configuration in the repository now names `ring` explicitly, and `main` installs `ring` as the process default for dependencies that ask for one. `aws-lc-rs` was already in the lockfile; this change activates it.

Returning to the first page of an editor result reruns the statement. This is safe because only reads and lightweight transaction results return rows, and writes never produce a second page.

The driver connects to each node at the address and port the node advertises. Clusters behind NAT or a port mapping must advertise reachable addresses; the fixtures do so. An address translator could cover this later if a real deployment needs it.

TLS verifies node certificates against the addresses the driver connects to. There is no TLS fixture yet.

## Alternatives

- **Separate `scylla` and `cassandra` kinds.** Clearer names in a connection list, but two registrations for one implementation, and a config would need to change if a cluster migrates between them.
- **DataStax C++ driver bindings.** Mature, but adds a system library, a CMake build and an FFI surface.
- **`cdrs-tokio`.** Pure Rust and lighter, but less maintained, with a weaker paging and TLS story.
- **Scylla's `openssl-010` TLS feature** to avoid `aws-lc-rs`. Every other connector uses rustls with native trust; OpenSSL would make CQL the one provider with different trust behavior.
- **Classify statements by parsing CQL.** Duplicates the server's grammar and drifts from it; the prepare metadata is the server's own answer.
