---
status: accepted
date: 2026-09-10
---

# ADR-0006: Browse Kafka with rust-rdkafka

## Decision

Use `rdkafka` from `fede1024/rust-rdkafka` for a separate `onetui-kafka` connector. Choose it for librdkafka's maturity and native Kafka feature coverage, with a Rust API that fits the existing provider interface.

The client provides:

- Tokio-compatible asynchronous consumption and production, plus explicit polling APIs.
- Broker, topic, partition and consumer-group metadata; partition watermarks and timestamp-to-offset lookup.
- Manual partition assignment and seek, explicit offset control and read-committed isolation.
- Native connection management, retries, error callbacks and statistics.
- TLS, SASL authentication and Kafka compression codecs, depending on build features.
- Idempotent and transactional producers, consumer-group coordination and administration APIs for future write capabilities.

Client capabilities are not automatically OneTUI features. The first connector exposes metadata and bounded read-only records; build features and authenticated transports must be validated before being advertised.

Register the connector through the built-in provider and executor enums in [ADR-0002](0002-use-static-enum-dispatch-for-built-in-providers.md). Keep native client types, configuration and tests in the connector package. Reuse the existing tables, value viewer, bookmarks and cancellation flow. No runtime plugin system or Kafka-specific branches in the renderer.

## Browsing contract

Browsing and following use one selected topic, optionally one partition. Cross-topic views are permanently rejected; topic selection is the browsing boundary.

The browsing path is `kafka.topics` → `kafka.partitions` → `kafka.records`. `check` reads metadata only. Opening records explicitly reads one selected partition at a numeric offset; it does not subscribe to a topic or participate in an application's consumer group.

Disable automatic commits, automatic offset storage and topic auto-creation. Set `auto.offset.reset=error` and assign explicit offsets rather than consulting committed positions. The high-level client requires a group ID for assignment, so generate a private session ID with the `onetui-` prefix. Its coordinator lookup requires group `Describe` permission; it does not require group `Read` permission or authorize subscribing, committing or resetting offsets. These safety settings are connector-owned, not arbitrary user overrides.

Read committed records by default. Capture the native read-committed stable end when opening the partition; refresh starts a new browsing window. Open transactions and records beyond that boundary are excluded from the window, even if committed later. Offsets are partition-local, not row numbers or a global topic order. Retention, compaction and aborted transactions can leave gaps. A timeout or quiet poll is not proof of end-of-data. Report unavailable positions or an incomplete read without silently moving the bookmark. The browsing window is not a retained snapshot.

Preserve exact offsets, timestamps, nullable key/value bytes and ordered headers, including duplicate header names. A tombstone has no value and must remain distinct from empty bytes. Use the existing text/hex/binary display options; do not assume UTF-8 or add automatic Avro/Protobuf decoding. `/` remains a filter of the displayed page, not a broker-side search.

## Lifetime and limits

Keep one selected-session executor and reuse its native client under [ADR-0001](0001-register-providers-and-own-session-lifecycles.md). Use native broker reconnect handling, not periodic checks or consumer-group heartbeats as a health probe. Stop record fetching between page requests and release native work on disconnect, alias switch and quit.

Reuse the 100-item and 1 MiB page limits. Bound native prefetch separately; Kafka fetch limits can be exceeded by a record batch, so neither limit is an RSS guarantee. Reject oversized data without advancing its bookmark. Preserve native errors, with the existing credential redaction and terminal-control escaping.

Metadata calls and native destruction can block. Keep them off the UI and Tokio async worker threads, with finite native timeouts and bounded ownership of blocking work. Cancelling `StreamConsumer::recv` is safe, but does not establish that the native client has stopped. Use one process-wide native owner slot, including destruction, so repeated connection switches cannot accumulate blocked workers. A replacement waits within its request deadline.

## Authentication ownership

Keep GSSAPI ticket acquisition and renewal outside the application. Use the system ticket cache instead of invoking `kinit` or changing process-wide credentials per alias. This preserves the native authentication boundary and avoids another subprocess lifecycle.

OAuth uses explicit client-credentials endpoints and independently configured trust. Refresh through librdkafka during active work; idle sessions make no token requests. After idle expiry, reconnect on the next read while preserving logical page bookmarks. The broker validates token signatures and authorization. See [the connector implementation](../../crates/kafka/src/) and its package-owned authentication tests.

## Rejected alternatives

- `kafka` (`kafka-rust/kafka-rust`): reject for this connector. A Rust protocol implementation avoids librdkafka, but its synchronous API and narrower published client surface give us less of the native functionality we need. This is a project choice, not a claim that the library is abandoned.
- A custom Kafka protocol client: reject. Maintaining protocol versions, security, compression and recovery would duplicate a mature client.
- Shelling out to Kafka CLI tools: reject as the connector transport. Subprocess output is a poor fit for typed byte values, cancellation and persistent sessions.
- Runtime-selectable client adapters: reject. Supporting multiple Kafka clients adds configuration and lifecycle paths without a concrete need.

## Consequences

`rdkafka` adds a C build and native TLS/compression dependencies. Compile and package the selected features on the supported macOS/Linux targets before advertising support. Its producer and administration APIs do not become OneTUI capabilities merely because the library includes them.

Cross-partition merging, production from the app, consumer-group administration and schema-registry decoding are outside this slice. Kafka has no SQL query mode here.
