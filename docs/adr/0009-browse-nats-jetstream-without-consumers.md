---
status: accepted
date: 2026-09-10
---

# ADR-0009: Browse NATS JetStream without consumers

## Decision

Use the official `async-nats` client in `onetui-nats`, registered through the existing static provider enums. Reuse the shared table, value viewer, bookmarks and `follow_page` interface.

Read stored messages through `$JS.API.STREAM.MSG.GET.<stream>`, using a sequence and `next_by_subj: ">"` to skip deleted messages. These reads do not create consumers or acknowledge messages. They also work without enabling direct access on a stream. The [JetStream API](https://docs.nats.io/reference/jetstream/api/) defines these management requests; the [Rust client's raw-message builder](https://docs.rs/async-nats/0.50.0/async_nats/jetstream/stream/struct.RawMessageBuilder.html) exposes the same sequence/subject selection.

For example, opening `DEMO_EVENTS` yields `nats.messages` at path `["DEMO_EVENTS"]`. Historical browsing captures an upper sequence boundary. Following starts after the current last sequence and polls for bounded batches. Both preserve payload and header bytes. A stream creation timestamp binds bookmarks to that stream instance; retention or replacement can invalidate them.

Keep one lazy client per selected connection. Use native PING/PONG, bounded reconnect attempts and request deadlines. Discard the client after a failed or cancelled request so unanswered reply entries cannot accumulate. Shutdown drains the client and waits for its native close event within the shutdown deadline. The [client API](https://docs.rs/async-nats/0.50.0/async_nats/struct.Client.html) queues draining; enqueueing alone is not proof of closure.

## Alternatives and consequences

- **Durable, ephemeral or ordered consumers:** useful for processing, but unnecessary state for a browser. Pulling or acknowledging through an application's consumer could change its delivery state.
- **Requiring direct access:** can reduce leader load, but requiring a stream configuration change conflicts with read-only access. Leader API reads cost one request per message; page and byte limits bound each browsing operation. Direct access can be considered separately if measured latency warrants it.
- **Automatic Core NATS wildcard subscriptions:** no historical inventory or replay equivalent. Core subscriptions need their own explicit scope and lifecycle; they are not implied by JetStream following.
- **The synchronous `nats` client:** would require blocking-worker coordination already avoided by `async-nats` on Tokio.

The app publishes read-only API requests and subscribes to private replies; it does not publish application data or send ACKs. Grant only the required API subjects and reply subscriptions. A page is not a snapshot, and restarting following does not recover messages missed while stopped.

Core subscriptions use explicit configured subjects and start only on `f`. Retain one bounded SDK subscription between batches, without a queue group. `stop_follow` unsubscribes and flushes before another read; errors or cancellation discard the session. Queue overflow and disconnection stop following with visible loss, including failures between polls. Core is [at-most-once](https://docs.nats.io/learn/core-nats/), so neither reconnect nor restart promises recovery. Disable JetStream checks for Core-only servers with `jetstream = false`. Core header fields expose SDK values rather than claiming to retain the original wire block.

Inspect consumer configuration and counters with `CONSUMER.LIST/INFO`, never by pulling from the consumer. Read KV keys/revisions through their backing stream, retaining delete/purge headers. Read object metadata first, then fetch bounded chunks for that object version. Do not assemble whole objects or follow links implicitly; reject changed versions and incomplete chunk counts.

Replay uses explicit subject and sequence bounds. Timestamp filtering scans bounded batches because leader `STREAM.MSG.GET` lacks a timestamp seek. Empty filtered pages can continue; neither enabling direct access nor creating an ordered consumer is required.

Use native NKEY nonce signing, standard JWT credentials, verified mTLS and domain routing. Keep authentication modes mutually exclusive. Credentials come from explicit environment references; TLS files load only on connection. Authentication rotation requires reselecting the connection.

Bind payload decoders to exact subjects with explicit framing. Reuse `onetui-avro` and `onetui-protobuf` for JSON/native inspection, preserve original bytes and report errors per message. Cache schemas for the selected connection and bound parsing outside the TUI. Share file, directory, registry and Buf loading through `onetui-schema-source`, not a dependency on Kafka. [ADR-0008](0008-detect-readable-bytes-and-decode-messages-with-schemas.md) records the format boundary.
