# Kafka browsing

The Kafka connector uses `rdkafka` with a dedicated native owner thread and the same provider interface as the other datasources. It supports historical browsing and live following of one partition.

## Configuration

Use `onetui schema --datasource kafka` to dump the current supported settings, resources and limits. No file or broker connection is needed for this command.

Configuration uses the normal `onetui.toml` discovery rules described in the [README](../README.md#configuration). To select a file explicitly:

```sh
onetui --config "$HOME/onetui.toml"
onetui --config "$HOME/onetui.toml" --check --connection local_kafka
```

Example for a local plaintext broker (this does not start a broker):

```toml
[connections.local_kafka]
kind = "kafka"
bootstrap_servers = ["127.0.0.1:19092"]
security_protocol = "PLAINTEXT"
```

Example for SASL over TLS:

```toml
[connections.kafka_tls]
kind = "kafka"
bootstrap_servers = ["broker.example:9093"]
security_protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-256"
username_env = "ONETUI_KAFKA_USER"
password_env = "ONETUI_KAFKA_PASSWORD"
# Optional private CA bundle; use an absolute path.
ca_file = "/absolute/path/to/kafka-ca.pem"
```

| Setting | Purpose, values and default |
| --- | --- |
| `kind` | Required string, exactly `kafka`. |
| `bootstrap_servers` | Required array of 1..32 `host:port` strings, each at most 255 bytes. IPv6 addresses use brackets. No URL scheme, credentials, path, query or fragment. Empty arrays and zero/missing ports are rejected. |
| `security_protocol` | `SSL` (default), `SASL_SSL`, or `PLAINTEXT`. TLS verifies certificates and hostnames. Plaintext is restricted to loopback bootstrap addresses for local development. |
| `ca_file` | Optional absolute path, at most 4096 bytes, to a CA bundle. No `~` or environment expansion; relative and empty paths are rejected. Omission uses librdkafka/OpenSSL trust discovery, which is not the same as the other connectors' Rustls trust lookup. Not allowed with `PLAINTEXT`. File access occurs on the first native request, not during offline schema/config validation. |
| `sasl_mechanism` | Required for `SASL_SSL`: `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`. Must be omitted otherwise. |
| `username_env`, `password_env` | Required for `SASL_SSL`. Nonempty environment-variable references containing ASCII letters, digits, underscores or hyphens. Both must be omitted otherwise. Only the selected alias resolves secrets. Missing, empty or NUL-containing credentials fail; values are never saved to the config file. |

Unknown settings are rejected. Broker metadata can advertise endpoints other than the bootstrap addresses; those addresses must be reachable from the machine running OneTUI. The plaintext bootstrap restriction is not an outbound network allowlist. Prefer TLS for all non-disposable environments. Broker ACLs remain the authorization boundary.

## Navigation and values

Enter on a connection opens topics. Enter on a topic opens partitions; Enter on a partition reads records starting at its earliest available offset. Enter on a record opens all its fields. `n/p` moves between pages, `r` refreshes and `/` filters only cached text on the displayed page.

Records include the partition offset, millisecond timestamp when available, key, value and headers. Keys and values remain bytes, even if valid UTF-8. Auto display shows valid UTF-8 as text or complete JSON objects/arrays; invalid UTF-8 falls back to hex. For example, `make dev-traffic` produces the readable key `demo` and a JSON value. Use `v` in field detail to choose text, JSON, hex or binary explicitly. Decoding never replaces invalid bytes or changes retained data. A tombstone has a null value; empty bytes remain an empty value. Headers use an ordered JSON list of names and nullable byte arrays, so duplicate names survive.

Protobuf, Avro and Schema Registry decoding are planned, not implemented. [ADR-0008](adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md) defines explicit key/value decoder bindings and schema lookup. Auto does not identify these binary formats. Page-local filter/sort still use the stable `\x...` hexadecimal projection for byte fields, independently of their readable display.

Each page contains at most 100 records and 1 MiB of retained page data. The first page captures the native read-committed stable end; continuation tokens carry that end and the next partition offset. Open transactions and records beyond that boundary are excluded, even if committed later. Refetching page 1 or refreshing captures a new window. Retention and compaction can remove records; offsets are not consecutive row numbers. An unavailable position or expired request reports an error and keeps the displayed page/bookmark, rather than claiming the partition ended.

Metadata has no native page API: OneTUI re-reads the bounded response, sorts topics/partitions and shows a page. No browsing mode promises a cross-page snapshot. Native receiving is capped at 4 MiB, with 1 MiB of configured prefetch. The larger native cap allows Zstandard's stepped buffer growth to decode a near-1-MiB value; the retained page still must fit 1 MiB. These limits do not guarantee process RSS limits.

`check` fetches metadata only. Record reads use manual assignment and explicit offsets, never a topic subscription. Auto-commit, automatic offset storage and topic auto-creation are disabled. OneTUI generates an internal session group ID for the client API; it does not use an application group or commit its offsets. No user override can enable these side effects.

For authenticated browsing, grant `Read` and `Describe` on the selected topics, plus `Describe` on groups prefixed `onetui-`. The native client requires that private group ID even with manual assignment and asks for its coordinator. Group `Read` permission is unnecessary: OneTUI does not join the group or commit offsets. Metadata listing alone does not prove record-reading permission; Kafka can omit unauthorized topics from a listing.

Native work stays outside the UI and Tokio async worker threads. Cancellation stops waiting; a native call or destructor may still be finishing. One process permits only one native owner, including cleanup, so repeated switches cannot accumulate blocked workers. A replacement waits within its request deadline. The native client is reused within the selected session and released on disconnect, switch or quit.

## Live following

Open a topic, partition and its `kafka.records` view, then press `f` or enter `:follow`. The initial request captures the current read-committed stable end and clears historical rows once it succeeds. The footer shows `LIVE`; subsequent requests fetch new committed records from that cursor, one second after the previous request finishes. Open transactions stay hidden until committed; aborted records never appear.

Press `f` or Ctrl-C to stop without quitting. Navigation, opening a record, menus and command entry also stop following before changing the view, keeping data steady for inspection. The stopped window remains readable. Pressing `f` again starts at a new current end and clears the old live window after that first request succeeds; it does not resume messages missed while stopped. `r` returns to historical browsing from the earliest available offset. Historical `n/p` paging is disabled in a live window.

The window retains at most 100 records and 1 MiB of page data, also respecting the separate 1 MiB display-projection budget. Older displayed rows are evicted as new ones arrive, with a visible `evicted locally` count. The cursor still reads forward without skipping records to catch up. A busy partition can outpace the one-batch-per-second reader. This is a bounded viewer, not a durable consumer or a complete traffic archive.

Retention invalidation, a backwards-moving boundary, an oversized value, authentication failure or deadline stops following with the last successful data retained. Restart is explicit. Normal offset gaps can come from compaction or transaction markers. There are no group subscriptions, acknowledgements or committed offsets.

Following adds no `onetui.toml` settings. The existing request timeout applies to each batch. `onetui schema --datasource kafka` lists `follow_page`, its resource and fixed limits; `onetui schema` lists the `follow` action. See [ADR-0007](adr/0007-follow-live-records-in-bounded-batches.md).

For local traffic, run these in separate terminals:

```sh
make dev-up
make dev-traffic
```

```sh
make run
```

Choose `local_kafka`, `demo_live`, partition `0`, then press `f`. The producer sends its first record immediately and another every 15 seconds until Ctrl-C. It creates `demo_live` if absent, preserves existing records and never modifies the fixed demo datasets. Details and a finite-run command are in the [hack guide](../hack/README.md#kafka-traffic).

SQL, publishing from the app, consumer-group administration, schema-registry decoding, mutual TLS, OAuth and GSSAPI are not exposed. Client support for these features does not imply OneTUI support.
