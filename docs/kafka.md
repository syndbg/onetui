# Kafka browsing

The Kafka connector uses `rdkafka` with a dedicated native owner thread and the same provider interface as the other datasources. It supports historical browsing and live following of a partition or a whole topic.

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

Enter on a connection opens a local resource menu with Topics, Brokers and Groups. The menu does not connect to Kafka; selecting an entry starts its native read. Topics lists topics; Enter on a topic offers Partitions, Configuration and `kafka.records` (all partitions). Enter on a partition reads records starting at its earliest available offset. Enter on a record opens all its fields. `n/p` moves between pages, `r` refreshes and `/` filters only cached text on the displayed page.

Brokers lists IDs, hosts and ports advertised by Kafka; Enter opens the selected broker's configuration. Groups lists names, state, protocol and member counts; Enter offers Members and Offsets. Members shows member IDs, client IDs/hosts, metadata and assignment bytes. Assignments remain raw protocol bytes, available in the value viewer; OneTUI does not claim they are decoded partition assignments. Group names must fit 1..1024 UTF-8 bytes without control characters. These views do not join, rebalance or commit for any inspected group. They re-read and locally page metadata, so membership may change between pages.

Group inspection requires `Describe` on the inspected groups. Kafka can omit unauthorized groups; an empty listing is not proof that no groups exist. The resource menu adds no permissions or connection settings by itself. Esc returns through retained parent views to the menu.

Group/member arrays are limited to 4 MiB of native structures during conversion, and each retained page is limited to 100 rows and 1 MiB including its continuation. These are not process-memory guarantees. Per-group broker errors remain errors, not empty membership. Missing or hidden groups cannot be distinguished from an omitted metadata entry.

### Configuration and group offsets

For local topic settings, run `make run` and choose `local_kafka` → Topics → `demo_events` → Configuration. The `kafka.topic_config` and `kafka.broker_config` views show each setting's name, value, source and `is_default`, `is_read_only`, `is_sensitive` flags. OneTUI withholds every value marked sensitive. Null also represents a value the broker did not return; an empty string remains distinct. Synonyms and secret retrieval are not supported.

Configuration reads require `DescribeConfigs` on the selected topic, or on the cluster for broker configuration. These permissions are separate from record-reading permissions. The disposable `fixture-reader` account deliberately lacks them so tests can verify native authorization errors; the local plaintext connection can inspect the fixtures.

Choose Groups → a group → Offsets to inspect `kafka.offsets`. It lists stored topic/partition offsets, not all possible subscriptions or partitions. A missing group and a group without stored offsets both yield an empty view. The browser never uses the inspected group as its client group ID.

| Column | Meaning |
| --- | --- |
| `committed` | Stored next offset; null when Kafka returns no commit. |
| `low` | Earliest retained offset. |
| `stable_end` | Exclusive read-committed boundary from the browser's native client. |
| `lag` | `stable_end - committed` when the commit is within the available window. This is offset distance, not message count; transactions and compaction can leave gaps. |
| `status` | `within window`, `no commit`, `before retained start`, or `beyond stable end`. The last three have null lag; OneTUI does not clamp invalid positions to zero. |

Offsets require group `Describe`; watermarks also require topic `Describe`. An authorization or partition error fails the request and retains the previous page. Each page re-reads offsets and fetches watermarks only for its displayed partitions, under the normal request deadline. These independent reads are not a snapshot and can race with commits, retention and transactions. Results sort by topic and partition; configuration sorts by name. `n/p` uses session/resource-bound bookmarks, with the usual 100-row and 1 MiB page limits and 4 MiB native-array conversion limit.

These views use the existing connection and add no `onetui.toml` settings or CLI options. Dump their descriptors with:

```sh
onetui schema --datasource kafka
```

### Browse or follow a whole topic

Run `make run`, then choose `local_kafka` → Topics → `demo_events` → `kafka.records`. This reads all three fixture partitions. Use `n/p` to page forward/backward, Enter to inspect a record, or `f` to follow new records across the topic. For the traffic producer, choose the same entry under `demo_live`.

The `kafka.records` resource accepts `[topic]` for all partitions or `[topic, partition]` for a single partition. Topic-wide rows add a leading `partition` column; single-partition columns are unchanged. Offset/timestamp replay remains single-partition only: open Partitions and select one before using `e`.

Topic-wide reads support at most 32 partitions and return at most 100 records and 1 MiB per page, including a cursor of at most 4 KiB. Larger topics fail explicitly; use their individual partitions. These are fixed connector limits, not new configuration settings.

Each partition has its own next offset and captured stable end. Pages rotate through partition batches, allocating `ceil(100 / active partitions)` records per active partition until the row or byte budget is reached. A sparse partition can leave unused space. Records remain offset-ordered within each partition, not globally timestamp-ordered. `/` and `s` still affect only the displayed page.

Historical bookmarks retain the captured ends; following updates each end on every batch. Metadata and watermarks are read sequentially on the existing native owner, under one request deadline. This is not an atomic topic snapshot. Empty partitions keep their cursors, and an empty follow batch does not mean following has ended. Retention invalidation, a backwards boundary or a changed partition set fails without advancing the successful bookmark. Refresh or restart following explicitly after a partition-set change.

### Replay from an offset or timestamp

Select a partition or open its records, then press `e` or use `:query`. Ctrl-U clears the draft; Enter or F5 executes JSON such as:

```json
{"offset":123,"end_offset":250}
```

This reads offsets `[123, 250)` in the selected partition. Use a Unix timestamp in milliseconds instead of `offset` to resolve a starting position:

```json
{"timestamp_ms":1750000000369}
```

| Field | Values and default |
| --- | --- |
| `offset` | Optional nonnegative signed 64-bit integer. Omitted or null starts at the earliest available offset, unless `timestamp_ms` is supplied. |
| `timestamp_ms` | Optional nonnegative signed 64-bit integer, milliseconds since the Unix epoch. Cannot be combined with a non-null `offset`. Kafka resolves the first offset at or after this time. This chooses a start position; it does not filter every record by timestamp. |
| `end_offset` | Optional nonnegative signed 64-bit integer, exclusive. Omitted or null captures the current read-committed end. An explicit end must lie within the available partition window and cannot precede an explicit start. |

`{}` replays from the earliest available offset. Unknown fields, fractions, negative values and out-of-range integers fail before a broker request. A timestamp with no matching record, or whose matching offset lies beyond the selected end, returns an empty page. Explicit offsets outside the available window fail rather than silently jumping elsewhere.

The existing 100-row and 1 MiB limits apply. `n/p` uses bookmarks bound to the exact query text, partition and session; changing the query starts a new window. Records keep their native bytes and headers. Replay never joins a group or commits offsets. The query stays above its results; Esc returns to ordinary browsing. Following is available in the ordinary `kafka.records` view, not query results.

Replay adds no connection settings or CLI flags. `onetui schema --datasource kafka` lists its inputs and result resource. See [editor keys](queries.md).

### Record values and bounds

Records include the partition offset, millisecond timestamp when available, key, value and headers. Keys and values remain bytes, even if valid UTF-8. Auto display shows valid UTF-8 as text or complete JSON objects/arrays; invalid UTF-8 falls back to hex. For example, `make dev-traffic` produces the readable key `demo` and a JSON value. Use `v` in field detail to choose text, JSON, hex or binary explicitly. Decoding never replaces invalid bytes or changes retained data. A tombstone has a null value; empty bytes remain an empty value. Headers use an ordered JSON list of names and nullable byte arrays, so duplicate names survive.

Protobuf/Avro bindings and Schema Registry decoding are not implemented in the Kafka browser. The standalone [codec package](../crates/codec/README.md) can decode raw messages against local schemas; [ADR-0008](adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md) defines the pending key/value bindings and schema lookup. Auto does not identify these binary formats. Page-local filter/sort still use the stable `\x...` hexadecimal projection for byte fields, independently of their readable display.

Each page contains at most 100 records and 1 MiB of retained page data. The first page captures the native read-committed stable end; continuation tokens carry that end and the next partition offset. Open transactions and records beyond that boundary are excluded, even if committed later. Refetching page 1 or refreshing captures a new window. Retention and compaction can remove records; offsets are not consecutive row numbers. An unavailable position or expired request reports an error and keeps the displayed page/bookmark, rather than claiming the partition ended.

Metadata has no native page API: OneTUI re-reads the bounded response, sorts topics/partitions and shows a page. No browsing mode promises a cross-page snapshot. Native receiving is capped at 4 MiB, with 1 MiB of configured prefetch. The larger native cap allows Zstandard's stepped buffer growth to decode a near-1-MiB value; the retained page still must fit 1 MiB. These limits do not guarantee process RSS limits.

The native client uses a 100 ms fetch-queue refill backoff with its one-message queue threshold. This avoids the default one-second refill delay while keeping prefetch bounded. It is a fixed connector setting, listed as `fetch_queue_backoff_ms` in the catalog, not an `onetui.toml` option or a latency guarantee.

`check` fetches metadata only. Record reads use manual assignment and explicit offsets, never a topic subscription. Auto-commit, automatic offset storage and topic auto-creation are disabled. OneTUI generates an internal session group ID for the client API; it does not use an application group or commit its offsets. No user override can enable these side effects.

For authenticated browsing, grant `Read` and `Describe` on the selected topics, plus `Describe` on groups prefixed `onetui-`. The native client requires that private group ID even with manual assignment and asks for its coordinator. Group `Read` permission is unnecessary: OneTUI does not join the group or commit offsets. Metadata listing alone does not prove record-reading permission; Kafka can omit unauthorized topics from a listing.

Native work stays outside the UI and Tokio async worker threads. Cancellation stops waiting; a native call or destructor may still be finishing. One process permits only one native owner, including cleanup, so repeated switches cannot accumulate blocked workers. A replacement waits within its request deadline. The native client is reused within the selected session and released on disconnect, switch or quit.

## Live following

Open `kafka.records` for a topic or one partition, then press `f` or enter `:follow`. The initial request captures the current read-committed stable end of each selected partition and clears historical rows once it succeeds. The footer shows `LIVE`; subsequent requests fetch new committed records from those cursors, one second after the previous request finishes. Open transactions stay hidden until committed; aborted records never appear.

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

Choose `local_kafka`, Topics, `demo_live`, Partitions, partition `0`, then press `f`. The producer sends its first record immediately and another every 15 seconds until Ctrl-C. It creates `demo_live` if absent, preserves existing records and never modifies the fixed demo datasets. Details and a finite-run command are in the [hack guide](../hack/README.md#kafka-traffic).

SQL, publishing from the app, consumer-group administration, schema-registry decoding, mutual TLS, OAuth and GSSAPI are not exposed. Client support for these features does not imply OneTUI support.
