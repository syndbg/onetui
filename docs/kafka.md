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

For mTLS, use `SSL` and configure both PEM files. These settings also work with `SASL_SSL`:

```toml
client_cert_file = "/absolute/path/to/client.pem"
client_key_file = "/absolute/path/to/client-key.pem"
# Only for an encrypted key:
client_key_password_env = "ONETUI_KAFKA_KEY_PASSWORD"
```

Restrict private-key file permissions. Files are read on the first native request; passwords resolve only for the selected alias. Certificate and hostname verification remain enabled. Omitting `ca_file` uses librdkafka/OpenSSL trust discovery. Paths must be absolute; `~` and environment expansion are not supported. Use the schema command above for all accepted values and defaults.

Unknown settings are rejected. Broker metadata can advertise endpoints other than the bootstrap addresses; those addresses must be reachable from the machine running OneTUI. The plaintext bootstrap restriction is not an outbound network allowlist. Prefer TLS for all non-disposable environments. Broker ACLs remain the authorization boundary.

### OAuth

Use a client-credentials token endpoint returning signed JWT access tokens:

```toml
[connections.kafka_oauth]
kind = "kafka"
bootstrap_servers = ["broker.example:9093"]
security_protocol = "SASL_SSL"
sasl_mechanism = "OAUTHBEARER"

[connections.kafka_oauth.oauth]
token_url = "https://identity.example/oauth/token"
client_id_env = "ONETUI_OAUTH_CLIENT_ID"
client_secret_env = "ONETUI_OAUTH_CLIENT_SECRET"
# Optional:
scope = "kafka.read"
# ca_file = "/absolute/path/to/identity-ca.pem"
```

Broker and token-endpoint CA bundles are separate. Credentials resolve only for the selected alias; reopen it after changing them. Token requests use HTTP Basic authentication, verified TLS, a two-second deadline and a 64 KiB response limit. Redirects and proxies are disabled; plain HTTP is allowed only on literal loopback IPs for tests.

librdkafka refreshes tokens during active reads and following; idle sessions make no token requests. After idle expiry, the next read reconnects without discarding page bookmarks. JWT `sub` and `exp` are required. The broker validates signatures, issuer, audience and permissions. Opaque tokens, interactive login, discovery and SASL extensions are not supported.

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

### Schema-bound key and value previews

Use `onetui schema --datasource kafka` to dump supported settings and defaults. Bindings belong inside the selected connection in the same `onetui.toml`. Config discovery is unchanged, including `onetui --config "$HOME/onetui.toml"`; there is no separate decoder configuration file.

```toml
[connections.events]
kind = "kafka"
bootstrap_servers = ["127.0.0.1:19092"]
security_protocol = "PLAINTEXT"

[[connections.events.decoders]]
topic = "events"
field = "value"
format = "avro"
framing = "raw"
schema_file = "/absolute/path/to/event.avsc"
```

Replace the schema path with your local writer-schema file and the topic with its exact name. The raw datum must use that schema. For Protobuf, set `format = "protobuf"`, point `schema_file` at a binary `FileDescriptorSet` including imports, and add `message_name = "demo.Event"`. The [Protobuf example](../crates/protobuf/README.md#try-a-raw-message) shows how to compile descriptors with `protoc`. [Example configuration](../hack/kafka-decoders.toml.example) includes independent Avro-value and Protobuf-key bindings.

```sh
onetui --config "$HOME/onetui.toml" --connection events --check
onetui --config "$HOME/onetui.toml" --connection events
```

| Setting | Accepted values and behavior |
| --- | --- |
| `decoders` | Optional array of tables; default/empty `[]` adds no previews. At most 32 bindings per connection. |
| `topic` | Required exact name, 1..249 ASCII letters/digits/dot/underscore/hyphen; not `.` or `..`. No wildcards. |
| `field` | Required `"key"` or `"value"`. Each topic/field pair may appear once. |
| `format` | Required `"avro"` or `"protobuf"`; case-sensitive. |
| `framing` | Required `"raw"` or `"confluent"`. Raw requires exactly one of `schema_file`, `catalog` or `buf`; Confluent requires `registry`. No automatic detection. |
| `schema_file` | Raw framing only, mutually exclusive with `catalog` and `buf`. Absolute regular-file path, at most 4,096 UTF-8 bytes without controls. File contents are limited to 256 KiB. No path expansion. |
| `catalog` | Optional local-directory source for raw framing; see below. |
| `buf` | Optional Buf descriptor source for raw Protobuf; explicit commit or label, pinned for the session. |
| `message_name` | Required for raw Protobuf: exact full name, 1..1,024 UTF-8 bytes without controls. Forbidden for Avro and Confluent framing; Protobuf registry records select their message through envelope indexes. |

Unknown keys, missing required fields, wrong types and duplicate bindings fail validation even for unselected aliases. Omitted bindings retain Auto display. Bindings apply to partition and topic-wide `kafka.records`, following, and `kafka.query` replay for that topic.

The worker loads a local schema on its first relevant read; `--check` loads each raw binding and checks configured registries for the selected alias before checking broker metadata. Config parsing and offline catalog output do not read schema files or contact registries. Local schemas and load errors stay cached for the session. Reopen the connection to reread files; refresh reuses cached schemas. Restart after editing binding settings. Registry bindings use the separate cache policy below.

Each bound field adds `FIELD_decoded`, `FIELD_schema` and `FIELD_decode_error` columns. Enter on a record lists all fields; Enter on a field opens its full value. The original `key` and `value` remain unchanged. Select those originals and use `v`, `:display format hex` or `:display format binary` to inspect wire bytes. Hex on a decoded JSON field shows serialized JSON bytes, not the original message.

Null keys/tombstones are not decoded; empty bytes are decoded and may be valid or invalid for the chosen schema. Schema, decode and JSON-conversion errors appear per value without dropping records or stopping following. Error text keeps the library wording, capped at 512 UTF-8 bytes with `...`; the shared renderer escapes terminal controls.

Previews use the library allocation/depth limits and accept at most 64 KiB of payload. JSON previews are capped at 64 KiB and must fit the remaining 1 MiB page budget. Bound topics reserve 2 KiB of raw-page capacity for column metadata; a raw record that cannot fit still fails the page explicitly. If preview metadata cannot fit, all preview cells are null and the page notice explains the omission. Columns stay stable across empty or budget-limited live batches. Omitting a preview does not change raw values or broker continuations.

JSON is not a lossless typed export: Protobuf JSON omits unknown fields and uses strings for 64-bit integers/base64 for bytes; Avro JSON can flatten union and logical-type information. The libraries retain native types during decoding, but the browser has no native-type inspector yet. Reader-schema resolution remains unsupported. See [ADR-0008](adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md).

### Local directory catalogs

Replace `schema_file` with an explicit catalog selection:

```toml
catalog = { directory = "/absolute/schemas", schema = "event", references = ["customer"] }
```

IDs are filename stems: `event.avsc` selects an Avro writer schema, `event.pb` a Protobuf descriptor set. List all Avro dependencies in `references` (default `[]`); Protobuf descriptors must include imports and use `message_name` instead. No trial decoding or recursive search.

The directory is limited to 128 entries and 64 schemas; the selected bundle to 256 KiB. Duplicate IDs, traversal, symlinks and non-regular schema files are rejected. Reopen the connection to reload a binding; refresh and following retain its cached schema, including failures. Retained values are unchanged. `FIELD_schema` includes the directory, ID and content hash.

Build `.pb` files outside OneTUI with the [protoc example](../crates/protobuf/README.md#try-a-raw-message) or `buf build --as-file-descriptor-set -o /absolute/schemas/event.pb` from your Buf workspace. See [Buf build](https://buf.build/docs/reference/cli/buf/build/). Catalogs require Linux/macOS. Use `onetui schema --datasource kafka` for supported settings and limits; [example bindings](../hack/kafka-decoders.toml.example) cover both formats.

### Buf Protobuf descriptors

For raw Protobuf, replace `schema_file` with a Buf binding. Set your published module, exact 32-character commit ID and fully qualified message name:

```toml
[[connections.events.decoders]]
topic = "buf_events"
field = "value"
format = "protobuf"
framing = "raw"
message_name = "demo.Event"
buf = { url = "https://buf.build", module = "your-org/your-module", revision = "0123456789abcdef0123456789abcdef" }
```

Add `token_env = "BUF_TOKEN"` inside `buf` for a private module, or `ca_file` for private TLS trust. Credentials are independent of Kafka. `url` accepts an HTTPS origin, without a base path; HTTP is limited to literal loopback addresses. Use `onetui schema --datasource kafka` for all settings and limits.

To select a moving label, replace `revision` with `label = "main"` or your release label. OneTUI resolves it through Buf's `GetCommits` API, then fetches descriptors by the returned commit. Exactly one of `revision` or `label` is required; there is no implicit latest/default lookup.

OneTUI downloads the [compiled descriptor set with imports](https://buf.build/docs/bsr/module/descriptor/), capped at 256 KiB per response. Label resolution and descriptor fetching share a two-second HTTP deadline. Redirects and environment proxies are disabled. `--check` validates the selected message. Successes and failures stay cached until reconnect; refresh and following do not re-resolve a label. `FIELD_schema` shows the source, resolved commit and message, plus the descriptor fingerprint after a successful load. Lookup failures show the requested label if no commit was resolved. Raw data remains available on failure.

Use `make test-buf-live` for a read-only public BSR check. Buf does not supply Avro schemas or identify the schema of an arbitrary record.

### Confluent Avro registry

The same settings support Protobuf with `format = "protobuf"`.

For Avro records with Confluent's version-zero payload prefix, resolve the exact schema ID from the configured registry. The original key/value keeps its five-byte prefix; only the derived preview strips it. Each record may use a different writer schema. OneTUI never requests `latest` or registers schemas. Protobuf registry records are also supported: set `format = "protobuf"` and omit `message_name`. Their envelope indexes select the top-level or nested message. Imports use exact registry reference versions or embedded Google types; no local filesystem search or external compiler is used. Header-GUID framing and Avro single-object/container framing are not supported.

Add this binding to a Kafka connection in your `onetui.toml`, replacing the endpoint and topic. Do not specify `schema_file` or `message_name`:

```toml
[[connections.events.decoders]]
topic = "registered_events"
field = "value"
format = "avro"
framing = "confluent"
[connections.events.decoders.registry]
url = "https://registry.example.com"
username_env = "REGISTRY_USER"
password_env = "REGISTRY_PASSWORD"
```

Use `onetui --config "$HOME/onetui.toml" --connection events --check` after setting the named environment variables. `--check` requests `GET /schemas/types` and requires the binding's AVRO or PROTOBUF type; it does not read records or prove that every schema ID is available. Browsing uses `GET /schemas/ids/{id}` and resolves dependencies through exact `/subjects/{subject}/versions/{version}` requests. Reference subjects are encoded as path segments, not interpreted as URLs.

| Registry setting | Values and default |
| --- | --- |
| `url` | Required HTTP(S) base URL, at most 512 UTF-8 bytes without controls, embedded credentials, query or fragment. Base paths are allowed. HTTPS is required remotely; HTTP is allowed only for literal loopback IPs such as `http://127.0.0.1:8081`, not `localhost`. |
| `ca_file` | Optional HTTPS trust bundle. Omission uses platform certificate verification. An absolute regular-file path, at most 4,096 UTF-8 bytes without controls, containing up to 1 MiB of PEM certificates. Explicit certificates replace platform trust for this binding. No environment or tilde expansion. |
| `username_env`, `password_env` | Optional pair of ASCII environment-variable names for HTTP Basic authentication. Both must be present or absent. The resolved username cannot contain `:`. |
| `token_env` | Optional ASCII environment-variable name for a Bearer token, instead of Basic authentication. This is registry authentication, not broker OAuth. |

Omitting credentials means anonymous registry access. Secrets are resolved only for the selected connection, must be nonempty and at most 4,096 UTF-8 bytes without controls, and are independent of broker credentials. Unknown registry keys and incompatible settings fail offline validation. Redirects and environment-proxy discovery are disabled; certificate and hostname verification cannot be disabled.

Lookups run on the existing Kafka worker. Each uncached schema and its reference requests share a two-second I/O deadline; foreground cancellation/deadlines remain active, with checks between requests and before publishing results. An active native request may finish after foreground cancellation, and shutdown retains ownership until that work exits. There is no renderer I/O. Decoding retains the library's size/depth limits rather than promising a process RSS or CPU-time bound.

Each response is limited to 256 KiB plus 16 KiB of headers. A writer bundle is limited to 256 KiB, 32 distinct referenced subject/version pairs and eight reference levels; the pending queue also holds at most 32 references. Each binding caches eight schema IDs, including lookup failures, with FIFO eviction. Repeated records reuse that binding's cache; another binding or connection has a separate cache and authentication context. Eviction or reopening permits another lookup. Refresh alone does not clear the cache. Cache limits count source schema bytes, not native parser allocation overhead.

`FIELD_schema` identifies the registry endpoint and schema ID. Lookup failures retain that identity when the prefix was valid and show the returned HTTP status/body or native error in `FIELD_decode_error`, subject to credential redaction and the existing 512-byte display limit. Malformed prefixes retain raw bytes without a schema identity. Errors on one record do not hide other records; an overall request deadline or cancellation still retains the previous page. Null fields trigger no lookup. Empty bytes are a truncated Confluent prefix.

Run `make dev-up`, then open `local_redpanda` in `make run`. The [Redpanda demo](../hack/README.md#redpanda-and-schema-registry) has four Avro/Protobuf registry and catalog topics, 1,000 messages each, with live traffic through `make dev-traffic`. Local validation is not certification against a hosted Confluent deployment.

### Record values and bounds

Records include the partition offset, millisecond timestamp when available, key, value and headers. Keys and values remain bytes, even if valid UTF-8. Auto display shows valid UTF-8 as text or complete JSON objects/arrays; invalid UTF-8 falls back to hex. For example, `make dev-traffic` produces the readable key `demo` and a JSON value. Use `v` in field detail to choose text, JSON, hex or binary explicitly. Decoding never replaces invalid bytes or changes retained data. A tombstone has a null value; empty bytes remain an empty value. Headers use an ordered JSON list of names and nullable byte arrays, so duplicate names survive.

Explicit [raw Protobuf/Avro bindings](#schema-bound-key-and-value-previews) add JSON previews alongside the original fields. Auto does not identify these binary formats. Page-local filter/sort still use the stable `\x...` hexadecimal projection for raw byte fields; added columns are independently searchable.

Each page contains at most 100 records and 1 MiB of retained page data. The first page captures the native read-committed stable end; continuation tokens carry that end and the next partition offset. Open transactions and records beyond that boundary are excluded, even if committed later. Refetching page 1 or refreshing captures a new window. Retention and compaction can remove records; offsets are not consecutive row numbers. An unavailable position or expired request reports an error and keeps the displayed page/bookmark, rather than claiming the partition ended.

Metadata has no native page API: OneTUI re-reads the bounded response, sorts topics/partitions and shows a page. No browsing mode promises a cross-page snapshot. Native receiving is capped at 4 MiB, with 1 MiB of configured prefetch. The larger native cap allows Zstandard's stepped buffer growth to decode a near-1-MiB value; the retained page still must fit 1 MiB. These limits do not guarantee process RSS limits.

The native client uses a 100 ms fetch-queue refill backoff with its one-message queue threshold. This avoids the default one-second refill delay while keeping prefetch bounded. It is a fixed connector setting, listed as `fetch_queue_backoff_ms` in the catalog, not an `onetui.toml` option or a latency guarantee.

`check` validates configured decoder files and fetches broker metadata; it does not read records. Record reads use manual assignment and explicit offsets, never a topic subscription. Auto-commit, automatic offset storage and topic auto-creation are disabled. OneTUI generates an internal session group ID for the client API; it does not use an application group or commit its offsets. No user override can enable these side effects.

For authenticated browsing, grant `Read` and `Describe` on the selected topics, plus `Describe` on groups prefixed `onetui-`. The native client requires that private group ID even with manual assignment and asks for its coordinator. Group `Read` permission is unnecessary: OneTUI does not join the group or commit offsets. Metadata listing alone does not prove record-reading permission; Kafka can omit unauthorized topics from a listing.

Native work stays outside the UI and Tokio async worker threads. Cancellation stops waiting; a native call or destructor may still be finishing. One process permits only one native owner, including cleanup, so repeated switches cannot accumulate blocked workers. A replacement waits within its request deadline. The native client is reused within the selected session and released on disconnect, switch or quit.

## Live following

Open `kafka.records` for a topic or one partition, then press `f` or enter `:follow`. The initial request captures the current read-committed stable end of each selected partition and clears historical rows once it succeeds. The footer shows `LIVE`; subsequent requests fetch new committed records from those cursors, one second after the previous request finishes. Open transactions stay hidden until committed; aborted records never appear.

Press `f` or Ctrl-C to stop without quitting. Navigation, opening a record, menus and command entry also stop following before changing the view, keeping data steady for inspection. The stopped window remains readable. Pressing `f` again starts at a new current end and clears the old live window after that first request succeeds; it does not resume messages missed while stopped. `r` returns to historical browsing from the earliest available offset. Historical `n/p` paging is disabled in a live window.

The window retains at most 100 records and 1 MiB of page data, also respecting the separate 1 MiB display-projection budget. Older displayed rows are evicted as new ones arrive, with a visible `evicted locally` count. The cursor still reads forward without skipping records to catch up. A busy partition can outpace the one-batch-per-second reader. This is a bounded viewer, not a durable consumer or a complete traffic archive.

Retention invalidation, a backwards-moving boundary, an oversized value, authentication failure or deadline stops following with the last successful data retained. Restart is explicit. Normal offset gaps can come from compaction or transaction markers. There are no group subscriptions, acknowledgements or committed offsets.

Following adds no `onetui.toml` settings. The existing request timeout applies to each batch. `onetui schema --datasource kafka` lists `follow_page`, its resource and fixed limits; `onetui schema` lists the `follow` action. See [ADR-0007](adr/0007-follow-live-records-in-bounded-batches.md).

For sample arrivals, use [Kafka traffic](../hack/README.md#kafka-traffic).

SQL, publishing from the app, consumer-group administration and GSSAPI are not exposed.
