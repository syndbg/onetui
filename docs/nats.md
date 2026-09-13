# NATS

Browse JetStream messages, consumer state, KV entries and objects, or subscribe to Core subjects. No publishing, consumer creation or ACKs.

## Configuration

Run `onetui schema --datasource nats` for the installed binary's resources, actions, settings and limits. The dump is offline; it does not test server permissions.

Add this to the single [OneTUI configuration file](../README.md#file-location), for example `$HOME/onetui.toml`:

```toml
[connections.events]
kind = "nats"
servers = ["tls://nats.example.net:4222"]
username_env = "NATS_USERNAME"
password_env = "NATS_PASSWORD"
subjects = ["demo.live", "orders.*"]
# ca_file = "/absolute/path/to/ca.pem"
```

Set those environment variables through your secret manager or shell, then run:

```sh
onetui --config "$HOME/onetui.toml" --check --connection events
onetui --config "$HOME/onetui.toml" --connection events
onetui schema --datasource nats
```

| Setting | Purpose, accepted values and default |
| --- | --- |
| `kind` | Required string, exactly `"nats"`. |
| `jetstream` | Boolean, default `true`. Set `false` for Core-only servers; `--check` then tests the connection without requesting JetStream metadata. |
| `subjects` | Core subscription choices, default `[]`. At most 32 unique subjects of 1..1024 bytes, without whitespace, controls or empty dot-separated tokens. `*` matches one token; `>` must be the final token. |
| `servers` | Required array of 1–32 explicit `nats://host:port` or `tls://host:port` URLs, at most 1,024 bytes each. Port must be nonzero. No embedded credentials, query, fragment or non-root path. No default server; advertised cluster addresses are ignored. |
| `tls` | Boolean, default `true`: certificate and hostname verification required, including for `nats://` URLs. `false` permits only loopback `nats://` endpoints for local development. No downgrade fallback. |
| `ca_file` | Optional absolute PEM path, at most 4,096 bytes; regular file at most 1 MiB. Requires TLS. The bundle replaces native trust roots; omission uses the SDK's native-root loader. Read on connection, not during offline validation. |
| `token_env` | Optional environment-variable name containing a token; cannot be combined with username/password references. |
| `username_env`, `password_env` | Optional paired environment-variable names. Both must be supplied, or both omitted. |
| `nkey_env` | Environment reference for the user NKEY seed; native nonce signing. |
| `credentials_env` | Environment reference for complete standard `.creds` text (user JWT and NKEY seed), at most 64 KiB; multiline allowed. |
| `cert_file`, `key_file` | Paired absolute PEM paths for mTLS, each at most 1 MiB. Requires TLS; private key must be unencrypted. |
| `tls_first` | Boolean, default `false`. Handshake before server INFO; requires TLS and a compatible server. |
| `domain` | JetStream domain, 1–255 ASCII letters, digits, underscores or hyphens. Omitted uses `$JS.API`; set uses `$JS.<domain>.API`. Requires `jetstream = true`. |

Omitted authentication fields send no credentials. References use nonempty ASCII letters, digits, underscores or hyphens; the selected alias alone resolves them. Referenced secrets must be present, nonblank Unicode and at most 65,536 bytes. Controls are rejected except line breaks and tabs in `.creds` text. Unknown fields, invalid types, empty URLs/reference names and incompatible options fail validation even on unselected aliases. No URL/path environment expansion, config merging or automatic file writes. Existing connection kinds need no migration.

Choose one authentication mode: token, username/password, NKEY, or JWT credentials. Supply standard `.creds` contents through your secret manager's environment injection. Authentication secrets are captured when selecting the connection; reselect after rotation. Arbitrary API prefixes and encrypted private keys are unsupported.

## Browsing and following

| View | How to open it | Contents |
| --- | --- | --- |
| `nats.resources` | Choose the connection | Streams, consumers, KV buckets, object buckets and configured Core subjects. |
| `nats.streams` | Open Streams | Name, configured subjects, message/byte counts and consumer count. |
| `nats.subjects` | Open Subjects | Configured subjects; does not subscribe yet. |
| `nats.core_messages` | Open a subject, then press `f` | Future payloads, subject, reply address, header values and status. |
| `nats.messages` | Enter on a stream | Stream sequence, subject, stored RFC3339 time, data bytes and original header-block bytes. |
| `nats.stream_info` | `m` / `:columns` on a stream or message view | Complete returned stream configuration/state as JSON. The shared metadata action is named `columns`; NATS has no SQL columns. |
| `nats.consumers`, `nats.consumer_info` | Consumers → stream → consumer | Pending, ACK-pending, redelivery and delivery positions; full configuration/state on Enter. |
| `nats.kv_keys`, `nats.kv_history` | KV → bucket → key | Retained revisions, including delete/purge headers. `f` watches future revisions; `m` reads the latest entry. |
| `nats.objects` | Objects → bucket | Object names, including deleted entries whose metadata remains. |
| `nats.object_info`, `nats.object_chunks` | Object → Metadata or Contents | Full metadata, or lazily fetched original chunk bytes. |

KV history is limited by the bucket's retention. Object contents stay chunked: page through them without assembling the whole object in memory. Bookmarks reject changed object versions; completed reads check byte/chunk counts, not the digest. Links remain visible in metadata but are not followed automatically. Bucket/key/object listings can shift while paginating.

Enter on a message lists every field; select a field and Enter for full-value inspection. Auto shows valid UTF-8 text/JSON and falls back to hex otherwise. Empty payloads are empty bytes, absent headers are null, and duplicate header lines remain intact. Use `v` for explicit text/JSON/hex/binary and the shared pretty-print, highlighting, wrapping and Unicode controls.

`n/p` pages forward/back, including refetching bookmarks beyond the three-page cache. `r` refreshes from the current retained beginning. `/` filters the displayed page as you type; `s` sorts it lexically. Neither performs a server-side query. Byte-field filtering uses the stable hexadecimal projection, regardless of display format.

Press `f` / `:follow` in a JetStream message view to start after the stream's current last sequence. The TUI polls once per second, retaining at most 100 messages / 1 MiB and reporting locally evicted rows. `f`, Ctrl-C, navigation, inspection or an error stops following and retains the window. Restart captures a new current end; it does not resume missed history. `r` returns to historical browsing.

Messages are fetched in stream-sequence order. Historical pages retain an upper sequence boundary, but are independent reads, not a snapshot. The server skips deleted sequences. Retention or stream recreation can invalidate bookmarks; refresh rather than silently starting elsewhere. Stream listings use server offset pagination and can shift under concurrent changes.

## Permissions, lifetime and limits

On a stream or message view, `e` / `:query` opens replay JSON above the results. Enter/F5 executes; Shift-Enter adds a line. For example:

```json
{"subject":"orders.*","start_sequence":1,"end_sequence":500}
```

`subject` defaults to `>`; `start_sequence` is inclusive and `end_sequence` exclusive. Omitted bounds use the retained beginning and captured end. Use `start_time` (RFC3339) instead of `start_sequence` to filter by stored timestamp. Time replay scans at most 100 subject-matching messages per page, so an empty page can still have a next page. Bookmarks bind the query and stream version. This does not create consumers or modify stream configuration.

Core subscriptions require subscribe permission on the selected subject. They use no queue group and never answer reply addresses. Stopping follow, inspecting a value or navigating away unsubscribes and discards queued arrivals. The SDK queue holds 16 messages; overflow or disconnect stops following with a loss warning. Core has no replay: stopped or disconnected messages cannot be recovered. Header values come from the SDK, not the original wire header block.

`--check` reads JetStream account metadata. Browsing needs permission to publish only these API requests, plus subscribe to private `_INBOX.>` replies:

```text
$JS.API.INFO
$JS.API.STREAM.LIST
$JS.API.STREAM.INFO.<stream>
$JS.API.STREAM.MSG.GET.<stream>
$JS.API.CONSUMER.LIST.<stream>
$JS.API.CONSUMER.INFO.<stream>.<consumer>
```

Scope stream subjects to the streams the user may inspect. The app does not create/pull consumers, ACK, delete messages or publish application data. The [decision record](adr/0009-browse-nats-jetstream-without-consumers.md) explains why it uses stream reads instead of consumers.

Domain requests use `$JS.<domain>.API`; the server can map them to `$JS.API` before checking permissions. Match permissions to the routed subjects, as in the local secure fixture.

The selected alias owns one lazy `async-nats` client. Native PING/PONG uses a 15-second interval. Consecutive reconnect attempts are capped at the configured server count; each TCP connection attempt has a one-second timeout. Failed/cancelled reads discard the client; later requests connect again. Request deadlines cover connection, locks and all page reads; `--timeout` defaults to five seconds (1–300). Alias changes and quit drain within the shell's shutdown deadline. There is no background application heartbeat or consumer task.

Each page has at most 100 rows and 1 MiB retained data. Each API response is rejected above 1 MiB before JSON decoding, although the SDK has already received its payload. Message pages reserve half that budget for the shared hexadecimal filter projection, plus 8 KiB for labels/cursor/notice. A single message that cannot fit fails explicitly; it is not skipped. Client/reply queues hold at most 16 entries. These limits are not an RSS guarantee.

Native server error JSON, codes, descriptions and SDK error causes are preserved. Configured secrets are redacted and terminal controls escaped. Validation, cancellation and limit errors remain local messages.

## Payload decoding

Bindings match exact subjects, not subscription wildcards. They add `data_decoded`, `data_schema`, `data_decode_error`, `data_native` and `data_native_error`; raw `data` stays unchanged. JSON and native inspection can fail independently. Avro union branches and Protobuf unknown fields remain available in native inspection.

```toml
[[connections.events.decoders]]
subject = "orders.created"
format = "avro"
schema_file = "/absolute/path/event.avsc"
# reader_schema_file = "/absolute/path/reader.avsc"

[[connections.events.decoders]]
subject = "orders.updated"
format = "protobuf"
schema_file = "/absolute/path/event.pb"
message_name = "demo.Event"
```

These examples use `framing = "raw"` (the default). Files contain self-contained Avro schemas or binary Protobuf `FileDescriptorSet` bundles with imports. `--check` loads configured files/catalogs/Buf descriptors; registry checks request supported schema types, not every record schema. Browsing loads on first use; failures stay beside the raw value. Reconnect to reload files, catalogs or Buf labels.

At most 32 bindings, 256 KiB per schema, 64 KiB per decoded payload/preview. Oversized previews become per-message errors rather than removing records. Parsing runs outside the TUI, with bounded workers and cancellation/deadline checks.

### Registries and Buf

```toml
[[connections.events.decoders]]
subject = "orders.framed"
format = "avro" # or protobuf; its envelope selects the message
framing = "confluent"
registry = { url = "https://registry.example.net", token_env = "SCHEMA_TOKEN" }

[[connections.events.decoders]]
subject = "orders.buf"
format = "protobuf"
message_name = "demo.Event"
buf = { url = "https://buf.build", module = "acme/events", revision = "0123456789abcdef0123456789abcdef" }
```

Confluent bindings use the payload's exact schema ID and versioned references; no latest-version lookup or framing detection. Raw bindings choose exactly one of `schema_file`, `catalog` or `buf`. Buf accepts a pinned commit or an explicit `label` resolved once per connection. Its descriptor bundle must include imports.

Source credentials are separate from NATS credentials. HTTPS verifies certificates; HTTP is restricted to literal loopback addresses. Redirects and environment proxies are disabled. Uncached resolution has a two-second I/O bound. Run `onetui schema --datasource nats` for authentication fields, cache and dependency limits.

### Directory catalogs

Use `catalog` instead of `schema_file` to select a filename stem from a flat directory:

```toml
[[connections.events.decoders]]
subject = "orders.created"
format = "avro"
catalog = { directory = "/absolute/schemas/avro", schema = "event", references = ["customer"] }

[[connections.events.decoders]]
subject = "orders.updated"
format = "protobuf"
message_name = "demo.Event"
catalog = { directory = "/absolute/schemas/protobuf", schema = "event" }
```

Avro reads `event.avsc` and its explicitly listed dependencies, such as `customer.avsc`; optional `reader_schema_file` still applies. Protobuf reads `event.pb`, which must include imports. `data_schema` includes the catalog ID and decoder fingerprint.

Catalogs are Unix-only, reject symlinks and duplicate stems, and load at most 256 KiB per writer bundle. They stay cached until you reopen the connection, including failures. `onetui schema --datasource nats` lists the remaining bounds.

## Local demo

```sh
make dev-up
make run                 # local_nats → Streams → DEMO_EVENTS or DEMO_LIVE
make dev-traffic         # separate terminal; Kafka, Redpanda and NATS, every 15 seconds
```

Open `DEMO_LIVE` and press `f` to follow future arrivals. See [fixture setup](../hack/README.md#nats-traffic) for the finite simulator and dataset inventory. The simulator writes only disposable fixture data; it is not an app publishing feature.

`KV → DEMO_SETTINGS` includes 125 service keys, retained history, binary/empty values and delete/purge markers. `Objects → DEMO_FILES` has 110 JSON objects, a multi-page binary object, empty and deleted objects. `Consumers → DEMO_EVENTS → demo_reader` shows an unconsumed durable's pending state.

`DEMO_AVRO` and `DEMO_PROTOBUF` each contain 250 schema-bound messages with Unicode and binary fields. `make run` prepares their local schema files and config paths.
