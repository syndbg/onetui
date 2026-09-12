# NATS JetStream

Browse streams, read their configuration/state, page through stored messages and follow new messages. Core NATS subscriptions, consumer administration, KV/object-store views, publishing and native queries are not implemented. DynamoDB remains planned.

## Configuration

Run `onetui schema --datasource nats` for the installed binary's resources, actions, settings and limits. The dump is offline; it does not test server permissions.

Add this to the single [OneTUI configuration file](../README.md#file-location), for example `$HOME/onetui.toml`:

```toml
[connections.events]
kind = "nats"
servers = ["tls://nats.example.net:4222"]
username_env = "NATS_USERNAME"
password_env = "NATS_PASSWORD"
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
| `servers` | Required array of 1–32 explicit `nats://host:port` or `tls://host:port` URLs, at most 1,024 bytes each. Port must be nonzero. No embedded credentials, query, fragment or non-root path. No default server; advertised cluster addresses are ignored. |
| `tls` | Boolean, default `true`: certificate and hostname verification required, including for `nats://` URLs. `false` permits only loopback `nats://` endpoints for local development. No downgrade fallback. |
| `ca_file` | Optional absolute PEM path, at most 4,096 bytes; regular file at most 1 MiB. Requires TLS. The bundle replaces native trust roots; omission uses the SDK's native-root loader. Read on connection, not during offline validation. |
| `token_env` | Optional environment-variable name containing a token; cannot be combined with username/password references. |
| `username_env`, `password_env` | Optional paired environment-variable names. Both must be supplied, or both omitted. |

Omitted authentication fields send no credentials. References use nonempty ASCII letters, digits, underscores or hyphens; the selected alias alone resolves them. Referenced secrets must be present, nonblank Unicode, contain no control characters and be at most 65,536 bytes. Unknown fields, invalid types, empty URLs/reference names and incompatible options fail validation even on unselected aliases. No URL/path environment expansion, config merging or automatic file writes. Existing connection kinds need no migration.

NKEY/JWT credentials, client TLS certificates and custom JetStream domain/API prefixes are not supported.

## Browsing and following

| View | How to open it | Contents |
| --- | --- | --- |
| `nats.streams` | Choose the connection | Name, configured subjects, message/byte counts and consumer count. |
| `nats.messages` | Enter on a stream | Stream sequence, subject, stored RFC3339 time, data bytes and original header-block bytes. |
| `nats.stream_info` | `m` / `:columns` on a stream or message view | Complete returned stream configuration/state as JSON. The shared metadata action is named `columns`; NATS has no SQL columns. |

Enter on a message lists every field; select a field and Enter for full-value inspection. Auto shows valid UTF-8 text/JSON and falls back to hex otherwise. Empty payloads are empty bytes, absent headers are null, and duplicate header lines remain intact. Use `v` for explicit text/JSON/hex/binary and the shared pretty-print, highlighting, wrapping and Unicode controls.

`n/p` pages forward/back, including refetching bookmarks beyond the three-page cache. `r` refreshes from the current retained beginning. `/` filters the displayed page as you type; `s` sorts it lexically. Neither performs a server-side query. Byte-field filtering uses the stable hexadecimal projection, regardless of display format.

Press `f` / `:follow` in a message view to start after the stream's current last sequence. The TUI polls once per second, retaining at most 100 messages / 1 MiB and reporting locally evicted rows. `f`, Ctrl-C, navigation, inspection or an error stops following and retains the window. Restart captures a new current end; it does not resume missed history. `r` returns to historical browsing.

Messages are fetched in stream-sequence order. Historical pages retain an upper sequence boundary, but are independent reads, not a snapshot. The server skips deleted sequences. Retention or stream recreation can invalidate bookmarks; refresh rather than silently starting elsewhere. Stream listings use server offset pagination and can shift under concurrent changes.

## Permissions, lifetime and limits

`--check` reads JetStream account metadata. Browsing needs permission to publish only these API requests, plus subscribe to private `_INBOX.>` replies:

```text
$JS.API.INFO
$JS.API.STREAM.LIST
$JS.API.STREAM.INFO.<stream>
$JS.API.STREAM.MSG.GET.<stream>
```

Scope stream subjects to the streams the user may inspect. The app does not create/pull consumers, ACK, delete messages or publish application data. The [decision record](adr/0009-browse-nats-jetstream-without-consumers.md) explains why it uses stream reads instead of consumers.

The selected alias owns one lazy `async-nats` client. Native PING/PONG uses a 15-second interval. Consecutive reconnect attempts are capped at the configured server count; each TCP connection attempt has a one-second timeout. Failed/cancelled reads discard the client; later requests connect again. Request deadlines cover connection, locks and all page reads; `--timeout` defaults to five seconds (1–300). Alias changes and quit drain within the shell's shutdown deadline. There is no background application heartbeat or consumer task.

Each page has at most 100 rows and 1 MiB retained data. Each API response is rejected above 1 MiB before JSON decoding, although the SDK has already received its payload. Message pages reserve half that budget for the shared hexadecimal filter projection, plus 8 KiB for labels/cursor/notice. A single message that cannot fit fails explicitly; it is not skipped. Client/reply queues hold at most 16 entries. These limits are not an RSS guarantee.

Native server error JSON, codes, descriptions and SDK error causes are preserved. Configured secrets are redacted and terminal controls escaped. Validation, cancellation and limit errors remain local messages.

## Local demo

```sh
make dev-up
make run                 # choose local_nats, then DEMO_EVENTS or DEMO_LIVE
make dev-traffic         # separate terminal; Kafka, Redpanda and NATS, every 15 seconds
```

Open `DEMO_LIVE` and press `f` to follow future arrivals. See [fixture setup](../hack/README.md#nats-traffic) for the finite simulator and dataset inventory. The simulator writes only disposable fixture data; it is not an app publishing feature.
