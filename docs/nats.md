# NATS

Browse JetStream messages, consumer state, KV history and object contents, follow Core subjects, and publish to JetStream. Consumer administration and acknowledging consumed messages are not supported.

## Configuration

```toml
[connections.events]
kind = "nats"
servers = ["tls://nats.example.net:4222"]
username = "user" # or username_env = "NATS_USERNAME"
password = "..." # or password_env = "NATS_PASSWORD"
subjects = ["demo.live", "orders.*"]
```

Run `onetui --connection events`. Use `onetui schema --datasource nats` for TLS, authentication, domains and decoder settings.

Use `username_env` or `password_env` instead to read either value from an environment variable. Protect the config file if it contains a password.

Choose one authentication mode: token, username/password, NKEY, or JWT credentials. For JWT credentials, `credentials_env` names a variable containing the complete `.creds` text. Set `jetstream = false` for a Core-only server. TLS is enabled by default; plaintext is restricted to local development.

## Browsing and following

Open **Streams** for stored messages, **Consumers** for delivery state, **KV** for retained revisions, or **Objects** for metadata and content chunks. Press `m` where available for metadata. Object links are not followed automatically.

Press `f` on messages or a KV key to follow future arrivals. For Core NATS, open a configured subject under **Subjects**, then press `f`. Core has no historical replay.

See [shared controls](ui.md) for navigation and value inspection.

## Replay

Press `e` on a stream or message view. The first line names the stream, then an optional subject and range:

```text
CONSUME ORDERS orders.* seq 1..500
```

`CONSUME` takes no body. Omit the subject to read every subject in the stream, and omit the range to read from the earliest retained sequence:

```text
CONSUME ORDERS
```

The range is `seq start..end` or `time start..end`. Either side may be omitted:

| Range | Reads |
| --- | --- |
| `seq 1..500` | Sequences `[1, 500)`. |
| `seq 1..` | From sequence 1 to the end of the stream. |
| `seq ..500` | From the earliest retained sequence up to 500. |
| `time 2026-01-01T00:00:00Z..` | From the first message at or after that RFC3339 time. |
| *omitted* | Everything retained. |

The start is inclusive and the end exclusive. A `time` start resolves to a sequence; it is not a per-message time filter, and the end is always a sequence.

## Publish

Press `e` on a stream and submit:

```text
PRODUCE DEMO_LIVE demo.live

hello
```

The text after the blank line is the payload and can span lines. The named stream must accept the subject. OneTUI shows the JetStream sequence after an acknowledgment. If storage fails or the acknowledgment is lost, Core subscribers may still have received the message. Inspect before submitting again.

### Headers

Put `Name: value` lines between the verb line and the blank line:

```text
PRODUCE DEMO_LIVE demo.live
Nats-Msg-Id: order-1
src: onetui

hello
```

Names and values are trimmed. Duplicate names are sent in the order written.

### Binary payloads

Add an `Onetui-Encoding` line to decode the payload text into bytes:

```text
PRODUCE DEMO_LIVE demo.live
Onetui-Encoding: base64

3q2+7w==
```

| Encoding | Payload |
| --- | --- |
| `base64` | Standard base64. Whitespace and line breaks are ignored, so long payloads can wrap. |
| `hex` | Pairs of hex digits, whitespace ignored. |
| `utf-8` | The text as written. Same as omitting the line. |

`Onetui-Encoding` selects the decoding and is not sent as a header. To send a literal `Content-Encoding` header, write that name instead. Without an encoding line the payload is sent as UTF-8.

Use the [shared editor controls](ui.md#query-editor) for confirmation, cancellation and history.

## Permissions

Core subscriptions require subscribe permission on the selected subject. JetStream browsing requires publish permission on these read API subjects and subscribe permission on private `_INBOX.>` replies:

```text
$JS.API.INFO
$JS.API.STREAM.LIST
$JS.API.STREAM.INFO.<stream>
$JS.API.STREAM.MSG.GET.<stream>
$JS.API.CONSUMER.LIST.<stream>
$JS.API.CONSUMER.INFO.<stream>.<consumer>
```

Scope access to the streams you need. Domains use `$JS.<domain>.API`, which the server may remap before checking permissions. `--check` does not prove access to every stream or subject.
Publishing also needs publish permission on the message subject.

## Server discovery

Set `system_discovery = true` on a system-account alias, then open **Servers**. It requires publish access to `$SYS.REQ.SERVER.PING.STATSZ` and private reply subscriptions. Use `jetstream = false` for a system-only alias.

The view shows responding servers, not guaranteed complete membership or verified health. `--check` does not perform discovery.

## Payload decoding

Bind Avro or Protobuf to an exact subject:

```toml
[[connections.events.decoders]]
subject = "orders.created"
format = "avro"
schema_file = "/absolute/path/event.avsc"
```

Subjects are exact; wildcards are not accepted. Each binding uses one schema source, and the format and framing must be explicit. For raw Protobuf, set `format = "protobuf"`, use a binary descriptor set including imports, and add `message_name = "demo.Event"`. Avro accepts an optional `reader_schema_file` for reader projections.

Decoded JSON, schema identity, native typed values and errors appear beside the original `data`. A decode error leaves the original bytes available. JSON is not a lossless typed export; inspect the native view or the original bytes when type details matter.

### Local directory catalogs

Replace `schema_file` with:

```toml
catalog = { directory = "/absolute/schemas", schema = "event", references = ["customer"] }
```

Names are filename stems: Avro uses `.avsc` and explicit dependencies; Protobuf uses `.pb` descriptor sets with imports and `message_name`. Catalogs do not follow symlinks or search recursively.

### Buf Protobuf descriptors

For raw Protobuf, replace `schema_file` with:

```toml
buf = { url = "https://buf.build", module = "your-org/your-module", label = "main" }
```

Use `revision` instead of `label` to pin a commit. Add `token_env` inside `buf` for private modules. Keep `message_name` in the decoder binding.

### Confluent registry framing

For messages with Confluent payload-prefix framing:

```toml
[[connections.events.decoders]]
subject = "registered.orders"
format = "avro"
framing = "confluent"
registry = { url = "https://registry.example.com", token_env = "SCHEMA_TOKEN" }
```

Use `format = "protobuf"` for Protobuf. Omit `schema_file` and `message_name`; the message envelope identifies its schema. Registry credentials are separate from NATS credentials.

Use `onetui schema --datasource nats` for all binding options. Try `local_nats` in the [demo fixtures](../hack/README.md#nats-traffic).
