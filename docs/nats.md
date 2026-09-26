# NATS

Browse JetStream messages, consumer state, KV history and object contents, follow Core subjects, and publish to JetStream. Consumer administration and acknowledging consumed messages are not supported.

## Configuration

```toml
[connections.events]
kind = "nats"
servers = ["tls://nats.example.net:4222"]
username_env = "NATS_USERNAME"
password_env = "NATS_PASSWORD"
subjects = ["demo.live", "orders.*"]
```

Set the referenced environment variables, then run `onetui --connection events`. Use `onetui schema --datasource nats` for TLS, authentication, domains and decoder settings.

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

The start is inclusive and the end exclusive. A `time` start resolves to a sequence; it is not a per-message time filter, and the end is always a sequence. See [query controls](queries.md).

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

For raw Protobuf, use a descriptor set including imports and set `message_name`. Directory catalogs, Confluent registries and Buf are also supported. See the [schema source examples](kafka.md#schema-bound-key-and-value-previews), replacing Kafka's `topic` and `field` selectors with `subject`.

Decoded and native views appear beside the original `data`. Errors leave the original bytes available. Use `onetui schema --datasource nats` for all binding options.

Try `local_nats` in the [demo fixtures](../hack/README.md#nats-traffic).
