# NATS

Browse JetStream messages, consumer state, KV history and object contents, or follow Core subjects. OneTUI does not publish application data, create consumers or acknowledge messages.

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

Press `e` on a stream or message view:

```json
{"subject":"orders.*","start_sequence":1,"end_sequence":500}
```

The start is inclusive and the end exclusive. Use `start_time` (RFC3339) instead of `start_sequence` for time filtering. An empty filtered page can still have a next page. See [query controls](queries.md).

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
