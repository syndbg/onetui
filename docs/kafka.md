# Kafka

Inspect topics, brokers, configuration, consumer groups and lag. Browse historical records or follow new arrivals within one topic. OneTUI does not publish, join application groups or commit offsets.

## Configuration

```toml
[connections.events]
kind = "kafka"
bootstrap_servers = ["broker.example:9093"]
security_protocol = "SASL_SSL"
sasl_mechanism = "SCRAM-SHA-256"
username_env = "ONETUI_KAFKA_USER"
password_env = "ONETUI_KAFKA_PASSWORD"
```

Set the referenced environment variables, then run `onetui --connection events`. Use `onetui schema --datasource kafka` for supported settings, defaults and limits.

For private CAs, set `ca_file` to an absolute PEM path. For mTLS, set `client_cert_file` and `client_key_file`; encrypted keys also need `client_key_password_env`. Use `SSL` without SASL or `SASL_SSL` with it. Advertised broker addresses must be reachable, not just bootstrap addresses.

### Kerberos (GSSAPI)

Use `sasl_mechanism = "GSSAPI"` and set `kerberos_principal` to match your system ticket cache. Obtain and renew tickets outside OneTUI. Set `KRB5_CONFIG` and `KRB5CCNAME` before launch if needed. Restart after replacing expired tickets or changing caches. See [system dependencies](../CONTRIBUTING.md#local-setup).

### OAuth

Use `sasl_mechanism = "OAUTHBEARER"` with a client-credentials endpoint:

```toml
[connections.events.oauth]
token_url = "https://identity.example/oauth/token"
client_id_env = "ONETUI_OAUTH_CLIENT_ID"
client_secret_env = "ONETUI_OAUTH_CLIENT_SECRET"
scope = "kafka.read"
```

The endpoint must return signed JWT access tokens with `sub` and `exp`. Opaque tokens and interactive login are unsupported. Token-endpoint credentials and optional `ca_file` are separate from broker authentication. Reopen the connection after rotating credentials.

## Browsing

Open **Topics** for records and configuration, **Brokers** for broker settings, or **Groups** for members and offsets. Enter on a record exposes its fields. Original key/value bytes remain available alongside any decoded views.

Group lag measures offset distance, not message count. Compaction and transactions can leave gaps. Missing permissions can hide topics or groups, so an empty list does not prove none exist.

### Browse or follow a whole topic

Choose a topic's records view to browse all its partitions, or open **Partitions** to select one. Records are ordered within each partition, not globally by timestamp. If a topic exceeds the whole-topic limit, browse individual partitions.

### Replay from an offset or timestamp

Select a partition, press `e`, and enter:

```json
{"offset":123,"end_offset":250}
```

This reads offsets `[123, 250)`. Use `timestamp_ms` instead of `offset` to start at a Unix timestamp in milliseconds. `{}` starts at the earliest retained offset. Replay is limited to one partition; unavailable positions produce errors instead of silently moving elsewhere.

See [query controls](queries.md). Use the ordinary records view for following.

## Live following

Press `f` on a topic or partition's records view to follow new committed records. Open transactions remain hidden until committed; aborted records are excluded. See [following controls](ui.md#following) for stopping and restarting.

Browsing depends on retention. Refresh to include newer records or after a partition-set change. Following retains a bounded window and can fall behind busy topics.

## Permissions

Record browsing needs topic `Read` and `Describe`, plus `Describe` on private groups prefixed `onetui-`. It does not need group `Read`. Inspecting other groups requires `Describe` on those groups.

Configuration reads need `DescribeConfigs` on the topic, or on the cluster for broker settings. `--check` validates metadata and configured schema sources, not record-reading access.

## Schema-bound key and value previews

Add a binding to your connection:

```toml
[[connections.events.decoders]]
topic = "orders"
field = "value"
format = "avro"
framing = "raw"
schema_file = "/absolute/path/event.avsc"
```

For raw Protobuf, set `format = "protobuf"`, use a binary descriptor set including imports, and add `message_name = "demo.Event"`. See the [protoc example](../crates/protobuf/README.md#try-a-raw-message) and [example bindings](../hack/kafka-decoders.toml.example). Avro supports an optional `reader_schema_file` for reader projections.

Decoded JSON, schema identity, native typed values and errors appear beside the original field. JSON is not a lossless typed export; inspect the native view or original bytes when type details matter. Schemas and framing must be explicit. Reopen the connection to reload schemas, and restart after editing binding settings.

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

Use `revision` instead of `label` to pin a commit. Add `token_env` inside `buf` for private modules. Keep `message_name` in the decoder binding. Reopen the connection to resolve a changed label.

### Confluent Avro registry

For Avro or Protobuf messages with Confluent payload-prefix framing:

```toml
[[connections.events.decoders]]
topic = "registered_orders"
field = "value"
format = "avro"
framing = "confluent"
registry = { url = "https://registry.example.com", token_env = "SCHEMA_TOKEN" }
```

Use `format = "protobuf"` for Protobuf. Omit `schema_file` and `message_name`; the message envelope identifies its schema. Registry credentials are separate from broker credentials. Header-GUID and Avro container framing are unsupported.

Use `onetui schema --datasource kafka` for all source settings. Try `local_kafka` or `local_redpanda` in the [demo fixtures](../hack/README.md).
