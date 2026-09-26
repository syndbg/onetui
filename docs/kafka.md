# Kafka

Inspect topics, brokers, configuration, consumer groups and lag. Browse historical records or follow new arrivals within one topic. Publish records from the query editor. OneTUI does not join application groups or commit offsets.

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

Use `sasl_mechanism = "GSSAPI"` and set `kerberos_principal` to match your system ticket cache. Obtain and renew tickets outside OneTUI. Set `KRB5_CONFIG` and `KRB5CCNAME` before launch if needed. See [system dependencies](../CONTRIBUTING.md#local-setup).

### OAuth

Use `sasl_mechanism = "OAUTHBEARER"` with a client-credentials endpoint:

```toml
[connections.events.oauth]
token_url = "https://identity.example/oauth/token"
client_id_env = "ONETUI_OAUTH_CLIENT_ID"
client_secret_env = "ONETUI_OAUTH_CLIENT_SECRET"
scope = "kafka.read"
```

The endpoint must return signed JWT access tokens with `sub` and `exp`. Opaque tokens and interactive login are unsupported. Token-endpoint credentials and optional `ca_file` are separate from broker authentication.

## Browsing

Open **Topics** for records and configuration, **Brokers** for broker settings, or **Groups** for members and offsets. Enter on a record exposes its fields. Original key/value bytes remain available alongside any decoded views.

Group lag measures offset distance, not message count. Compaction and transactions can leave gaps. Missing permissions can hide topics or groups, so an empty list does not prove none exist.

### Browse or follow a whole topic

Choose a topic's records view to browse all its partitions, or open **Partitions** to select one. Records are ordered within each partition, not globally by timestamp. If a topic exceeds the whole-topic limit, browse individual partitions.

## Queries

Press `e` on a topic or partition to open the query editor. A query has a verb line, a blank line, then a body:

```
VERB topic[/partition]

body
```

The verb is `CONSUME` to read records or `PRODUCE` to publish them. The verb line names its own target, so a query means the same thing wherever you run it. The editor prefills the line from the view you opened. `CONSUME` states its range on that line and takes no body; only `PRODUCE` has one. Use Shift+Enter for the newlines; see [query controls](queries.md).

### CONSUME: read from an offset or timestamp

```
CONSUME orders/0 offsets 123..250
```

This reads offsets `[123, 250)` on partition 0. `CONSUME` reads one named partition, so the verb line always needs a partition, and it takes no body.

The range is `offsets start..end` or `time start..end`. Either side may be omitted:

| Range | Reads |
| --- | --- |
| `offsets 123..250` | Offsets `[123, 250)`. |
| `offsets 123..` | From offset 123 to the current read-committed end. |
| `offsets ..250` | From the earliest retained offset up to 250. |
| `time 1750000000369..` | From the first record at or after that Unix millisecond timestamp. |
| *omitted* | The whole retained partition. |

A `time` start resolves to an offset; it is not a per-record time filter. The end is always an exclusive offset.

Omit the range to read everything retained:

```
CONSUME orders/0
```

Despite the name, `CONSUME` reads directly by offset. It joins no consumer group and commits no offsets, so it never affects another application's position. Unavailable positions produce errors instead of silently reading elsewhere.

### PRODUCE: publish records

Write one JSON record per line. Blank lines are ignored:

```
PRODUCE orders

{"key":"order-1","value":"created"}
{"key":"order-2","value":"created","headers":{"source":"onetui"}}
```

| Field | Meaning |
| --- | --- |
| `key` | Optional record key. |
| `value` | Optional record value. A record needs a key or a value. |
| `partition` | Optional target partition for this record. |
| `headers` | Optional object of header names and values. |
| `key_encoding` | How to decode `key`: `base64`, `hex` or `utf-8` (default). |
| `value_encoding` | How to decode `value`, same values. |

At most 100 records per submission.

#### Binary payloads and tombstones

Keys and values are UTF-8 text unless an encoding says otherwise:

```
PRODUCE orders

{"key":"order-1","value":"3q2+7w==","value_encoding":"base64"}
{"key":"order-2","value":"deadbeef","value_encoding":"hex"}
```

Whitespace inside a `base64` or `hex` payload is ignored. Use an encoding to publish to a topic you read through an Avro or Protobuf decoder: OneTUI does not encode the record for you, so supply the already-encoded bytes.

A `null` value is a tombstone, the deletion marker for its key on a compacted topic:

```
PRODUCE orders

{"key":"order-1","value":null}
```

A tombstone needs a key, since the key names the record being retired. An empty string value is not a tombstone; it stores an empty record.

#### Choosing a partition

The first of these that is present decides where a record goes:

1. The record's own `partition` field.
2. The partition on the verb line, such as `PRODUCE orders/2`.
3. The broker's partitioner, which hashes the key, or round-robins when there is no key.

So `PRODUCE orders` with keys lets Kafka place each record, while `PRODUCE orders/2` pins the whole batch to partition 2.

#### Results

A publish reports one outcome for the whole submission, with the offsets it was given:

| outcome | summary |
| --- | --- |
| applied | Published 2 records to orders; delivered at partition:offset 0:1042, 1:377 |

Records are sent with `acks=all`, so a reported offset is on every in-sync replica.

Publishing is never retried. `applied` means every record was delivered and `rejected` that the broker refused all of them. Anything in between is `unknown`, including a batch where some records landed and others did not: resending it would duplicate the records that already arrived, so check the topic before submitting again.

## Live following

Press `f` on a topic or partition's records view to follow new committed records. Open transactions remain hidden until committed; aborted records are excluded. See [following controls](ui.md#following) for stopping and restarting.

Browsing depends on retention. Refresh to include newer records or after a partition-set change. Following retains a bounded window and can fall behind busy topics.

## Permissions

Record browsing needs topic `Read` and `Describe`, plus group `Describe` for its temporary ID, `onetui-<process-id>-<session-id>`. OneTUI does not join that group or commit offsets, so group `Read` is not required. Inspecting consumer groups requires `Describe` for those groups. Publishing needs topic `Write`; OneTUI never creates a topic, so the topic must already exist.

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

Set `field` to `key` or `value`. Add separate bindings to decode both. Each binding can use Avro or Protobuf and its own schema source. Key-derived columns always appear before value-derived columns.

For raw Protobuf, set `format = "protobuf"`, use a binary descriptor set including imports, and add `message_name = "demo.Event"`. See the [protoc example](../crates/protobuf/README.md#try-a-raw-message) and [example bindings](../hack/kafka-decoders.toml.example). Avro supports an optional `reader_schema_file` for reader projections.

Decoded JSON, schema identity, native typed values and errors appear beside the original field. JSON is not a lossless typed export; inspect the native view or original bytes when type details matter. Schemas and framing must be explicit.

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
