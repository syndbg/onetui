# Local fixtures

Use [Makefile](../Makefile) tasks; `make help` lists them. [Prerequisites](../CONTRIBUTING.md#local-setup).

```sh
make dev-up
make check-local
make run
```

`make dev-down` deletes fixture data. These are disposable, loopback-only services with fake credentials and temporary storage, not production templates. Do not run setup/tests concurrently; ports and the Compose project are fixed.

For a clean restart, stop `make dev-traffic`, then run `make dev-reset`. This deletes all fixture data, recreates services/certificates and reseeds demos. Reopen `make run` afterward.

## Fixture security and configuration

`make` tasks generate `target/demo-onetui.toml` from [connections.toml](connections.toml), with absolute catalog paths under `target/demo-schemas` for Redpanda and `target/nats-schemas` for NATS. `make run` loads it and supplies fixture credentials. For service settings, see [compose.yaml](compose.yaml).

| Alias | Endpoint |
| --- | --- |
| `local_pg` | PostgreSQL: `127.0.0.1:15432` |
| `local_dynamodb` | DynamoDB Local: `http://127.0.0.1:18000` |
| `local_qdrant` | gRPC: `127.0.0.1:16334`; TLS fixture: `16335` |
| `local_kafka` | Kafka: `127.0.0.1:19092`; TLS: `19093`; SASL/TLS: `19094`; mTLS: `19095`; OAuth: `19096`; GSSAPI: `19097`; KDC: `18888` (TCP/UDP) |
| `local_redpanda` | Kafka: `127.0.0.1:29092`; Schema Registry: `http://127.0.0.1:18081` |
| `local_nats` | NATS: `127.0.0.1:14222`; TLS: `14223`; mTLS/NKEY/domain: `14224`; JWT Core: `14225` |

TLS certificates expire after two days; recreate disposable fixtures when expired. The setup does not modify the host trust store. Remote Docker contexts are refused.

OAuth tests start their own token endpoint and use disposable RSA keys. Kafka's [JWT validator](https://kafka.apache.org/42/javadoc/org/apache/kafka/common/security/oauthbearer/OAuthBearerValidatorCallbackHandler.html) checks signatures, issuer and audience; no external identity provider is needed.

GSSAPI tests use a disposable `ONETUI.TEST` KDC and private child-process ticket caches. Host Kerberos credentials stay untouched. All fixture keys and tickets disappear with `make dev-down`.

## Redpanda and Schema Registry

Choose `local_redpanda` → Topics → a topic below → Records. Each starts with 1,000 messages; `make dev-traffic` appends to all four every 15 seconds. Decoder bindings and local schemas are generated automatically.

| Topic | Encoding and schema source |
| --- | --- |
| `demo_protobuf` | Confluent Protobuf: two writer versions, imported Customer schema |
| `demo_protobuf_catalog` | Raw Protobuf: local descriptor set with imports |
| `demo_avro` | Confluent Avro: two writer versions, referenced Customer schema |
| `demo_avro_catalog` | Raw Avro: local writer schema and Customer reference |

Enter opens a record; select `value_decoded` for readable JSON. `value` retains wire bytes. `value_schema` identifies the source; `value_decode_error` should be NULL. Protobuf samples include nested records, Unicode, arrays, bytes, booleans, numbers and optional fields. Sources: [event.proto](fixtures/schemas/event.proto), [customer.proto](fixtures/schemas/customer.proto).

For an existing setup, use `make dev-seed`, then restart `make run`. Seeding preserves existing messages, including traffic. Apache Kafka remains alongside Redpanda for broker-specific security and transaction tests.

## Shared traffic

Run `make dev-traffic` in another terminal. Kafka, Redpanda and NATS receive messages immediately after setup, then every 15 seconds; Ctrl-C stops all producers. For Redpanda, choose any topic above and press `f`.

### Kafka traffic

Choose `local_kafka` → `demo_live` → partition `0`, then `f`. Use `make dev-traffic-kafka` for Kafka alone.

### NATS traffic

Choose `local_nats` → Streams → `DEMO_LIVE`, then `f`. For Core NATS, choose Subjects → `demo.live`, then `f`. Use `make dev-traffic-nats` for NATS alone.

Following starts at the current end. `f` or Ctrl-C stops; navigation pauses it. Restarting follows new arrivals, not missed history.

## Demo data

| Datasource | Try |
| --- | --- |
| PostgreSQL | `demo` schema: customers, events, type samples and a 65-column table; 8,750 rows |
| DynamoDB | `demo_events`: 1,205 typed items, GSI and LSI; `demo_wide`: 32 items with 65 extra columns; `demo_empty` |
| Qdrant | `demo_*` collections: 3,062 points with payloads, dense/sparse/multivectors and an empty collection |
| Kafka | `demo_events`, `demo_binary`, `demo_tombstones`, `demo_empty` |
| NATS | `DEMO_EVENTS`, `DEMO_BINARY`, `DEMO_WIDE`, `DEMO_EMPTY`, `DEMO_LIVE`, catalog-decoded `DEMO_AVRO`/`DEMO_PROTOBUF`, registry-decoded `DEMO_AVRO_REGISTRY`/`DEMO_PROTOBUF_REGISTRY` (250 messages each), KV `DEMO_SETTINGS`, objects `DEMO_FILES`, durable `demo_reader` |

Use `make dev-seed` to seed existing fixtures without resetting them. Interrupted or changed datasets may need an explicit reset. Small `sample_rows` fixtures test NULL, empty text, Unicode and controls; use `demo` for realistic browsing volume.

## Validation

Run `make verify`, `make test-integration` and `make workflow-lint`. Integration tests refuse existing fixtures; stop your development session with `make dev-down` only when its temporary data can be deleted.

## Troubleshooting

Use `make dev-logs`. Check Docker is running, listed ports are free and fixture certificates have not expired. Do not bypass existing-fixture or remote-context guards.
