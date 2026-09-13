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

`make` tasks generate `target/demo-onetui.toml` from [connections.toml](connections.toml), with absolute catalog paths under `target/demo-schemas`. `make run` loads it and supplies fixture credentials. For service settings, see [compose.yaml](compose.yaml).

| Alias | Endpoint |
| --- | --- |
| `local_pg` | PostgreSQL: `127.0.0.1:15432` |
| `local_qdrant` | gRPC: `127.0.0.1:16334`; TLS fixture: `16335` |
| `local_kafka` | Kafka: `127.0.0.1:19092`; TLS: `19093`; SASL/TLS: `19094`; mTLS test listener: `19095` |
| `local_redpanda` | Kafka: `127.0.0.1:29092`; Schema Registry: `http://127.0.0.1:18081` |
| `local_nats` | NATS: `127.0.0.1:14222`; TLS fixture: `14223` |

TLS certificates expire after two days; recreate disposable fixtures when expired. The setup does not modify the host trust store. Remote Docker contexts are refused.

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

Choose `local_nats` → `DEMO_LIVE`, then `f`. Use `make dev-traffic-nats` for NATS alone.

Following starts at the current end. `f` or Ctrl-C stops; navigation pauses it. Restarting follows new arrivals, not missed history.

## Demo data

| Datasource | Try |
| --- | --- |
| PostgreSQL | `demo` schema: customers, events, type samples and a 65-column table; 8,750 rows |
| Qdrant | `demo_*` collections: 3,062 points with payloads, dense/sparse/multivectors and an empty collection |
| Kafka | `demo_events`, `demo_binary`, `demo_tombstones`, `demo_empty` |
| NATS | `DEMO_EVENTS`, `DEMO_BINARY`, `DEMO_WIDE`, `DEMO_EMPTY`, `DEMO_LIVE` |

Use `make dev-seed` to seed existing fixtures without resetting them. Interrupted or changed datasets may need an explicit reset. Small `sample_rows` fixtures test NULL, empty text, Unicode and controls; use `demo` for realistic browsing volume.

## Validation

Run `make verify`, `make test-integration` and `make workflow-lint`. Integration tests refuse existing fixtures; stop your development session with `make dev-down` only when its temporary data can be deleted.

## Troubleshooting

Use `make dev-logs`. Check Docker is running, listed ports are free and fixture certificates have not expired. Do not bypass existing-fixture or remote-context guards.
