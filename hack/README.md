# Local fixtures

Use [Makefile](../Makefile) tasks; `make help` lists them. [Prerequisites](../CONTRIBUTING.md#local-setup).

```sh
make dev-up
make check-local
make run
```

`make dev-down` deletes fixture data. These are disposable, loopback-only services with fake credentials and temporary storage, not production templates. Do not run setup/tests concurrently; ports and the Compose project are fixed.

## Fixture security and configuration

`make run` supplies fixture credentials and selects [connections.toml](connections.toml). For exact service settings, see [compose.yaml](compose.yaml).

| Alias | Endpoint |
| --- | --- |
| `local_pg` | PostgreSQL: `127.0.0.1:15432` |
| `local_qdrant` | gRPC: `127.0.0.1:16334`; TLS fixture: `16335` |
| `local_kafka` | Kafka: `127.0.0.1:19092`; TLS: `19093`; SASL/TLS: `19094` |
| `local_redpanda` | Kafka: `127.0.0.1:29092`; Schema Registry: `http://127.0.0.1:18081` |
| `local_nats` | NATS: `127.0.0.1:14222`; TLS fixture: `14223` |

TLS certificates expire after two days; recreate disposable fixtures when expired. The setup does not modify the host trust store. Remote Docker contexts are refused.

## Redpanda and Schema Registry

Choose `local_redpanda` → Topics → `demo_avro` → Records. The fixture contains 1,000 Avro records using two writer versions and a referenced schema. Decoder bindings are already configured.

Apache Kafka remains alongside Redpanda for broker-specific security and transaction tests. Protobuf registry decoding has a separate integration fixture; `demo_avro` remains Avro.

## Shared traffic

Run `make dev-traffic` in another terminal. Kafka and NATS receive a message immediately, then every 15 seconds; Ctrl-C stops both. This does not produce Redpanda traffic.

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
