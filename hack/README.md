# Local fixtures

Install the [prerequisites](../CONTRIBUTING.md#local-setup), then run from the repository:

```sh
make dev-up
make check-local
make dev-run
```

These are disposable local services with fake credentials, not production templates. Do not run setup or tests concurrently. Use `make help` for tasks, [connections.toml](connections.toml) for aliases and endpoints, and [compose.yaml](compose.yaml) for service settings.

`make dev-run` loads the generated demo configuration and credentials. `make run` uses your normal configuration instead.

## Demo data

| Connection | Try |
| --- | --- |
| `local_pg` | The `demo` schema's customers, typed values and wide table |
| `local_pg_replica` | WAL receiver statistics; use the primary for Replicas |
| `local_qdrant` | `demo_*` collections, payloads, vectors and shard placement |
| `local_dynamodb` | `demo_events`, indexes, Streams and wide items |
| `local_kafka` | `demo_events`, binary records, tombstones and `demo_live` |
| `local_nats` | Streams, consumers, KV history and object contents |
| `local_nats_system` | Server discovery |
| `local_rabbitmq` | Queues, bindings, policies and populated connection metrics |

## Redpanda and Schema Registry

Choose `local_redpanda` → Topics → a topic → Records. Decoder bindings and local schemas are prepared automatically.

| Topic | Encoding and source |
| --- | --- |
| `demo_protobuf` | Confluent Protobuf |
| `demo_protobuf_catalog` | Raw Protobuf from local descriptors |
| `demo_avro` | Confluent Avro |
| `demo_avro_catalog` | Raw Avro from a local catalog |

Open a record's `value_decoded`, `value_native` or original `value` to compare representations.

## Shared traffic

Run `make dev-traffic` in another terminal for Kafka, Redpanda and NATS arrivals. Ctrl-C stops the producers. Press `f` in a records view to follow.

### Kafka traffic

Choose `local_kafka` → Topics → `demo_live` → Partitions → `0`. Use `make dev-traffic-kafka` for Kafka alone.

### NATS traffic

Choose `local_nats` → Streams → `DEMO_LIVE`, or Subjects → `demo.live`. Use `make dev-traffic-nats` for NATS alone.

## Cleanup and troubleshooting

`make dev-down` deletes fixture data. To recreate everything, stop traffic and run `make dev-reset`. `make dev-seed` seeds an existing setup without resetting it. Reopen `make dev-run` after reseeding or resetting.

Use `make dev-logs` for failures. Check Docker is running and ports are free. Fixture certificates expire after two days; reset disposable fixtures when needed. Do not bypass reset or remote-context guards. See [Contributing](../CONTRIBUTING.md) for checks.
