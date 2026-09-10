# onetui-nats

Owns NATS configuration, async-nats connection lifetime, JetStream descriptors, bounded stream/message reads, live batches and sequence bookmarks. Core and TUI remain SDK-free. Tests and fixture producers live in this package.

See [usage, configuration and limits](../../docs/nats.md) and [ADR-0009](../../docs/adr/0009-browse-nats-jetstream-without-consumers.md).

```sh
cargo test -p onetui-nats --locked
make dev-up
cargo test -p onetui-nats --locked --test fixtures -- --ignored --test-threads=1
make dev-down
```

Build the CLI and `produce_nats` example before running the fixture suite directly. Root `make test-integration` does this automatically and creates fresh fixtures. Default tests do not need Docker.
