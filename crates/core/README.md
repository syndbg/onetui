# onetui-core

Configuration parsing, native async `Provider`/`Executor` contracts, request/shutdown deadlines, transport status and shared resource/page/action descriptors. No database SDK or terminal dependency. Catalog validation checks kinds, resource IDs and navigation targets; each provider validates its own options. Shared tests use a synthetic provider, not production connectors.

## Validation

```sh
make build
cargo test -p onetui-core --locked
```

Run from the workspace root. `make verify` checks all packages. Keep tests and helpers package-local, even when setup is duplicated; never combine PostgreSQL and Qdrant in one test harness.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits.
