# onetui-core

Configuration parsing, shared resource/page/display contracts and action descriptors. No database SDK or terminal dependency.

## Validation

```sh
make build
cargo test -p onetui-core --locked
```

Run from the workspace root. `make verify` checks all packages. Keep tests and helpers package-local, even when setup is duplicated; never combine PostgreSQL and Qdrant in one test harness.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits.
