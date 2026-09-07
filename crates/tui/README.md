# onetui-tui

Navigation state, request identity/cancellation, terminal lifecycle and rendering. Tests cover shell state, dynamic columns, chunked cached field detail and real PTYs. This package also owns a keyboard/request-state journey against the PostgreSQL fixture; connector contracts remain in their connector package.

## Validation

```sh
make build
cargo test -p onetui-tui --locked
```

Run from the workspace root. `make verify` checks all packages. Tests remain package-owned; do not introduce a shared PostgreSQL/Qdrant test matrix or harness. Small duplicated setup is intentional for isolation.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits. Enter on a relation now opens rows; use `m` for column metadata. No CLI or TOML structure change. Run `make test-integration` for the package-owned live journey (included alongside, not parameterized over, the separate connector suites).
