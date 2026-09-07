# onetui-tui

Navigation state, request identity/cancellation, terminal lifecycle and rendering. Tests cover shell state and real PTYs; connector behavior is tested in its connector package.

## Validation

```sh
make build
cargo test -p onetui-tui --locked
```

Run from the workspace root. `make verify` checks all packages. Tests remain package-owned; do not introduce a shared PostgreSQL/Qdrant test matrix or harness. Small duplicated setup is intentional for isolation.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits. Package boundaries do not change the public CLI or TOML structure.
