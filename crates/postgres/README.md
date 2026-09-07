# onetui-postgres

PostgreSQL row/metadata/check queries, verified TLS, cancellation, descriptors and PostgreSQL-only tests. Row requests use short read-only transactions, native bigint/text keysets where eligible and explicit best-effort OFFSET otherwise. Field text is guarded on the server; oversized pages fail without advancing continuation. No idle browsing cursor.

## Validation

```sh
make build
cargo test -p onetui-postgres --locked
```

Run from the workspace root. `make verify` checks all packages. Tests remain package-owned; do not introduce a shared PostgreSQL/Qdrant test matrix or harness. Small duplicated setup is intentional for isolation.

For live fixtures, use `make dev-up`, then:

```sh
cargo test -p onetui-postgres --locked --test fixtures -- --ignored --test-threads=1
```

Finish with `make dev-down` only when ready to discard the disposable data. CLI tests use the built workspace binary; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path. Use an absolute path for custom target directories.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits. Package boundaries do not change the public CLI or TOML structure.
