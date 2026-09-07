# onetui-qdrant

Qdrant headless checks, verified gRPC/TLS, bounded response decoding, descriptors and Qdrant-only tests. Interactive collection/point browsing is not implemented.

## Validation

```sh
make build
cargo test -p onetui-qdrant --locked
```

Run from the workspace root. `make verify` checks all packages. Tests remain package-owned; do not introduce a shared PostgreSQL/Qdrant test matrix or harness. Small duplicated setup is intentional for isolation.

For live fixtures, use `make dev-up`, then:

```sh
cargo test -p onetui-qdrant --locked --test fixtures -- --ignored --test-threads=1
```

Finish with `make dev-down` only when ready to discard the disposable data. CLI tests use the built workspace binary; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path. Use an absolute path for custom target directories. Qdrant TLS tests also need OpenSSL to generate an unrelated test CA.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits. Package boundaries do not change the public CLI or TOML structure.
