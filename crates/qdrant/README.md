# onetui-qdrant

Qdrant headless checks, verified gRPC/TLS, bounded response decoding, descriptors and Qdrant-only tests. Interactive collection/point browsing is not implemented.

`QdrantProvider` validates its own options and creates a lazy executor. Checks reuse a generated, size-capped `CollectionsClient`; cancellation/failure discards its channel. Shutdown releases it. HTTP/2 keepalive has no configured interval, and idle pings are disabled; no periodic metadata checks or heartbeat setting. Local protocol tests cover reuse, cancellation, no idle queries and channel release separately from PostgreSQL.

## Validation

```sh
make build
cargo test -p onetui-qdrant --locked
```

Run from the workspace root. `make verify` checks all packages. Keep tests and helpers package-local, even when setup is duplicated; never combine PostgreSQL and Qdrant in one test harness.

For live fixtures, use `make dev-up`, then:

```sh
cargo test -p onetui-qdrant --locked --test fixtures -- --ignored --test-threads=1
```

Finish with `make dev-down` only when ready to discard the disposable data. CLI tests use the built workspace binary; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path. Use an absolute path for custom target directories. Qdrant TLS tests also need OpenSSL to generate an unrelated test CA.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits.
