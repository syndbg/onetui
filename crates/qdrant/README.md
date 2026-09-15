# onetui-qdrant

Qdrant collection/point browsing, lazy payload/vector reads and read-only cluster topology, with package-owned tests.

`QdrantProvider` validates its own options and creates a lazy executor. Generated, size-capped `CollectionsClient` and `PointsClient` instances share its channel; cancellation/failure discards it. Shutdown releases it. HTTP/2 keepalive has no configured interval, and idle pings are disabled; no periodic metadata checks or heartbeat setting. Local protocol tests cover reuse, cancellation, no idle queries and channel release separately from PostgreSQL.

`browse.rs` and `topology.rs` own resources, scoped continuations and bounded display formatting. Topology uses a lazy async Reqwest client with verified TLS and the same API key, through optional `rest_url`. Redirects, retries and proxy discovery are disabled. The client is released on failure, cancellation or shutdown. Menus use ordinary row targets, so the shared shell needs no backend branches or new keys.

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

`make dev-up` / `make dev-seed` run this package's `seed_demo` example against the fixed local fixture, adding five `demo_*` collections with 3,062 points. It uses the existing SDK and fake admin key, not a production write operation. Existing collections are preserved; count mismatches fail. The package-owned `demo_data_browses_all_points_and_vector_shapes` test reads every demo page and checks payload/vector shapes. See [inventory and repeat-run behavior](../../hack/README.md#demo-data).

Finish with `make dev-down` only when ready to discard the disposable data. CLI tests use the built workspace binary; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path. Use an absolute path for custom target directories. Qdrant TLS tests also need OpenSSL to generate an unrelated test CA.

The production fixture test also drives the built CLI through a Qdrant-only PTY helper, including paging, cached detail and quit. Its child receives the existing `ONETUI_QDRANT_API_KEY` reference with a fake fixture value. PostgreSQL tests are not imported or parameterized here.

Use `onetui schema --datasource qdrant` for settings, defaults and resources. See [Qdrant usage](../../docs/qdrant.md) for navigation and limits.
