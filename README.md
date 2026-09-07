# BlackPearl (`bpearl`)

Explore databases and streams from your terminal.

BlackPearl is an independent project for navigating data with a familiar k9s-style terminal interface: connection switching, a command palette, tables, filtering, and drill-down inspection.

**Status: first implementation slice. Headless PostgreSQL/Qdrant checks work; the TUI, browsing commands, schema dump and configurable keybindings are not implemented yet.**

The first release targets **PostgreSQL and Qdrant**, using **Rust, Ratatui, and Tokio**. DynamoDB, Kafka, NATS, and RabbitMQ are later targets.

Start with:

Project name: **BlackPearl**. Repository and intended executable: **`bpearl`**.

The initial product is a read-only browser. Backend-specific querying follows the browsing foundation; data editing and broker administration are outside the initial scope.

## Run the first slice

Use a recent stable Rust toolchain (validated with Rust 1.95 on macOS arm64). Build locally; there is no published package:

```sh
cargo build --locked
./target/debug/bpearl --help
./target/debug/bpearl --check --config ./fixtures/connections.toml --connection local_pg
```

`--check` performs a real metadata read, prints a short result using the connection alias, and returns nonzero on failure. It does not inspect rows/points or require a privileged health endpoint. `--timeout 5` is the default connection-check deadline (1–300 seconds); Ctrl-C cancels a pending check. Running without `--check` currently reports that the TUI is not implemented.

Configuration defaults to `$XDG_CONFIG_HOME/bpearl/config.toml`, or `$HOME/.config/bpearl/config.toml` when XDG_CONFIG_HOME is absent, empty or relative. `--config` selects exactly that file. Only the selected connection's environment references are resolved. Unknown configuration fields are rejected; errors do not echo config contents or driver error chains.

```toml
[connections.local_pg]
kind = "postgres"
url_env = "BPEARL_POSTGRES_URL"
# Optional: absolute PEM trust-store path, replacing native roots for PostgreSQL.
# ca_file = "/absolute/path/to/ca.pem"

[connections.local_qdrant]
kind = "qdrant"
url = "http://127.0.0.1:16334"
api_key_env = "BPEARL_QDRANT_API_KEY"
```

PostgreSQL requires certificate/hostname-verified TLS by default; `sslmode=prefer` is promoted to `require`, never downgraded to plaintext. The driver accepts `disable`, `prefer`, and `require` (not libpq's `verify-full` spelling). An explicit `sslmode=disable` is allowed only for loopback hosts/local Unix sockets. The check sets its own read-only/statement-timeout startup options; DSN `options` are replaced. Restricted database credentials remain the authorization boundary.

Remote Qdrant URLs must use `https://` with native trust roots; `http://` is limited to loopback hosts. Specify the gRPC port (normally 6334), not the REST port; ports are never rewritten. URL credentials, path prefixes, queries and fragments are unsupported. Qdrant custom-CA config and client certificates are not implemented. Collection-scoped keys may not permit listing; that is reported as denied, not an empty result.

The Qdrant check caps its protobuf metadata response at 1 MiB; this is not a process-memory ceiling. PostgreSQL returns one boolean catalog result. Neither check proves permission to read every table/collection.

## Disposable local fixtures and tests

The fixtures use PostgreSQL 16.13 and Qdrant 1.18.2, bind only to loopback ports 15432/16334, and store data in temporary container memory. Credentials below are **fake, fixture-only values**. The Compose project is `bpearl-fixtures`; don't reuse it for valuable data.

```sh
docker compose -f fixtures/compose.yaml up -d --wait
export BPEARL_POSTGRES_URL='postgresql://bpearl_reader:fixture-reader-only@127.0.0.1:15432/bpearl_fixture?sslmode=disable'
export BPEARL_QDRANT_API_KEY='fixture-reader-only'
cargo run --locked -- --check --config fixtures/connections.toml --connection local_pg
cargo run --locked -- --check --config fixtures/connections.toml --connection local_qdrant
cargo test --locked
cargo test --locked --test fixtures -- --ignored --test-threads=1
docker compose -f fixtures/compose.yaml down
```

Allow Qdrant to finish startup before running tests; if a check reports connection refused immediately after `up`, retry once it is ready. PostgreSQL has a readiness check. Fixture initialization generates a private CA and localhost-only server certificate valid for two days; recreate the disposable containers if these expire.

The opt-in fixture tests use only the fixed loopback fixture endpoints. They exercise real authentication failures, PostgreSQL text/cursor paging and rollback, TLS CA/hostname verification, and Qdrant numeric/UUID paging with on-demand payload/vector retrieval. The Qdrant test creates and removes its own collection using the fixture admin key. These client experiments are not shipped browsing commands. `down` removes the fixture containers/network and their temporary data; it does not touch external databases.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
```

License and package/publication availability remain to be decided before distribution; Cargo publishing is disabled.
