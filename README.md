# OneTUI (`onetui`)

Explore databases and streams from your terminal.

OneTUI brings k9s-style navigation to databases and streams: connection switching, a command palette, tables, filtering, and drill-down inspection.

The initial version, v0.1.0, is read-only. PostgreSQL and Qdrant browsing are implemented; write operations and additional datasources are planned for later versions. See [validation and release status](CONTRIBUTING.md#validation-status).

## Features and datasource support

```text
OneTUI
├── Supported now (read-only)
│   ├── PostgreSQL
│   │   ├── Schemas, tables, views and column metadata
│   │   ├── Row paging and cached field detail
│   │   └── Headless connection checks
│   └── Qdrant
│       ├── Collections and collection metadata
│       ├── Point ID paging
│       ├── On-demand payload and dense/sparse/multivector detail
│       └── Headless connection checks
├── Shared interface
│   ├── Connection switching and command palette
│   ├── Page-local filtering and lexical sorting
│   ├── Request cancellation
│   └── Offline resource/action/configuration catalog (`onetui schema`)
└── Planned (not implemented)
    ├── Datasources
    │   ├── DynamoDB
    │   ├── Kafka
    │   ├── NATS
    │   └── RabbitMQ
    ├── Write operations
    ├── Backend-specific querying
    └── Customizable keybindings
```

Read and write support is the direction for OneTUI, not a capability of the initial release. Write behavior and permissions will be defined per datasource; broker administration is outside v0.1.

## Tech stack

- Rust (2024 edition), with a Cargo workspace and separate datasource packages.
- Ratatui and Crossterm for terminal rendering, keyboard input and terminal lifecycle.
- Tokio for asynchronous requests, cancellation and connection tasks.
- `tokio-postgres` with Rustls for PostgreSQL; `qdrant-client` and Tonic for Qdrant gRPC.
- Clap for CLI arguments; Serde, JSON and TOML for configuration and the offline catalog.

Datasources are compiled into the binary using static enum dispatch. Adding one requires a connector package and a rebuild; runtime plugins are not planned.

## Build and run

Install Rust through rustup, Make, and Docker with Compose for the local databases. The repository pins Rust 1.95.0, rustfmt and Clippy; rustup installs the pinned toolchain on first use. Build locally; there is no published package:

```sh
make dev-up
./target/debug/onetui --help
make check-local
make run                # browse PostgreSQL rows/metadata; q returns to the shell
make dev-down
```

To build without Docker and use your own configured databases:

```sh
make build-release
./target/release/onetui --config "$HOME/onetui.toml"
```

The binary is `target/release/onetui`; add its directory to your `PATH` or copy it to a directory already on your `PATH`. Configure connections as described below before browsing.

Prefer a shorter command? Once `onetui` is on your `PATH`, add this to `~/.zshrc` (or `~/.bashrc` for Bash):

```sh
alias ot='onetui'
```

Open a new shell, then use `ot --help` or `ot --check --connection local_pg`. This is only a shell shortcut; the executable and configuration directory remain `onetui`.

`--check` performs a real metadata read, prints a short result using the connection alias, and returns nonzero on failure. It does not inspect rows/points or require a privileged health endpoint. `--timeout 5` is the default active-request deadline (1-300 seconds); Ctrl-C cancels pending work. Running without `--check` opens the TUI; displayed data does not expire while idle. See [PostgreSQL usage](docs/postgres.md) and [Qdrant usage](docs/qdrant.md).

## Browsing and offline catalog

```sh
onetui --config "$HOME/onetui.toml"
onetui --config "$HOME/onetui.toml" --connection local_pg
onetui --config "$HOME/onetui.toml" --connection local_qdrant
onetui schema
onetui schema --datasource postgres
onetui schema --datasource qdrant
```

Interactive mode navigates schemas → tables/views → row pages → field/type detail. `Enter` opens rows/detail; `m` opens column metadata; `h/l` selects fields; `/` filters this page's cached text; `s` cycles lexical sort on the selected field; `n/p` pages (or text chunks inside detail). Row pages use eligible unique bigint/text keysets, otherwise visibly best-effort OFFSET. Both use short independent reads, not a cross-page snapshot. `schema` prints implemented resources, columns, action IDs/default keys, configuration fields, defaults and examples without loading config, resolving secrets, connecting, or taking over the terminal. Its optional `--datasource` accepts only `postgres` or `qdrant`. An explicit `--config` before `schema` is ignored, so the config-based `ot` alias works; `--check` and `--connection` cannot be combined with `schema`.

Qdrant navigation opens collections, then a choice of points or metadata. Point pages fetch IDs only. Open a point, then select payload or vectors for a separate read. Dense, sparse and multivectors retain their values and names; limits reject oversized detail explicitly. `make run` starts on PostgreSQL; press `c` to select `local_qdrant` with the supplied fixture credentials. Fresh Qdrant fixtures are empty.

See [PostgreSQL usage](docs/postgres.md) and [Qdrant usage](docs/qdrant.md) for keys, limits and cancellation behavior. Browsing uses the connection configuration described below.

## Workspace packages

The root's `src/providers.rs` registers the compiled provider/executor enums and immutable catalog. Core defines the interfaces; each connector owns strict options, resource/action descriptors, native clients and continuation. Config loading, headless `check`, offline `schema` and the generic TUI worker use those interfaces. Adding a datasource requires its package, enum delegation and catalog entry, then a rebuild. No runtime plugins or backend selection branches in the shell.

Selected sessions connect lazily and retain healthy transports. PostgreSQL finishes each transaction before display; cancellation or failed reads discard the connection, and a later explicit read reconnects. Alias changes and quit close the executor. See [session lifetime and native keepalive defaults](docs/postgres.md#request-and-terminal-lifecycle).

| Package | Owns |
| --- | --- |
| `onetui` (root) | CLI dispatch, catalog assembly and release/developer workflow tests |
| [`onetui-core`](crates/core/README.md) | Configuration, shared display/resource contracts and actions; no database SDKs |
| [`onetui-postgres`](crates/postgres/README.md) | PostgreSQL TLS/checks, row/metadata queries, descriptors and PostgreSQL-only tests |
| [`onetui-qdrant`](crates/qdrant/README.md) | Qdrant TLS/checks, collection/point/detail reads, descriptors and Qdrant-only tests |
| [`onetui-tui`](crates/tui/README.md) | Navigation, request lifecycle, rendering and terminal tests |

Tests belong to their owning package; there is no shared PostgreSQL/Qdrant test loop. `make verify` builds and tests the whole workspace. For focused checks, build the CLI first, then use `cargo test -p onetui-postgres --locked` (or another package name). Connector CLI tests use `target/debug/onetui` by default; test-only `ONETUI_TEST_BIN` selects a prebuilt binary path when using a custom target directory (an absolute path is recommended). This variable is not application configuration.

## Configuration

### File location

OneTUI reads **one TOML file**, selected in this order on both Linux and macOS:

1. `--config <path>`, when provided. Relative paths are relative to the current working directory.
2. `$XDG_CONFIG_HOME/onetui/config.toml`, when `XDG_CONFIG_HOME` is a nonempty absolute path.
3. `$HOME/.config/onetui/config.toml` otherwise, provided `HOME` is an absolute path.

Use `--config` to select `~/onetui.toml`; it is not discovered automatically. OneTUI does not search the project directory or merge configuration files. It does not create a config file or populate connections automatically; a missing/unreadable selected file is an error, not a reason to try another location. `hack/connections.toml` is only the disposable development example selected by the Make setup.

To keep your configuration directly in your home directory, save the TOML below as `~/onetui.toml` and select it explicitly:

```sh
onetui --config "$HOME/onetui.toml" --check --connection local_pg
```

This reads only the specified file, without merging the default config. Set its referenced environment variables before checking a connection. To make this location the default for your shortcut, use this alias instead of the plain `ot` alias above:

```sh
alias ot='onetui --config "$HOME/onetui.toml"'
```

### Supported settings and defaults

Currently, the only top-level setting is `[connections]`, containing named `[connections.<alias>]` entries. PostgreSQL supports row/metadata browsing; Qdrant supports collections, metadata, point IDs and separate payload/vector reads. Both support headless checks. There are no built-in connection aliases or default endpoints; pass `--connection <alias>` or use the interactive picker.

| Field | Applies to | Required / default |
| --- | --- | --- |
| `kind` | Every connection | Required: exactly `"postgres"` or `"qdrant"` |
| `url_env` | PostgreSQL | Required: name of the environment variable containing the connection string; no inline `url` field |
| `ca_file` | PostgreSQL | Optional absolute path to a regular PEM file, at most 1 MiB (1,048,576 bytes); omitted uses the native-root loader. An explicit file replaces those roots and cannot be combined with `sslmode=disable` |
| `url` | Qdrant | Required: explicit HTTP(S) gRPC endpoint; no default server is supplied |
| `api_key_env` | Qdrant | Optional environment-variable name; omitted sends no API key. If configured, its value must be present and nonempty |

Connection aliases and environment-reference names must contain only ASCII letters, digits, underscores or hyphens, and cannot be empty. Only the selected connection's environment references are resolved. The names `ONETUI_POSTGRES_URL` and `ONETUI_QDRANT_API_KEY` below are examples, not automatically read variables; you may explicitly reference other names. Store credentials in those environment variables, not inline password/API-key fields.

Every connection entry is validated, including unselected aliases: unknown fields/kinds, invalid environment-reference names, relative PostgreSQL CA paths and invalid Qdrant URLs fail when loading the file. Only the selected connection resolves secrets or opens files/transports. Existing valid configuration needs no migration; fix invalid unused entries rather than relying on them being ignored.

Keybindings, themes, views, plugins, heartbeats and timeout settings are **not supported in this TOML file yet**. The active-request deadline is a CLI option: `--timeout` defaults to **5 seconds**, with a supported range of **1-300 seconds**. Errors do not echo config contents or driver error chains.

Example config (save it at the default location or at the explicit path passed to `--config`):

```toml
[connections.local_pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
# Optional: absolute regular PEM file, at most 1 MiB; replaces PostgreSQL native roots.
# ca_file = "/absolute/path/to/ca.pem"

[connections.local_qdrant]
kind = "qdrant"
url = "http://127.0.0.1:16334"
api_key_env = "ONETUI_QDRANT_API_KEY"
```

PostgreSQL requires certificate/hostname-verified TLS by default; `sslmode=prefer` is promoted to `require`, never downgraded to plaintext. The driver accepts `disable`, `prefer`, and `require` (not libpq's `verify-full` spelling). An explicit `sslmode=disable` is allowed only for loopback hosts/local Unix sockets. The check sets its own read-only/statement-timeout startup options; DSN `options` are replaced. Restricted database credentials remain the authorization boundary.

PostgreSQL opens the selected `ca_file` without waiting on a FIFO, then checks the opened file type and limits the bytes read. Directories, FIFOs, devices, oversized files, malformed PEM and files with no usable certificates fail explicitly. Symlinks to regular files are allowed. Existing special-file paths or bundles above 1 MiB must be replaced with a regular, smaller PEM bundle; other configuration keys and discovery rules are unchanged.

PostgreSQL certificate loading, including native-root lookup when `ca_file` is omitted, runs outside Tokio's async workers and remains inside the request deadline. Cancellation stops waiting; the OS read itself cannot always be interrupted. At most one PostgreSQL trust-loading job can remain in progress per process, so retries cannot accumulate blocked jobs. Later TLS requests wait for that slot within their own deadlines. CLI shutdown does not wait indefinitely for an abandoned OS trust read. This does not change Qdrant's trust-loading implementation.

Remote Qdrant URLs must use `https://` with native trust roots; `http://` is limited to loopback hosts. Specify the gRPC port (normally 6334), not the REST port; ports are never rewritten. URL credentials, path prefixes, queries and fragments are unsupported. Qdrant custom-CA config and client certificates are not implemented. Collection-scoped keys may not permit listing; that is reported as denied, not an empty result.

Qdrant checks and browsing cap each protobuf response at 1 MiB and report an explicit limit error if exceeded; this is not a process-memory ceiling. PostgreSQL's check returns one boolean catalog result. Neither check proves permission to read every table/collection.

## Disposable local fixtures and tests

The [hack setup](hack/README.md) uses PostgreSQL 16.13 and Qdrant 1.18.2, binds only to loopback ports 15432/16334 (plus 16335 for a separate Qdrant TLS fixture), and stores data in temporary container memory. Its credentials are **fake, fixture-only values**. The Compose project is `onetui-fixtures`; don't reuse it for valuable data.

```sh
make help
make verify              # build, formatting, Clippy, shell syntax, non-Docker tests
make test-integration    # fresh databases -> readiness -> all fixture tests -> cleanup
make workflow-lint       # workflow gate; requires Go to run pinned actionlint
```

No manual secret exports or startup retries are needed. Readiness requires a successful authenticated metadata check against each backend. `test-integration` refuses existing fixture containers instead of deleting a running development setup; use `make dev-down` first. Fixture initialization generates a private CA and localhost-only server certificate valid for two days; recreate the disposable containers if these expire.

The opt-in fixture tests use only the fixed loopback fixture endpoints. They exercise authentication failures, independent PostgreSQL offset/keyset paging, transaction-free reading pauses, TLS CA/hostname verification, cancel-over-TLS, connection loss and oversized fields. Qdrant tests cover numeric/UUID paging, lazy payload retrieval, named dense/sparse/multivectors, point deletion and oversized payload rejection. The PostgreSQL tests mutate only a dedicated fixture table and terminate only their own reader session; Qdrant tests create and remove their own collections using the fixture admin key. Default tests also check oversized metadata and sanitized gRPC errors through a local server. Historical fixed-query experiments remain separate from the production PostgreSQL row/metadata tests. Connector-specific tests live under their own packages; the TUI package owns its keyboard/request-state PostgreSQL journey. Production coverage includes typed composite keysets, server-side size guards, cancellation and connection cleanup. `dev-down` removes the fixture containers/network and their temporary data; it does not touch external databases.

## Contributing

Separate PR and main workflows run the same Make targets on Linux x86_64 and macOS arm64; Docker integration tests run on Linux. Main also builds the release profile. Dependencies are locked and workflow actions are pinned to commit SHAs. See [validation status](CONTRIBUTING.md#validation-status) for the verified commit and remaining release checks.

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, pull requests, required checks and release instructions.

See [performance checks](docs/performance.md) for reproducible timing samples and memory-limit caveats. Cargo registry publishing is disabled; releases use the archive workflow described in CONTRIBUTING.md.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Release archives include the license text.
