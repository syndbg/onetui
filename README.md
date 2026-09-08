# OneTUI (`onetui`)

Explore databases and streams from your terminal.

OneTUI brings k9s-style navigation to databases and streams: connection switching, a command palette, tables, filtering, and drill-down inspection.

The initial version, v0.1.0, is read-only. PostgreSQL and Qdrant browsing are implemented; write operations and additional datasources are planned for later versions. See [validation and release status](CONTRIBUTING.md#validation-status).

## Features and datasource support

| Datasource | Status | Supported | Not supported yet |
| --- | --- | --- | --- |
| PostgreSQL | Implemented, read-only | Schemas, tables/views, column metadata, row paging, cached field detail, headless checks | SQL editor, server-side query/filter controls, writes |
| Qdrant | Implemented, read-only | Collections and metadata, point ID paging, on-demand payload and dense/sparse/multivector detail, headless checks | Server-side payload filtering, similarity search, writes |
| DynamoDB | Planned | None | Connector and all datasource operations |
| Kafka | Planned | None | Connector and all datasource operations |
| NATS | Planned | None | Connector and all datasource operations |
| RabbitMQ | Planned | None | Connector and all datasource operations |

Qdrant browsing means opening a collection, paging through its point IDs, then opening a point's payload or vectors. Those are existing reads, not a similarity search. See [Qdrant usage](docs/qdrant.md) for navigation and limits.

PostgreSQL and Qdrant share connection switching, a command palette, page-local filtering and lexical sorting, request cancellation, and the offline resource/action/configuration catalog (`onetui schema`). Local filtering only searches cached text on the displayed page. Customizable keybindings and backend-specific querying are planned.

Read and write support is the direction for OneTUI, not a capability of the initial release. Write behavior and permissions will be defined per datasource; broker administration is outside v0.1.

## Tech stack

- Rust (2024 edition), with a Cargo workspace and separate datasource packages.
- Ratatui and Crossterm for terminal rendering, keyboard input and terminal lifecycle.
- Tokio for asynchronous requests, cancellation and connection tasks.
- `tokio-postgres` with Rustls for PostgreSQL; `qdrant-client` and Tonic for Qdrant gRPC.
- Clap for CLI arguments; Serde, JSON and TOML for configuration and the offline catalog.

Datasources are compiled into the binary using static enum dispatch. Adding one requires a connector package and a rebuild; runtime plugins are not planned.

## Build and run

Install Rust through rustup, Make, and Docker with Compose for the local databases. The repository requires and pins Rust 1.98.1, with rustfmt and Clippy; rustup installs the pinned toolchain on first use. Build locally; there is no published package:

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

The TUI puts connection and resource context above the data table, with available action keys alongside it on wide terminals. Context controls change with the current mode. A plain bar below context appears only while typing `:` commands or `/` page filters. Applied filters appear in the table title; sorting uses column arrows, and theme controls stay in context. Ten built-in themes color the shared layout; Catppuccin is the default. Contrasting selection, sort arrows and labeled status colors distinguish navigation, loading and errors. The footer keeps status and paging visible. Smaller terminals use a compact header and single-line input bar; `?` opens action help.

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
| [`onetui-theme`](crates/theme/README.md) | Built-in theme names and semantic RGB palettes; no terminal or datasource dependencies |

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

Start with the CLI dump when configuring OneTUI. It describes the settings, defaults, examples, resources and actions known to your installed version:

```sh
onetui schema
onetui schema --datasource postgres
onetui schema --datasource qdrant
```

The commands print JSON offline without reading your config, resolving secrets or connecting to a datasource. This is OneTUI's configuration and capability catalog, not the schema of a live database or a dump of your current settings. Use it as the version-specific reference; the table below summarizes the connection settings.

Top-level settings are the optional `theme` string and required `[connections]` table, containing named `[connections.<alias>]` entries. Use an empty `[connections]` table when no aliases are configured. PostgreSQL supports row/metadata browsing; Qdrant supports collections, metadata, point IDs and separate payload/vector reads. Both support headless checks. There are no built-in connection aliases or default endpoints; pass `--connection <alias>` or use the interactive picker.

`theme` colors every TUI screen. Accepted names are exactly `catppuccin`, `gruvbox`, `solarized`, `nord`, `dracula`, `tokyo-night`, `one-dark`, `rose-pine`, `monokai` and `flexoki`. Omission selects `catppuccin`; each name selects one fixed dark palette. Names are case-sensitive. Empty/unknown names and non-string values fail validation, including with `--check`, without echoing the supplied value. Theme names are not environment-expanded. There is no separate theme file, CLI override or file watching; restart to read configuration changes. Existing files without `theme` keep the original colors. See [palette variants and sources](crates/theme/README.md#palettes).

Press `T` or type `:themes` and Enter to list all themes inside the app. `j`/`k` or arrows preview immediately; Enter keeps the choice for this session. Esc or Ctrl-C restores the previous theme. The menu leaves browsing and pending requests alone and never writes your config. Set top-level `theme` in your TOML file to change the startup default. Run `onetui schema` for supported settings and actions.

| Field | Applies to | Required / default |
| --- | --- | --- |
| `kind` | Every connection | Required: exactly `"postgres"` or `"qdrant"` |
| `url_env` | PostgreSQL | Required: name of the environment variable containing the connection string; no inline `url` field |
| `ca_file` | PostgreSQL | Optional absolute path to a regular PEM file, at most 1 MiB (1,048,576 bytes); omitted uses the native-root loader. An explicit file replaces those roots and cannot be combined with `sslmode=disable` |
| `url` | Qdrant | Required: explicit HTTP(S) gRPC endpoint; no default server is supplied |
| `api_key_env` | Qdrant | Optional environment-variable name; omitted sends no API key. If configured, its value must be present and nonempty |

Connection aliases and environment-reference names must contain only ASCII letters, digits, underscores or hyphens, and cannot be empty. Only the selected connection's environment references are resolved. The names `ONETUI_POSTGRES_URL` and `ONETUI_QDRANT_API_KEY` below are examples, not automatically read variables; you may explicitly reference other names. Store credentials in those environment variables, not inline password/API-key fields.

Every connection entry is validated, including unselected aliases: unknown fields/kinds, invalid environment-reference names, relative PostgreSQL CA paths and invalid Qdrant URLs fail when loading the file. Only the selected connection resolves secrets or opens files/transports. Existing valid configuration needs no migration; fix invalid unused entries rather than relying on them being ignored.

Keybindings, views, plugins, heartbeats and timeout settings are **not supported in this TOML file yet**. The active-request deadline is a CLI option: `--timeout` defaults to **5 seconds**, with a supported range of **1-300 seconds**. Errors do not echo config contents or driver error chains.

Example config (save it at the default location or at the explicit path passed to `--config`):

```toml
# Optional; put this before connection tables. Omitted uses "catppuccin".
theme = "monokai"

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
make dev-up              # start local fixtures and seed browsing demos
make dev-seed            # add demos to already-running fixtures; no reset
make run                 # browse PostgreSQL; c switches to local_qdrant
make verify              # build, formatting, Clippy, shell syntax, non-Docker tests
make test-integration    # fresh databases -> readiness -> all fixture tests -> cleanup
make workflow-lint       # workflow gate; requires Go to run pinned actionlint
```

No manual secret exports or startup retries are needed. Readiness requires a successful authenticated metadata check against each backend. `test-integration` refuses existing fixture containers instead of deleting a running development setup; use `make dev-down` first. Fixture initialization generates a private CA and localhost-only server certificate valid for two days; recreate the disposable containers if these expire.

The demo includes 8,750 PostgreSQL rows across typed, wide and relational tables, plus 3,062 Qdrant points covering dense, named, sparse and multivectors. Open PostgreSQL's `demo` schema or a Qdrant `demo_*` collection. See the [dataset inventory](hack/README.md#demo-data) for counts and types. The small `public.sample_rows`/`sample_view` fixtures remain separate: SQL NULL, empty text, literal `"NULL"`, Unicode and escaped controls test display correctness, not realistic browsing volume.

The opt-in fixture tests use only the fixed loopback fixture endpoints. They exercise authentication failures, independent PostgreSQL offset/keyset paging, transaction-free reading pauses, TLS CA/hostname verification, cancel-over-TLS, connection loss and oversized fields. Qdrant tests cover numeric/UUID paging, lazy payload retrieval, named dense/sparse/multivectors, point deletion and oversized payload rejection. The PostgreSQL tests mutate only a dedicated fixture table and terminate only their own reader session; Qdrant tests create and remove their own collections using the fixture admin key. Default tests also check oversized metadata and sanitized gRPC errors through a local server. Historical fixed-query experiments remain separate from the production PostgreSQL row/metadata tests. Connector-specific tests live under their own packages; the TUI package owns its keyboard/request-state PostgreSQL journey. Production coverage includes typed composite keysets, server-side size guards, cancellation and connection cleanup. `dev-down` removes the fixture containers/network and their temporary data; it does not touch external databases.

## Contributing

Separate PR and main workflows run the same Make targets on Linux x86_64 and macOS arm64; Docker integration tests run on Linux. Main also builds the release profile. Dependencies are locked and workflow actions are pinned to commit SHAs. See [validation status](CONTRIBUTING.md#validation-status) for the verified commit and remaining release checks.

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, pull requests, required checks and release instructions.

See [performance checks](docs/performance.md) for reproducible timing samples and memory-limit caveats. Cargo registry publishing is disabled; releases use the archive workflow described in CONTRIBUTING.md.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Release archives include the license text and [third-party theme notices](THIRD_PARTY_NOTICES.md).
