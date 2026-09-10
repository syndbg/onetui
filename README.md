# OneTUI (`onetui`)

Explore databases and streams from your terminal.

OneTUI brings k9s-style navigation to databases and streams: connection switching, a command palette, tables, filtering, and drill-down inspection.

The initial version, v0.1.0, is read-only. PostgreSQL, Qdrant, Kafka and NATS JetStream browsing are implemented; write operations and additional datasources are planned for later versions. See [validation and release status](CONTRIBUTING.md#validation-status).

## Features and datasource support

| Datasource | Status | Supported | Not supported yet |
| --- | --- | --- | --- |
| PostgreSQL | Implemented, read-only | Schemas, tables/views, column metadata, row paging, SQL query editor, cached field detail, headless checks | Query parameters, writes |
| Qdrant | Implemented, read-only | Collections and metadata, point ID paging, filtered Scroll JSON editor, on-demand payload and dense/sparse/multivector detail, headless checks | Advanced/nested filters, similarity search, writes |
| DynamoDB | Planned | None | Connector and all datasource operations |
| Kafka | [Implemented](docs/kafka.md) | Resource menu, broker/topic/partition metadata, consumer groups and members, read-committed record paging and live following, offset/timestamp replay JSON editor, byte-value inspection, headless checks, verified TLS and SASL PLAIN/SCRAM | Lag/committed offsets, broker/topic configuration, cross-partition following, publishing, group administration, schema registry; hosted release validation pending |
| NATS | [Implemented, read-only](docs/nats.md) | JetStream streams and configuration/state, sequence-based message paging and bookmarks, live following, byte/header inspection, headless checks, TLS and token/username-password configuration | Core NATS subscriptions, KV/object-store views, native queries, publishing, consumer administration, NKEY/JWT, schema registry |
| RabbitMQ | Planned | None | Connector and all datasource operations |

Qdrant browsing means opening a collection, paging through its point IDs, then opening a point's payload or vectors. Those are existing reads, not a similarity search. See [Qdrant usage](docs/qdrant.md) for navigation and limits.

All implemented datasources share connection switching, a command palette, page-local filtering and lexical sorting, request cancellation, and the offline resource/action/configuration catalog (`onetui schema`). Local filtering updates as you type and only searches cached text on the displayed page. Enter on a data row lists all fields; Enter on a field opens its full value. Single-value results such as Qdrant payloads open directly in the value viewer. PageUp/PageDown scrolls one screen within loaded data; Ctrl-U/Ctrl-D scrolls half a screen. Customizable keybindings are planned.

Press `e` or use `:query` for native querying: SQL on PostgreSQL, filtered Scroll JSON on the selected Qdrant collection, offset/timestamp replay JSON on a Kafka partition. The compact editor sits above retained rows. Enter or F5 executes; Shift+Enter adds a line; Ctrl-U clears the draft; Esc returns to rows. Ctrl-R also executes. After execution, the query stays visible above its results; press `e` to edit again. Drafts stay in memory, and `/` stays page-local. See [query examples, supported syntax and terminal requirements](docs/queries.md).

Use `n/p` to move between datasource pages. Previous pages come from the row cache or are refetched from in-memory bookmarks, so cache eviction does not prevent returning to page 1. Refetched data may have changed. [Navigation and bookmark limits](docs/ui.md#navigation) describe the behavior; `onetui schema` prints the limits.

Read and write support is the direction for OneTUI, not a capability of the initial release. Write behavior and permissions will be defined per datasource; broker administration is outside v0.1.

## Tech stack

- Rust (2024 edition), with a Cargo workspace and separate datasource packages.
- Ratatui and Crossterm for terminal rendering, keyboard input and terminal lifecycle.
- Tokio for asynchronous requests, cancellation and connection tasks.
- `tokio-postgres` with Rustls for PostgreSQL; `qdrant-client` and Tonic for Qdrant gRPC.
- `rdkafka` with native librdkafka/OpenSSL for Kafka; `async-nats` with Rustls for NATS JetStream.
- Clap for CLI arguments; Serde, JSON and TOML for configuration and the offline catalog.

Datasources are compiled into the binary using static enum dispatch. Adding one requires a connector package and a rebuild; runtime plugins are not planned.

## Build and run

Install Rust through rustup, Make, and Docker with Compose for the local databases. The repository requires and pins Rust 1.98.1, with rustfmt and Clippy; rustup installs the pinned toolchain on first use. Build locally; there is no published package:

```sh
make dev-up
./target/debug/onetui --help
make check-local
make run                # choose a local connection; q returns to the shell
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

The Context panel is the header: its title includes `read-only`, and its fields identify the selected connection and resource. There is no duplicate top strip. Available action keys sit alongside context on wide terminals and change with the current mode. A plain bar below context appears only while typing `:` commands or `/` page filters. Applied filters appear in the table title; sorting uses column arrows, and theme controls stay in context. Ten built-in themes color the shared layout; Catppuccin is the default. The footer keeps status and paging visible, with the version at bottom-right. Smaller terminals use a compact header and single-line input bar; `?` opens action help. See [UI panels](docs/ui.md) for panel contents and sizing rules.

```sh
onetui --config "$HOME/onetui.toml"
onetui --config "$HOME/onetui.toml" --connection local_pg
onetui --config "$HOME/onetui.toml" --connection local_qdrant
onetui schema
onetui schema --datasource postgres
onetui schema --datasource qdrant
onetui schema --datasource kafka
onetui schema --datasource nats
```

Interactive mode navigates schemas → tables/views → row pages → field/type detail. `Enter` opens rows/detail; `m` opens column metadata; `h/l` selects fields; `/` filters this page's cached text; `s` cycles lexical sort on the selected field; `n/p` pages (or text chunks inside detail). Row pages use eligible unique bigint/text keysets, otherwise visibly best-effort OFFSET. Both use short independent reads, not a cross-page snapshot. `schema` prints implemented resources, columns, action IDs/default keys, configuration fields, defaults and examples without loading config, resolving secrets, connecting, or taking over the terminal. Its optional `--datasource` accepts `postgres`, `qdrant`, `kafka` or `nats`. An explicit `--config` before `schema` is ignored, so the config-based `ot` alias works; `--check` and `--connection` cannot be combined with `schema`.

Qdrant navigation opens collections, then a choice of points or metadata. Point pages fetch IDs only. Open a point, then select payload or vectors for a separate read. Dense, sparse and multivectors retain their values and names; limits reject oversized detail explicitly. `make run` opens the connection picker with fixture credentials supplied; select `local_qdrant` to browse the seeded demo collections.

Without `--connection`, the TUI always starts at the connection picker, even with a single configured alias, and makes no datasource request until you choose one. Only an explicit flag such as `onetui --config "$HOME/onetui.toml" --connection local_pg` opens that datasource on startup. No previous or default connection is selected automatically.

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
| [`onetui-kafka`](crates/kafka/README.md) | Kafka TLS/SASL, native client lifetime, metadata/record reads, live batches, offset bookmarks and Kafka-only tests |
| [`onetui-nats`](crates/nats/README.md) | NATS TLS/authentication, native client lifetime, JetStream metadata/message reads, live batches, sequence bookmarks and NATS-only tests |
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
onetui schema --datasource kafka
onetui schema --datasource nats
```

The commands print JSON offline without reading your config, resolving secrets or connecting to a datasource. This is OneTUI's configuration and capability catalog, not the schema of a live database or a dump of your current settings. Use it as the version-specific reference; the table below summarizes the connection settings.

Top-level settings are the optional `theme` string, optional `[display]` table and required `[connections]` table, containing named `[connections.<alias>]` entries. Use an empty `[connections]` table when no aliases are configured. PostgreSQL supports row/metadata browsing; Qdrant supports collections, metadata, point IDs and separate payload/vector reads. Kafka browses partition records; NATS browses JetStream messages. All four support headless checks. See [Kafka settings](docs/kafka.md) and [NATS settings](docs/nats.md#configuration) for broker authentication and limits. There are no built-in connection aliases or default endpoints; pass `--connection <alias>` or use the interactive picker.

`theme` colors every TUI screen. Accepted names are exactly `catppuccin`, `gruvbox`, `solarized`, `nord`, `dracula`, `tokyo-night`, `one-dark`, `rose-pine`, `monokai` and `flexoki`. Omission selects `catppuccin`; each name selects one fixed dark palette. Names are case-sensitive. Empty/unknown names and non-string values fail validation, including with `--check`, without echoing the supplied value. Theme names are not environment-expanded. There is no separate theme file, CLI override or file watching; restart to read configuration changes. Existing files without `theme` keep the original colors. See [palette variants and sources](crates/theme/README.md#palettes).

Press `T` or type `:themes` and Enter to list all themes inside the app. `j`/`k` or arrows preview immediately; Enter keeps the choice for this session. Esc or Ctrl-C restores the previous theme. The menu leaves browsing and pending requests alone and never writes your config. Set top-level `theme` in your TOML file to change the startup default. Run `onetui schema` for supported settings and actions.

| Field | Applies to | Required / default |
| --- | --- | --- |
| `kind` | Every connection | Required: exactly `"postgres"`, `"qdrant"`, `"kafka"` or `"nats"` |
| `url_env` | PostgreSQL | Required: name of the environment variable containing the connection string; no inline `url` field |
| `ca_file` | PostgreSQL | Optional absolute path to a regular PEM file, at most 1 MiB (1,048,576 bytes); omitted uses the native-root loader. An explicit file replaces those roots and cannot be combined with `sslmode=disable` |
| `url` | Qdrant | Required: explicit HTTP(S) gRPC endpoint; no default server is supplied |
| `api_key_env` | Qdrant | Optional environment-variable name; omitted sends no API key. If configured, its value must be present and nonempty |
| `bootstrap_servers`, `security_protocol` | Kafka | Explicit broker addresses required; verified `SSL` by default. [TLS/SASL settings, values and examples](docs/kafka.md#configuration) |
| `servers`, `tls` | NATS | Explicit server URLs required; verified TLS by default. `tls=false` only for loopback development |
| `ca_file` | NATS | Optional absolute regular PEM file, at most 1 MiB; replaces native roots |
| `token_env` or paired `username_env` / `password_env` | NATS | Optional secret references, resolved on selection; mutually exclusive. [Values, validation and examples](docs/nats.md#configuration) |

Connection aliases and environment-reference names must contain only ASCII letters, digits, underscores or hyphens, and cannot be empty. Only the selected connection's environment references are resolved. The names `ONETUI_POSTGRES_URL` and `ONETUI_QDRANT_API_KEY` below are examples, not automatically read variables; you may explicitly reference other names. Store credentials in those environment variables, not inline password/API-key fields.

Every connection entry is validated, including unselected aliases: unknown fields/kinds, invalid environment-reference names, relative PostgreSQL CA paths and invalid Qdrant URLs fail when loading the file. Only the selected connection resolves secrets or opens files/transports. Existing valid configuration needs no migration; fix invalid unused entries rather than relying on them being ignored.

Keybindings, views, plugins, heartbeats and timeout settings are **not supported in this TOML file yet**. The active-request deadline is a CLI option: `--timeout` defaults to **5 seconds**, with a supported range of **1-300 seconds**. Configuration validation does not echo config contents. Datasource failures preserve native diagnostics as described below.

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

### Datasource errors

Both the TUI and `--check` preserve native error messages. PostgreSQL includes SQLSTATE, severity, message, and any detail, hint or context returned by the server. Qdrant includes the gRPC code and original status message. Connection failures include the driver's underlying network/TLS causes. For example:

```text
PostgreSQL [42501] ERROR: permission denied for table restricted_rows
```

OneTUI replaces occurrences of the selected PostgreSQL DSN/password or Qdrant API key with `[REDACTED]` and visibly escapes terminal controls. It does not print transport metadata, authorization headers or driver Debug structures. Backend messages can still contain database names or data; review diagnostics before sharing them. Local validation, cancellation and application limits retain OneTUI messages when no backend error exists. This behavior is always enabled and adds no configuration key. The TUI footer can clip long messages; `--check` writes its complete diagnostic to stderr.

### Display settings

Press `v` or enter `:display` for text, JSON, hex and binary views and independent display switches. `j/k` selects, Enter applies, Esc closes. Format selection requires field detail and lasts until detail closes. Other switches last for the session across connections; no config writes or datasource reads occur. Run `onetui schema` for supported settings, defaults and command syntax.

In Row data, the focused field expands vertically without preview truncation. PageUp/PageDown or Ctrl-U/Ctrl-D scrolls long values in place; `j/k` selects another field. Main record tables and unfocused fields keep compact previews. See [row inspection](docs/ui.md) for scrolling and display limits.

Put startup defaults in the same configuration file, for example `~/onetui.toml` selected with `onetui --config "$HOME/onetui.toml"`:

```toml
[display]
format = "auto"
pretty_print = true
highlight = true
word_wrap = true
unicode = "literal"
```

| Setting | Purpose and accepted values | Default |
| --- | --- | --- |
| `format` | Detail preference: `auto`, `text`, `json`, `hex`, `binary`; table previews use auto | `auto` |
| `pretty_print` | Boolean; indent JSON when true, preserve retained whitespace when false | `true` |
| `highlight` | Boolean; data colors, independent of selection and UI theme | `true` |
| `word_wrap` | Boolean; app-wide read-only text wrapping; `H/L` scroll content when off | `true` |
| `unicode` | `literal` for printable Unicode, `escaped` for ASCII escapes | `literal` |

The table and every field are optional. Omission uses the listed defaults. Unknown fields, empty/unknown names and wrong types fail validation, including `--check`. String values are case-sensitive and are not environment-expanded. TOML booleans are `true`/`false`; commands such as `:display word-wrap off` use `on`/`off`. Existing config discovery and relative-path rules apply; there is no display file, CLI override flag, file watching or merge layer. Runtime switches override startup defaults in memory. Restart to reread the file.

Auto displays valid UTF-8 bytes as text or complete JSON objects/arrays, falling back to hex for invalid UTF-8. Invalid bytes never become replacement characters. Hex/binary expose retained bytes with provenance; JSON formatting preserves keys and number text. Terminal controls remain escaped even with highlighting and pretty printing off. See [display behavior and bounds](docs/ui.md#value-display-controls), including preview limits and the single-line editor exceptions to wrapping. Protobuf/Avro decoding is [planned separately](docs/adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md), not included in Auto.

## Disposable local fixtures and tests

The [hack setup](hack/README.md) uses PostgreSQL 16.13, Qdrant 1.18.2, Apache Kafka 4.2.0 and NATS 2.12.15. It binds only to loopback: PostgreSQL 15432, Qdrant 16334/16335 and Kafka 19092/19093/19094 (plaintext/TLS/SASL over TLS), and NATS 14222/14223 (plaintext/TLS). Data lives in temporary container memory. Its credentials are **fake, fixture-only values**. The Compose project is `onetui-fixtures`; don't reuse it for valuable data.

For live Kafka and NATS traffic, run `make dev-traffic` after `make dev-up`. Both producers send immediately and every 15 seconds; Ctrl-C stops both. Use `make dev-traffic-kafka` or `make dev-traffic-nats` to run only one. In another terminal, `make run`, choose `local_kafka`, open `demo_live` and partition `0`, then press `f`. The fixture producer sends one record immediately and every 15 seconds. `f` or Ctrl-C stops following and retains data; navigation/inspection also pauses it. Restart begins at a new current end. See [traffic setup](hack/README.md#kafka-traffic) and [following limits](docs/kafka.md#live-following). Following adds no configuration keys.

```sh
make help
make dev-up              # start local fixtures and seed browsing demos
make dev-seed            # add demos to already-running fixtures; no reset
make run                 # choose local_pg, local_qdrant, local_kafka or local_nats
make verify              # build, formatting, Clippy, shell syntax, non-Docker tests
make test-integration    # fresh databases -> readiness -> all fixture tests -> cleanup
make workflow-lint       # workflow gate; requires Go to run pinned actionlint
```

No manual secret exports or startup retries are needed. Readiness requires a successful authenticated metadata check against each backend. `test-integration` refuses existing fixture containers instead of deleting a running development setup; use `make dev-down` first. Fixture initialization generates a private CA and localhost-only server certificate valid for two days; recreate the disposable containers if these expire.

The demo includes 8,750 PostgreSQL rows across typed, wide and relational tables, plus 3,062 Qdrant points covering dense, named, sparse and multivectors. Open PostgreSQL's `demo` schema or a Qdrant `demo_*` collection. See the [dataset inventory](hack/README.md#demo-data) for counts and types. The small `public.sample_rows`/`sample_view` fixtures remain separate: SQL NULL, empty text, literal `"NULL"`, Unicode and escaped controls test display correctness, not realistic browsing volume.

The opt-in fixture tests use only the fixed loopback fixture endpoints. They exercise authentication failures, independent PostgreSQL offset/keyset paging, transaction-free reading pauses, TLS CA/hostname verification, cancel-over-TLS, connection loss and oversized fields. Qdrant tests cover numeric/UUID paging, lazy payload retrieval, named dense/sparse/multivectors, point deletion and oversized payload rejection. The PostgreSQL tests mutate only a dedicated fixture table and terminate only their own reader session; Qdrant tests create and remove their own collections using the fixture admin key. Default tests also check oversized metadata and sanitized gRPC errors through a local server. Historical fixed-query experiments remain separate from the production PostgreSQL row/metadata tests. Connector-specific tests live under their own packages; the TUI package owns its keyboard/request-state PostgreSQL journey. Production coverage includes typed composite keysets, server-side size guards, cancellation and connection cleanup. `dev-down` removes the fixture containers/network and their temporary data; it does not touch external databases.

For NATS traffic, use the same `make dev-traffic` command after setup. `make dev-traffic-nats` runs only NATS. Choose `local_nats`, open `DEMO_LIVE`, then press `f` to follow future arrivals. `DEMO_EVENTS` has 1,205 messages; `DEMO_BINARY` has 260 byte/Unicode cases, and `DEMO_WIDE` has 110 nested 40-field messages. An empty stream is included. See [NATS usage and limits](docs/nats.md) and [fixture traffic](hack/README.md#nats-traffic). No consumer is created or acknowledged.

## Contributing

Separate PR and main workflows run the same Make targets on Linux x86_64 and macOS arm64; Docker integration tests run on Linux. Main also builds the release profile. Dependencies are locked and workflow actions are pinned to commit SHAs. See [validation status](CONTRIBUTING.md#validation-status) for the verified commit and remaining release checks.

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, pull requests, required checks and release instructions.

See [performance checks](docs/performance.md) for reproducible timing samples and memory-limit caveats. Cargo registry publishing is disabled; releases use the archive workflow described in CONTRIBUTING.md.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Release archives include the license text and [third-party theme notices](THIRD_PARTY_NOTICES.md).
