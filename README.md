# OneTUI (`onetui`)

Explore databases and streams from your terminal.

OneTUI is an independent project for navigating data with a familiar k9s-style terminal interface: connection switching, a command palette, tables, filtering, and drill-down inspection.

Headless PostgreSQL/Qdrant checks work; the TUI, browsing commands, schema dump and configurable keybindings are not implemented yet.

The first release targets **PostgreSQL and Qdrant**, using **Rust, Ratatui, and Tokio**. DynamoDB, Kafka, NATS, and RabbitMQ are later targets.

Start with:

Project name: **OneTUI**. Repository and intended executable: **`onetui`**.

The initial product is a read-only browser. Backend-specific querying follows the browsing foundation; data editing and broker administration are outside the initial scope.

## Run the first slice

Install Rust through rustup, Make, and Docker with Compose for the local databases. The repository pins Rust 1.95.0, rustfmt and Clippy; rustup installs the pinned toolchain on first use. Build locally; there is no published package:

```sh
make dev-up
./target/debug/onetui --help
make check-local
make dev-down
```

Prefer a shorter command? Once `onetui` is on your `PATH`, add this to `~/.zshrc` (or `~/.bashrc` for Bash):

```sh
alias ot='onetui'
```

Open a new shell, then use `ot --help` or `ot --check --connection local_pg`. This is only a shell shortcut; the executable and configuration directory remain `onetui`.

`--check` performs a real metadata read, prints a short result using the connection alias, and returns nonzero on failure. It does not inspect rows/points or require a privileged health endpoint. `--timeout 5` is the default connection-check deadline (1–300 seconds); Ctrl-C cancels a pending check. Running without `--check` currently reports that the TUI is not implemented.

## Configuration

### File location

OneTUI reads **one TOML file**, selected in this order on both Linux and macOS:

1. `--config <path>`, when provided. Relative paths are relative to the current working directory.
2. `$XDG_CONFIG_HOME/onetui/config.toml`, when `XDG_CONFIG_HOME` is a nonempty absolute path.
3. `$HOME/.config/onetui/config.toml` otherwise, provided `HOME` is an absolute path.

**`~/onetui.toml` is an explicit alternative, not an automatically discovered default.** OneTUI does not search the project directory or merge configuration files. It does not create a config file or populate connections automatically; a missing/unreadable selected file is an error, not a reason to try another location. `hack/connections.toml` is only the disposable development example selected by the Make setup.

To keep your configuration directly in your home directory, save the TOML below as `~/onetui.toml` and select it explicitly:

```sh
onetui --config "$HOME/onetui.toml" --check --connection local_pg
```

This reads only the specified file, without merging the default config. Set its referenced environment variables before checking a connection. To make this location the default for your shortcut, use this alias instead of the plain `ot` alias above:

```sh
alias ot='onetui --config "$HOME/onetui.toml"'
```

### Supported settings and defaults

Currently, the only top-level setting is `[connections]`, containing named `[connections.<alias>]` entries. **PostgreSQL and Qdrant headless metadata checks are the only implemented datasource operations.** There are no built-in connection aliases or default endpoints; pass `--connection <alias>` to select an entry.

| Field | Applies to | Required / default |
| --- | --- | --- |
| `kind` | Every connection | Required: exactly `"postgres"` or `"qdrant"` |
| `url_env` | PostgreSQL | Required: name of the environment variable containing the connection string; no inline `url` field |
| `ca_file` | PostgreSQL | Optional absolute PEM trust-store path; omitted uses the native-root loader. An explicit file replaces those roots and cannot be combined with `sslmode=disable` |
| `url` | Qdrant | Required: explicit HTTP(S) gRPC endpoint; no default server is supplied |
| `api_key_env` | Qdrant | Optional environment-variable name; omitted sends no API key. If configured, its value must be present and nonempty |

Connection aliases and environment-reference names must contain only ASCII letters, digits, underscores or hyphens, and cannot be empty. Only the selected connection's environment references are resolved. The names `ONETUI_POSTGRES_URL` and `ONETUI_QDRANT_API_KEY` below are examples, not automatically read variables; you may explicitly reference other names. Store credentials in those environment variables, not inline password/API-key fields.

Unknown settings are rejected, including unsupported connection fields. Keybindings, themes, views, plugins and timeout settings are **not supported in this TOML file yet**. The check deadline is a CLI option: `--timeout` defaults to **5 seconds**, with a supported range of **1–300 seconds**. Errors do not echo config contents or driver error chains.

Example config (save it at the default location or at the explicit path passed to `--config`):

```toml
[connections.local_pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
# Optional: absolute PEM trust-store path, replacing native roots for PostgreSQL.
# ca_file = "/absolute/path/to/ca.pem"

[connections.local_qdrant]
kind = "qdrant"
url = "http://127.0.0.1:16334"
api_key_env = "ONETUI_QDRANT_API_KEY"
```

PostgreSQL requires certificate/hostname-verified TLS by default; `sslmode=prefer` is promoted to `require`, never downgraded to plaintext. The driver accepts `disable`, `prefer`, and `require` (not libpq's `verify-full` spelling). An explicit `sslmode=disable` is allowed only for loopback hosts/local Unix sockets. The check sets its own read-only/statement-timeout startup options; DSN `options` are replaced. Restricted database credentials remain the authorization boundary.

Remote Qdrant URLs must use `https://` with native trust roots; `http://` is limited to loopback hosts. Specify the gRPC port (normally 6334), not the REST port; ports are never rewritten. URL credentials, path prefixes, queries and fragments are unsupported. Qdrant custom-CA config and client certificates are not implemented. Collection-scoped keys may not permit listing; that is reported as denied, not an empty result.

The Qdrant check caps its protobuf metadata response at 1 MiB and reports an explicit limit error if exceeded; this is not a process-memory ceiling. PostgreSQL returns one boolean catalog result. Neither check proves permission to read every table/collection.

## Disposable local fixtures and tests

The [hack setup](hack/README.md) uses PostgreSQL 16.13 and Qdrant 1.18.2, binds only to loopback ports 15432/16334 (plus 16335 for a separate Qdrant TLS fixture), and stores data in temporary container memory. Its credentials are **fake, fixture-only values**. The Compose project is `onetui-fixtures`; don't reuse it for valuable data.

```sh
make help
make verify              # build, formatting, Clippy, shell syntax, non-Docker tests
make test-integration    # fresh databases -> readiness -> all fixture tests -> cleanup
make workflow-lint       # optional locally; requires Go to run pinned actionlint
```

No manual secret exports or startup retries are needed. Readiness requires a successful authenticated metadata check against each backend. `test-integration` refuses existing fixture containers instead of deleting a running development setup; use `make dev-down` first. Fixture initialization generates a private CA and localhost-only server certificate valid for two days; recreate the disposable containers if these expire.

The opt-in fixture tests use only the fixed loopback fixture endpoints. They exercise authentication failures, independent PostgreSQL offset/keyset paging, transaction-free reading pauses, TLS CA/hostname verification, cancel-over-TLS, connection loss and oversized fields. Qdrant tests cover numeric/UUID paging, lazy payload retrieval, named dense/sparse/multivectors, point deletion and oversized payload rejection. The PostgreSQL tests mutate only a dedicated fixture table and terminate only their own reader session; Qdrant tests create and remove their own collections using the fixture admin key. Default tests also check oversized metadata and sanitized gRPC errors through a local server. These client experiments are not shipped browsing commands. `dev-down` removes the fixture containers/network and their temporary data; it does not touch external databases.

## CI and releases

Separate PR and main workflows run the same Make targets on Linux x86_64 and macOS arm64; Docker integration tests run on Linux. Main also builds the release profile. Dependencies are locked and workflow actions are pinned to commit SHAs. Workflow definitions are present; hosted results are not yet verified.

Versions follow Semantic Versioning; the current development target is **v0.1.0**, represented as `0.1.0` in Cargo. Releases are published manually through the **GitHub Releases UI**, not by pushes or automatic version-bump bots. Publishing a release triggers validated binary packaging and attaches native archives plus SHA-256 files. See the [release checklist](docs/releases.md), including tag/version checks and current unsigned-platform limitations.

License and package/publication availability remain to be decided before distribution; Cargo registry publishing is disabled.
