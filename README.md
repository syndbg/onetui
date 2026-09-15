<p align="center">
  <img src="docs/assets/onetui-logo.png" alt="OneTUI: a gold ring bearing datasource symbols and the words One TUI for them all" width="640">
</p>

<h1 align="center">OneTUI</h1>

<p align="center">One TUI for them all.</p>

<p align="center">
  <a href="https://github.com/syndbg/onetui/actions/workflows/main.yaml"><img src="https://img.shields.io/badge/CI-GitHub_Actions-2088FF" alt="CI: GitHub Actions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License: Apache-2.0"></a>
  <a href="rust-toolchain.toml"><img src="https://img.shields.io/badge/Rust-1.98.1-orange" alt="Rust 1.98.1"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#features-and-datasource-support">Datasources</a> ·
  <a href="#themes">Themes</a> ·
  <a href="#configuration">Configuration</a> ·
  <a href="docs/ui.md">User guide</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

OneTUI is a keyboard-driven terminal browser for databases and message streams, with navigation inspired by k9s. Inspect rows, points and messages, run native queries, and follow live streams without switching tools.

The project is working toward **v0.1.0**. This initial version is **read-only**. Writes and broker administration are planned for later versions.

## Why OneTUI?

- Use the same connection picker, navigation and value inspector across datasources.
- Browse one page at a time, return through page bookmarks, and cancel active requests.
- Inspect JSON, text, hex and binary values. Decode Avro and Protobuf without losing access to the original bytes.
- Filter loaded data as you type, sort columns, and choose from ten color themes.
- Check connections and inspect available settings and actions without opening the TUI.

## Install

### Homebrew

Install the development version from [syndbg/tap](https://github.com/syndbg/homebrew-tap):

```sh
brew install --HEAD syndbg/tap/onetui
```

Homebrew builds from source on macOS or Linux and installs the build dependencies. It needs Rust 1.98.1 or newer. There are no OneTUI bottles yet, and `--HEAD` follows `main`, not a release. While the repository is private, Git access is required.

### From source

Build from source with the pinned Rust toolchain and [native build tools](CONTRIBUTING.md#local-setup):

```sh
git clone https://github.com/syndbg/onetui.git
cd onetui
cargo install --path . --locked
```

Cargo installs `onetui` in its binary directory, normally `$HOME/.cargo/bin`. Add that directory to your `PATH`. To build without installing, use `make build-release` and run `./target/release/onetui`.

## Quick start

Try the sample databases from the repository with Docker Compose running:

```sh
make dev-up
make run
```

Choose a connection and press Enter. The demos include wide tables, typed and binary values, vector collections, and Avro/Protobuf messages with a local Schema Registry.

Run `make dev-traffic` in another terminal for Kafka, Redpanda and NATS traffic. Press `f` on a resource that supports following. Use `make dev-down` when finished. It deletes the disposable fixture data.

For your own databases, run:

```sh
onetui
```

Press `a` to add a connection. Choose a datasource, fill in the fields, then press F2 to save. Without `--connection <alias>`, OneTUI stays at the picker and makes no datasource request. [Local setup](hack/README.md) covers sample datasets and endpoints. Use `make help` for available tasks.

For a shorter command, add this to your shell configuration:

```sh
alias ot='onetui'
# Or always use ~/onetui.toml:
alias ot='onetui --config "$HOME/onetui.toml"'
```

## Features and datasource support

| Datasource | Supported reads | Not yet supported |
| --- | --- | --- |
| [PostgreSQL](docs/postgres.md) | Schemas, tables/views, columns, row paging, SQL queries | Query parameters, writes |
| [Qdrant](docs/qdrant.md) | Collections, point paging, filtered Scroll, payloads, dense/sparse/multivectors | Advanced filters, similarity search, writes |
| [Kafka](docs/kafka.md) | Broker/topic metadata, configuration and synonyms, consumer groups and lag, partition/topic browsing and following, offset/timestamp replay, Avro/Protobuf inspection and Avro reader schemas | Publishing, administration |
| [NATS](docs/nats.md) | Core subscriptions, JetStream streams/messages, live following, subject/sequence/time replay, consumer state, KV history/watch, object metadata/chunks, Avro/Protobuf decoding and domains | Publishing, administration |
| [DynamoDB](docs/dynamodb.md) | Table/index metadata, replicas, typed items, bounded Scan/Query/GetItem, batch/transactional reads, read-only PartiQL, vector search, backup/import/export inspection, Streams/shards/records, sequence replay and following | Writes and administration |
| RabbitMQ | Planned | All operations |

Kafka and NATS support schema files, directory catalogs, Confluent registries and Buf Protobuf sources. Their guides cover TLS/mTLS and authentication: Kafka supports SASL PLAIN/SCRAM, OAuth client credentials and GSSAPI ticket caches. NATS supports tokens, user/password and NKEY/JWT.

Kafka browsing and following stay within one selected topic. Combining topics in one view is out of scope. DynamoDB cloud-only metadata and vector search have protocol tests, not live AWS validation.

All implemented connectors support headless checks. Browsing uses independent reads, not a cross-page snapshot. Use read-only datasource credentials where available. Reads may consume capacity even though they do not change stored data.

## Controls

The context header shows actions available in the current view. Press `?` for help or `:` to enter a command. Keybindings are currently fixed.

| Key | Action |
| --- | --- |
| `j` / `k`, arrows | Move between rows |
| Enter / Esc | Open a resource or value / go back |
| `h` / `l` | Select a field |
| `n` / `p` | Next / previous data page or value chunk |
| PageUp / PageDown | Scroll within loaded data |
| Ctrl-U / Ctrl-D | Scroll half a screen outside text entry |
| `/` | Filter the loaded page as you type |
| `s` | Cycle column sort |
| `e` | Open the native query editor |
| `f` | Start or stop following, where supported |
| `T` / `v` | Choose a theme / change value display |
| `c` / `r` | Choose a connection / refresh |
| Ctrl-C / `q` | Cancel active work, or quit when idle / quit |

In the query editor, Enter or F5 executes and Shift+Enter inserts a line. See [query examples and terminal requirements](docs/queries.md) and the [UI guide](docs/ui.md) for details.

## Themes

OneTUI includes **Catppuccin** (default), **Gruvbox**, **Solarized**, **Nord**, **Dracula**, **Tokyo Night**, **One Dark**, **Rosé Pine**, **Monokai** and **Flexoki**.

[![OneTUI with the default Catppuccin theme and synthetic customer data](docs/assets/themes/catppuccin.svg)](docs/themes.md)

Press `T` or enter `:themes` to preview themes. Enter keeps the choice for this session. Esc restores the previous theme. Set `theme = "monokai"` in your config to keep a preference between runs.

See the [gallery of all ten themes](docs/themes.md) for previews and exact configuration names.

## Configuration

### File location

OneTUI reads one file, in this order:

1. Explicit `--config <path>`.
2. `$XDG_CONFIG_HOME/onetui/config.toml` when XDG_CONFIG_HOME is absolute and nonempty.
3. `$HOME/.config/onetui/config.toml` otherwise, with an absolute HOME.

If the default file is missing, the TUI opens an empty connection picker. `a` or `:add` creates a connection. Tab moves between fields, F2 saves, and Esc discards. The footer shows the config path. Nothing is written until you save.

Files are not merged. An explicit `--config` path must exist, and `--check` still requires a file. Invalid or unreadable files remain errors. To use `~/onetui.toml`, create it and pass `--config "$HOME/onetui.toml"`.

The form covers endpoints, TLS and authentication references. Lists use commas; booleans use `true` or `false`; blank optional fields keep provider defaults. `_env` fields take environment variable names, not secret values. Set those variables before launching OneTUI. Edit TOML for nested OAuth or decoder settings.

Saving adds an entry without changing existing text or session-only display preferences. New files are private (0600 on Unix). Detected external edits, duplicate aliases, read-only files and symlinks block saving. Reopen after external edits; for a symlink, pass its target path.

### Supported settings and defaults

Use the installed CLI as the settings and capability reference:

```sh
onetui schema
onetui schema --datasource kafka
```

This prints defaults, examples, resources and actions offline, not a live database schema. Top-level configuration supports `theme`, `display` and named `connections`. No endpoints or aliases are built in.

Example `~/onetui.toml`:

```toml
theme = "monokai"

[connections.local_pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"

[connections.local_qdrant]
kind = "qdrant"
url = "http://127.0.0.1:16334"
api_key_env = "ONETUI_QDRANT_API_KEY"
```

Set the referenced environment variables before connecting. They are explicit secret references, not automatically discovered settings. Qdrant requires its gRPC endpoint, not its REST port.

```sh
onetui --config "$HOME/onetui.toml" --check --connection local_pg
```

See connector guides above for TLS/authentication. Use plaintext only for local development. `--timeout` sets the active-request deadline (default 5 seconds, range 1-300). It is not a TOML setting.

### Display settings

The defaults are Catppuccin and Auto format, with pretty printing, highlighting and word wrap enabled. Auto shows readable UTF-8 bytes as text and falls back to hex. Explicit text, JSON, hex and binary views are available through `v`. Theme and display changes are session-only. See [display controls](docs/ui.md#value-display-controls) or `onetui schema` for persistent settings.

### Datasource errors

OneTUI preserves native diagnostics, redacts configured secrets and escapes terminal controls. Errors may still contain data returned by the server. Review them before sharing.

## Tech stack

OneTUI uses Rust 2024, Ratatui/Crossterm and Tokio. Its native clients are `tokio-postgres`, `qdrant-client`/Tonic, `rdkafka`/librdkafka, `async-nats` and the AWS SDK for Rust. Decoding uses `apache-avro`, `prost-reflect` and `protox`. Clap, Serde and TOML handle the CLI and configuration.

Providers live in separate crates and use [static enum dispatch](docs/adr/0002-use-static-enum-dispatch-for-built-in-providers.md). Datasources are built into the binary. There are no runtime plugins.

## Documentation

- [UI layout and controls](docs/ui.md)
- [Theme gallery](docs/themes.md)
- [Native queries](docs/queries.md)
- [Local demos and troubleshooting](hack/README.md)
- [Architecture decisions](docs/adr/)

## Contributing

Bug reports and focused pull requests are welcome. For a bug, include the OneTUI version, datasource, reproduction steps and sanitized diagnostics in a [GitHub issue](https://github.com/syndbg/onetui/issues).

Read [CONTRIBUTING.md](CONTRIBUTING.md) for setup, required checks, pull requests and the release process. Run `make help` for development tasks.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Redistributed binaries must retain the [third-party notices](THIRD_PARTY_NOTICES.md).
