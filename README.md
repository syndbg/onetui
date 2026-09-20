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

OneTUI is a keyboard-driven terminal browser for databases and message streams, with navigation inspired by k9s. Inspect data, run native queries and follow live messages without switching tools.

The project is working toward **v0.1.0**. This version is **read-only**.

## Install

### GitHub Releases

Download an archive or Linux package from [Releases](https://github.com/syndbg/onetui/releases). Archives support Linux x86_64 and macOS Apple Silicon/Intel. Linux archives and `.deb` packages target Ubuntu 24.04 or newer; `.rpm` packages target Fedora 43. macOS requires version 15 or newer. Binaries are not Developer ID signed or notarized; Linux packages are unsigned.

For a user-local install, download and inspect the release's `install.sh`, then run:

```sh
sh install.sh --version v0.1.0
```

The installer verifies the checksum and binary version, then installs to `~/.local/bin`. Add that directory to `PATH`. Omit `--version` for the latest stable release. Run it again to upgrade. Configuration is preserved. Linux archives need the [runtime libraries](CONTRIBUTING.md#local-packaging-and-verification).

For system installation, use `sudo apt install ./onetui-v0.1.0-x86_64.deb` or `sudo dnf install ./onetui-v0.1.0-x86_64.rpm`. Upgrade through the same package manager. OneTUI does not update itself.

### Homebrew

```sh
brew install --HEAD syndbg/tap/onetui
```

This builds the development version from `main`, not a release. It requires Rust 1.98.1 or newer. While the repository is private, Git access is required.

### From source

Install the [build prerequisites](CONTRIBUTING.md#local-setup), then:

```sh
git clone https://github.com/syndbg/onetui.git
cd onetui
cargo install --path . --locked
```

Add Cargo's binary directory, normally `$HOME/.cargo/bin`, to `PATH`.

## Quick start

Run `onetui`. Press `a` to add a connection, fill in its fields and press F2 to save. Select it and press Enter. Set any referenced secret environment variables before launching.

Enter opens a resource or value; Esc goes back. Press `?` for available actions, `/` to filter rows, `e` for a native query or `f` to follow where supported. See the [user guide](docs/ui.md).

To try disposable sample databases from a source checkout:

```sh
make dev-up
make dev-run
```

Use `make dev-traffic` for live messages. `make dev-down` deletes the fixture data. See [local demos](hack/README.md) for prerequisites and examples.

## Features and datasource support

| Datasource | Functionality |
| --- | --- |
| [PostgreSQL](docs/postgres.md) | Tables, views, typed values, SQL and replication statistics |
| [Qdrant](docs/qdrant.md) | Collections, points, payloads, vectors, filtered Scroll and topology |
| [Kafka](docs/kafka.md) | Metadata, configuration, groups, lag, record browsing, replay and following |
| [NATS](docs/nats.md) | Core subscriptions, JetStream messages, replay, consumers, KV and objects |
| [DynamoDB](docs/dynamodb.md) | Metadata, typed items, native reads, PartiQL, vector search and Streams |
| [RabbitMQ](docs/rabbitmq.md) | Management metadata and metrics, without message inspection |

Kafka and NATS can decode Avro and Protobuf using files, directory catalogs, Confluent registries or Buf. ScyllaDB/Cassandra is planned.

Use read-only credentials. Reads still consume server resources. Connector guides explain their distinctive limits.

## Themes

[![OneTUI with the default Catppuccin theme and synthetic customer data](docs/assets/themes/catppuccin.svg)](docs/themes.md)

Press `T` to preview ten built-in themes. Set `theme` in your config to keep a preference between runs. See the [theme gallery](docs/themes.md).

## Configuration

### File location

OneTUI reads one file: explicit `--config <path>`, otherwise `$XDG_CONFIG_HOME/onetui/config.toml`, or `$HOME/.config/onetui/config.toml` when XDG_CONFIG_HOME is unset or invalid. Environment paths must be absolute. Files are not merged.

A missing default file opens an empty picker. The connection form creates it when you save. An explicit `--config` file must already exist. The footer shows the selected path.

Fields ending in `_env` name environment variables, not secret values. Use TOML for nested OAuth or decoder settings. For example:

```toml
theme = "monokai"

[connections.local_pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
```

### Settings and checks

Use the installed CLI for current settings, examples and capabilities:

```sh
onetui --help
onetui schema
onetui schema --datasource kafka
onetui --check --connection local_pg
```

`schema` works offline. `--check` requires configuration and tests the selected connector's check operation, not every resource permission. Use `--config <path>` to select another file or `--timeout <seconds>` to change the active-request deadline.

See connector guides for authentication and TLS. Use plaintext only for local development.

### Display settings

Theme and value-display changes in the TUI last for the session. Set persistent defaults through `theme` and `[display]` in TOML. Use `onetui schema` for fields and defaults, or the [display guide](docs/ui.md#value-display-controls) for interactive controls.

## Documentation

- [Navigation and value inspection](docs/ui.md)
- [Native queries](docs/queries.md)
- [Theme gallery](docs/themes.md)
- [Local demos](hack/README.md)

Connector guides are linked in the datasource table above. For implementation decisions, see the [ADRs](docs/adr/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and releases. `make help` lists development tasks. Planned work lives on the [roadmap board](https://github.com/users/syndbg/projects/3/views/1).

For bugs, include `onetui --version`, the datasource, reproduction steps and sanitized diagnostics in a [GitHub issue](https://github.com/syndbg/onetui/issues). Configured secrets are redacted, but errors can contain server-returned data. Review them before sharing.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Redistributed binaries must retain the [third-party notices](THIRD_PARTY_NOTICES.md).
