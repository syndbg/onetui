<p align="center">
  <img src="docs/assets/onetui-logo.png" alt="OneTUI: a gold ring bearing datasource symbols and the words One TUI for them all" width="640">
</p>

<h1 align="center">OneTUI</h1>

<p align="center">One TUI for them all.</p>

<p align="center">
  <a href="https://github.com/syndbg/onetui/actions/workflows/main.yaml"><img src="https://img.shields.io/badge/CI-GitHub_Actions-2088FF" alt="CI: GitHub Actions"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License: Apache-2.0"></a>
  <a href="Cargo.toml"><img src="https://img.shields.io/badge/source-0.2.2-green" alt="Source version: 0.2.2"></a>
  <a href="rust-toolchain.toml"><img src="https://img.shields.io/badge/Rust-1.98.1-orange" alt="Rust 1.98.1"></a>
</p>

<p align="center">
  <a href="#install">Install</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#demo">Demo</a> ·
  <a href="#features-and-datasource-support">Datasources</a> ·
  <a href="#themes">Themes</a> ·
  <a href="#configuration">Configuration</a> ·
  <a href="docs/ui.md">User guide</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

OneTUI is a keyboard-driven terminal browser for databases and message streams, with navigation inspired by k9s. Inspect data, run native queries with syntax highlighting and follow live messages without switching tools.

## Install

### GitHub Releases

Download an archive or Linux package from [Releases](https://github.com/syndbg/onetui/releases).

Run the install command from the directory containing the downloaded package. Replace `VERSION` with the release version, for example `0.2.1`:

```sh
sudo apt install ./onetui-vVERSION-x86_64.deb
sudo dnf install ./onetui-vVERSION-x86_64.rpm
```

The `.deb` supports Debian 12+ and Ubuntu 22.04+. The `.rpm` supports Fedora and EL 9+. The `.tar.gz` links Debian/Ubuntu libraries, so use a package on other distros.

### Arch Linux

Install the prebuilt `.pkg.tar.zst` from [Releases](https://github.com/syndbg/onetui/releases):

```sh
sudo pacman -U ./onetui-*.pkg.tar.zst
```

Or build from source with the [PKGBUILD](packaging/arch/PKGBUILD):

```sh
curl -fsSLO https://raw.githubusercontent.com/syndbg/onetui/main/packaging/arch/PKGBUILD
makepkg -si
```

### Homebrew

```sh
# For the latest from origin/main
brew install --HEAD syndbg/tap/onetui
```

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

## Demo

Each recording is one datasource, from browsing to the query editor, against the [disposable demos](hack/README.md).

### PostgreSQL

Schemas and rows, a local filter and sort, row detail and columns, a SQL read and a write rejected for the read-only role, query history, then replicas and the WAL receiver.

![PostgreSQL in OneTUI](docs/assets/demo/postgres.gif)

### ScyllaDB and Cassandra

One `cql` connection kind for both: a connection error, keyspaces, tables, native paging, row detail, CQL reads, a rejected write, and a write applied on Cassandra.

![ScyllaDB and Cassandra in OneTUI](docs/assets/demo/cql.gif)

### Kafka

Broker configuration, a record published from the editor, then Avro and Protobuf records decoded against the schema registry, with the decoded value in full and the raw bytes as hex.

![Kafka in OneTUI](docs/assets/demo/kafka.gif)

### NATS

JetStream Avro and Protobuf messages decoded against their schemas, KV keys, object buckets, and a `CONSUME` statement.

![NATS in OneTUI](docs/assets/demo/nats.gif)

### RabbitMQ

The overview, queue metrics and detail, exchanges, bindings and policies, then a management API request and a `DECLARE` rejected for the read-only user.

![RabbitMQ in OneTUI](docs/assets/demo/rabbitmq.gif)

### Qdrant

Collections, points and a point's payload, cluster state and peers, then an HTTP request from the editor.

![Qdrant in OneTUI](docs/assets/demo/qdrant.gif)

### DynamoDB

Tables, typed items, row detail, streams and shards, then PartiQL through `ExecuteStatement`.

![DynamoDB in OneTUI](docs/assets/demo/dynamodb.gif)

See [themes](docs/themes.md) for every built-in theme.

Try the disposable demos from a source checkout:

```sh
make dev-up
make dev-run
make dev-traffic # optional live Kafka, Redpanda and NATS messages
```

See [local demos](hack/README.md) for what to open. `make dev-down` deletes the fixture data.

## Features and datasource support

| Datasource | Functionality |
| --- | --- |
| [PostgreSQL](docs/postgres.md) | Tables, views, typed values, SQL and replication statistics |
| [Qdrant](docs/qdrant.md) | Collections, points, payloads, vectors, HTTP requests and topology |
| [Kafka](docs/kafka.md) | Metadata, configuration, groups, lag, record browsing, replay, following and publishing |
| [NATS](docs/nats.md) | Core subscriptions, JetStream messages, replay, publishing, consumers, KV and objects |
| [DynamoDB](docs/dynamodb.md) | Metadata, typed items, native reads, PartiQL, vector search and Streams |
| [RabbitMQ](docs/rabbitmq.md) | Management metadata, metrics, publishing and resource administration |
| [ScyllaDB and Cassandra](docs/cql.md) | Keyspaces, tables, typed rows with native paging, and CQL |

Kafka and NATS can decode Avro and Protobuf using files, directory catalogs, Confluent registries or Buf.

The query editor highlights each datasource's language. Value inspection also supports JSON highlighting.

Use least-privilege credentials. Queries still consume server resources.

## Themes

Press `T` to preview ten built-in themes. Set `theme` in your config to keep a preference between runs. See the [theme gallery](docs/themes.md).

## Configuration

### File location

OneTUI reads one file: explicit `--config <path>`, otherwise `$XDG_CONFIG_HOME/onetui/config.toml`, or `$HOME/.config/onetui/config.toml` when XDG_CONFIG_HOME is unset or invalid. Environment paths must be absolute. Files are not merged.

A missing default file opens an empty picker. The connection form creates it when you save. An explicit `--config` file must already exist. The footer shows the selected path.

Fields ending in `_env` name environment variables, not secret values. CQL, Kafka, NATS and RabbitMQ also accept literal `username = "..."` and `password = "..."` in TOML. Each field uses either a literal value or its `_env` form. Keep config files with literal passwords private. Use TOML for nested OAuth or decoder settings. For example:

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

`schema` works offline. `--check` requires configuration and verifies the selected connection, not every resource permission. Use `--config <path>` to select another file or `--timeout <seconds>` to change the request timeout.

See connector guides for authentication and TLS. Use plaintext only for local development.

### Display settings

Theme and value-display changes in the TUI last for the session. Set persistent defaults through `theme` and `[display]` in TOML. Use `onetui schema` for fields and defaults, or the [display guide](docs/ui.md#value-display-controls) for interactive controls.

## Documentation

- [Navigation and value inspection](docs/ui.md)
- [Theme gallery](docs/themes.md)
- [Local demos](hack/README.md)

Connector guides are linked in the datasource table above. For implementation decisions, see the [ADRs](docs/adr/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and releases. `make help` lists development tasks.

For bugs, include `onetui --version`, the datasource, reproduction steps and sanitized diagnostics in a [GitHub issue](https://github.com/syndbg/onetui/issues). Configured secrets are redacted, but errors can contain server-returned data. Review them before sharing.

## License

OneTUI is licensed under [Apache-2.0](LICENSE). Redistributed binaries must retain the [third-party notices](THIRD_PARTY_NOTICES.md).
