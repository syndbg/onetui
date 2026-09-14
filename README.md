# OneTUI (`onetui`)

Browse databases and streams with k9s-style terminal navigation.

The initial version, v0.1.0, is read-only. Writes and broker administration are planned.

## Features and datasource support

| Datasource | Supported | Not yet supported |
| --- | --- | --- |
| [PostgreSQL](docs/postgres.md) | Schemas, tables/views, columns, row paging, SQL queries | Query parameters, writes |
| [Qdrant](docs/qdrant.md) | Collections, point paging, filtered Scroll, payloads, dense/sparse/multivectors | Advanced filters, similarity search, writes |
| [Kafka](docs/kafka.md) | Metadata/configuration with synonyms, consumer groups and lag, partition/topic browsing and following, offset/timestamp replay, TLS/mTLS, SASL PLAIN/SCRAM, OAuth client credentials and GSSAPI ticket caches, Avro/Protobuf JSON and typed inspection, optional Avro reader schemas; files, directory catalogs, Confluent registries and Buf Protobuf sources | Publishing, administration |
| [NATS](docs/nats.md) | Core subscriptions, JetStream streams/messages, live following, subject/sequence/time replay, consumer state, KV history/watch, object metadata/chunks, Avro/Protobuf decoding from files, directory catalogs, Confluent registries or Buf Protobuf sources, domains, TLS/mTLS, token/user-password and NKEY/JWT authentication | Publishing, administration |
| [DynamoDB](docs/dynamodb.md) | Table/index metadata, replicas, typed items, bounded Scan/Query/GetItem, batch/transactional reads, read-only PartiQL, vector search, backup/import/export inspection, Streams/shards/records, sequence replay and following | Writes and administration |
| RabbitMQ | Planned | All operations |

Kafka browsing and following stay within one selected topic. Combining topics in one view is out of scope, not a planned feature.

All implemented connectors support headless checks. The shared UI provides connection switching, filtering, sorting, row/value inspection, cancellation and ten themes. Keybindings are currently fixed.

## Tech stack

Rust 2024, Ratatui/Crossterm and Tokio. Clients: `tokio-postgres`, `qdrant-client`/Tonic, `rdkafka`/librdkafka, `async-nats` and AWS SDK for Rust. Decoding: `apache-avro`, `prost-reflect` and `protox`. Configuration: Clap, Serde and TOML.

Providers are separate crates, registered through [static enum dispatch](docs/adr/0002-use-static-enum-dispatch-for-built-in-providers.md). No runtime plugins.

## Build and run

See [prerequisites](CONTRIBUTING.md#local-setup), then try the disposable local databases:

```sh
make dev-up
make run
```

Use `make dev-traffic` for Kafka/NATS traffic and `make dev-down` to delete fixture data. More tasks: [Makefile](Makefile), `make help`. [Local setup](hack/README.md) lists endpoints and sample datasets.

For your own databases:

```sh
make build-release
./target/release/onetui --config "$HOME/onetui.toml"
```

Once `onetui` is on your `PATH`, add a shortcut to your shell configuration:

```sh
alias ot='onetui'
# Or always use ~/onetui.toml:
alias ot='onetui --config "$HOME/onetui.toml"'
```

## Browsing and offline catalog

Without `--connection <alias>`, OneTUI starts at the connection picker without connecting.

Use Enter to open, Esc to return, `n/p` for pages, `/` for live page-local filtering and `?` for available actions. `T` chooses a theme; `v` changes value display. `e` opens a query editor: Enter/F5 executes, Shift+Enter inserts a line.

See [UI layout and controls](docs/ui.md) and [query examples](docs/queries.md). Browsing uses independent reads, not a cross-page snapshot.

## Configuration

### File location

OneTUI reads one file, in this order:

1. Explicit `--config <path>`.
2. `$XDG_CONFIG_HOME/onetui/config.toml` when XDG_CONFIG_HOME is absolute and nonempty.
3. `$HOME/.config/onetui/config.toml` otherwise, with an absolute HOME.

Files are not merged or created automatically. `~/onetui.toml` requires `--config`.

### Supported settings and defaults

Use the installed CLI as the settings and capability reference:

```sh
onetui schema
onetui schema --datasource kafka
```

This prints defaults, examples, resources and actions offline, not a live database schema. Top-level configuration supports `theme`, `display` and named `connections`; no endpoints or aliases are built in.

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

See connector guides above for TLS/authentication. Use plaintext only for local development. `--timeout` sets the active-request deadline (default 5 seconds, range 1-300); it is not a TOML setting.

### Display settings

Defaults: Catppuccin theme; Auto format with pretty printing, highlighting and word wrap enabled. Auto shows readable UTF-8 bytes as text, falling back to hex. Explicit text, JSON, hex and binary views are available through `v`; theme and display changes are session-only. See [display controls](docs/ui.md#value-display-controls) or `onetui schema` for persistent settings.

### Datasource errors

Native diagnostics are preserved, configured secrets redacted and terminal controls escaped. Errors may still contain data returned by the server; review them before sharing.

## Contributing

Use [CONTRIBUTING.md](CONTRIBUTING.md) for checks, pull requests and releases.

## License

[Apache-2.0](LICENSE). Redistributed binaries must retain [third-party notices](THIRD_PARTY_NOTICES.md).
