# Local development

Run these commands from the repository root. Requirements: rustup, Make, Bash, CMake, a C/C++ compiler, Perl, Docker Engine/Desktop and Docker Compose with `up --wait` support. Kafka's native libraries build through Cargo. The OpenSSL command is also required for TLS fixtures. macOS and Linux are the initial targets. No database installation, manual secret exports or RTK installation is required.

Linux builds also require CURL development headers (`libcurl4-openssl-dev` on Debian/Ubuntu) for the pinned librdkafka CMake build, even though its CURL runtime feature is disabled. See [contributor setup](../CONTRIBUTING.md#local-setup) for build prerequisites.

```sh
make dev-up        # builds, starts databases, waits for reads, seeds demos
make dev-seed      # adds demos to running fixtures without resetting them
make check-local   # checks all fixture aliases, including Redpanda's registry
make run           # connection picker; uses existing fixtures, q quits
make dev-logs
make dev-down      # removes this fixture project and its temporary data
```

`make run` builds and supplies fake PostgreSQL, Qdrant and NATS credentials; it does not start/recreate containers. It opens the connection picker without contacting a datasource. Choose `local_pg`, `local_qdrant`, `local_kafka`, `local_redpanda` or `local_nats` and press Enter. PostgreSQL opens schemas, relations, then rows and field detail; `m` opens column metadata, `/` filters the displayed page and `s` cycles local lexical sort. Open schema `demo` for larger tables, a Qdrant `demo_*` collection, or a Kafka `demo_*` topic and partition. Press `c` to return to the picker. Only an explicit CLI `--connection <alias>` skips the picker. See [PostgreSQL usage](../docs/postgres.md), [Qdrant usage](../docs/qdrant.md), [Kafka usage](../docs/kafka.md) and [NATS usage](../docs/nats.md).

`compose.yaml` is the only Compose definition. PostgreSQL 16.13 listens on `127.0.0.1:15432`; Qdrant 1.18.2 gRPC listens on `127.0.0.1:16334`; Apache Kafka 4.2.0 listens on `127.0.0.1:19092`. Kafka runs one combined KRaft broker/controller with auto topic creation disabled and no authentication on the local development listener. The `onetui-fixtures` project is reserved for disposable data. Storage is tmpfs, so even restarting containers can lose fixture state; recreate with `dev-down` then `dev-up`. There are no persistent data volumes. Do not place valuable data in these containers.

A separate `qdrant-tls` fixture exposes gRPC on `127.0.0.1:16335` with a localhost-only certificate. Its private CA/keys are generated inside tmpfs at startup. Compose waits for a verified TLS handshake; the integration test performs authenticated metadata reads and negative trust/hostname/authentication/protocol checks. It supplies the public CA to only the child CLI through `SSL_CERT_FILE`/`SSL_CERT_DIR`; nothing is installed in the OS trust store. `make check-local` checks all five connection aliases, including Redpanda and its registry.

Redpanda 26.2.2 also runs in this Compose project, with its Kafka API at `127.0.0.1:29092` and Schema Registry at `http://127.0.0.1:18081`. It uses one CPU shard, a 512 MiB broker-memory setting and disposable tmpfs storage. Both published ports bind only to loopback, without authentication or TLS. Metrics reporting and automatic topic creation are disabled. Apache Kafka remains alongside it for the existing broker-specific configuration, authentication and transaction tests. No separate registry server or Console is needed. See Redpanda's [single-broker setup](https://docs.redpanda.com/labs/docker-compose/single-broker/).

## Redpanda and Schema Registry

`make dev-up` starts the real registry and seeds `demo_avro` with 1,000 records. In `make run`, choose `local_redpanda` → Topics → `demo_avro` → Records. This alias still uses `kind = "kafka"`; Redpanda is not another connector. Its `value` decoder binding already points at the local registry in [connections.toml](connections.toml).

The records alternate between two versions of an Avro `Event` writer schema. Both reference a named `Customer` schema; the second adds an optional `note` field. Values include nested records, arrays and Unicode text. `value_decoded` shows JSON, `value_schema` identifies the exact registry ID, and the original `value` retains the Confluent prefix and datum bytes. Enter opens row data; select the original field and choose hex to inspect the wire representation.

```sh
make dev-up
make run
# Or open just this connection; no environment secrets required:
./target/debug/onetui --config hack/connections.toml --connection local_redpanda
curl --fail http://127.0.0.1:18081/subjects
curl --fail http://127.0.0.1:18081/subjects/demo_avro_value/versions
```

`make dev-seed` preserves an existing `demo_avro` only when it still has one partition and offsets `[0, 1000)`. Unexpected or partial data fails without deleting it. Schema registration is idempotent for these fixed schemas; IDs are returned by the registry, never hard-coded. Setup uses only the fixed local endpoints. Registry mutations belong to fixture setup/tests, not the read-only application. The real-registry integration case owns a separate topic and subjects, exercises paging/replay/following, then deletes its topic and soft-deletes its subjects. A fixture reset removes the remaining registry storage.

`make dev-traffic` remains Apache Kafka plus NATS; it does not append to `demo_avro`. Protobuf registry decoding is still unsupported. Redpanda's local registry is plaintext; registry TLS/authentication, redirects and timeout failures remain covered by separate HTTP/TLS protocol fixtures. This setup is not suitable for exposed or production deployments.

## Fixture security and configuration

Apache Kafka also binds verified TLS on `127.0.0.1:19093` and SASL over TLS on `127.0.0.1:19094`, advertised as `localhost`. Its self-signed localhost certificate and keys live in container tmpfs for two days. Tests copy only the public certificate into a temporary `ca_file`; the OS trust store is unchanged. `kafka-security.sh` sets fixture-only PLAIN/SCRAM credentials and grants `fixture-reader` topic `Read`/`Describe` on `demo_*` and group `Describe` on `onetui-*`. It grants no group `Read`. `fixture-denied` has no ACLs. These accounts and the unauthenticated development listener are only for this disposable broker, not a deployment template.

The scripts set fixture-only credentials in their own process and do not modify your shell/config. `connections.toml` contains the theme, connection aliases, endpoint and environment references. Press `T` or use `:themes` to preview all palettes with `j`/`k` or arrows; Enter keeps for this session, Esc or Ctrl-C restores the previous theme. To persist a startup preference, edit top-level `theme` in that file; the menu never writes it. `onetui schema` lists known settings and actions. A bare `onetui --check --config hack/connections.toml` still needs `--connection` and the referenced environment variables; use `make check-local` to supply them automatically.

If you ran the previous `bpearl-fixtures` project, it remains untouched by this rename and may still occupy the same ports. When ready to discard its temporary data, run `docker compose --project-name bpearl-fixtures -f hack/compose.yaml down`, then `make dev-up` for the new `onetui-fixtures` project. Configuration now defaults to `~/.config/onetui/config.toml`; old configuration is not moved automatically. Environment references are explicit TOML values, so existing custom variable names still work when explicitly configured.

## Shared traffic

Run `make dev-traffic` after `make dev-up` to produce Kafka and NATS traffic together. It checks both connections and builds both package-owned producers before starting either. Each producer sends immediately, then every 15 seconds. Output identifies Kafka's `demo_live` partition/offset or NATS's `DEMO_LIVE` stream sequence.

Ctrl-C or SIGTERM stops and reaps both producers. If either exits, the command stops the other and returns the exited producer's status. It does not restart producers or remove fixtures. Both brokers must be ready; a failed preflight/build starts neither producer. No new TOML settings or environment variables are needed.

```sh
make dev-up
make dev-traffic         # Kafka and NATS together
make dev-traffic-kafka   # Kafka only, instead of the shared command
make dev-traffic-nats    # NATS only, instead of the shared command
```

These commands append to disposable fixtures without reading user connection configuration. Do not run multiple traffic commands unless you want multiple independent producers.

## Kafka traffic

Kafka supports [raw decoder bindings](../docs/kafka.md#schema-bound-key-and-value-previews). Copy the relevant binding from [kafka-decoders.toml.example](kafka-decoders.toml.example), replacing the absolute schema path and topic. For ready-made registry data, use the [Redpanda fixture](#redpanda-and-schema-registry). HTTP/TLS protocol tests create their own loopback registry servers without changing host trust. The traffic producers still send their existing JSON/byte fixtures; they do not generate schema-bound messages. Isolated Kafka integration tests create their own Avro and Protobuf topics and remove them afterward. Without a broker, use the standalone [Protobuf](../crates/protobuf/README.md#try-a-raw-message) or [Avro](../crates/avro/README.md#try-a-raw-message) example.

To browse all seeded partitions together, choose `local_kafka` → Topics → `demo_events` → `kafka.records`. The extra `partition` column identifies each record's source; `n/p` replays pages and `f` follows the whole topic. The same entry under `demo_live` follows fixture traffic. Topic-wide reads allow up to 32 partitions, without global timestamp ordering; use Partitions for larger topics or offset/timestamp replay.

In OneTUI, choose `local_kafka`, Topics, `demo_live`, Partitions, partition `0`, then `f` to follow. A topic also offers Configuration; Enter on a broker opens its settings. Groups offers Members or Offsets for the selected group. To replay seeded data, open Topics → `demo_events` → Partitions → partition `0`, press `e`, clear with Ctrl-U and execute `{"offset":123,"end_offset":250}`. [Kafka usage](../docs/kafka.md) documents the inputs, limits and permissions. No connection configuration changes are required.

Auto display shows the demo key as `demo` and the value as JSON; invalid UTF-8 in the other datasets remains hex. Retained bytes are unchanged. Open a field and use `v` to select hex/binary explicitly.

After `make dev-up`, run `make dev-traffic` (both brokers) or `make dev-traffic-kafka` (Kafka only) in a separate terminal. The Kafka producer sends one JSON record immediately, then one every 15 seconds until Ctrl-C. It connects only to the fixed disposable broker at `127.0.0.1:19092`, creates `demo_live` with one partition if absent and appends without resetting existing data. Newly created topics use a one-hour / 64 MiB retention policy; Kafka applies retention asynchronously. Existing topic settings are preserved. The four seeded Kafka datasets are unchanged.

In another terminal run `make run`, select `local_kafka`, open `demo_live` and partition `0`, then press `f` in the record view. `f` or Ctrl-C stops following and keeps the displayed window. Navigation/inspection also stops it. Starting again reads only from a new current end. `r` returns to historical browsing.

For a finite simulator run:

```sh
cargo run -p onetui-kafka --example produce_demo --locked -- --count 2
```

`--count` is an optional positive integer number of records. Omission runs until interrupted; zero, invalid or unknown arguments fail. Two records take at least 15 seconds. The endpoint, topic and interval are fixed; the simulator does not read `onetui.toml` or environment credentials. Each record has a run-local sequence, send timestamp and synthetic message. Delivery failures exit with the native error. This example is a fixture tool, not an app write capability.

The integration runner builds the example. Kafka's live CLI test runs the two-record producer and checks timed arrival, stop, retained row inspection, restart and connection switching. It uses only the disposable fixture.

## NATS traffic

NATS 2.12.15 runs on `127.0.0.1:14222`; a separate verified-TLS fixture uses `127.0.0.1:14223`. Their local Docker build adds OpenSSL to generate two-day fixture certificates in tmpfs. No host trust-store changes. Monitoring stays inside the containers. JetStream memory is capped at 256 MiB per server. Fake `fixture-reader` credentials permit only account/stream metadata and stored-message GET requests plus private replies—not application publishing, consumer creation or ACKs. `fixture-admin` is used only by setup/tests; `fixture-denied` denies all publishing. This is not a deployment template.

`hack/dev.sh` sets child-process `ONETUI_NATS_USERNAME=fixture-reader` and `ONETUI_NATS_PASSWORD=fixture-reader-only` for the `local_nats` alias. These names are fixture references, not automatically discovered application settings. `ONETUI_NATS_TLS=true` is a container-entrypoint switch for the separate TLS server; unset/false selects its plaintext listener, not an app setting.

After `make dev-up`, run `make dev-traffic` (both brokers) or `make dev-traffic-nats` (NATS only) in a separate terminal. It sends one JSON message immediately, then every 15 seconds until Ctrl-C. In `make run`, choose `local_nats`, open `DEMO_LIVE` and press `f`. Existing messages are browsable before following; `f` starts at the current end. Stop with `f` or Ctrl-C; navigation/inspection also stops updates.

```sh
cargo run -p onetui-nats --example produce_nats --locked -- --count 2
```

The optional `--count` is a positive integer message count; omission runs until interrupted. Invalid/unknown arguments fail. Two messages take at least 15 seconds. Endpoint `127.0.0.1:14222`, subject `demo.live`, fixture credentials and interval are fixed; no application config or environment credentials are read. `DEMO_LIVE` must already exist from setup. This example does not make publishing available inside OneTUI.

## Demo data

NATS streams are `DEMO_EVENTS` (1,205 nested JSON messages), `DEMO_BINARY` (260 invalid-UTF-8, empty, Unicode and JSON payloads), `DEMO_WIDE` (110 messages with 40 nested fields), `DEMO_EMPTY` and `DEMO_LIVE`. Headers include duplicate `X-Demo` values. Each stream has a 32 MiB memory-storage limit. The NATS seeder only connects to the fixed fixture and never purges data; an existing nonempty stream is preserved, including a partial seed. Reset fixtures explicitly if a seed was interrupted. Tests create their own streams for sparse sequences, retention, consumer-state checks and oversized data.

Kafka includes `demo_events` (1,500 JSON records across three partitions), `demo_binary` (512 records across two partitions), `demo_tombstones` (64 null/empty/JSON values) and `demo_empty`. Records include exact timestamps, binary keys, duplicate headers, invalid UTF-8 and terminal-control bytes. Retention is disabled for this disposable broker so historical fixture timestamps do not expire. The seeder uses Zstandard compression and only connects to `127.0.0.1:19092`. Repeated seeding preserves topics with the expected partition/offset counts; a mismatch fails without replacing data. It never targets a user-supplied broker.

Use `e` or `:query` after choosing a datasource to try [native query examples](../docs/queries.md). On PostgreSQL, query `demo.customers`; on Qdrant, select `demo_products` before opening the editor. Ctrl-U clears the draft, Enter or F5 executes, Shift+Enter adds a line and Esc returns. These queries need no additional setup or configuration. See the query guide for terminals that cannot distinguish Shift+Enter from Enter.

`make dev-up` seeds these datasets after readiness; `make dev-seed` adds them to an already-running setup. The reserved demo names are separate from the small `public` edge-case fixtures and collections owned by tests.

| PostgreSQL relation | Rows | Columns | Coverage |
| --- | ---: | ---: | --- |
| `demo.customers` | 2,000 | 16 | UUIDs, nullable email, Unicode, money-like decimals, dates, arrays, nested JSONB, long notes |
| `demo.events` | 5,000 | 10 | Customer foreign keys, timestamps, categories, duration, IP addresses, JSONB |
| `demo.type_samples` | 250 | 40 | Integers beyond JavaScript precision, numeric, NaN/infinity, NULL/empty text, binary, dates/times/intervals, JSON, arrays, network types, bits, ranges, text search, geometry, money, enum/domain |
| `demo.wide_rows` | 1,500 | 65 | ID plus 64 text columns, nullable cells, horizontal navigation |
| `demo.empty_rows` | 0 | 2 | Empty results |
| `demo.active_customers` | Derived | 16 | View over active customers |

| Qdrant collection | Points | Coverage |
| --- | ---: | --- |
| `demo_products` | 1,500 | Numeric IDs, unnamed 8D dense vectors, 22 payload fields |
| `demo_documents` | 1,200 | UUID IDs, named 8D title and 16D content vectors |
| `demo_vectors` | 350 | Named 8D dense, sparse and 3×8 multivectors |
| `demo_payload_cases` | 12 | Empty/missing/null values, arrays, nested objects, large integers, Unicode, escaped controls, long text |
| `demo_empty` | 0 | Empty collection |

Product/document/vector payloads mix strings, numbers, booleans, nulls, arrays, nested objects, geo coordinates and date/UUID strings. These cover common data shapes, not every database extension or possible vector layout. Point lists intentionally show IDs; open a point, then payload or vectors for lazy detail reads.

The four-row `public.sample_rows`/`sample_view` fixtures test display distinctions: SQL NULL is absent data, empty text is present but blank, and the string `"NULL"` is ordinary text. Unicode must survive; newline/terminal controls are escaped so database content cannot control the terminal.

PostgreSQL seeds run in one transaction and insert only missing primary keys; existing rows are unchanged. Qdrant writes new collections in batches of at most 100 points. Repeating setup preserves an existing demo collection when its exact count matches; a mismatch fails without modifying it. This also catches an interrupted partial seed. Inspect such a collection before explicitly removing it or resetting disposable fixtures. The seeder accepts no external endpoint or credentials: it uses only the fixed local fixture and fake admin key. Source: [PostgreSQL SQL](fixtures/postgres-demo.sql), [Qdrant example](../crates/qdrant/examples/seed_demo.rs).

## Validation

```sh
make verify
make test-integration
```

`verify` needs no Docker but its protocol tests bind ephemeral loopback ports. `test-integration` requires no existing `onetui-fixtures` containers, creates fresh fixtures, runs each package's ignored fixture tests serially, and tears down on success, failure, SIGINT or SIGTERM. SIGKILL/daemon loss can prevent cleanup; recover with `make dev-down`. Do not run fixture commands concurrently in different terminals/checkouts: they deliberately share fixed ports/project names.

To retain containers for debugging, use `make dev-up` followed by:

```sh
cargo test -p onetui-postgres --locked --test fixtures -- --ignored --test-threads=1
cargo test -p onetui-qdrant --locked --test fixtures -- --ignored --test-threads=1
cargo build -p onetui-nats --example produce_nats --locked
cargo test -p onetui-nats --locked --test fixtures -- --ignored --test-threads=1
```

Keep these package-owned suites separate; do not parameterize a harness over both backends. `make verify` runs workspace-wide build/lint/default tests. For non-Docker package tests, run `make build` followed by `cargo test -p onetui-postgres --locked` or `cargo test -p onetui-qdrant --locked`. CLI tests use `target/debug/onetui`; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path (prefer an absolute path for custom target directories).

Those tests change only disposable fixture tables/collections and their own reader sessions. Finish with `make dev-down`.

Provider lifecycle checks need no extra configuration: PostgreSQL fixtures observe TLS session reuse, idle transaction state, cancellation and reconnection. Qdrant's default local protocol tests count connections/RPCs and verify channel cleanup. Its fixture suite tests production collection/point/detail reads and a Qdrant-only CLI/PTY journey. The TUI journey runs both full browsing paths in one process, switches from a delayed PostgreSQL read to Qdrant, and returns to PostgreSQL after point detail inspection. It seeds 105 small points in its own `onetui_tui_<pid>` collection and deletes that collection afterward, even after a caught assertion panic. The harness supplies its child-only `ONETUI_LIVE_PTY_KEY` reference with the fake reader key; it is not a production setting.

With fixtures already started by `make dev-up`, run just that coordination check:

```sh
cargo test -p onetui-tui --locked --test fixtures actual_cli_terminal_worker_and_datasource_switching -- --ignored --test-threads=1
```

The TUI suite switches PostgreSQL aliases during active reads and tests worker panic, forced abort and stalled shutdown with real connections. Its test child receives `ONETUI_LIFECYCLE_TEST_MODE` (`panic`, `read`, `shutdown`, `quit_read`) and `ONETUI_LIFECYCLE_TEST_DSN` from the harness only; these are not CLI settings. The child waits for the parent to inspect terminal flags and server PIDs before exiting. Default PostgreSQL tests use temporary FIFOs and child-only `SSL_CERT_FILE`/`SSL_CERT_DIR` values to verify certificate-load deadlines and SIGINT; the host trust store is unchanged.

For optimized render, connector and cancellation timings, see [performance checks](../docs/performance.md). The timing tests use existing fixtures and add no application settings.

## Troubleshooting

- Docker unavailable: start Docker Desktop/Engine and check `docker info`; setup failures return nonzero. Remote/TCP contexts are refused: these clients require a local Unix-socket Docker daemon and loopback port forwarding.
- Ports occupied: stop the conflicting local service yourself; this setup never kills unrelated processes.
- Existing fixtures: inspect with `make dev-logs`; `make test-integration` refuses to reset them. Explicitly run `make dev-down` when ready to discard them.
- Readiness fails: `make dev-logs`; each real metadata check has a two-second timeout, with a bounded retry window. Failed integration runs print recent container logs before cleanup. `dev-up` keeps failed containers for inspection.
- TLS certificate expired: fixture certificates last two days. Recreate the disposable project.
- First build fails to download Rust/crates/images: restore network/registry access and rerun; no unlocked dependency fallback is used.

`make workflow-lint` uses Go to download/run actionlint v1.7.12. It validates workflow syntax and expressions; `make lint` separately checks Rust and shell syntax. Neither replaces an actual GitHub Actions run.
