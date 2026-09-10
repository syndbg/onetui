# Local development

Run these commands from the repository root. Requirements: rustup, Make, Bash, CMake, a C/C++ compiler, Perl, Docker Engine/Desktop and Docker Compose with `up --wait` support. Kafka's native libraries build through Cargo. The OpenSSL command is also required for TLS fixtures. macOS and Linux are the initial targets. No database installation, manual secret exports or RTK installation is required.

Linux builds also require CURL development headers (`libcurl4-openssl-dev` on Debian/Ubuntu) for the pinned librdkafka CMake build, even though its CURL runtime feature is disabled. See [contributor setup](../CONTRIBUTING.md#local-setup) for build prerequisites.

```sh
make dev-up        # builds, starts databases, waits for reads, seeds demos
make dev-seed      # adds demos to running fixtures without resetting them
make check-local   # checks PostgreSQL, Qdrant and Kafka with fixture settings
make run           # connection picker; uses existing fixtures, q quits
make dev-logs
make dev-down      # removes this fixture project and its temporary data
```

`make run` builds and supplies fake PostgreSQL and Qdrant credentials; it does not start/recreate containers. It opens the connection picker without contacting a datasource. Choose `local_pg`, `local_qdrant` or `local_kafka` and press Enter. PostgreSQL opens schemas, relations, then rows and field detail; `m` opens column metadata, `/` filters the displayed page and `s` cycles local lexical sort. Open schema `demo` for larger tables, a Qdrant `demo_*` collection, or a Kafka `demo_*` topic and partition. Press `c` to return to the picker. Only an explicit CLI `--connection <alias>` skips the picker. See [PostgreSQL usage](../docs/postgres.md), [Qdrant usage](../docs/qdrant.md) and [Kafka usage](../docs/kafka.md).

`compose.yaml` is the only Compose definition. PostgreSQL 16.13 listens on `127.0.0.1:15432`; Qdrant 1.18.2 gRPC listens on `127.0.0.1:16334`; Apache Kafka 4.2.0 listens on `127.0.0.1:19092`. Kafka runs one combined KRaft broker/controller with auto topic creation disabled and no authentication on the local development listener. The `onetui-fixtures` project is reserved for disposable data. Storage is tmpfs, so even restarting containers can lose fixture state; recreate with `dev-down` then `dev-up`. There are no persistent data volumes. Do not place valuable data in these containers.

A separate `qdrant-tls` fixture exposes gRPC on `127.0.0.1:16335` with a localhost-only certificate. Its private CA/keys are generated inside tmpfs at startup. Compose waits for a verified TLS handshake; the integration test performs authenticated metadata reads and negative trust/hostname/authentication/protocol checks. It supplies the public CA to only the child CLI through `SSL_CERT_FILE`/`SSL_CERT_DIR`; nothing is installed in the OS trust store. `make check-local` checks the normal PostgreSQL, Qdrant and Kafka connection aliases.

Kafka also binds verified TLS on `127.0.0.1:19093` and SASL over TLS on `127.0.0.1:19094`, advertised as `localhost`. Its self-signed localhost certificate and keys live in container tmpfs for two days. Tests copy only the public certificate into a temporary `ca_file`; the OS trust store is unchanged. `kafka-security.sh` sets fixture-only PLAIN/SCRAM credentials and grants `fixture-reader` topic `Read`/`Describe` on `demo_*` and group `Describe` on `onetui-*`. It grants no group `Read`. `fixture-denied` has no ACLs. These accounts and the unauthenticated development listener are only for this disposable broker, not a deployment template.

The scripts set fixture-only credentials in their own process and do not modify your shell/config. `connections.toml` contains the theme, connection aliases, endpoint and environment references. Press `T` or use `:themes` to preview all palettes with `j`/`k` or arrows; Enter keeps for this session, Esc or Ctrl-C restores the previous theme. To persist a startup preference, edit top-level `theme` in that file; the menu never writes it. `onetui schema` lists known settings and actions. A bare `onetui --check --config hack/connections.toml` still needs `--connection` and the referenced environment variables; use `make check-local` to supply them automatically.

If you ran the previous `bpearl-fixtures` project, it remains untouched by this rename and may still occupy the same ports. When ready to discard its temporary data, run `docker compose --project-name bpearl-fixtures -f hack/compose.yaml down`, then `make dev-up` for the new `onetui-fixtures` project. Configuration now defaults to `~/.config/onetui/config.toml`; old configuration is not moved automatically. Environment references are explicit TOML values, so existing custom variable names still work when explicitly configured.

## Kafka traffic

After `make dev-up`, run `make dev-traffic` in a separate terminal. This opt-in producer sends one JSON record immediately, then one every 15 seconds until Ctrl-C. It connects only to the fixed disposable broker at `127.0.0.1:19092`, creates `demo_live` with one partition if absent and appends without resetting existing data. Newly created topics use a one-hour / 64 MiB retention policy; Kafka applies retention asynchronously. Existing topic settings are preserved. The four seeded Kafka datasets are unchanged.

In another terminal run `make run`, select `local_kafka`, open `demo_live` and partition `0`, then press `f` in the record view. `f` or Ctrl-C stops following and keeps the displayed window. Navigation/inspection also stops it. Starting again reads only from a new current end. `r` returns to historical browsing.

For a finite simulator run:

```sh
cargo run -p onetui-kafka --example produce_demo --locked -- --count 2
```

`--count` is an optional positive integer number of records. Omission runs until interrupted; zero, invalid or unknown arguments fail. Two records take at least 15 seconds. The endpoint, topic and interval are fixed; the simulator does not read `onetui.toml` or environment credentials. Each record has a run-local sequence, send timestamp and synthetic message. Delivery failures exit with the native error. This example is a fixture tool, not an app write capability.

The integration runner builds the example. Kafka's live CLI test runs the two-record producer and checks timed arrival, stop, retained row inspection, restart and connection switching. It uses only the disposable fixture.

## Demo data

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
