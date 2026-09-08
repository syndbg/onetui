# Local development

Run these commands from the repository root. Requirements: rustup, Make, Bash, Docker Engine/Desktop and Docker Compose with `up --wait` support. OpenSSL is also required for the Qdrant TLS test's unrelated test CA. macOS and Linux are the initial targets. No database installation, manual secret exports or RTK installation is required.

```sh
make dev-up        # builds, starts databases, waits for reads, seeds demos
make dev-seed      # adds demos to running fixtures without resetting them
make check-local   # runs both headless checks with fake reader credentials
make run           # PostgreSQL row/metadata TUI; uses existing fixtures, q quits
make dev-logs
make dev-down      # removes this fixture project and its temporary data
```

`make run` builds and supplies fake PostgreSQL and Qdrant credentials; it does not start/recreate containers. It opens PostgreSQL initially. Enter opens schemas, relations, then rows and field detail; `m` opens column metadata, `/` filters the displayed page and `s` cycles local lexical sort. Open schema `demo` for larger tables. Press `c` and select `local_qdrant`, then a `demo_*` collection. See [PostgreSQL usage](../docs/postgres.md) and [Qdrant usage](../docs/qdrant.md).

`compose.yaml` is the only Compose definition. PostgreSQL 16.13 listens on `127.0.0.1:15432`; Qdrant 1.18.2 gRPC listens on `127.0.0.1:16334`. The `onetui-fixtures` project is reserved for disposable data. Storage is tmpfs, so even restarting containers can lose fixture state; recreate with `dev-down` then `dev-up`. There are no persistent data volumes. Do not place valuable data in these containers.

A separate `qdrant-tls` fixture exposes gRPC on `127.0.0.1:16335` with a localhost-only certificate. Its private CA/keys are generated inside tmpfs at startup. Compose waits for a verified TLS handshake; the integration test performs authenticated metadata reads and negative trust/hostname/authentication/protocol checks. It supplies the public CA to only the child CLI through `SSL_CERT_FILE`/`SSL_CERT_DIR`; nothing is installed in the OS trust store. `make check-local` continues to check the two normal connection aliases.

The scripts set fixture-only credentials in their own process and do not modify your shell/config. `connections.toml` contains the theme, connection aliases, endpoint and environment references. Press `T` or use `:themes` to preview all palettes with `j`/`k` or arrows; Enter keeps for this session, Esc or Ctrl-C restores the previous theme. To persist a startup preference, edit top-level `theme` in that file; the menu never writes it. `onetui schema` lists known settings and actions. A bare `onetui --check --config hack/connections.toml` still needs `--connection` and the referenced environment variables; use `make check-local` to supply them automatically.

If you ran the previous `bpearl-fixtures` project, it remains untouched by this rename and may still occupy the same ports. When ready to discard its temporary data, run `docker compose --project-name bpearl-fixtures -f hack/compose.yaml down`, then `make dev-up` for the new `onetui-fixtures` project. Configuration now defaults to `~/.config/onetui/config.toml`; old configuration is not moved automatically. Environment references are explicit TOML values, so existing custom variable names still work when explicitly configured.

## Demo data

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
