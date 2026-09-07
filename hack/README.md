# Local development

Run these commands from the repository root. Requirements: rustup, Make, Bash, Docker Engine/Desktop and Docker Compose with `up --wait` support. OpenSSL is also required for the Qdrant TLS test's unrelated test CA. macOS and Linux are the initial targets. No database installation, manual secret exports or RTK installation is required.

```sh
make dev-up        # builds onetui, starts databases, waits for authenticated reads
make check-local   # runs both headless checks with fake reader credentials
make run           # PostgreSQL metadata TUI; uses existing fixtures, q quits
make dev-logs
make dev-down      # removes this fixture project and its temporary data
```

`make run` builds and supplies fake PostgreSQL credentials; it does not start/recreate containers. The current journey ends at column metadata, not row values. See [PostgreSQL usage](../docs/postgres.md).

`compose.yaml` is the only Compose definition. PostgreSQL 16.13 listens on `127.0.0.1:15432`; Qdrant 1.18.2 gRPC listens on `127.0.0.1:16334`. The `onetui-fixtures` project is reserved for disposable data. Storage is tmpfs, so even restarting containers can lose fixture state; recreate with `dev-down` then `dev-up`. There are no persistent data volumes. Do not place valuable data in these containers.

A separate `qdrant-tls` fixture exposes gRPC on `127.0.0.1:16335` with a localhost-only certificate. Its private CA/keys are generated inside tmpfs at startup. Compose waits for a verified TLS handshake; the integration test performs authenticated metadata reads and negative trust/hostname/authentication/protocol checks. It supplies the public CA to only the child CLI through `SSL_CERT_FILE`/`SSL_CERT_DIR`; nothing is installed in the OS trust store. `make check-local` continues to check the two normal connection aliases.

The scripts set fixture-only credentials in their own process and do not modify your shell/config. `connections.toml` contains only aliases and environment references. A bare `onetui --check --config hack/connections.toml` still needs `--connection` and the referenced environment variables; use `make check-local` to supply them automatically.

If you ran the previous `bpearl-fixtures` project, it remains untouched by this rename and may still occupy the same ports. When ready to discard its temporary data, run `docker compose --project-name bpearl-fixtures -f hack/compose.yaml down`, then `make dev-up` for the new `onetui-fixtures` project. Configuration now defaults to `~/.config/onetui/config.toml`; old configuration is not moved automatically. Environment references are explicit TOML values, so existing custom variable names still work when explicitly configured.

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

These are separate package-owned suites, not a backend-parameterized test harness. `make verify` runs workspace-wide build/lint/default tests. For non-Docker package tests, run `make build` followed by `cargo test -p onetui-postgres --locked` or `cargo test -p onetui-qdrant --locked`. CLI tests use `target/debug/onetui`; test-only `ONETUI_TEST_BIN` optionally selects another prebuilt binary path (prefer an absolute path for custom target directories).

Those tests change only disposable fixture tables/collections and their own reader sessions. Finish with `make dev-down`.

## Troubleshooting

- Docker unavailable: start Docker Desktop/Engine and check `docker info`; setup failures return nonzero. Remote/TCP contexts are refused: these clients require a local Unix-socket Docker daemon and loopback port forwarding.
- Ports occupied: stop the conflicting local service yourself; this setup never kills unrelated processes.
- Existing fixtures: inspect with `make dev-logs`; `make test-integration` refuses to reset them. Explicitly run `make dev-down` when ready to discard them.
- Readiness fails: `make dev-logs`; each real metadata check has a two-second timeout, with a bounded retry window. Failed integration runs print recent container logs before cleanup. `dev-up` keeps failed containers for inspection.
- TLS certificate expired: fixture certificates last two days. Recreate the disposable project.
- First build fails to download Rust/crates/images: restore network/registry access and rerun; no unlocked dependency fallback is used.

`make workflow-lint` uses Go to download/run actionlint v1.7.12. It validates workflow syntax and expressions; `make lint` separately checks Rust and shell syntax. Neither replaces an actual GitHub Actions run.
