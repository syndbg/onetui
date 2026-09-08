# onetui-tui

Navigation state, request identity/cancellation, terminal lifecycle and rendering. Tests cover shell state, dynamic columns, chunked cached field detail and real PTYs. This package also owns keyboard/request-state and actual-CLI PTY journeys against PostgreSQL fixtures; connector contracts remain in their connector package. Local filter/sort reorder visible indices only, preserving native paging tokens and cached row identity.

## Layout

The top bar shows OneTUI, the selected alias, read-only mode and version. At 60 columns and 20 rows or larger, a bordered context panel shows the datasource, resource path, loaded/shown item counts and transport state. These counts describe cached data, not a database-wide total. At 100 columns, the panel also shows primary keys for actions available in the current context, using the existing action descriptors. Text entry replaces those hints with input instructions. Smaller terminals use a compact breadcrumb header.

The table has rounded borders, alternating dark rows, a contrasting selected row and arrows for page-local sort direction. NULL cells are muted but retain their literal label. Loading, errors and ready states use palette-specific status colors with text labels; a connected transport does not mean live data refresh. Paging, filter/sort state and command input remain in the footer. The context panel never reads or displays connection strings, URLs or secret values.

Run `make run` from the workspace root with fixtures already running. `?` opens action descriptions. Top-level `theme` selects one of ten built-in palettes, defaulting to Catppuccin; see [names, variants and configuration](../theme/README.md). The `onetui-theme` crate owns RGB roles; this renderer converts them to Ratatui colors. Every screen uses the same palette, with no per-theme renderer or provider branch. Restart to apply a configuration change; navigation and request lifecycles are unchanged.

## Validation

The worker owns a generic executor for one selected alias, with one active request and one replaceable pending request. Descriptors supply entry resources and navigation targets. Production code imports no connector or database SDK; PostgreSQL is a dev dependency for this package's live journey. Session/request identities reject stale pages, errors and transport status. Unix input uses Crossterm's polling backend; the PTY journey also checks repeated resize/input interleaving.

Live tests run both full browsing journeys in one CLI process: PostgreSQL rows/detail, an active-query switch to Qdrant paging/payload/vectors/metadata, then a successful return to PostgreSQL. The Qdrant client is a dev-only fixture seeder; it creates and removes the test's own collection, including cleanup after an assertion panic. Connector protocol/value assertions remain in their packages, without a parameterized backend harness.

Separate tests inject worker panic, uncooperative reads and stalled shutdown through a test-only provider wrapper. Exact terminal flags and PostgreSQL PIDs are checked before the fault-test child exits, so process termination cannot hide a leaked driver. PostgreSQL's native client is a dev-only observer dependency; no production backend branch or fault-injection option is added.

```sh
make build
cargo test -p onetui-tui --locked
```

Run from the workspace root. `make verify` checks all packages. Keep connector tests and helpers package-local, even when setup is duplicated; do not parameterize a test harness over PostgreSQL and Qdrant. Cross-alias worker coordination belongs here.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits. Enter on a relation opens rows; use `m` for column metadata, `/` for a case-sensitive page filter and `s` for selected-field lexical sort. Run `make test-integration` for the live TUI journey and separate connector suites.
