# onetui-tui

Navigation state, request identity/cancellation, terminal lifecycle and rendering. Tests cover shell state, dynamic columns, chunked cached field detail and real PTYs. This package also owns keyboard/request-state and actual-CLI PTY journeys against PostgreSQL fixtures; connector contracts remain in their connector package. Local filter/sort reorder visible indices only, preserving native paging tokens and cached row identity.

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
