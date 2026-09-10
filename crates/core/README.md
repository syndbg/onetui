# onetui-core

Configuration parsing, native async `Provider`/`Executor` contracts, request/shutdown deadlines, transport status and shared resource/page/action descriptors. No database SDK or terminal dependency. Catalog validation checks kinds, resource IDs and navigation targets; each provider validates its own options. Shared tests use a synthetic provider, not production connectors.

`follow_resource` declares optional live following. `Executor::follow_page` returns a bounded batch with a continuation even when empty; its default implementation rejects unsupported operations. The existing request deadline/cancellation contract applies to every batch. Core does not own polling timers or live display buffers. See [ADR-0007](../../docs/adr/0007-follow-live-records-in-bounded-batches.md).

Configuration uses `onetui-theme`'s selector for the optional top-level `theme` string. Omission preserves Catppuccin; invalid names/types fail before configuring a provider. Core does not convert RGB values or import Ratatui. See [theme configuration](../theme/README.md#usage).

## Validation

`Value` retains text, serialized SDK JSON or binary bytes; `None` represents null. Core owns serializable `[display]` options and format descriptors, without terminal rendering. TUI owns escaped projections and formatting. See [display settings](../../README.md#display-settings).

```sh
make build
cargo test -p onetui-core --locked
```

Run from the workspace root. `make verify` checks all packages. Keep tests and helpers package-local, even when setup is duplicated; never combine PostgreSQL and Qdrant in one test harness.

See [configuration](../../README.md#configuration) for settings, defaults, accepted values and examples, and [PostgreSQL usage](../../docs/postgres.md) for navigation and fixed limits.
