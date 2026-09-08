# Performance checks

Run the render sample without databases:

```sh
cargo test -p onetui-tui --release --locked --lib large_page_render_timings -- --ignored --nocapture
cargo test -p onetui-tui --release --locked --lib cancel_then_new_request_reuses_executor_and_shutdown_drops_it -- --nocapture
```

For connector timings, build the release binary and start the disposable fixtures. Run each package separately; finish with `make dev-down`, including after a test failure.

```sh
make build-release dev-up
export ONETUI_TEST_BIN="$PWD/target/release/onetui"
cargo test -p onetui-postgres --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
cargo test -p onetui-qdrant --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
cargo test -p onetui-tui --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
unset ONETUI_TEST_BIN
make dev-down
```

`ONETUI_TEST_BIN` selects the prebuilt executable for CLI/PTY tests only. Cargo's `--release` selects optimization for the test harness and in-process connectors. These tests print durations without credentials, resource paths or cell contents. They do not enforce machine-dependent latency thresholds.

## Local sample

Measured on September 7, 2026: Apple M5 Pro, 48 GiB RAM, macOS 26.6.2, Rust 1.95.0, optimized Cargo release profile. PostgreSQL 16.13 and Qdrant 1.18.2 ran in local Docker fixtures. These are individual local samples, not throughput or production latency guarantees.

| Operation | Observed time |
| --- | --- |
| Escape and construct 100 rows × 4 Unicode cells, 822,440 retained bytes | 1.818 ms |
| Install that page and rebuild its visible-row indices | 0.0033 ms |
| Unchanged 120×40 TestBackend draw, 100 samples | Median 0.632 ms; max 1.954 ms |
| `j` key handling through TestBackend draw while loading, 100 samples | Median 0.632 ms; max 1.958 ms |
| Cancel a pending test executor through the worker | 0.0047 ms |
| Cancel PostgreSQL after observing its 30-second `pg_sleep` | 1.056 ms |
| Ctrl-C through the extracted release CLI to visible cancellation status | 12.508 ms |
| PostgreSQL first 100-row bigint page, including connection and metadata | 7.471 ms |
| PostgreSQL reject a 1,048,577-byte server cell | 8.788 ms |
| PostgreSQL reject a page of 100 × 20,000-byte cells | 9.087 ms |
| Qdrant 100 point IDs, existing channel, 107-point collection | 0.746 ms |
| Qdrant reject a 2 MiB payload | 16.875 ms |
| Qdrant fetch dense/sparse/multivectors after that rejection | 0.981 ms |

The Unicode fixture repeats 512 wave characters plus newline/escape characters in each cell. The render sample reuses the stored escaped strings and cached detail text. Table draws still construct 128-character previews; the event loop redraws on input or worker events, not an idle timer.

TestBackend timings exclude terminal I/O and display-server latency. The PTY check polls every 10 ms and can force a redraw after 150 ms, so its result is a coarse input-to-visible-output observation. It also checks that PostgreSQL removes the cancelled session. Connector timings cover the entire `fetch_page` call: server work, transport, SDK decoding and display formatting. They do not isolate SDK decode CPU time. Use phase tracing if that distinction becomes necessary.

The context-panel layout was also sampled on the same host with Rust 1.98.1 and the same 822,440-byte, 120×40 fixture. Across 100 draws, unchanged frames measured 0.735 ms median / 1.121 ms max; key-to-TestBackend-draw measured 0.737 ms median / 1.130 ms max. Formatting took 0.992 ms and page installation 0.003 ms. The different compiler and separate run prevent attributing the difference solely to the layout; terminal I/O remains excluded.

## Bounds

Oversized PostgreSQL cells/pages and Qdrant payloads fail explicitly. Fixture tests verify continuation preservation and successful reads after failures. TUI tests retain at most three pages per view, each with a 1 MiB string budget; delayed-worker tests verify cancellation and stale-result rejection. The worker has one active request and the app keeps one replaceable pending request, with bounded command/result channels.

These are retained-data limits, not a process RSS cap. PostgreSQL decodes a protocol row before the application can reject its contents; server-side guards limit normal row reads but cannot bound a hostile server's allocations. Qdrant caps protobuf message bytes, not the size of the decoded object graph. Formatting, terminal buffers and retained parent views also consume memory. This sample does not measure peak RSS or allocation counts.
