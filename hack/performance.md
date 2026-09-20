# Performance checks

Run the render sample without databases:

```sh
cargo test -p onetui-tui --release --locked --lib large_page_render_timings -- --ignored --nocapture
```

For connector timings, prepare [disposable fixtures](README.md) and run each package separately:

```sh
make build-release dev-up
export ONETUI_TEST_BIN="$PWD/target/release/onetui"
cargo test -p onetui-postgres --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
cargo test -p onetui-qdrant --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
cargo test -p onetui-tui --release --locked --test fixtures -- --ignored --test-threads=1 --nocapture
unset ONETUI_TEST_BIN
```

`ONETUI_TEST_BIN` selects the executable for CLI/PTY tests. Cargo's `--release` optimizes the test harness and in-process connectors. Use `make dev-down` afterward only when the disposable data can be deleted.

These tests print timings without enforcing machine-dependent thresholds. TestBackend measurements exclude terminal rendering; connector timings include server and transport work. Page limits do not establish a process-memory ceiling. Record measurements, environment and limitations in the issue or pull request.
