# onetui-kafka

Kafka provider, strict configuration, metadata, bounded record browsing, offset/timestamp replay and live follow batches through `rdkafka`. The package owns its native client thread, offset tokens and tests. Core and TUI do not import Kafka types.

Topic/broker configuration and consumer-group committed offsets use read-only native admin calls on the same client. Results preserve native errors and withhold sensitive settings. Group lag is read-committed offset distance, not a message count. The fixture suite checks configuration paging, secret masking, group offsets, ACL errors and CLI submenu navigation; the broker-stall test covers cancellation. These views add no connection settings.

The native build uses CMake, a C/C++ toolchain, Make and Perl. Cargo builds librdkafka, OpenSSL, zlib and Zstandard from source; do not install a system librdkafka for this build. TLS uses OpenSSL rather than the other connectors' Rustls stack.

On Debian/Ubuntu, install `build-essential cmake perl pkg-config libcurl4-openssl-dev`. The pinned librdkafka CMake template defines a disabled OIDC macro as zero, but one source include checks whether it exists; CURL headers are therefore needed even though CURL linking and OAuth are disabled. macOS supplies these headers through its SDK. Recheck this prerequisite when updating librdkafka.

```sh
cargo test -p onetui-kafka --locked
```

The default native protocol test starts librdkafka's loopback mock cluster. It does not need Docker or an external broker. It covers topic/partition traversal, 250-record paging, backward offset bookmarks, binary values, empty partitions, untouched committed offsets, repeated cancellation after broker loss, owner-slot release and reconnection. Mocks do not prove real-broker security or transaction behavior.

`cargo test -p onetui-kafka --test fixtures --locked -- --ignored --test-threads=1` runs the Kafka-only fixture suite after `make dev-up`. It checks seeded data, transactions, native group state, size limits and retention-invalidated bookmarks. Security cases verify TLS trust/hostnames, PLAIN and both SCRAM mechanisms, native password/ACL failures and redaction. The transaction test creates a uniquely named topic/group on the fixed disposable broker and removes them afterward, including after assertion failures. The Unix terminal test drives the built CLI through pages, byte inspection, alias switching and terminal-mode restoration. It uses `target/debug/onetui` unless test-only `ONETUI_TEST_BIN` names another built binary. Never point fixture tests at production.

The broker-stall test pauses only the disposable Kafka container, checks repeated cancellation and a request deadline, then unpauses it before propagating assertion failures. It verifies reconnection and another record page afterward. Do not run it alongside another fixture user.

The replay fixture test checks offset ranges, timestamp lookup, an empty result after the latest timestamp, query-bound bookmarks and cancellation. The CLI browsing test also executes replay JSON, pages its results, retains data after an invalid draft and returns to historical browsing.

The live CLI test also needs `cargo build -p onetui-kafka --example produce_demo --locked`; the full integration runner builds it automatically. It sends two records 15 seconds apart to `demo_live` and verifies live arrival, stop, inspection, restart and connection switching. The example is a fixture-only producer; `make dev-traffic` runs it continuously after `make dev-up`.

See [Kafka usage/configuration](../../docs/kafka.md), [ADR-0006](../../docs/adr/0006-browse-kafka-with-rust-rdkafka.md), [ADR-0007](../../docs/adr/0007-follow-live-records-in-bounded-batches.md).
