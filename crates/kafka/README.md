# onetui-kafka

Kafka provider, strict configuration, metadata and bounded record browsing through `rdkafka`. The package owns its native client thread, offset tokens and tests. Core and TUI do not import Kafka types.

The native build uses CMake, a C/C++ toolchain, Make and Perl. Cargo builds librdkafka, OpenSSL, zlib and Zstandard from source; do not install a system librdkafka for this build. TLS uses OpenSSL rather than the other connectors' Rustls stack.

```sh
cargo test -p onetui-kafka --locked
```

The default native protocol test starts librdkafka's loopback mock cluster. It does not need Docker or an external broker. It covers topic/partition traversal, 250-record paging, backward offset bookmarks, binary values, empty partitions, untouched committed offsets and shutdown. Mocks do not prove real-broker security or transaction behavior.

See [Kafka usage/configuration](../../docs/kafka.md) and [ADR-0006](../../docs/adr/0006-browse-kafka-with-rust-rdkafka.md). Real Kafka fixtures and authenticated transport validation remain in progress.
