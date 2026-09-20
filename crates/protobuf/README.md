# onetui-protobuf

Raw Protobuf decoding with an explicit descriptor set and message name, using `prost-reflect` 0.16; `protox` compiles supplied registry sources in memory. The library performs no file or network access and has no Avro, datasource SDK or terminal dependency. `Decoder` returns original bytes, a schema identity and a `prost_reflect::DynamicMessage`. JSON is a separate, fallible presentation.

The [Kafka](../../docs/kafka.md#schema-bound-key-and-value-previews) and [NATS](../../docs/nats.md#payload-decoding) browsers support explicit decoder bindings. Use `onetui schema --datasource kafka` or `onetui schema --datasource nats` for settings.

## Try a raw message

From the workspace root, compile a descriptor set with imports using an installed `protoc`:

```sh
protoc -I crates/protobuf/examples --include_imports --descriptor_set_out=/tmp/onetui-event.pb event.proto
cargo run -p onetui-protobuf --example decode_protobuf --locked -- /tmp/onetui-event.pb demo.Event 080712024869
```

This prints Protobuf JSON: `{"id":"7","name":"Hi"}`. Arguments are a binary descriptor-set file, an exact case-sensitive full message name and hexadecimal payload bytes. Hex must contain complete byte pairs, without spaces or `0x`; an empty argument represents an empty payload. File paths resolve from the current directory. The example does not read OneTUI configuration, expand environment variables, guess framing or contact a datasource.

## API and interpretation

```rust
use onetui_protobuf::Decoder;
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};

fn main() -> anyhow::Result<()> {
    let schema = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Event".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }.encode_to_vec();
    let decoder = Decoder::new(&schema, "demo.Event")?;
    let decoded = decoder.decode(&[])?;
    assert!(decoded.raw().is_empty());
    assert_eq!(decoded.json()?, "{}");
    Ok(())
}
```

`Decoder::new` loads a binary `FileDescriptorSet` including its imports and selects the full message name (required, at most 1,024 UTF-8 bytes). The entire wire stream is decoded, including valid unknown fields. Concatenated Protobuf messages can have valid merge semantics; raw decoding cannot detect a missing application framing boundary. The decoder does not strip Confluent prefixes or message indexes.

`schema_id()` includes SHA-256 of the exact supplied descriptor bytes and the selected message name. This identifies an interpretation, not the producer's actual schema or a registry ID. A successful decode does not prove the schema was correct.

`Decoded::value()` preserves bytes, unknown fields and enum numbers. Protobuf JSON represents 64-bit integers as strings and bytes as base64; it can omit type information and unknown fields, so it is not a lossless export. Raw bytes remain available after a JSON-presentation error. `decode` borrows its input; an error leaves the caller's data untouched. The decoder never falls back to another format.

`Decoded::native()` produces typed JSON with field names/numbers/types, byte arrays, enum numbers and unknown field wire types/re-encoded bytes. It includes populated fields and extensions, not a dump of every absent schema field. Maps have stable key ordering. Re-encoding is not an exact original wire slice; retain `raw()` for that. The 1 MiB streaming output limit applies, independently of JSON conversion, including for `Any` and non-finite floats.

## Bounds and limitations

| Limit | Enforcement |
| --- | --- |
| Schema input | 256 KiB; preflight limits descriptor depth and nodes before native schema construction. |
| Payload input | 64 KiB; checked before decoding. |
| Nesting | Maximum depth 32, with the root at zero. |
| Decode work | 4,096 visited nodes, including repeated/overwritten fields, packed values and unknown groups. |
| JSON output | 1 MiB; the writer refuses growth before allocating past this size. |

These are fixed library constants, not app settings. A schema-aware wire scan checks lengths and counts before constructing values. Typed values, schemas, JSON conversion and allocator overhead have separate costs; these limits are not process RSS or wall-clock guarantees. Run decoding outside rendering.

Automatic `google.protobuf.Any` JSON unpacking is unavailable because embedded bytes would bypass preflight. Its typed fields and raw bytes remain readable. `SourceSchema::compile(root, imports)` accepts in-memory source plus up to 32 named imports. Source and compiled descriptors each have a 256 KiB cap; source nesting is capped at 32 and declarations at 4,096. Imports never read local files or make requests; embedded Google types are available. `SourceSchema::decoder(&[1, 0])` selects a declaration index path. Kafka owns registry requests and Confluent framing. [ADR-0008](../../docs/adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md) records the app and registry boundary.

## Validation

```sh
cargo test -p onetui-protobuf --locked
```

This package owns its tests, bounds and example. Tests build descriptor sets in Rust without `protoc`, covering typed values, raw retention, schema identity, malformed/truncated input, recursion, packed/overwritten fields and JSON failures. Kafka owns its separate binding and broker-fixture tests.
