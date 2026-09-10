# onetui-codec

Raw Protobuf and Avro decoding with explicit schemas. The library performs no file or network access and has no datasource SDK or terminal dependency. `Decoder` uses built-in enum dispatch and returns the original bytes, a schema identity and a native typed value. JSON is a separate, fallible presentation.

The OneTUI app does not use these decoders yet. There are no codec settings in `onetui.toml`, and `onetui schema` does not advertise codec bindings. Kafka and NATS retain their existing Auto/text/hex behavior.

## Try a raw message

From the workspace root:

```sh
cargo run -p onetui-codec --example decode --locked -- avro crates/codec/examples/event.avsc 0e044869
```

This prints `{"id":7,"name":"Hi"}`. The example takes a UTF-8 writer-schema file and hexadecimal datum bytes. Hex must contain complete byte pairs, without spaces or `0x`; an empty argument represents an empty payload. File paths resolve from the current directory. It does not read OneTUI configuration, expand environment variables, guess framing, or contact a datasource.

For Protobuf, compile a descriptor set with imports using an installed `protoc`:

```sh
protoc -I crates/codec/examples --include_imports --descriptor_set_out=/tmp/onetui-event.pb event.proto
cargo run -p onetui-codec --example decode --locked -- protobuf /tmp/onetui-event.pb demo.Event 080712024869
```

The full message name is exact and case-sensitive. The example prints Protobuf JSON: `{"id":"7","name":"Hi"}`. The JSON mapping represents 64-bit integers as strings and bytes as base64. Tests build descriptor sets in Rust and do not require `protoc`.

## API and interpretation

```rust
use onetui_codec::{Decoder, TypedValue};

fn main() -> anyhow::Result<()> {
    let decoder = Decoder::avro(r#""long""#)?;
    let decoded = decoder.decode(&[14])?;
    assert_eq!(decoded.raw(), &[14]);
    assert!(matches!(decoded.value(), TypedValue::Avro(value)
        if matches!(value, apache_avro::types::Value::Long(7))));
    assert_eq!(decoded.json()?, "7");
    Ok(())
}
```

`Decoder::protobuf` loads a binary `FileDescriptorSet` including its imports. `Decoder::avro` loads one self-contained writer schema; named references within it are resolved locally. The native libraries are `prost-reflect` 0.16 and `apache-avro` 0.22. The Avro path uses `GenericDatumReader`, not the object-container reader.

`schema_id()` includes SHA-256 of the exact supplied schema bytes; Protobuf also includes the selected message name. Whitespace changes in Avro therefore change its identity. This identifies an interpretation, not the producer's actual schema or a registry ID. A successful decode does not prove the schema was correct.

`TypedValue` preserves Protobuf unknown fields and enum numbers, bytes, Avro union indexes and logical types. JSON can omit or flatten that information, so it is not a lossless export. Raw bytes remain available after a JSON-presentation error. `decode` borrows its input; a decode error leaves the caller's original data untouched. An explicit decoder never falls back to text or another codec.

Avro consumes exactly one datum and rejects trailing bytes. Protobuf consumes the whole wire stream, including valid unknown fields. Concatenated Protobuf messages can be valid merge semantics; raw decoding cannot detect a missing application framing boundary. Neither decoder strips Confluent prefixes, fingerprints or container headers.

## Bounds and limitations

| Limit | Enforcement |
| --- | --- |
| Schema input | 256 KiB; preflight limits descriptor/JSON depth and nodes before native schema construction. |
| Payload input | 64 KiB; checked before decoding. |
| Nesting | Maximum depth 32, with the root at zero. Avro reference/union traversal also counts. |
| Decode work | 4,096 visited nodes across the whole datum, including repeated/overwritten Protobuf fields, packed values, Avro collection entries and map keys. |
| Copied Avro names | 256 KiB across record field names and enum symbols. Map-key bytes also fit the payload limit. |
| JSON output | 1 MiB; the writer refuses growth before allocating past this size. |

These fixed limits are library constants, not app configuration. A schema-aware wire scan validates lengths and counts without constructing decoded values. This catches large zero-width Avro arrays, nested collection amplification, recursive records and truncated fields before native allocation. It does not change Apache Avro's process-global allocation setting. Typed values, schema storage, JSON conversion and allocator overhead have separate bounded costs; these are not process RSS or wall-clock guarantees. Run decoding outside the draw path.

Automatic `google.protobuf.Any` JSON unpacking is unavailable because embedded bytes would bypass preflight. Its typed fields and raw bytes remain readable. Avro `big-decimal` is rejected because its arbitrary scale needs a separate bound; fixed-scale decimal is supported. Avro NaN/infinity remains a typed float but JSON conversion fails instead of replacing it with null.

Reader-schema resolution, external schema reference resolution, registry requests, `.proto` source compilation, Confluent payload/header framing, Avro single-object framing and object containers are not implemented here. [ADR-0008](../../docs/adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md) records the intended app and registry boundary.

## Validation

```sh
cargo test -p onetui-codec --locked
```

Protobuf and Avro tests live in this package. They cover valid typed values, raw retention, exact schema identity, malformed/truncated input, recursive data, node/byte bounds and JSON failures. Existing connector fixtures remain independent and do not yet produce schema-bound messages for the app.
