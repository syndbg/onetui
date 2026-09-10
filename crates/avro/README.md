# onetui-avro

Raw Avro decoding with an explicit writer schema, using `apache-avro` 0.22. The library performs no file or network access and has no Protobuf, datasource SDK or terminal dependency. `Decoder` returns original bytes, a schema identity and an `apache_avro::types::Value`. JSON is a separate, fallible presentation.

The Kafka browser supports explicit [raw bindings](../../docs/kafka.md#schema-bound-key-and-value-previews) and [Confluent Avro registry bindings](../../docs/kafka.md#confluent-avro-registry) in `onetui.toml`; `onetui schema --datasource kafka` lists their settings. Unbound fields and NATS retain Auto/text/hex behavior.

## Try a raw message

From the workspace root:

```sh
cargo run -p onetui-avro --example decode_avro --locked -- crates/avro/examples/event.avsc 0e044869
```

This prints `{"id":7,"name":"Hi"}`. Arguments are a UTF-8 writer-schema file and hexadecimal datum bytes. Hex must contain complete byte pairs, without spaces or `0x`; an empty argument represents an empty payload. File paths resolve from the current directory. The example does not read OneTUI configuration, expand environment variables, guess framing or contact a datasource.

## API and interpretation

```rust
use onetui_avro::Decoder;
use apache_avro::types::Value;

fn main() -> anyhow::Result<()> {
    let decoder = Decoder::new(r#""long""#)?;
    let decoded = decoder.decode(&[14])?;
    assert_eq!(decoded.raw(), &[14]);
    assert!(matches!(decoded.value(), Value::Long(7)));
    assert_eq!(decoded.json()?, "7");
    Ok(())
}
```

`Decoder::new` loads one self-contained writer schema and resolves its named references locally. Decoding uses `GenericDatumReader`, consumes exactly one datum and rejects trailing bytes. It does not strip Confluent prefixes, fingerprints or container headers.

`Decoder::with_references(writer_schema, &[reference_schema])` accepts up to 32 named writer-schema dependencies supplied by the caller. The combined bundle is limited to 256 KiB and shares the schema node/name budget. It resolves dependencies with Apache Avro's native schema resolver, without reading files or contacting registries. With references, the identity hashes length-prefixed UTF-8 schemas in supplied order, followed by the writer schema; reordering references changes that identity.

`schema_id()` includes SHA-256 of the exact supplied UTF-8 schema bytes, so whitespace changes its identity. This identifies an interpretation, not the producer's actual schema or a registry ID. A successful decode does not prove the schema was correct.

`Decoded::value()` preserves bytes, union indexes and logical types. JSON can flatten that information, so it is not a lossless export. Raw bytes remain available after a JSON-presentation error. `decode` borrows its input; an error leaves the caller's data untouched. The decoder never falls back to another format.

## Bounds and limitations

| Limit | Enforcement |
| --- | --- |
| Schema input | 256 KiB; preflight limits JSON depth and nodes before native schema construction. |
| Payload input | 64 KiB; checked before decoding. |
| Nesting | Maximum depth 32, with the root at zero; reference/union traversal also counts. |
| Decode work | 4,096 visited nodes across the datum, including collection entries and map keys. |
| Copied names | 256 KiB across record field names and enum symbols. Map-key bytes also fit the payload limit. |
| JSON output | 1 MiB; the writer refuses growth before allocating past this size. |

These are fixed library constants, not app settings. A schema-aware wire scan checks lengths and counts before constructing values. It bounds zero-width arrays, nested collection amplification, recursive records and truncated fields without changing Apache Avro's process-global allocation setting. Typed values, schemas, JSON conversion and allocator overhead have separate costs; these limits are not process RSS or wall-clock guarantees. Run decoding outside rendering.

`big-decimal` is rejected because its arbitrary scale needs a separate bound; fixed-scale decimal is supported. NaN/infinity remains a typed float but JSON conversion fails instead of replacing it with null.

Reader-schema resolution, single-object framing and object containers are not implemented. The Kafka adapter owns registry requests and Confluent payload-prefix parsing; this library only accepts caller-supplied schemas and raw datum bytes. Header-GUID framing remains unsupported. [ADR-0008](../../docs/adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md) records the app and registry boundary.

## Validation

```sh
cargo test -p onetui-avro --locked
```

This package owns its tests, bounds and example. Tests cover typed values, raw retention, schema identity, malformed/truncated input, recursive data, collection amplification and JSON failures. Kafka owns its separate binding and broker-fixture tests.
