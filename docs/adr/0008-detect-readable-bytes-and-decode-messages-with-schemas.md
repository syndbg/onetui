---
status: accepted
date: 2026-09-10
---

# ADR-0008: Detect readable bytes and decode messages with explicit schemas

## Decision

Auto display validates UTF-8 for byte values, just as it does for text. Complete JSON objects/arrays use JSON formatting; other valid UTF-8 uses escaped text. Invalid UTF-8 falls back to hex. Formatting limits still apply: malformed JSON-looking input can fall back to text, and text that cannot be rendered within the limits falls back to hex. Explicit format selection remains available.

This replaces only the opaque-bytes-always-use-hex rule in [ADR-0004](0004-preserve-values-and-select-display-formats.md). Retained bytes, provenance, null/empty distinctions, terminal escaping and bounded caches remain unchanged. Filter/sort keep their stable projection; readable previews do not change byte-field matching.

Protobuf and Avro decoding requires explicit schema and framing choices. Auto is a display convenience, not serialization detection: binary messages can happen to be valid UTF-8. A successful decode also cannot prove that the selected schema is correct.

The [Protobuf](../../crates/protobuf/README.md) and [Avro](../../crates/avro/README.md) guides describe library bounds; [Kafka usage](../kafka.md#schema-bound-key-and-value-previews) documents bindings.

## Decoder boundary

Keep message decoding separate from value formatting. A decoder interprets retained bytes using a schema; the existing TUI formatter controls pretty printing, highlighting, wrapping and Unicode display of the result.

Keep schema parsing and pure decoding in independent `onetui-protobuf` and `onetui-avro` packages. Each owns its native library dependencies, result types, bounds, tests and examples. Neither depends on the other, Ratatui, core or datasource SDKs. Small bounds helpers remain package-local so format-specific validation can evolve independently.

Select these concrete decoders through a built-in enum in the connector's worker adapter, following [ADR-0002](0002-use-static-enum-dispatch-for-built-in-providers.md). Keep native codec types out of core. The standalone libraries need no dispatch facade, shared trait or boxed future. Loading user schemas does not require loading user code or dynamic plugins.

Bind a decoder to an exact connection, resource/topic and field. Kafka keys and values need separate bindings; a text key must not inherit an Avro value decoder. Omitted bindings retain Auto. Framing and schema source are explicit, not inferred from topic names or a few leading bytes. Reject conflicting bindings during configuration validation.

The connector retains original bytes and message metadata. A worker resolves the binding and schema, then decodes outside rendering. The result is a derived typed value with schema identity and provenance, or a per-value error. The renderer keeps raw hex/binary accessible, including any framing bytes. Decoding never changes offsets, acknowledgements, pagination or retained data.

## Schema choices and examples

Schema choices:

| Message | Explicit inputs | Decoding |
| --- | --- | --- |
| `orders` key: UTF-8 customer ID | No binding | Existing Auto text display |
| `orders` value: raw Protobuf | Descriptor-set file including imports; full message name such as `shop.Order`; raw framing | Look up the message descriptor, decode the entire payload |
| `events` value: raw Avro datum | Writer schema file and referenced named types; raw framing | Decode one datum; reject trailing bytes |
| Registered Protobuf or Avro | Configured registry; explicit Confluent framing | Resolve the message's exact schema ID and references; Protobuf also selects its message index path |

For Protobuf, use `prost-reflect`: its `DescriptorPool` loads a `FileDescriptorSet`, and `DynamicMessage` decodes against a selected message descriptor. It supports the Protobuf JSON mapping without generating application-specific Rust types. [Library documentation](https://docs.rs/prost-reflect/latest/prost_reflect/)

For Avro, use `apache-avro`'s generic datum reader with the writer schema. Reader-schema resolution is an explicit optional choice, not a substitution of the latest schema. Raw datums are not object-container files; the container reader is not interchangeable with datum decoding. [Datum-reader documentation](https://docs.rs/apache-avro/latest/apache_avro/reader/datum/fn.from_avro_datum.html)

Preserve decoded type information for Protobuf bytes, unknown fields and enum values, and Avro bytes, unions and logical types. A JSON presentation is a derived interpretation, not a lossless export format; raw bytes remain authoritative. Missing Protobuf imports or `Any` descriptors produce a visible limitation rather than an automatic network fetch.

Expose typed inspection through the existing row/value viewer, alongside JSON. Keep each representation's errors separate. Protobuf unknown fields include their number, wire type and re-encoded bytes; do not present those bytes as an exact original wire slice.

An optional Avro reader schema changes the JSON projection, not the retained writer value. Inspect writer and reader values together, identify both schemas, and keep writer inspection available when resolution fails. Bound default expansion before native resolution. Never choose a reader schema automatically from a registry's latest version.

## Framing and registry access

Start with raw messages and Confluent's payload-prefix framing: version byte, four-byte big-endian schema ID, then format-specific content. Protobuf includes a variable-length message-index path before its payload; stripping five bytes is insufficient. Confluent's newer header-GUID framing is a separate variant and must remain explicitly unsupported until implemented. [Wire formats](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)

Avro single-object framing uses a schema fingerprint, while an object container carries its schema and blocks. Neither is a raw datum or Confluent payload. Add them as explicit framing variants when supported; do not silently reinterpret them. [Avro specification](https://avro.apache.org/docs/1.12.0/specification/)

Registry access is opt-in and read-only, with its own configured endpoint, verified TLS and secret references; do not inherit broker credentials. Resolve exact IDs and versioned references, never `latest`. Protobuf registry schemas and their imports must be compiled into descriptors by a bounded resolver before decoding; `prost-reflect` is not a `.proto` source compiler.

Cache schemas by registry/authentication context and exact identity, with bounded entries and bytes. Resolve only configured local files or references within the configured registry; message data cannot supply arbitrary URLs. Apply request deadlines, cancellation and stale-result rejection outside the draw path. Bound schema size, reference traversal, decoded depth, allocations and formatted output. A worker thread alone does not bound a malicious decode.

Unknown schemas, unavailable registries, invalid framing and decode-limit failures show the error alongside raw hex. An explicitly selected codec must not silently fall back to another interpretation. One bad message must not stop following or hide other records.

## Alternatives

- Always-hex Auto preserves bytes but makes ordinary text/JSON messages unnecessarily difficult to read. Strict UTF-8 plus an unchanged raw view provides both.
- Guessing Protobuf/Avro from successful parsing can produce plausible but incorrect fields. Require a binding.
- Generated Rust structs fit application-owned schemas, not an inspector loading arbitrary user schemas.
- A combined codec package couples unrelated native dependencies and tests. Separate packages let callers use either format independently.
- Runtime plugins and codec-specific UI panels duplicate machinery already covered by static dispatch and the shared value viewer.
