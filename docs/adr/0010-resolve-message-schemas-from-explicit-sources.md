---
status: accepted
date: 2026-09-11
---

# ADR-0010: Resolve message schemas from explicit sources

## Decision

Load schemas dynamically through built-in source implementations. Keep `onetui-avro` and `onetui-protobuf` independent: they parse schemas and decode bytes, while source adapters handle files, registry protocols, authentication and caching. Use static enum dispatch as in [ADR-0002](0002-use-static-enum-dispatch-for-built-in-providers.md). User schemas are data, not runtime plugins.

This extends [ADR-0008](0008-detect-readable-bytes-and-decode-messages-with-schemas.md). Kafka supports explicit files, directory catalogs and Confluent Avro/Protobuf registries. Buf discovery remains pending.

Keep schema source, framing and message selection distinct. Discovering a directory or registry's schemas does not identify which schema encoded a record. Raw messages require an explicit binding; framed messages resolve the identity carried by their configured envelope. Kafka keys and values remain independently bound. Future NATS bindings use the same decoding rules without inheriting Kafka topic semantics.

```text
bytes + headers + binding
          |
interpret configured framing
          |
resolve exact schema and dependencies
          |
Avro decoder or Protobuf decoder
          |
decoded view + schema provenance + retained raw bytes
```

## Sources and identity

| Source | Schema material | Message selection |
| --- | --- | --- |
| Local file or directory catalog | Avro writer schemas; Protobuf descriptor sets including imports | Explicit binding to an artifact and, for Protobuf, a fully qualified message name |
| Confluent-compatible registry | Exact schema ID and versioned references | Configured payload-prefix framing; Protobuf also uses the message-index path |
| Buf Schema Registry | Protobuf module descriptors including imports | Pinned module revision and message name, or an explicitly configured application type identifier |

Local directories provide an inventory, not trial decoding. Reject ambiguous catalog identities and confine dependency resolution to configured roots. Reload catalogs explicitly; do not silently reinterpret retained records when files change. For local `.proto` source trees, use Buf or `protoc` to produce descriptor sets outside the application rather than adding a directory compiler to OneTUI. Buf supports `buf build --as-file-descriptor-set` and can return compiled descriptors directly from its registry. Buf is a Protobuf registry, not an Avro source. [Buf descriptor documentation](https://buf.build/docs/bsr/module/descriptor/)

Resolve a registry message's writer schema by exact identity, never by a subject's latest version. Fetch referenced versions explicitly. Protobuf registry sources compile with `protox` inside the Protobuf crate, using an in-memory resolver and bounded sources/descriptors. This avoids an external `protoc` process and ambient filesystem imports; `prost-reflect` does not compile `.proto` files. A registry endpoint supporting a compatible API still needs integration tests for its formats and references. [Confluent formats and references](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)

Confluent header-GUID framing is distinct from its schema-ID payload prefix. Avro single-object encoding carries a fingerprint; an Avro object container embeds its writer schema and blocks. These need explicit framing implementations and remain unsupported until validated. A directory could resolve an Avro fingerprint once that framing exists. [Confluent wire formats](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format), [Avro specification](https://avro.apache.org/docs/1.12.0/specification/)

AWS Glue supports Avro and Protobuf but needs its own identity, framing and authentication adapter. Native Apicurio integration likewise requires its own protocol handling; do not assume every registry uses Confluent framing. These adapters are deferred. [AWS Glue registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html), [Apicurio identities](https://www.apicur.io/registry/docs/apicurio-registry/3.2.x/getting-started/assembly-registry-concepts-glossary.html)

## Resolution and failure behavior

Directory catalogs use filename stems as IDs in a flat inventory. Avro bindings list dependencies explicitly; Protobuf artifacts include their imports. Files open relative to a pinned directory descriptor without following symlinks. Each binding snapshots its schema until the connection is reopened; no watcher or refresh-time reload. See [catalog settings](../kafka.md#local-directory-catalogs) for bounds and usage.

Resolve outside rendering, with deadlines, cancellation and stale-result rejection. Bound schema bytes, dependency depth/count, compilation work, concurrent requests and cache bytes/entries. Reuse schema results across records with the same identity. Cache keys include the configured source, authentication context and immutable schema identity; moving labels resolve to a pinned revision. Refreshes must not replace the schema attached to an already decoded value.

Registry access is opt-in and read-only, with independently configured credentials and verified TLS. Message data cannot choose a network endpoint or local path. Restrict dependency fetches and redirects to the configured source; do not send credentials to another host. Offline catalog output describes supported settings without contacting registries.

Show the resolved source, identity/revision, selected message type and errors with the decoded value. A lookup or decode failure retains raw bytes, including framing, and does not stop other records from displaying. No silent fallback to another schema or format is allowed for an explicit binding. A schema failure must not change offsets, acknowledgements or pagination.

## Alternatives and consequences

- Trying schemas until one parses can produce plausible but incorrect data. Protobuf wire bytes carry field numbers and wire types, not their names or full declared types. Explicit identity is required. [Protobuf encoding](https://protobuf.dev/programming-guides/encoding/)
- Using the latest schema simplifies lookup but can misread historical records. Reader-schema resolution, when supported, is a separate explicit choice from selecting the writer schema.
- A combined codec package couples unrelated dependencies and tests. Source adapters can supply either format without merging the decoders or putting registry SDKs in core.
- Automatic directory watching and arbitrary network discovery add lifecycle and trust boundaries. Explicit reloads and configured sources cover the initial use case.

The cost is configuration for raw messages and visible failures when schemas are unavailable. The benefit is a reproducible interpretation whose source can be inspected alongside the original bytes.
