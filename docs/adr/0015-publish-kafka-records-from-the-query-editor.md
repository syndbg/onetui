---
status: accepted
date: 2026-09-26
---

# ADR-0015: Publish Kafka records from the query editor

## Decision

Kafka query submissions start with a verb line that names their target, then a blank line, then a body:

```
CONSUME orders/0 offsets 123..250
```

```
PRODUCE orders

{"key":"order-1","value":"created"}
```

`CONSUME` reads one named partition and states its range on the same line, as `offsets start..end` or `time start..end`; it takes no body. The verb line therefore carries the whole read: the target it addresses, and how much of it to return. `PRODUCE` publishes one JSON record per line, at most 100 per submission. Both verbs go through `Executor::execute_query`, as [ADR-0012](0012-submit-editor-queries-through-one-provider-operation.md) requires. The TUI does not classify a submission as a read or a write, and the provider applies the same confirmation, history and display rules to both.

The verb line names the topic, so a query means the same thing wherever it runs, and the editor opens on a topic as well as on a partition. The editor prefills the line from the open view. A record goes to its own `partition` field if it has one, otherwise to the partition on the verb line, otherwise to the broker's partitioner, which hashes the key and round-robins without one.

Publishing uses a second native client with retries disabled and `acks=all`. A failed record is reported, never resent. A submission that ends without a delivery report reports the outcome as unknown.

Keys and values are UTF-8 text unless a per-record `key_encoding` or `value_encoding` of `base64` or `hex` decodes them to bytes. An explicit `null` value is a tombstone and requires a key.

## Context and consequences

The editor previously accepted a bare replay JSON object for the selected partition, which left no room for a second operation and no way to name a target. A verb line adds one, following the `METHOD /path` form Qdrant already uses ([ADR-0005](0005-run-native-queries-through-providers.md)), rather than a discriminated JSON object that would have to guess the operation from which keys are present.

Retries stay off for the reason [ADR-0014](0014-send-dynamodb-partiql-to-the-service.md) gives for DynamoDB: a resent record can duplicate one the first attempt already appended, and only the user can decide whether that is acceptable. A rejected publish keeps the session, per [ADR-0013](0013-keep-connections-after-query-rejections.md).

A produce returns `QueryExecution::Write`, as PostgreSQL and NATS do, rather than a page of rows. The summary carries each delivered record's partition and offset. A batch has no partial outcome: `Applied` requires every record to be delivered and `Rejected` every one to be refused by a completed broker response, so a partly delivered batch reports `Unknown`. Resending it would duplicate the records that already landed, which is the same judgment ADR-0014 leaves to the user.

OneTUI does not encode records against a schema. A topic bound to an Avro or Protobuf decoder is written by supplying the already-encoded bytes through `base64` or `hex`, which keeps the editor out of the business of resolving a writer schema at publish time.

This supersedes the read-only scope stated for Kafka in [ADR-0006](0006-browse-kafka-with-rust-rdkafka.md). Topic auto-creation stays disabled, so publishing requires an existing topic and `Write` permission on it.
