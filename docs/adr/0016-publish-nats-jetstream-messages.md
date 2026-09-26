---
status: accepted
date: 2026-09-27
---

# ADR-0016: Publish JetStream messages from the query editor

## Decision

NATS query submissions start with a verb line naming their target, then a blank line, then a body:

```
CONSUME EVENTS orders.* seq 1..500
```

```
PRODUCE EVENTS orders.created

hello
```

`CONSUME` reads a stream, optionally narrowed to a subject and a `seq start..end` or `time start..end` range, all on the verb line; it takes no body. The subject is a target, so it sits beside the stream rather than inside a body, and the verb line carries the whole read: what it addresses, and how much to return. Kafka uses the same grammar with `offsets` in place of `seq`. `PRODUCE` sends one JetStream publish with optional headers and a literal payload, setting `Nats-Expected-Stream` to the named stream, then waits for the native acknowledgment. Both verbs go through `Executor::execute_query`, as [ADR-0012](0012-submit-editor-queries-through-one-provider-operation.md) requires. The TUI does not classify a submission as a read or a write.

A publish returns `QueryExecution::Write`. An acknowledgment reports `Applied` with the stream and sequence. OneTUI does not retry the submission.

## Context and consequences

The editor previously took a JSON object whose `operation` field selected the request. A verb line names the target instead, so a query means the same thing wherever it runs, matching the Kafka ([ADR-0015](0015-publish-kafka-records-from-the-query-editor.md)) and Qdrant ([ADR-0005](0005-run-native-queries-through-providers.md)) editors.

Retries stay off for the reason [ADR-0014](0014-send-dynamodb-partiql-to-the-service.md) gives: only the user can judge whether resending risks a duplicate. Even when JetStream rejects storage, a Core subscriber may already have received the message. Every failure after dispatch therefore reports `Unknown`, and the user must inspect before submitting again. A rejected publish keeps the connection, per [ADR-0013](0013-keep-connections-after-query-rejections.md).

Header lines sit between the verb line and the blank line, as `Name: value`, mirroring how headers travel on the wire and keeping the payload literal. A reserved `Onetui-Encoding` line decodes the payload as `base64` or `hex` instead of UTF-8; it is consumed rather than transmitted, so an ordinary `Content-Encoding` header can still be sent as itself.

Schema-encoded publishing is outside this slice: a subject whose messages OneTUI decodes with a schema must be written as the encoded bytes. Core-only publishing has no storage acknowledgment and is also out of scope.
