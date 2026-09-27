---
status: accepted
date: 2026-09-27
---

# ADR-0017: Address RabbitMQ objects with verbs

## Decision

Replace the RabbitMQ editor's `METHOD /path` passthrough with a verb language whose
arguments are literal virtual host and object names. `RAW METHOD /path` remains for
endpoints the verbs do not model.

```
PUBLISH / amq.default demo          payload in the body
GET     / demo [count] [requeue|ack]
DECLARE object vhost name           definition in the body
DELETE  object vhost name
PURGE   / demo
RAW     PUT /api/users/alice        native path, optional body
```

[ADR-0005](0005-run-native-queries-through-providers.md) gave each provider its own query
syntax and named the Qdrant HTTP passthrough as the model for HTTP datasources. RabbitMQ
followed it. This decision moves RabbitMQ to the verb form Kafka
([ADR-0015](0015-publish-kafka-records-from-the-query-editor.md)) and NATS
([ADR-0016](0016-publish-nats-jetstream-messages.md)) already use, and narrows ADR-0005's
passthrough statement to Qdrant.

## Why the passthrough did not hold

RabbitMQ's default virtual host is named `/`. The management API carries a virtual host in
a path segment, so that name reaches the wire only as `%2F`. Passthrough made this the
user's problem in three ways:

- Every path in the editor read `/api/queues/%2F/demo`. The encoding is not a convention
  one can opt out of: `Url::join` collapses an empty segment, so a typed `//` cannot mean a
  virtual host named `/`.
- The watermark was the static `GET /api/overview` with `contextual_watermark: None`,
  discarding the virtual host the user had already navigated to. Kafka, NATS and DynamoDB
  all prefill from the open resource.
- A mis-encoded name returned an opaque native 404. Nothing connected the failure to the
  encoding.

Pushing `%2F` onto the user was a datasource detail the client is positioned to handle:
the browse paths already encode it ([browse.rs](../../crates/rabbitmq/src/browse.rs)), so
only the editor asked for it by hand.

## What the client decides, and what it does not

The verbs are a client-defined surface, which ADR-0005 avoided. The boundary is drawn so
the client owns addressing and nothing else:

- **Client:** which management path a verb maps to, and percent-encoding each name segment
  through `Url::path_segments_mut`. This is the reason the language exists.
- **Client:** the `PUBLISH` payload wrapper and the `GET` request envelope, because both
  are fixed shapes the user would otherwise retype. Payloads travel as base64 so bytes
  that are not valid UTF-8 survive in both directions, matching the NATS editor's
  `Onetui-Encoding` header.
- **Datasource:** every definition body, sent unchanged. An empty `DECLARE` body becomes
  `{}` and nothing more.
- **Datasource:** which object kinds and message counts exist. The `DECLARE`/`DELETE`
  object word is pluralised and sent without an allowlist, so a kind OneTUI has never
  heard of reaches the broker and a kind RabbitMQ drops needs no client change. `GET`
  sends the count as typed.
- **Datasource:** permissions. A 403 is a `Rejected` write carrying the native reason, not
  a client error, preserving
  [ADR-0013](0013-keep-connections-after-query-rejections.md).

Client-side errors are confined to statement shape: an unknown verb, a missing argument, a
body where the verb takes none, a `RAW` path with an empty segment. These describe the
editor's own grammar rather than RabbitMQ's.

## Consequences

Writes report outcomes instead of rendering a status row, so a publish the broker accepted
but routed nowhere reads as applied with "no queue matched the routing key" rather than
a `routed:false` body the user must notice. `GET` returns a page of decoded messages with
a destructive-read notice.

Existing `GET /api/overview` drafts stop parsing; they become `RAW GET /api/overview`.
Persisted query history from an earlier version fails on submission rather than silently
addressing something else.

The verbs cover queues, exchanges, bindings, policies, virtual hosts and publishing. Users,
permissions and node operations stay reachable only through `RAW`, where native paths and
`%2F` still apply. Without `RAW` this decision would remove capability; with it the long
tail costs one extra word.

A new RabbitMQ endpoint worth first-class addressing needs a verb here. That is the
maintenance cost accepted in exchange for removing `%2F` from the common paths.

## Alternatives

- **Keep the passthrough and add a contextual watermark that prefills `%2F`.** No
  ADR conflict and a smaller change, but the encoding stays visible in every path the user
  edits, which is the complaint.
- **Two verbs, as Kafka and NATS have.** Their wire protocols have two operations;
  RabbitMQ's management API has roughly forty endpoints across eleven resource types.
  Two verbs would reach publishing and reading while dropping administration.
- **Verbs with no escape hatch.** Consistent and smaller, but a capability regression for
  users, permissions and node operations.
- **Auto-encode a raw `/` inside a passthrough path.** A path separator and a literal
  slash are genuinely ambiguous in `METHOD /path`; guessing would silently address the
  wrong object.
