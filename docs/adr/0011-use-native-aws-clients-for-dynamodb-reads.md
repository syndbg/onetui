---
status: accepted
date: 2026-09-14
---

# ADR-0011: Use native AWS clients for DynamoDB reads

## Decision

Keep DynamoDB in `onetui-dynamodb`, registered through the built-in provider enums. Use `aws-sdk-dynamodb`, `aws-sdk-dynamodbstreams` and `aws-config` for signing, credential refresh, endpoint resolution, retries and HTTP pooling. Configuration selects a region and optional profile or explicit credential references. HTTP endpoints require literal loopback addresses and explicit fixture credentials.

Allow only implemented read operations. Opening metadata never starts a Scan; scanning and following are explicit. Preserve native continuation keys, including after empty filtered pages. Bind bookmarks to the connection session, resource and query. Bound requests by time, rows and bytes; cancellation must not advance the caller's bookmark.

Retain AttributeValue tags, decimal strings and binary bytes in their base64 wire representation. Capture bounded JSON responses through an SDK interceptor so metadata fields are not lost when the SDK model lags the service. Native errors retain their returned details after secret redaction and terminal escaping. [AWS interceptor API](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/interceptors.html)

Stream bookmarks retain the last displayed sequence and native iterator. Explicit inclusive replay retains its requested start until the first record is displayed. Expired iterators renew from that position; an unanchored expiration fails visibly. Shard closure retains the final batch and requires explicit child-shard selection. This avoids skipping records by silently restarting at `LATEST`. [Streams iterator behavior](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_GetRecords.html)

For successful Streams responses that the SDK model cannot decode, keep the original bounded JSON with a decoding warning. DynamoDB Local returns untagged `{}` for some empty-map images; inferring `M` would alter the returned data. HTTP failures, malformed JSON and missing record identities still fail the request.

## Rejected alternatives

- Shelling out to the AWS CLI adds a subprocess lifecycle and output-format dependency.
- The SDK's optional `credential_process` is disabled until its subprocess output and cancellation cleanup are bounded. Other native credential providers remain available.
- Custom HTTP signing duplicates credential, retry and authentication code already provided by the SDK.
- Flattening attributes to ordinary JSON loses number precision and distinctions between sets, lists, binary and null.
- Automatic whole-table traversal can consume substantial read capacity. A page is an independent read, not a snapshot.
