# DynamoDB

```toml
[connections.aws]
kind = "dynamodb"
region = "eu-west-1"
profile = "readonly"
```

Use `onetui --connection aws`. Credentials follow the AWS SDK chain: environment credentials precede the selected profile. They load only when the selected connection makes a request. Use `onetui schema --datasource dynamodb` for settings and available resources.

Native profiles, SSO and role credentials are supported. `credential_process` is disabled: the SDK subprocess currently lacks bounded output and cancellation cleanup.

Request deadlines and Ctrl-C also cover native TLS trust loading. A stalled OS read retains one background slot until it finishes; retries cannot accumulate trust-loading threads.

Open **Tables**, then a table. **Scan items** reads one bounded page; opening table metadata does not scan items. `n`/`p` navigate retained page bookmarks. Reads consume capacity and are not snapshots. A filtered page can be empty and still have a next page.

`:query` accepts native read-operation JSON for the selected table:

```json
{
  "operation": "Query",
  "key_condition_expression": "pk = :key",
  "expression_attribute_values": {":key": {"S": "customer-1"}},
  "limit": 50
}
```

`Scan` and `GetItem` are also supported. Keys and expression values use DynamoDB's tagged AttributeValue JSON. Item cells retain those tags, exact decimal strings and base64 binary values; missing attributes differ from `{"NULL":true}`. Reads evaluate at most 100 items per request. Reduce `limit` or use `projection_expression` if a page exceeds 1 MiB.

Table/index descriptions, replicas, TTL, continuous backups, tags, policies, contributor insights, Kinesis destinations and replica scaling are inspectable. Account views list existing backups/imports/exports, legacy global tables, limits and endpoints. These views require their corresponding AWS read permissions; they never create jobs or follow returned endpoints.

Open **Streams** → a stream → **Shards** → a shard to read retained records. `n`/`p` use sequence bookmarks; `f` starts following new records in that shard. Reads retain keys, old/new images and native attribute tags. Following keeps 100 records within 1 MiB. Shard closure stops following after its final batch; select child shards explicitly. Expired iterators renew from the last retained sequence. Without a sequence anchor, expiration returns the native error instead of restarting at the latest position.

For custom endpoints, also set `streams_endpoint_url`; OneTUI never guesses it. DynamoDB Local sometimes returns untagged `{}` for empty maps in stream images. The SDK rejects those values; OneTUI shows the original JSON and a decoding warning, without guessing the missing type.

On a shard or record, `:query` fills in the selected shard and sequence for replay:

```json
{"operation":"GetRecords","shard_id":"shard-id","sequence_number":"123","after":false,"limit":100}
```

Replace the sample identifiers with values from the stream. `after: false` includes the requested sequence; `true` starts after it. Stream scope comes from the current view. Empty pages retain that start position, and trimmed sequences return the native error. All fields are listed in `onetui schema --datasource dynamodb`.

Batch/transactional reads and PartiQL are not supported yet. No writes or administration.

For local samples, use `make dev-up`, then `make run` → `local_dynamodb`. The fixture uses fake credentials and keeps its data in memory. `make test-integration` includes package-owned DynamoDB Local tests; it refuses to reset existing fixtures.
