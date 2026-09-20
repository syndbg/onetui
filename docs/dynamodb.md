# DynamoDB

Browse table and index metadata, typed items, account resources and Streams. Run bounded native reads or read-only PartiQL.

## Configuration

```toml
[connections.aws]
kind = "dynamodb"
region = "eu-west-1"
profile = "readonly"
```

Run `onetui --connection aws`. Credentials follow the AWS SDK chain, with environment credentials before the selected profile. Native profiles, SSO and roles are supported; `credential_process` is disabled. Use `onetui schema --datasource dynamodb` for settings and available operations.

Custom endpoints need a separate `streams_endpoint_url` for Streams. Give the account read permissions for the resources you intend to inspect.

## Browsing

Open **Tables**, select a table, then choose metadata, indexes or **Scan items**. Opening metadata does not scan items. Values retain DynamoDB type tags, decimal strings and base64 binary values.

Reads consume capacity. Narrow the query or projection if a result exceeds the size limit.

## Queries

Press `e` on a selected table and enter native read-operation JSON:

```json
{
  "operation": "Query",
  "key_condition_expression": "pk = :key",
  "expression_attribute_values": {":key": {"S": "customer-1"}},
  "limit": 50
}
```

`Scan`, `GetItem`, batch and transactional reads, read-only PartiQL and vector search are also available. Use `onetui schema --datasource dynamodb` for examples and fields. Keys and expression values use tagged AttributeValue JSON.

PartiQL accepts one `SELECT` from the selected table per statement. Prefer typed parameters for values. A query without a key condition may scan the table.

## Streams

Open **Streams** → a stream → **Shards** → a shard. Use `f` to follow new records or `e` to replay from a sequence. When a shard closes, select its child shard explicitly.

Try `local_dynamodb` in the [demo fixtures](../hack/README.md). DynamoDB Local does not implement every AWS feature. Cloud-only metadata and vector search have protocol tests, not live AWS validation.
