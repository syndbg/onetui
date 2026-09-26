# DynamoDB

Browse table and index metadata, typed items, account resources and Streams. Run native reads and PartiQL statements.

## Configuration

```toml
[connections.aws]
kind = "dynamodb"
region = "eu-west-1"
```

Run `onetui --connection aws`. Credentials follow the AWS SDK chain, with environment credentials before the selected profile. Native profiles, SSO and roles are supported; `credential_process` is disabled. Use `onetui schema --datasource dynamodb` for settings and available operations.

Custom endpoints need a separate `streams_endpoint_url` for Streams.

## Browsing

Open **Tables**, select a table, then choose metadata, indexes or **Scan items**. Opening metadata does not scan items. Values retain DynamoDB type tags, decimal strings and base64 binary values.

Reads consume capacity. Narrow the query or projection if a result exceeds the size limit.

## Queries

Press `e` on a selected table and enter operation JSON:

```json
{
  "operation": "Query",
  "key_condition_expression": "pk = :key",
  "expression_attribute_values": {":key": {"S": "customer-1"}},
  "limit": 50
}
```

Operations include Scan, GetItem, batch and transactional reads, PartiQL statements, and vector search. Use `onetui schema --datasource dynamodb` for examples and fields. Keys and expression values use tagged AttributeValue JSON.

PartiQL statements can read or write the table they name. Prefer typed parameters for values. A query without a key condition may scan the table. If a write times out, inspect the target before retrying.

### Editor controls

Enter, F5 or Ctrl-R submits the draft. Shift+Enter inserts a newline, and Ctrl-U clears the draft. Esc returns to browsing; Ctrl-C cancels active work. A new query starts empty, and its watermark is a hint that disappears when you type. Press `e` to edit and submit again; `r` refreshes ordinary resource views.

OneTUI asks before running a query. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Ctrl-P and Ctrl-N browse recent queries submitted on this connection. Shift+H or `:history` while browsing opens them as a list: Enter opens one for editing, Esc closes the list. History stays in memory for the session. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) then holds full query text, including any passwords or tokens; turning the setting off does not delete that file.

Shift+Enter needs a terminal that reports modified keys. OneTUI requests that, so it works wherever the terminal supports it. Where it does not, Shift+Enter is indistinguishable from Enter and submits instead: configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`), or paste multiline text, which preserves newlines without executing. Inside tmux this also needs `set -g extended-keys on`.

## Streams

Open **Streams** → a stream → **Shards** → a shard. Use `f` to follow new records or `e` to replay from a sequence. When a shard closes, select its child shard explicitly.

Try `local_dynamodb` in the [demo fixtures](../hack/README.md). DynamoDB Local does not implement every AWS feature.
