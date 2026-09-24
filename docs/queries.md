# Native queries

Press `e` or enter `:query` on a supported resource. Enter, F5 or Ctrl-R submits the draft; Shift+Enter inserts a newline. Ctrl-U clears it. Esc returns to browsing, and Ctrl-C cancels active work.
OneTUI asks before running a query by default. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Press `e` to edit again, `n/p` to page through results, or `r` to rerun.
In the query editor, Ctrl-P and Ctrl-N browse recent queries submitted on this connection.
Press Shift+H or enter `:history` while browsing to choose one from a list. Enter opens it for editing; Esc closes the list.
History stays in memory for the current session by default. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) contains full query text, including any passwords or tokens. Turning the setting off does not delete that file.

| Datasource | Query |
| --- | --- |
| [PostgreSQL](#postgresql) | SQL row queries |
| [Qdrant](#qdrant) | Filtered Scroll and point upsert JSON |
| [Kafka](kafka.md#replay-from-an-offset-or-timestamp) | Partition offset or timestamp replay |
| [NATS](nats.md#replay) | Stream subject, sequence or time replay |
| [DynamoDB](dynamodb.md#queries) | Native reads and PartiQL SELECT |

## PostgreSQL

```sql
SELECT id, name, country, balance
FROM demo.customers
WHERE active AND balance > 0
ORDER BY id
```

Use one `SELECT`, `VALUES`, or `WITH` query that reads data. Parameters, multiple statements, and utility commands such as `EXPLAIN` are unsupported. Include a unique ordering when page order matters.

Use least-privilege credentials. Queries consume database resources and may be logged.

## Qdrant

Select a collection and enter Scroll JSON:

```json
{
  "filter": {
    "must": [
      {"key": "active", "match": {"value": true}},
      {"key": "price", "range": {"gte": 1, "lt": 10}}
    ]
  },
  "limit": 100
}
```

Use `must`, `should` and `must_not` with exact-value matches or numeric ranges. `{}` browses unfiltered point IDs. Open a point for payloads or vectors. Advanced filters, custom ordering and similarity queries are unsupported.

To insert or replace one point in the selected collection, enter an upsert:

```json
{"operation":"upsert","points":[{"id":42,"vector":[0.1,0.2,0.3],"payload":{"label":"example"}}]}
```

Upserting an existing ID replaces that point. If a request times out after sending, check the collection before retrying because the result may be unknown.

Use `onetui schema --datasource qdrant` for the accepted fields and limits.

## Terminal input

Shift+Enter needs a terminal that sends a distinct modified key event. If it executes instead of inserting a newline, configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`). Bracketed multiline paste also preserves newlines without executing.
