# Native queries

Press `e` or enter `:query` on a supported resource. Enter, F5 or Ctrl-R executes the draft; Shift+Enter inserts a newline. Ctrl-U clears it. Esc returns to browsing, and Ctrl-C cancels active work.

Press `e` to edit again, `n/p` to page through results, or `r` to rerun.
In the query editor, Ctrl-P and Ctrl-N browse queries submitted on this connection during the current session.
Press Shift+H or enter `:history` while browsing to choose one from a list. Enter opens it for editing; Esc closes the list.

| Datasource | Query |
| --- | --- |
| [PostgreSQL](#postgresql) | Read-only SQL |
| [Qdrant](#qdrant) | Filtered Scroll JSON |
| [Kafka](kafka.md#replay-from-an-offset-or-timestamp) | Partition offset or timestamp replay |
| [NATS](nats.md#replay) | Stream subject, sequence or time replay |
| [DynamoDB](dynamodb.md#queries) | Native read-operation JSON and read-only PartiQL |

## PostgreSQL

```sql
SELECT id, name, country, balance
FROM demo.customers
WHERE active AND balance > 0
ORDER BY id
```

Use one `SELECT`, `VALUES` or read-only `WITH` statement. Parameters, multiple statements, utility commands such as `EXPLAIN`, and writes are unsupported. Include a unique ordering when page order matters.

Use least-privilege credentials. Read-only transactions do not sandbox functions with external effects, and queries still consume database resources. The database may log query text.

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

Use `onetui schema --datasource qdrant` for the accepted fields and limits.

## Terminal input

Shift+Enter needs a terminal that sends a distinct modified key event. If it executes instead of inserting a newline, configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`). Bracketed multiline paste also preserves newlines without executing.
