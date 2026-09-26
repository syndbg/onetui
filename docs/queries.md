# Native queries

Press `e` or enter `:query` on a supported resource. Enter, F5 or Ctrl-R submits the draft; Shift+Enter inserts a newline. Ctrl-U clears it. Esc returns to browsing, and Ctrl-C cancels active work.
A new query starts empty. Its watermark is a hint that disappears when you type.
OneTUI asks before running a query by default. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Press `e` to edit and submit a query again. Use `n/p` where results support paging. `r` refreshes ordinary resource views.
In the query editor, Ctrl-P and Ctrl-N browse recent queries submitted on this connection.
Press Shift+H or enter `:history` while browsing to choose one from a list. Enter opens it for editing; Esc closes the list.
History stays in memory for the current session by default. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) contains full query text, including any passwords or tokens. Turning the setting off does not delete that file.

| Datasource | Query |
| --- | --- |
| [PostgreSQL](#postgresql) | SQL queries |
| [Qdrant](#qdrant) | HTTP requests |
| [Kafka](kafka.md#replay-from-an-offset-or-timestamp) | Partition offset or timestamp replay |
| [NATS](nats.md#replay) | Stream subject, sequence or time replay |
| [DynamoDB](dynamodb.md#queries) | Native reads and PartiQL statements |

## PostgreSQL

```sql
SELECT id, name, country, balance
FROM demo.customers
WHERE active AND balance > 0
ORDER BY id
```

Enter or F5 runs one SQL statement. Results show up to 100 rows and do not page. PostgreSQL checks the SQL; parameters and multiple statements are not supported. If execution times out, inspect the target before retrying.

Use least-privilege credentials. Queries consume database resources and may be logged.

## Qdrant

Configure `rest_url`, then put `METHOD /path` on the first line. Leave a blank line before an optional body:

```http
POST /collections/demo_products/points/scroll

{"limit": 10}
```

OneTUI sends the body unchanged and shows the HTTP status and response body, including Qdrant errors. Requests can write data. If a write times out, inspect the target before retrying.

## Terminal input

Shift+Enter needs a terminal that sends a distinct modified key event. If it executes instead of inserting a newline, configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`). Bracketed multiline paste also preserves newlines without executing.
