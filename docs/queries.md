# Native queries

Press `e` or type `:query` on a connected datasource to open its query editor. PostgreSQL accepts SQL. Qdrant accepts filtered Scroll JSON for the current collection, or the selected collection in the collection list. `/` remains a filter over the displayed page; it never sends a query.

| Editor key | Purpose |
| --- | --- |
| Ctrl-R or F5 | Execute the draft from page 1 |
| Enter | Insert a newline |
| Arrows, Home/End | Move the cursor within the draft |
| Ctrl-Home/Ctrl-End | Move to the start/end of the draft |
| Backspace/Delete | Remove the previous/next character |
| Tab | Insert four spaces |
| Ctrl-U | Clear the draft |
| Esc or Ctrl-C | Cancel active work and return to the retained view |

The editor accepts bracketed paste and at most 16 KiB of UTF-8 text. It normalizes pasted line endings and tabs and rejects other control characters. While a request runs, editing and execution keys are disabled; Esc/Ctrl-C still cancel. A compact, full-width editor sits above the retained rows, which remain visible during editing, loading and errors. It follows the session's wrapping setting, wrapping long input at grapheme boundaries and scrolling to keep the cursor visible. With wrapping off it scrolls horizontally. Query text uses plain styling; value highlighting applies to results.

Successful requests focus the result table, including one-cell results. The executed query remains visible above it, labeled `executed`; press `e` to edit again. While editing, the table is labeled `retained data`, since its rows do not yet reflect the draft. `Enter` opens row fields or point details; `v` controls value formatting. `n/p` pages through the result, and `r` reruns from the beginning. A query error keeps the draft and the previous results. Esc closes the editor first; from results, Esc returns to the browsing view that opened the query. Enter in the editor never executes SQL: use Ctrl-R if your terminal or keyboard intercepts F5.

Drafts and paging tokens stay in memory. OneTUI does not save them in `onetui.toml`, a history file or application logs; the database may log received queries. Drafts belong to retained views and disappear when those views or the connection session are discarded. Executing an edited query replaces its previous results and clears its filter, sort and bookmarks. Tokens are bound to the executor, resource and exact query text. Bookmark limits also apply to query tokens, which include that text; long queries can reach the 1 MiB token budget before the 4,096-bookmark limit.

## PostgreSQL

Open the local PostgreSQL connection, press `e`, then Ctrl-U and paste:

```sql
SELECT id, name, country, balance
FROM demo.customers
WHERE active AND balance > 0
ORDER BY id
```

Ctrl-R or F5 executes one `SELECT`, `VALUES` or read-only `WITH` statement. An optional semicolon must be the last non-whitespace character; omit it before a trailing comment. Parameters such as `$1`, multiple statements, utility commands such as `EXPLAIN` and writes are unsupported. Result columns come from PostgreSQL, including duplicate names. Values retain server text and exact numeric precision; native `bytea` remains bytes for hex/binary inspection.

Each page runs in a server-enforced read-only transaction. PostgreSQL parses the statement; OneTUI does not decide whether it is safe by checking its first word. The query must fit a derived table. OneTUI bounds its result to 100 rows, guards oversized rows on the server, and rejects pages over 1 MiB or results outside 1..256 columns. Failed or cancelled requests retire the connection. Successful reads roll back and run `DISCARD ALL` to clear session settings and advisory locks before the connection is reused. No transaction or cursor remains open while you inspect results.

Use least-privilege credentials. Read-only SQL is not a sandbox: functions can have external effects, and expensive queries still consume database resources. The existing `--timeout` bounds each request (default 5 seconds; accepted values 1..300 seconds).

Paging reruns the query with an outer LIMIT/OFFSET. Include `ORDER BY` with a unique tie-breaker when page order matters. Pages are independent reads, not a snapshot; changes can shift rows and deep offsets can be slow. Cached pages preserve the data already seen, while evicted bookmarks rerun the query.

## Qdrant

Select `demo_products` in the collection list, press `e`, then Ctrl-U and paste:

```json
{
  "filter": {
    "must": [
      {"key": "active", "match": {"value": true}},
      {"key": "price", "range": {"gte": 1, "lt": 10}}
    ],
    "must_not": [
      {"key": "category", "match": {"value": "books"}}
    ]
  },
  "limit": 100
}
```

The supported JSON is a subset of Qdrant's Scroll request:

| Field | Values and default |
| --- | --- |
| `filter` | Optional object; omitted or null means no filter |
| `filter.must` | Array of field conditions; all must match; defaults to `[]` |
| `filter.should` | Array of field conditions; at least one must match when nonempty; defaults to `[]` |
| `filter.must_not` | Array of field conditions; none may match; defaults to `[]` |
| Condition `key` | Required nonempty payload field path, using Qdrant's field-path syntax |
| Condition `match` | `{"value": ...}` with a string, boolean or signed 64-bit integer |
| Condition `range` | Numeric `gt`, `gte`, `lt`, `lte` bounds; at least one is required |
| `limit` | Optional integer 1..100; omitted or null defaults to 100 |

Each condition needs exactly one of `match` or `range`. Unknown fields are errors, including misspelled filter keys. Nested conditions, match-any/except/text, geo/datetime conditions, custom ordering and vector similarity queries are not supported. Offset and payload/vector selectors are not editor inputs. OneTUI owns native Scroll offsets and leaves payloads/vectors unloaded until you open a point. `{}` browses unfiltered point IDs. The RPC response and retained page each have a 1 MiB limit.

## Provider contract and configuration

[ADR-0005](adr/0005-run-native-queries-through-providers.md) records the provider-owned query decision and rejected alternatives.

`ProviderDescriptor.query` advertises the query language, example, result resource and required path depth. The TUI submits `QueryRequest` through `Executor::query_page`; the built-in enum delegates to the selected connector. Each connector validates its syntax and continuation and returns the existing `Page` type. The worker supplies the same cancellation token, deadline and stale-result checks used by browsing.

This feature adds no configuration keys or CLI flags. Inspect the compiled capabilities and limits with:

```sh
onetui schema --datasource postgres
onetui schema --datasource qdrant
```
