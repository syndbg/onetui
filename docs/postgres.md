# PostgreSQL browsing

PostgreSQL browsing supports row and metadata navigation, keyset/offset paging, cached field/type detail, page-local filter/sort and the offline catalog.

## Usage

```sh
make dev-up
make dev-run
# Or, with your own configured connection and its secret environment variable:
onetui --config "$HOME/onetui.toml" --connection local_pg
```

`make dev-run` builds and supplies fake fixture credentials; it does not start, restart or delete databases. Select `local_pg` in the connection picker, then **Schemas**. Interactive stdin/stdout are required. Browse schemas → tables/views → rows → all row fields → field detail. Enter on a relation opens rows; `m` opens column metadata. Enter on a row lists every field with its type and value preview. Select a field with `j/k`, then Enter for its full value. Esc returns one level without another read.

Schemas require USAGE. Relations include tables, partitioned tables, views, materialized views and foreign tables; SELECT permission is shown separately. Row browsing selects all visible columns, so partial column-only grants may allow metadata but deny rows. Column inspection accepts table-level or some column-level SELECT permission. `column NOT NULL` reports the column declaration, not domain/check constraints.

| Key | Purpose |
| --- | --- |
| `j/k`, Up/Down | Select row; scroll text inside detail |
| PageUp/PageDown | Scroll one screen within loaded rows, row fields or detail |
| Ctrl-U/Ctrl-D | Scroll half a screen; never fetch another datasource page |
| `h/l`, Left/Right | Select field; table shows a window of four columns |
| `Enter`, `Esc` | Open selected resource/detail; close help/detail or return to retained parent |
| `m` | Column metadata for selected relation or current row view |
| `/` | Filter as you type; Enter keeps, empty clears, Esc restores the previous filter and selection |
| `s` | Cycle selected-field local sort: ascending → descending → source order |
| `n`, `p` | Fetch next page / restore or refetch the previous page from its bookmark; next/previous text chunk inside detail |
| `r`, `c` | Refresh from page one; return to configured connections |
| `?`, `:` | Contextual help; command prompt |
| `q` | Quit and restore terminal modes/cursor |
| `Ctrl-c` | Cancel active work; quit when idle |

Table cells preview up to 128 Unicode characters, adding an ellipsis for longer values; terminal width may clip them further. Detail reads the cached value without another query, in complete 4,096-character chunks: `n/p` changes chunk, `j/k` scrolls, `h/l` changes field. Its header distinguishes SQL NULL, empty text and non-null text (including literal `NULL`). PostgreSQL text output preserves decimal precision and supports bytea, arrays, enums, domains and other text-convertible types; this is not a binary export format.

The command/filter bar appears below context only while typing; context shows the current mode's controls. Press `:` to enter action IDs shown by help/catalog, such as `filter`, `sort`, `columns`, `connections`, `refresh`, `help` and `quit`. Enter executes, Esc closes, Backspace edits. Text entry consumes navigation keys; commands use lowercase ASCII letters and are limited to 32 characters. Keybindings are fixed. [Qdrant browsing](qdrant.md) uses the same shell.

## Replication

The resource menu also offers **Replicas** (`pg_stat_replication`) and **WAL receiver** (`pg_stat_wal_receiver`). Both retain all columns returned by the connected PostgreSQL version, including positions, lag and connection state. PostgreSQL hides restricted fields as NULL without `pg_read_all_stats`; OneTUI preserves those NULLs. An empty view means no visible replication process in that direction, not that an HA deployment has no other members.

These are bounded SELECT queries with the same page, value, cancellation and transaction limits as SQL browsing. They do not create replication slots, promote a standby or change replication settings. The server masks sensitive values in WAL receiver `conninfo`. See [PostgreSQL statistics views](https://www.postgresql.org/docs/16/monitoring-stats.html).

For the local demo, open **Replicas** on `local_pg` or **WAL receiver** on `local_pg_replica`. The fixture reader has `pg_read_all_stats`, not replication privileges. No extra application setting is required.

## Page-local filter and sort

For a server-side predicate or projection, use `e` or `:query` to edit read-only SQL, then F5 to execute it. Query results have the same table and field viewers. See [native queries](queries.md#postgresql) for examples, supported statements and execution limits.

Use `/` and type `paid`: matching rows update with every keystroke. Enter keeps the filter and closes input; Esc restores the previous filter and selection. Matching is a case-sensitive literal substring, not regex, SQL, fuzzy matching or a server-side filter. It searches every cached field, including fields outside the four-column window and text beyond the 128-character table preview. SQL NULL is searched as `NULL`, so that query also matches literal text containing `NULL`. Search escaped controls by their visible spelling, such as `\n`. The default filter is empty (all rows); input is limited to 256 UTF-8 bytes, additional characters beyond the limit are ignored, and Backspace removes one Unicode character. Erasing the text immediately shows all loaded rows. Navigation letters are ordinary text inside the prompt.

Select a field with `h/l`, then press `s`: ascending, descending, then original fetched order. Sorting is lexical on cached server text, without numeric conversion or locale collation (`10` precedes `2` ascending). SQL NULL comes before non-null values ascending and after them descending; empty text stays distinct. Equal values retain their fetched order. Selecting another field and pressing `s` starts ascending for that field. Filter/sort are unavailable inside field detail; return to the table first. Help/catalog expose both actions.

Filter/sort only rebuild a local list of row indices. Native row order and continuation never change. Selection stays on the same cached row when still visible, otherwise on the first match. Zero matches disables open/detail but does not disable a backend next page. The table title shows applied filter text and visible/loaded counts; column arrows show local sort direction. Neither keeps an input bar open. Long input scrolls to keep the edited end visible. Changes never fetch, retry or advance the backend token.

Local settings survive next/retained-previous pages and refresh, and parent views restore their own settings on return. Each newly opened resource starts unfiltered in source order; `c` resets the connection picker. Refresh still resets backend paging. A renamed/removed sort field clears sorting on a newly fetched page. No new TOML settings.

## Paging and limits

- Pages contain at most 100 items, with one lookahead row to determine next-page availability. No automatic total count.
- Keyset paging requires a valid, ready, immediate, non-partial, non-expression unique B-tree index whose key columns are all NOT NULL bigint or text, use default operator classes and match column collations. Every composite component is bound in its native type; numeric values are never routed through floating point. Primary keys are preferred. Ordinary inheritance parents are excluded because local uniqueness does not cover inherited rows; eligible partitioned-table indexes are allowed.
- Other relations use LIMIT/OFFSET without a guaranteed unique order. The status explicitly says best-effort; deep OFFSET may be slow. Views, nullable/partial/expression/unsupported-type keys commonly take this path. Neither mode provides a cross-page snapshot: concurrent changes can affect later reads. Refresh starts over.
- Continuation is connector-owned, kept only in memory, and never derived from escaped display values. Catalog changes to column names/types/collations or the selected index invalidate continuation and require refresh. Previous-page navigation restores cached rows or refetches an evicted page using its incoming token; it does not reverse a database cursor or recover a snapshot.
- Row browsing supports 1-256 columns. Zero-column or wider relations get an explicit error; column metadata remains available.
- PostgreSQL guards total projected text and binary bytes per row at 1 MiB before sending fields (therefore each field is also bounded). Native `bytea` retains bytes; other types retain server-text output. A separate oversize flag prevents omitted data from appearing as SQL NULL. Only the requested 101 rows are materialized by the page CTE, not the entire relation. Query planning, sorting, text conversion and server-side work are not bounded by the display budget.
- All fetched pages have a separate 1 MiB retained-value budget, including column labels and continuation. At most three pages are retained per view. Stable filter/sort projections, table previews and formatted detail each have separate 1 MiB limits; [display controls](ui.md#value-display-controls) describe chunking and fallback behavior. Metadata size checking occurs after decoding. These limits are fixed, not TOML settings, total wire-byte caps or RSS guarantees.
- Oversized rows/pages fail explicitly and preserve current data, offset and continuation. No skipped rows, automatic retries or limit increases. An oversized lookahead row does not discard the preceding valid page; the next explicit request fails at that row.

The three-page cache does not limit how far `p` can return. Each view also retains up to 4,096 previous-page bookmarks and 1 MiB of bookmark token text. Reaching either limit stops forward paging with an error and preserves the route back. Successful refresh clears history; leaving the connection discards it. See [bookmark navigation](ui.md#navigation) for usage and refetch behavior.

## Request and terminal lifecycle

One worker owns the selected connection's executor. It connects lazily and reuses a healthy PostgreSQL connection across successful row/metadata reads. Each row request uses a short read-only transaction; a relation lock protects catalog shape during that request, and COMMIT completes before display. The connection stays open while reading, but no transaction or cursor does. There is no idle-data expiry. Restricted server credentials remain the authorization boundary; views/functions and foreign-table reads can still perform server-side work.

`--timeout` defaults to 5 seconds; accepted integers are 1-300. The request budget starts when queued and includes connection establishment, not idle reading time. Cancellation, timeout or a failed PostgreSQL read retires the transport, with a separate one-second cancellation/cleanup budget. Cached pages survive; the next explicit read may connect again, without replaying the failed operation. Continuations are tied to the executor, resource and native query shape, not to a particular physical connection.

A single worker and one replaceable pending request bound concurrent work. Navigation/refresh/cancellation invalidate request IDs immediately; late successes and errors are ignored. Alias changes also invalidate status updates, even when reopening the same alias. Returning to the connection picker or quitting closes the executor; the TUI waits at most two seconds before aborting its worker. PostgreSQL driver ownership ensures an aborted worker cannot leave a detached driver task. Parents retain selection and bounded history. No background polling or implicit fetch on return.

The header shows the last observed transport state: Configured, Connecting, Connected, Disconnected, Closing or Closed. It does not prove permissions or freshness. PostgreSQL keeps the native TCP keepalive default enabled, with a 7,200-second idle threshold and OS interval/retry defaults; Unix sockets have no TCP probes. No recurring SQL health query runs. Qdrant checks reuse a bounded native gRPC channel; its HTTP/2 keepalive interval remains unset and idle pings are disabled. Neither provider adds a heartbeat timer or TOML setting. Servers may still close idle connections.

PostgreSQL trust loading uses Tokio's blocking pool, with one process-wide slot. The request deadline includes waiting for that slot and loading certificates. Cancellation stops the async wait, but a blocked OS read may retain the slot until it finishes; retries remain bounded and cannot launch more blocked loads. The CLI exits after session cleanup without waiting indefinitely for that OS operation. Explicit `ca_file` paths must resolve to regular PEM files of at most 1 MiB; see [configuration](../README.md#configuration).

Terminal controls and bidirectional overrides are escaped before storing display text; ordinary Unicode remains intact. The native continuation retains unescaped key data separately. Connection aliases, not DSNs, identify active connections. Normal quit, errors and panic restore terminal state. Worker panics terminate the UI because the global panic hook has already restored its terminal.

## Configuration and capability output

Browsing uses the existing connection settings. See [configuration](../README.md#configuration) for file selection, required fields, defaults, accepted values and examples.

`onetui schema` is an offline, versioned JSON capability/configuration catalog. `--datasource postgres` or `--datasource qdrant` filters backend entries. It never loads configuration, resolves secrets, initializes a terminal or contacts a database. It ignores an explicit preceding `--config`, and rejects combination with `--check` or `--connection`. It is not a live database schema or permission guarantee. `postgres.rows` has runtime column names/types; its descriptor therefore has no static columns. Qdrant advertises its checks and seven browsing resources. Custom keys, user-defined columns and schema-derived configuration validation remain future work.
