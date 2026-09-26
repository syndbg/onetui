# PostgreSQL

Browse schemas, tables, views and column metadata, inspect typed values, and run SQL queries.

## Configuration

```toml
[connections.pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
```

Set the environment variable to your PostgreSQL connection string, then run `onetui --connection pg`. Use `onetui schema --datasource postgres` for TLS settings and other options.

Browsing needs schema `USAGE` and relation `SELECT`. Grant only the write permissions you intend to use.

## Usage

Open Schemas, choose a table or view, then Enter to browse rows. Press `m` for column metadata or `e` for [SQL queries](#queries). The [shared controls](ui.md) cover paging, filtering and value inspection.

Values preserve PostgreSQL text output, including numeric precision. Native `bytea` values can be inspected as bytes. NULL and empty values remain distinct.

## Queries

Press `e` on a table or view and enter one SQL statement:

```sql
SELECT id, name, country, balance
FROM demo.customers
WHERE active AND balance > 0
ORDER BY id
```

Enter or F5 runs the statement once. Results show up to 100 rows and do not page. PostgreSQL parses the SQL; parameters and multiple statements per submission are not supported. If execution times out, inspect the target before retrying.

A statement may write. Use least-privilege credentials: queries consume database resources and may be logged.

### Editor controls

Enter, F5 or Ctrl-R submits the draft. Shift+Enter inserts a newline, and Ctrl-U clears the draft. Esc returns to browsing; Ctrl-C cancels active work. A new query starts empty, and its watermark is a hint that disappears when you type. Press `e` to edit and submit again; `r` refreshes ordinary resource views.

OneTUI asks before running a query. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Ctrl-P and Ctrl-N browse recent queries submitted on this connection. Shift+H or `:history` while browsing opens them as a list: Enter opens one for editing, Esc closes the list. History stays in memory for the session. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) then holds full query text, including any passwords or tokens; turning the setting off does not delete that file.

Shift+Enter needs a terminal that reports modified keys. OneTUI requests that, so it works wherever the terminal supports it. Where it does not, Shift+Enter is indistinguishable from Enter and submits instead: configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`), or paste multiline text, which preserves newlines without executing. Inside tmux this also needs `set -g extended-keys on`.

## Replication

Open **Replicas** or **WAL receiver** for the connected server's replication statistics. Some fields are NULL without `pg_read_all_stats`. An empty view does not establish that the deployment has no other members.

For a local example, choose `local_pg` or `local_pg_replica` in the [demo fixtures](../hack/README.md).
