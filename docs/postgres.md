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

Use the [shared editor controls](ui.md#query-editor) for confirmation, cancellation and history.

## Replication

Open **Replicas** or **WAL receiver** for the connected server's replication statistics. Some fields are NULL without `pg_read_all_stats`. An empty view does not establish that the deployment has no other members.

For a local example, choose `local_pg` or `local_pg_replica` in the [demo fixtures](../hack/README.md).
