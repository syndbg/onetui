# PostgreSQL

Browse schemas, tables, views and column metadata, inspect typed values, and run read-only SQL.

## Configuration

```toml
[connections.pg]
kind = "postgres"
url_env = "ONETUI_POSTGRES_URL"
```

Set the environment variable to your PostgreSQL connection string, then run `onetui --connection pg`. Use `onetui schema --datasource postgres` for TLS settings and other options.

Use credentials with schema `USAGE` and relation `SELECT` permissions. Column-only grants may allow metadata inspection but prevent browsing all fields.

## Usage

Open Schemas, choose a table or view, then Enter to browse rows. Press `m` for column metadata or `e` for [SQL queries](queries.md#postgresql). The [shared controls](ui.md) cover paging, filtering and value inspection.

Values preserve PostgreSQL text output, including numeric precision. Native `bytea` values can be inspected as bytes. NULL and empty values remain distinct.

## Replication

Open **Replicas** or **WAL receiver** for the connected server's replication statistics. Some fields are NULL without `pg_read_all_stats`. An empty view does not establish that the deployment has no other members.

For a local example, choose `local_pg` or `local_pg_replica` in the [demo fixtures](../hack/README.md).
