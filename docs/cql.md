# ScyllaDB and Apache Cassandra

Browse keyspaces, tables, column metadata and rows, and run CQL statements. One `cql` connection kind serves both databases over the CQL native protocol.

## Configuration

```toml
[connections.cluster]
kind = "cql"
nodes = ["db1.example.com:9042", "db2.example.com:9042"]
username = "user" # or username_env = "CQL_USERNAME"
password = "..." # or password_env = "CQL_PASSWORD"
tls = true
```

`nodes` are contact points; the driver discovers the rest of the cluster from them and connects to each node at the address and port it advertises. Credentials require `tls = true` except on loopback contact points. TLS verifies certificates against the addresses the driver connects to. Use `onetui schema --datasource cql` for settings, including `ca_file` and a default `keyspace`.

Use `username_env` or `password_env` instead to read either value from an environment variable. Protect the config file if it contains a password.

Browsing needs `SELECT` on `system_schema` and on each table you open.

## Browse

Keyspaces open to their tables, and tables to their rows. Press the columns action on a table for column kinds, clustering positions and CQL types. Rows arrive in native pages of 100, each read when requested.

Values are shown exactly: `varint` and `decimal` keep every digit, `timestamp`, `date` and `time` render in UTC like cqlsh, `blob` stays bytes, and collections, tuples and user-defined types render as JSON. Maps are lists of `{"key", "value"}` pairs because CQL keys need not be strings.

Try `local_scylla` and `local_cassandra` in the [demo fixtures](../hack/README.md).

## Queries

Press `e` and enter one CQL statement:

```sql
SELECT * FROM "onetui_demo"."events" WHERE bucket = 0
```

The server prepares the statement first, so one it cannot parse is rejected before anything runs. A statement that returns rows shows them in native pages. Any other statement reports applied, rejected with the server's error, or unknown. Unknown covers a response lost after the statement may have reached a node, and coordinator timeouts or replica failures, where some replicas may already hold the write. Inspect the target before retrying: a counter or list append applies twice. There are no automatic retries.

Returning to the first page reruns the statement. Only reads and lightweight transaction results return rows, so this never repeats a plain write.

Use the [shared editor controls](ui.md#query-editor) for confirmation, cancellation and history.
