mod config;
mod provider;
mod value;

use onetui_core::catalog::{Action, ActionSource, ResourceAction, ResourceDescriptor};
pub use provider::{CqlExecutor, CqlProvider};

const fn descriptor(
    id: &'static str,
    description: &'static str,
    columns: &'static [&'static str],
    actions: &'static [ResourceAction],
) -> ResourceDescriptor {
    ResourceDescriptor {
        id,
        description,
        columns,
        paging: true,
        actions,
    }
}

pub(crate) const RESOURCES: &[&ResourceDescriptor] = &[
    &descriptor(
        "cql.keyspaces",
        "Keyspaces, including system keyspaces",
        &["keyspace", "replication", "durable_writes"],
        &[],
    ),
    &descriptor(
        "cql.tables",
        "Tables in a keyspace",
        &["table", "comment"],
        &[ResourceAction {
            id: Action::Columns,
            target: "cql.columns",
            source: ActionSource::SelectedTarget,
        }],
    ),
    &descriptor(
        "cql.columns",
        "Column metadata: kind, clustering position and CQL type",
        &["column", "kind", "position", "type"],
        &[],
    ),
    &descriptor(
        "cql.rows",
        "Rows; column names and CQL types discovered at runtime",
        &[],
        &[ResourceAction {
            id: Action::Columns,
            target: "cql.columns",
            source: ActionSource::Current,
        }],
    ),
    &descriptor(
        "cql.query",
        "CQL statement rows, or the outcome of a statement that returns none",
        &[],
        &[],
    ),
];

/// CQL words beyond the shared SQL set, colored as keywords in the editor. `key` and
/// `type` are left out: they are common column names, and coloring those misleads.
pub(crate) const CQL_KEYWORDS: &[&str] = &[
    "ALLOW",
    "APPLY",
    "BATCH",
    "CONTAINS",
    "COUNTER",
    "FILTERING",
    "FROZEN",
    "KEYSPACE",
    "KEYSPACES",
    "LIST",
    "LOGGED",
    "MAP",
    "MATERIALIZED",
    "PERMISSIONS",
    "ROLE",
    "ROLES",
    "STATIC",
    "TIMEUUID",
    "TOKEN",
    "TTL",
    "TUPLE",
    "UNLOGGED",
    "USE",
    "WRITETIME",
];

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "configuration": {
            "kind": {"required": true, "values": ["cql"], "purpose": "Apache Cassandra or ScyllaDB over the CQL native protocol"},
            "nodes": {"required": true, "type": "1..32 host:port contact points", "purpose": "Initial contact points; the driver discovers the rest of the cluster from them", "example": ["127.0.0.1:9042"]},
            "username_env": {"default": null, "purpose": "Environment variable containing the username; paired with password_env"},
            "password_env": {"default": null, "purpose": "Environment variable containing the password; requires tls = true except on loopback contact points"},
            "tls": {"default": false, "purpose": "Verify node certificates with rustls; certificates must name the addresses the driver connects to"},
            "ca_file": {"default": null, "purpose": "Absolute path to PEM trust roots replacing native trust; requires tls = true"},
            "keyspace": {"default": null, "purpose": "Default keyspace for unqualified names in the editor, used case-sensitively; the driver reports a name it cannot use when connecting"}
        },
        "paths": {
            "cql.keyspaces": [], "cql.tables": ["keyspace"],
            "cql.columns": ["keyspace", "table"], "cql.rows": ["keyspace", "table"],
            "cql.query": []
        },
        "query_syntax": {
            "format": "One CQL statement, sent as written. The server prepares it first, so a statement it cannot parse is rejected before anything runs.",
            "result": "A statement that returns rows shows them in native pages of 100. Any other statement reports applied, rejected with the server's error, or unknown when the response was lost after dispatch.",
            "safety": "No automatic retries. Refresh is disabled on results; returning to the first page reruns the statement, which only returns rows for reads and lightweight transaction results."
        },
        "paging": "Native CQL paging state, bound to the session, resource and statement. Each page is read when requested, not from a snapshot.",
        "limits": {"page_rows": 100, "page_bytes": onetui_core::PAGE_BYTES},
        "values": "Scalars as text; varint and decimal exact; timestamp, date and time in UTC like cqlsh; blob as bytes; collections, tuples and user-defined types as JSON with maps as key/value pairs.",
        "session": "Lazy driver session with per-node pools; the driver reconnects nodes itself. Metadata comes from system_schema (Cassandra 3+ and ScyllaDB).",
        "permissions": "SELECT on system_schema for browsing and on each table for rows. Editor statements need the role's native permissions; server errors are shown as returned."
    })
}
