mod browse;
mod check;
mod config;
mod provider;
mod query;
mod replication;
mod rows;
use onetui_core::catalog::{Action, ActionSource, ResourceAction, ResourceDescriptor};
pub use provider::{PostgresExecutor, PostgresProvider};

pub const SCHEMAS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.schemas",
    description: "Schemas with USAGE permission, including system schemas",
    columns: &["schema"],
    paging: true,
    actions: &[],
};
pub const RELATIONS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.relations",
    description: "Tables, partitioned tables, views, materialized views and foreign tables in a schema",
    columns: &["relation", "kind", "select permission"],
    paging: true,
    actions: &[ResourceAction {
        id: Action::Columns,
        target: "postgres.columns",
        source: ActionSource::SelectedTarget,
    }],
};
pub const COLUMNS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.columns",
    description: "Column metadata; names and PostgreSQL types are discovered at runtime, not row values",
    columns: &["column", "type", "column NOT NULL"],
    paging: true,
    actions: &[],
};

pub const ROWS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.rows",
    description: "Rows; column names and PostgreSQL types discovered at runtime",
    columns: &[],
    paging: true,
    actions: &[ResourceAction {
        id: Action::Columns,
        target: "postgres.columns",
        source: ActionSource::Current,
    }],
};

pub(crate) fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "id": "postgres", "operations": ["check", "metadata_browse", "row_browse", "query_page", "execute_query"],
        "query_syntax": "Enter/F5: one server-parsed SQL statement executed once, with up to 100 returned rows or an affected-row count. No parameters or multiple statements. The 1 MiB display limit applies.",
        "resources": [&SCHEMAS, &RELATIONS, &COLUMNS, &ROWS],
        "row_paging": "100 rows; non-null unique default-B-tree bigint/text keysets (all composite components), otherwise best-effort OFFSET. No cross-page snapshot.",
        "replication": {"resources": ["postgres.replication", "postgres.wal_receiver"], "path": [], "columns": "All native pg_stat_replication / pg_stat_wal_receiver columns for the connected server version", "permissions": "PostgreSQL may return NULL for restricted fields without pg_read_all_stats; preserve those NULLs. Server obfuscates sensitive conninfo fields.", "limits": "100 rows per page, 1 MiB; independent reads, not an HA membership inventory. No changes to replication slots or configuration."},
        "session": "Lazy reusable connection; no idle transaction/cursor. Completed queries roll back and clear session settings before reuse, including after server rejection. Failed paged reads do the same. Cancellation, transport failure or failed cleanup retires the connection. Shutdown/cancel cleanup: 1 second. Native TCP keepalive enabled with 7200-second idle threshold and OS interval/retry defaults; no SQL heartbeat or heartbeat TOML setting.",
        "configuration": {
            "kind": {"required": true, "values": ["postgres"], "purpose": "Select the PostgreSQL connector"},
            "url_env": {"required": true, "type": "string", "purpose": "Environment variable containing the DSN; explicit host required", "values": "Nonempty ASCII letters, digits, underscores or hyphens", "example": "ONETUI_POSTGRES_URL"},
            "ca_file": {"required": false, "type": "absolute path to a regular PEM certificate file", "max_bytes": crate::check::CA_BYTES, "default": "native trust roots", "purpose": "Replace the PostgreSQL trust store; incompatible with sslmode=disable. Reject empty/invalid files, directories, FIFOs, devices and oversized files; symlinks must resolve to regular files."}
        },
        "tls": "sslmode=prefer (default) is promoted to require with certificate/hostname verification; disable is local-only"
    })
}
