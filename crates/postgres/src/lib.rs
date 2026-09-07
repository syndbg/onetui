mod browse;
mod check;
mod rows;
pub use browse::fetch;
pub use check::check;
use onetui_core::catalog::ResourceDescriptor;

pub const SCHEMAS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.schemas",
    description: "Schemas with USAGE permission, including system schemas",
    columns: &["schema"],
};
pub const RELATIONS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.relations",
    description: "Tables, partitioned tables, views, materialized views and foreign tables in a schema",
    columns: &["relation", "kind", "select permission"],
};
pub const COLUMNS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.columns",
    description: "Column metadata; names and PostgreSQL types are discovered at runtime, not row values",
    columns: &["column", "type", "column NOT NULL"],
};

pub fn descriptor(id: &str) -> Option<&'static ResourceDescriptor> {
    [&SCHEMAS, &RELATIONS, &COLUMNS, &ROWS]
        .into_iter()
        .find(|entry| entry.id == id)
}

pub const ROWS: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.rows",
    description: "Read-only rows; column names and PostgreSQL types discovered at runtime",
    columns: &[],
};

pub fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "id": "postgres", "operations": ["check", "metadata_browse", "row_browse"],
        "resources": [&SCHEMAS, &RELATIONS, &COLUMNS, &ROWS],
        "row_paging": "100 rows; non-null unique default-B-tree bigint/text keysets (all composite components), otherwise best-effort OFFSET. No cross-page snapshot.",
        "configuration": {
            "kind": {"required": true, "values": ["postgres"], "purpose": "Select the PostgreSQL connector"},
            "url_env": {"required": true, "type": "string", "purpose": "Environment variable containing the DSN; explicit host required", "values": "Nonempty ASCII letters, digits, underscores or hyphens", "example": "ONETUI_POSTGRES_URL"},
            "ca_file": {"required": false, "type": "absolute path to a PEM certificate file", "default": "native trust roots", "purpose": "Replace the PostgreSQL trust store; incompatible with sslmode=disable"}
        },
        "tls": "sslmode=prefer (default) is promoted to require with certificate/hostname verification; disable is local-only"
    })
}
