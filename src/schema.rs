use anyhow::{Result, ensure};
use onetui_core::catalog::{ACTIONS, CONNECTIONS};

pub fn dump(datasource: Option<&str>) -> Result<String> {
    ensure!(
        datasource.is_none_or(|kind| matches!(kind, "postgres" | "qdrant")),
        "unknown datasource; expected postgres or qdrant"
    );
    let postgres = onetui_postgres::capabilities();
    let qdrant = onetui_qdrant::capabilities();
    let datasources: Vec<_> = [postgres, qdrant]
        .into_iter()
        .filter(|entry| datasource.is_none_or(|kind| entry["id"] == kind))
        .collect();
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "schema_format_version": 1,
        "purpose": "Offline implemented-capability catalog, not a live database schema or permission guarantee",
        "datasources": datasources,
        "shell": {"resources": [&CONNECTIONS], "actions": ACTIONS, "keybindings_configurable": false,
            "action_context": "Help shows currently available actions. Navigation keys do not apply inside the ':' command prompt. Qdrant browsing is not implemented."},
        "configuration": {
            "format": "TOML; only connections is a supported top-level table; unknown fields are rejected",
            "location_order": ["--config <path> (relative paths use the working directory)", "$XDG_CONFIG_HOME/onetui/config.toml (absolute XDG_CONFIG_HOME only)", "$HOME/.config/onetui/config.toml (absolute HOME only)"],
            "behavior": "Read exactly one file; no merge, creation, project search or fallback on missing file. ~/onetui.toml requires --config.",
            "aliases": "Nonempty ASCII letters, digits, underscores or hyphens; no built-in aliases/endpoints",
            "secrets": "Resolve selected connection only; configured environment references must be present, nonblank and Unicode. Paths and URLs are not environment-expanded.",
            "example_toml": "[connections.local_pg]\nkind = \"postgres\"\nurl_env = \"ONETUI_POSTGRES_URL\"\n"
        },
        "timeout": {"flag": "--timeout", "purpose": "Active request deadline, not displayed-data expiry", "default_seconds": 5, "min_seconds": 1, "max_seconds": 300},
        "browsing_limits": {"metadata_page_rows": 100, "row_page_rows": 100, "retained_pages_per_view": 3, "page_bytes": 1048576,
            "postgres_row_text_bytes": 1048576, "postgres_max_columns": 256, "detail_chunk_characters": 4096,
            "notes": "Fixed limits, not TOML settings or an RSS guarantee. Row text is guarded server-side; page bytes include escaped values, column labels and continuation. No cross-request snapshot. Detail reads cached text; table previews show up to 128 characters. Page-local filter/sort not implemented."}
    }))?)
}
