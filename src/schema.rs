use anyhow::Result;
use onetui_core::catalog::{ACTIONS, CONNECTIONS};
use onetui_core::provider::{Provider, find_provider, validate_catalog};
use onetui_theme::Theme;

pub fn dump<P: Provider>(catalog: &[P], datasource: Option<&str>) -> Result<String> {
    validate_catalog(catalog)?;
    if let Some(kind) = datasource {
        find_provider(catalog, kind)?;
    }
    let datasources: Vec<_> = catalog
        .iter()
        .filter(|provider| datasource.is_none_or(|kind| provider.descriptor().kind == kind))
        .map(|provider| provider.descriptor().capabilities())
        .collect();
    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "schema_format_version": 1,
        "purpose": "Offline implemented-capability catalog, not a live database schema or permission guarantee",
        "datasources": datasources,
        "shell": {"resources": [&CONNECTIONS], "actions": ACTIONS, "keybindings_configurable": false,
            "action_context": "Help shows currently available actions. Navigation keys do not apply inside the ':' command prompt."},
        "configuration": {
            "format": "TOML; top-level theme string and connections table. Every entry is validated, including unselected aliases; unknown themes/kinds/fields, invalid reference names, relative PostgreSQL CA paths and invalid Qdrant URLs are rejected.",
            "theme": {"type": "string", "purpose": "Color palette for every TUI screen", "enum": Theme::ALL, "default": Theme::default(), "required": false,
                "behavior": "Case-sensitive built-in name; omission uses the default. Empty/unknown names and non-string values fail, including with --check. No environment expansion, separate theme file, CLI override or live reload; restart to apply."},
            "location_order": ["--config <path> (relative paths use the working directory)", "$XDG_CONFIG_HOME/onetui/config.toml (absolute XDG_CONFIG_HOME only)", "$HOME/.config/onetui/config.toml (absolute HOME only)"],
            "behavior": "Read exactly one file; no merge, creation, project search or fallback on missing file. ~/onetui.toml requires --config.",
            "aliases": "Nonempty ASCII letters, digits, underscores or hyphens; no built-in aliases/endpoints",
            "secrets": "Resolve selected connection only; configured environment references must be present, nonblank and Unicode. Paths and URLs are not environment-expanded.",
            "example_toml": format!("theme = {}\n\n[connections.local_pg]\nkind = \"postgres\"\nurl_env = \"ONETUI_POSTGRES_URL\"\n", serde_json::to_string(&Theme::default())?)
        },
        "timeout": {"flag": "--timeout", "purpose": "Active request deadline, not displayed-data expiry", "default_seconds": 5, "min_seconds": 1, "max_seconds": 300},
        "browsing_limits": {"metadata_page_rows": 100, "row_page_rows": 100, "retained_pages_per_view": 3, "page_bytes": 1048576,
            "postgres_row_text_bytes": 1048576, "postgres_max_columns": 256, "detail_chunk_characters": 4096, "page_filter_utf8_bytes": 256,
            "notes": "Fixed limits, not TOML settings or an RSS guarantee. Row text is guarded server-side; page bytes include escaped values, column labels and continuation. No cross-request snapshot. Detail reads cached text; table previews show up to 128 characters. Page-local filter searches all cached fields with a case-sensitive literal substring (SQL NULL is searched as NULL); empty clears. Sort cycles lexical ascending/descending/source order on the selected field, NULL first ascending, stable ties. Neither changes native continuation."}
    }))?)
}
