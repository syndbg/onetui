use anyhow::Result;
use onetui_core::catalog::{ACTIONS, CONNECTIONS};
use onetui_core::provider::{Provider, find_provider, validate_catalog};
use onetui_core::value::{DisplayOptions, FORMATS};
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
        "diagnostics": "Native server codes, messages, details and transport causes, including HTTP error bodies. Known selected-connection secrets are redacted and terminal controls escaped. Transport metadata is excluded. Local validation, cancellation and application limits retain local messages. Always enabled; no TOML setting.",
        "shell": {"resources": [&CONNECTIONS], "actions": ACTIONS, "keybindings_configurable": false,
            "data_navigation": "Enter on a data row lists every field/type/value preview across the panel width (25%/25%/50%); Enter on a field opens its full cached value. Complete single-value results open directly. Esc returns one level. PageUp/PageDown scrolls one screen; Ctrl-u/Ctrl-d scrolls half a screen within loaded data or detail. These keys do not fetch data or change value chunks; n/p handles paging/chunks. Filter input updates cached matches on every character/Backspace; Enter keeps, Esc restores the previous filter and selection. No new configuration settings.",
            "action_context": "Help shows currently available actions. Navigation keys do not apply inside the ':' command prompt. T or :themes opens the theme menu: j/k or arrows preview, Enter keeps for this session, Esc or Ctrl-C restores the previous theme without cancelling requests."},
        "configuration": {
            "format": "TOML",
            "persist_query_history": {"type": "boolean", "default": false, "purpose": "Save submitted query text across restarts; query text may contain secrets"},
            "ask_for_query_confirm": {"type": "boolean", "default": true, "purpose": "Ask before submitting a query from the editor or rerunning it"},
            "display": {"type": "table", "required": false, "defaults": DisplayOptions::default(), "formats": FORMATS,
                "fields": {
                    "format": {"type": "string", "enum": FORMATS.iter().map(|f| f.id).collect::<Vec<_>>(), "purpose": "Startup detail format; table previews use auto"},
                    "pretty_print": {"type": "boolean", "purpose": "Two-space JSON indentation in row/detail views; off preserves retained whitespace. Record tables always use compact JSON"},
                    "highlight": {"type": "boolean", "purpose": "Data colors; selection, errors and UI key hints remain visible when off"},
                    "word_wrap": {"type": "boolean", "purpose": "Wrap displayed text; H/L scroll content when off. Single-line editors and structural labels remain clipped"},
                    "unicode": {"type": "string", "enum": ["literal", "escaped"], "purpose": "Keep printable Unicode or show ASCII escapes; terminal controls always escaped"}
                },
                "behavior": "Omitted fields use defaults. Names are case-sensitive; unknown fields, empty/unknown names and wrong types fail validation, including --check. No environment expansion, extra file, CLI flags or file watching. v or :display opens the menu. Format overrides last until detail closes; other switches last for the session, across connections. No config writes.",
                "commands": [":display format auto|text|json|hex|binary (field detail only)", ":display pretty-print on|off", ":display highlight on|off", ":display word-wrap on|off", ":display unicode literal|escaped"]},
            "theme": {"type": "string", "purpose": "Color palette for every TUI screen", "enum": Theme::ALL, "default": Theme::default(), "required": false,
                "behavior": "Case-sensitive startup default; omission uses the default. Empty/unknown names and non-string values fail, including with --check. T or :themes previews all built-in themes in-app; Enter keeps the selection for this session, Esc restores it. The menu never writes configuration. No environment expansion, separate theme file, CLI override or file watching; restart to read configuration changes."},
            "location_order": ["--config <path> (relative paths use the working directory)", "$XDG_CONFIG_HOME/onetui/config.toml (absolute XDG_CONFIG_HOME only)", "$HOME/.config/onetui/config.toml (absolute HOME only)"],
            "behavior": "Read one file, at most 1 MiB; no merge or project search. A missing implicit default opens an empty interactive picker without writing a file. Explicit --config paths and --check require an existing readable file. Malformed/unreadable files are errors. ~/onetui.toml requires --config.",
            "connection_form": "a or :add on the picker selects a built-in provider and its connection_form fields. Tab/Shift-Tab moves, F2/Ctrl-s saves, Esc/Ctrl-c discards. Text is literal, string_list uses comma-separated values, boolean uses true/false; blank omits a field. Total input limit 16 KiB. Save validates through Provider::validate_config, preserves existing text, creates missing parent directories and a private file (0600 on Unix), then returns to the picker without connecting. Duplicate aliases, detected external edits, symlinks and read-only files block saving. Config uses standard TOML table sections for appending entries; inline connections tables must be expanded manually. Nested OAuth/decoder settings remain file-only. Secret fields take environment references, not credentials.",
            "aliases": "Nonempty ASCII letters, digits, underscores or hyphens; no built-in aliases/endpoints",
            "secrets": "Resolve selected connection only; configured environment references must be present, nonblank and Unicode. Paths and URLs are not environment-expanded.",
            "example_toml": format!("theme = {}\n\n[display]\nformat = \"auto\"\npretty_print = true\nhighlight = true\nword_wrap = true\nunicode = \"literal\"\n\n[connections.local_pg]\nkind = \"postgres\"\nurl_env = \"ONETUI_POSTGRES_URL\"\n", serde_json::to_string(&Theme::default())?)
        },
        "timeout": {"flag": "--timeout", "purpose": "Active request deadline, not displayed-data expiry", "default_seconds": 5, "min_seconds": 1, "max_seconds": 300},
        "browsing_limits": {"metadata_page_rows": 100, "row_page_rows": 100, "retained_pages_per_view": 3, "page_bytes": 1048576,
            "page_bookmarks_per_view": 4096, "page_bookmark_token_bytes_per_view": 1048576,
            "previous_page_behavior": "p restores a cached page or explicitly refetches its incoming provider token. Bookmarks are in-memory and scoped to the view/executor. Refetched rows may have changed. Failed/cancelled reads preserve the current page and bookmark. Successful refresh clears history; leaving the connection discards it. At the bookmark count/byte limit, forward paging stops with an error rather than dropping the route back.",
            "postgres_row_text_bytes": 1048576, "postgres_max_columns": 256, "detail_chunk_characters": 4096, "page_filter_utf8_bytes": 256,
            "page_projection_bytes": 1048576, "table_preview_bytes": 1048576, "detail_format_bytes": 1048576, "json_max_depth": 64, "detail_chunk_bytes": 256,
            "notes": "Fixed limits, not TOML settings or an RSS guarantee. PostgreSQL guards row text/binary bytes server-side; page bytes count retained values, labels and continuation. Separate 1 MiB limits cover stable filter/sort projections, table previews and formatted detail. Text chunks preserve graphemes; oversized graphemes use hex. Hex/binary chunks use byte offsets, not character positions. Formatting failures show a reason and retain a safe text/hex view. Table previews use auto format, at most 128 characters plus a truncation marker and three visual lines. No cross-request snapshot or reads on display changes. Page-local filter searches the stable escaped text projection (byte values use hexadecimal with a backslash-x prefix, SQL NULL uses NULL); sorting is lexical, NULL first ascending, stable ties. Display settings never alter matching, ordering or continuation."}
    }))?)
}
