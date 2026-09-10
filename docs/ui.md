# UI panels

OneTUI has one context header, an input bar that appears only while typing, a content panel and a footer. There is no separate branding or connection strip above context.

| Panel | Contents | Behavior |
| --- | --- | --- |
| Context | `read-only` in the title; connection alias, datasource, resource, path, loaded/shown counts and transport state | The alias appears here once. Counts describe cached data, not database totals. Connected means transport state, not automatic data refresh. |
| Context actions | Colored keys beside their descriptions | Browsing hints come from the action catalog. `n/p` stay visible at page/chunk boundaries, and loading does not rearrange the shortcuts. Unavailable keys are muted and remain inactive. Command/filter entry and theme selection replace browsing hints with their own controls. |
| Input | `:` command or `/` filter text inside a plain border | Visible only while typing. Filters update live; Enter keeps a filter or executes a command. Esc restores/discards. Long input scrolls to show its end. Closing it returns the space to the content panel. |
| Content | Connection picker, resource table, field detail, query editor, help or theme picker | Tables show loaded/shown counts and any applied filter in the title. Column arrows indicate page-local lexical sort. Selection and errors use text as well as color. |
| Footer | Status/error on the first line; paging and read-scope information on the second; version at bottom-right | The version has reserved space and does not overwrite status or paging text. |

## Layout sketch

Wide-terminal schematic; spacing and action placement are condensed. This is browsing mode, so no input bar is present.

```text
+ Context | read-only -------------------------------------------------------+
| Connection local_qdrant       T      themes       Enter  open              |
| Datasource qdrant             /      filter       c      connections       |
| Resource   qdrant.collections s      sort         r      refresh           |
| Path       /                 j/k    move         ?      help               |
| Loaded     5 items | 5 shown                     q      quit               |
| Transport  Connected                                                       |
+----------------------------------------------------------------------------+
+ qdrant.collections [5 shown / 5 loaded] -----------------------------------+
|   name                                                                     |
| > demo_products                                                            |
|   demo_documents                                                           |
|   demo_vectors                                                             |
|   demo_payload_cases                                                       |
|   demo_empty                                                               |
|                                                                            |
+----------------------------------------------------------------------------+
Ready
Page 1 | 5 items | next: false | read-scope information                 v0.1.0
```

While typing, this plain bar sits between context and content. Context replaces browsing actions with the input controls; closing input gives the space back to content.

```text
+----------------------------------------------------------------------------+
|:refresh                                                                    |
+----------------------------------------------------------------------------+
```

The same bar shows `/4b` while editing a filter. Help, field detail and the theme picker replace the content panel, not the header or footer.

`e` or `:query` opens a compact multiline editor above the retained rows. Context shows Enter/F5 to execute, Shift+Enter for a newline, Ctrl-U to clear and Esc to return to rows. Ctrl-R also executes. The editor takes roughly one third of the content height, bounded to 3..8 rows including borders when space permits. Short terminals use a single input line or omit the query panel when fewer than four content rows remain. Its text scrolls to keep the cursor visible. The command/filter bar stays hidden. [Terminal requirements](queries.md) explain how Shift+Enter is distinguished from Enter.

After execution, the table has keyboard focus and the executed query stays above it. Press `e` to edit again. Help, menus and row/value detail use the full content area. PostgreSQL uses SQL; Qdrant uses filtered Scroll JSON scoped to a collection. [Native queries](queries.md) documents syntax, paging, draft lifetime and safety limits.

```text
+ Context | read-only -------------------------------------------------------+
| Connection local_pg                   Enter/F5    execute read-only query  |
| ...                                  Shift-Enter new line                 |
+----------------------------------------------------------------------------+
+ SQL query | Enter/F5 run | Esc rows ---------------------------------------+
| SELECT id, name FROM demo.customers ORDER BY id                            |
+----------------------------------------------------------------------------+
+ postgres.query [100 shown / 100 loaded] | retained data -------------------+
|   id                 name                                                  |
| > 1                  Mina 1                                                |
|   2                  Jose 2                                                |
| ...                                                                        |
+----------------------------------------------------------------------------+
Ready                                                                 v0.1.0
```

## Navigation

Resources that support live following advertise `f` / `:follow` in context and help. Kafka exposes it on a selected partition's record view; NATS exposes it on a JetStream stream's message view. The footer shows `LIVE`, retained rows and local eviction count instead of historical page numbers. `f` or Ctrl-C stops and retains data; navigation, inspection or command entry also stops updates. `f` starts again at a new current end, while `r` returns to historical browsing. See [Kafka following](kafka.md#live-following) for transaction behavior and [NATS following](nats.md#browsing-and-following) for stream-sequence and retention behavior. The command bar still appears only while typing; following adds no panel or configuration setting.

Without an explicit `--connection <alias>`, startup shows the connection picker and makes no datasource request. Select an alias and press Enter. `c` returns to the picker.

Press `?` for Key, Command and Description columns. Press `T` or enter `:themes` to select a theme; its controls stay in context, not an extra action bar. Type `/4b` to filter the loaded page immediately, then Enter to keep it. After entry closes, the filter remains in the table title. Sorting and applied filters never open an input bar.

Enter on a data row opens a Field / Type / Value list for every column, including columns outside the table's four-column window. Use `j/k` or arrows to select a field, then Enter for its full cached value. Esc returns to the field list, then to the row table. These steps retain the selected row and never fetch again. Row values in the field list use bounded previews; field detail exposes the complete retained value in chunks.

The field list uses the full panel width: Field and Type each get a quarter of the available columns, and Value gets the remainder. Resizing the terminal resizes all three columns. The focused field expands vertically from its retained value, without the 128-character or three-line preview cutoff. Other fields stay compact. Select a field with `j/k` or `h/l`; Enter opens its dedicated value viewer with explicit format selection.

For example, open a Kafka record, select `value`, then press PageDown or Ctrl-D to read the rest of its JSON in place. When the expanded value exceeds the viewport, PageUp/PageDown scrolls that value by a screen and Ctrl-U/Ctrl-D by half a screen, stopping at its ends without selecting another field or record. Short values keep screen-wise field navigation. Changing fields resets value scrolling. Expansion uses Auto, honors pretty printing, Unicode display and word wrapping, and does not fetch data. Hex fallback also exposes the entire retained value through scrolling, generating byte chunks without storing a fully expanded hex string.

Use PageUp/PageDown to move one screen, or Ctrl-U/Ctrl-D for half a screen, within loaded rows, the row field list or field detail. Table movement accounts for wrapped preview heights and stops at the first or last loaded item. These shortcuts do not fetch data or change value chunks; `n/p` keeps that role. They are ignored while typing a command/filter or choosing a theme/display setting. The equivalent commands are `:page_up`, `:page_down`, `:half_page_up` and `:half_page_down`; `?` and `onetui schema` list them. You do not need configuration settings for scrolling or column sizing.

A complete one-row, one-column data result opens directly in the value viewer. Qdrant payloads therefore use the content panel for JSON instead of a three-line table preview. Use `j/k` to scroll, `n/p` for chunks and `v` for formatting; Esc returns to the parent resource.

In resource tables, `n` reads the next page and `p` returns to the previous page. The three-page row cache is separate from navigation history: each visited page has an in-memory bookmark containing its incoming provider token. `p` restores a cached page immediately or refetches an evicted page from that bookmark. For example, browse to page 100 with `n`, then press `p` repeatedly to return to page 1. An evicted page requires a working connection, and its rows may have changed since the first read. PostgreSQL views such as `active_customers` use best-effort OFFSET paging without a guaranteed row order.

Bookmarks retain the current path back to page 1, up to 4,096 previous-page bookmarks and 1 MiB of bookmark token text per view. Forward paging stops with an error at either limit instead of discarding history. `p` remains usable; `r` starts a new traversal and clears history after a successful read. Failed or cancelled reads preserve the current page and bookmark for another explicit attempt. Returning to a parent keeps its bookmarks; switching connections or quitting discards them. These are fixed limits, not configuration settings. `onetui schema` lists them.

Filtering updates on each character or Backspace, including held-key repeats. Enter closes the input and keeps the filter; Esc restores the filter and selected row from before editing. Empty input immediately shows all loaded rows. Filtering remains case-sensitive and page-local, searches all cached fields, and neither reformats values nor sends a datasource request. Input still has a 256-byte UTF-8 limit.

See [PostgreSQL usage](postgres.md), [Qdrant usage](qdrant.md) and [configuration](../README.md#configuration) for data navigation and connection settings.

Datasource failures retain the native message and SQLSTATE or gRPC code. PostgreSQL detail/hint/context and network/TLS cause chains are included when available. Known connection secrets are redacted and terminal controls escaped. Errors appear in the footer; its fixed height can clip long diagnostics. See [datasource errors](../README.md#datasource-errors) for the shared TUI/headless behavior and disclosure limits.

## Small terminals

- At least 60 columns and 20 rows: context uses eight rows. At least 100 columns: it also shows dynamic action hints beside the connection details.
- Below those context dimensions: a one-line header shows `read-only`, the alias and the resource breadcrumb, clipped to fit.
- Below 12 rows: input uses one unbordered line and the footer shows status only. Otherwise input uses three rows and the footer uses two.
- Below 40 columns: the version is hidden to preserve status space.
- Below 62 columns: help stacks each key/command above its wrapped description.

All panels use the selected theme. There are no panel-layout configuration keys. Resizing or opening an input bar changes layout without changing keybindings, connection lifetime or terminal cursor visibility.

## Value display controls

Press `v` or enter `:display` to open formats and settings. Move with `j/k` or arrows; Enter applies a format or toggles a setting. Esc or Ctrl-C closes the menu and keeps applied session settings. The input bar stays hidden while selecting settings. Open a field with Enter before choosing its format. Format overrides last until detail closes; other switches remain in effect across connections. No choice writes configuration or fetches data.

```text
:display format json
:display pretty-print off
:display highlight off
:display word-wrap off
:display unicode escaped
:display format hex
:display format binary
```

`auto` validates UTF-8 for both text and byte values. It uses JSON for declared JSON or complete JSON objects/arrays, plain text otherwise, and hex when UTF-8 is invalid. A malformed JSON-looking byte value remains readable as escaped text. Explicit `json` also accepts JSON scalars. `text` requires valid UTF-8; invalid bytes are never replaced or discarded. Hex shows two digits per byte; binary shows eight digits, most significant bit first. Both show byte offsets. Null, empty text and empty bytes remain distinct.

This changes presentation only. Byte fields still use their stable `\x...` hexadecimal projection for page-local filter/sort, even when Auto renders readable text. Protobuf/Avro interpretation requires planned schema-driven decoders, not UTF-8 detection; see [ADR-0008](adr/0008-detect-readable-bytes-and-decode-messages-with-schemas.md).

Pretty printing defaults to on and adds two-space JSON indentation. Off preserves retained JSON whitespace except that terminal controls such as tabs and carriage returns remain visibly escaped; it does not minify. JSON key order, duplicate keys, number text and existing string escapes are preserved. Plain text is not parsed recursively or converted into another format. Data highlighting defaults to on; turning it off retains selection, focus, error colors and UI key hints. Highlight/wrap changes retain the current detail chunk; changing format, indentation or Unicode rendering rebuilds detail from its first chunk.

Word wrapping defaults to on for read-only text throughout the app, including table previews, help and status. Record tables and unfocused inspector fields allow at most three visual lines per preview; the focused inspector field expands, and Enter opens dedicated detail. With wrapping off, use `H`/`L` for horizontal content scrolling, `h/l` to choose fields and `j/k` to scroll detail/help. Newlines remain logical line boundaries. Command/filter editors stay single-line; structural labels and headings retain their layout constraints. The fixed-height footer may clip long status text.

Unicode defaults to `literal`, preserving printable characters and emoji already in data. `escaped` shows non-ASCII code points as ASCII escapes; JSON uses JSON escape syntax. Both choices escape terminal controls and bidirectional overrides. The fixture emoji exercises Unicode rendering and is not a UI decoration. Hex/binary always use retained bytes, not display escapes.

The detail header identifies the effective format and byte provenance. PostgreSQL native `bytea` supplies binary content; other types, including domains, retain server-text output. Qdrant JSON comes from the SDK's structured response, not the original document or wire bytes. Unsupported or oversized formatting shows a reason and a safe text/hex fallback without changing the value.

Each retained page has a 1 MiB value budget. The current page's stable filter/sort projection and table preview cache each have a separate 1 MiB budget; a preview-budget notice directs users to detail. Detail formatting and the focused row-value formatting cache are each capped at 1 MiB with at most 64 JSON nesting levels. Dedicated detail uses text chunks of up to 4,096 characters without splitting a grapheme; row inspection scrolls through the full prepared text. An oversized grapheme falls back to hex. Hex/binary chunks cover 256 source bytes; row inspection generates them incrementally while selecting the visible lines. Compact table previews show up to 128 characters plus a truncation marker. These are fixed limits, not settings or an RSS ceiling. Filter/sort always use the stable unformatted projection and ignore display choices.

Use `onetui schema` for settings, defaults and format descriptors. Persistent defaults belong in the optional `[display]` table in your [configuration file](../README.md#display-settings); runtime choices override them only in memory. [ADR-0004](adr/0004-preserve-values-and-select-display-formats.md) records the value and renderer boundaries.
