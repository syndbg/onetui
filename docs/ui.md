# UI panels

OneTUI has one context header, an input bar that appears only while typing, a content panel and a footer. There is no separate branding or connection strip above context.

| Panel | Contents | Behavior |
| --- | --- | --- |
| Context | `read-only` in the title; connection alias, datasource, resource, path, loaded/shown counts and transport state | The alias appears here once. Counts describe cached data, not database totals. Connected means transport state, not automatic data refresh. |
| Context actions | Colored keys beside their descriptions | Available browsing actions come from the action catalog. Command/filter entry and theme selection replace them with their own controls, using the same key styling. |
| Input | `:` command or `/` filter text inside a plain border | Visible only while typing. Enter applies/executes; Esc discards/closes. Long input scrolls to show its end. Closing it returns the space to the content panel. |
| Content | Connection picker, resource table, field detail, help or theme picker | Tables show loaded/shown counts and any applied filter in the title. Column arrows indicate page-local lexical sort. Selection and errors use text as well as color. |
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

## Navigation

Without an explicit `--connection <alias>`, startup shows the connection picker and makes no datasource request. Select an alias and press Enter. `c` returns to the picker.

Press `?` for Key, Command and Description columns. Press `T` or enter `:themes` to select a theme; its controls stay in context, not an extra action bar. Use `/4b` then Enter to filter the loaded page. After entry closes, the filter remains in the table title. Sorting and applied filters never open an input bar.

See [PostgreSQL usage](postgres.md), [Qdrant usage](qdrant.md) and [configuration](../README.md#configuration) for data navigation and connection settings.

## Small terminals

- At least 60 columns and 20 rows: context uses eight rows. At least 100 columns: it also shows dynamic action hints beside the connection details.
- Below those context dimensions: a one-line header shows `read-only`, the alias and the resource breadcrumb, clipped to fit.
- Below 12 rows: input uses one unbordered line and the footer shows status only. Otherwise input uses three rows and the footer uses two.
- Below 40 columns: the version is hidden to preserve status space.
- Below 62 columns: help stacks each key/command above its wrapped description.

All panels use the selected theme. There are no panel-layout configuration keys. Resizing or opening an input bar changes layout without changing keybindings, connection lifetime or terminal cursor visibility.
