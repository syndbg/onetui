# Using OneTUI

Start `onetui`, choose a connection and press Enter. For a new connection, press `a`, fill in the form and press F2 to save. See [configuration](../README.md#configuration).

The header shows your current connection, resource and available actions. Press `?` for contextual help or `:` for commands. Keybindings are fixed.

## Navigation

| Key | Action |
| --- | --- |
| `j/k`, arrows | Move between rows |
| Enter / Esc | Open an item / go back |
| lowercase `h/l` | Select a field |
| `n/p` | Next / previous data page or value chunk |
| PageUp / PageDown | Scroll within loaded data |
| Ctrl-U / Ctrl-D | Scroll half a screen outside text entry |
| `/` | Filter rows |
| `s` | Cycle column sort |
| `e` | Open a native query, where supported; see each datasource guide |
| Shift+H | Open query history, where supported |
| `f` | Start or stop following, where supported |
| `c/r` | Choose a connection / refresh |
| Ctrl-C | Cancel active work, or quit when idle |
| `q` | Open quit confirmation |

## Value display controls

Enter on a row opens its fields. Select a field and press Enter again to inspect its full value. Use `v` to choose text, JSON, hex or binary, and adjust pretty printing, highlighting, wrapping or Unicode display.

Auto shows readable text or JSON and falls back to hex for other bytes. With wrapping disabled, use `H/L` to scroll horizontally. Shift+H opens query history on supported resources.

Display changes last for the session. Use `onetui schema` for persistent `[display]` settings. Press `T` to preview [themes](themes.md).

## Query editor

Enter, F5 or Ctrl-R submits the draft. Shift+Enter inserts a newline, and Ctrl-U clears the draft. Esc returns to browsing; Ctrl-C cancels active work. A new query starts empty, and its watermark is a hint that disappears when you type. Press `e` to edit and submit again; `r` refreshes ordinary resource views.

OneTUI asks before running a query. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Ctrl-P and Ctrl-N browse recent queries submitted on this connection. Shift+H or `:history` while browsing opens them as a list: Enter opens one for editing, Esc closes the list. History stays in memory for the session. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) then holds full query text, including any passwords or tokens; turning the setting off does not delete that file.

Shift+Enter needs a terminal that reports modified keys. OneTUI requests that, so it works wherever the terminal supports it. Where it does not, Shift+Enter is indistinguishable from Enter and submits instead: configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`), or paste multiline text, which preserves newlines without executing. Inside tmux this also needs `set -g extended-keys on`.


## Following

Press `f` on a supported resource to follow new arrivals. Press `f` or Ctrl-C to stop. Use historical browsing or replay to inspect older messages.

## Connection problems

If a connection fails to open, a popup shows the reason. Enter or Esc returns to the connection list. Errors after a connection has loaded stay in the footer.

Check an alias without opening the TUI:

```sh
onetui --check --connection my_alias
```

This verifies the connection, not access to every resource. Confirm the endpoint, secret environment variables and required permissions in the connector guide. Use `--timeout <seconds>` for slow requests.

The TUI requires interactive stdin and stdout. See [query editor controls](#query-editor), including Shift+Enter setup. Before sharing an error, review it for server-returned data even though configured secrets are redacted.
