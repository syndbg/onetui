# Using OneTUI

Start `onetui`, choose a connection and press Enter. For a new connection, press `a`, fill in the form and press F2 to save. See [configuration](../README.md#configuration).

The header shows your current connection, resource and available actions. Press `?` for contextual help or `:` for commands. Keybindings are fixed.

## Navigation

| Key | Action |
| --- | --- |
| `j/k`, arrows | Move between rows |
| Enter / Esc | Open an item / go back |
| `h/l` | Select a field |
| `n/p` | Next / previous data page or value chunk |
| PageUp / PageDown | Scroll within loaded data |
| Ctrl-U / Ctrl-D | Scroll half a screen outside text entry |
| `/` | Filter rows |
| `s` | Cycle column sort |
| `e` | Open a [native query](queries.md), where supported |
| `f` | Start or stop following, where supported |
| `c/r` | Choose a connection / refresh |
| Ctrl-C | Cancel active work, or quit when idle |
| `q` | Quit |

## Value display controls

Enter on a row opens its fields. Select a field and press Enter again to inspect its full value. Use `v` to choose text, JSON, hex or binary, and adjust pretty printing, highlighting, wrapping or Unicode display.

Auto shows readable text or JSON and falls back to hex for other bytes. With wrapping disabled, use `H/L` to scroll horizontally.

Display changes last for the session. Use `onetui schema` for persistent `[display]` settings. Press `T` to preview [themes](themes.md).

## Following

Press `f` on a supported resource to follow new arrivals. Press `f` or Ctrl-C to stop. Use historical browsing or replay to inspect older messages.

## Connection problems

Check an alias without opening the TUI:

```sh
onetui --check --connection my_alias
```

This verifies the connection, not access to every resource. Confirm the endpoint, secret environment variables and required permissions in the connector guide. Use `--timeout <seconds>` for slow requests.

The TUI requires interactive stdin and stdout. For query keyboard issues, see [terminal input](queries.md#terminal-input). Before sharing an error, review it for server-returned data even though configured secrets are redacted.
