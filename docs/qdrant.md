# Qdrant

Browse collections and points, inspect payloads and vectors, and view cluster topology.

## Configuration

```toml
[connections.qdrant]
kind = "qdrant"
url = "https://qdrant.example.com:6334"
api_key_env = "ONETUI_QDRANT_API_KEY"
rest_url = "https://qdrant.example.com:6333"
```

`url` is the gRPC endpoint. `rest_url` enables topology views and HTTP requests. Both use the same API key. Use `onetui schema --datasource qdrant` for settings.

## Usage

Open a collection, choose Points, then select a point to inspect its payload or vectors. Press `e` for an [HTTP request](#queries). See [shared controls](ui.md) for navigation and value display.

## Queries

Configure `rest_url`, press `e`, then put `METHOD /path` on the first line. Leave a blank line before an optional body:

```http
POST /collections/demo_products/points/scroll

{"limit": 10}
```

OneTUI sends the body unchanged and shows the HTTP status and response body, including Qdrant errors. Requests stay on the configured endpoint. A request can write data; if a write times out, inspect the target before retrying.

### Editor controls

Enter, F5 or Ctrl-R submits the draft. Shift+Enter inserts a newline, and Ctrl-U clears the draft. Esc returns to browsing; Ctrl-C cancels active work. A new query starts empty, and its watermark is a hint that disappears when you type. Press `e` to edit and submit again; `r` refreshes ordinary resource views.

OneTUI asks before running a query. Set `ask_for_query_confirm = false` in your config to skip the prompt.

Ctrl-P and Ctrl-N browse recent queries submitted on this connection. Shift+H or `:history` while browsing opens them as a list: Enter opens one for editing, Esc closes the list. History stays in memory for the session. To keep the last 100 submissions across restarts, add `persist_query_history = true` at the top of your config. The unencrypted file beside it (`config.history.json` for `config.toml`) then holds full query text, including any passwords or tokens; turning the setting off does not delete that file.

Shift+Enter needs a terminal that reports modified keys. OneTUI requests that, so it works wherever the terminal supports it. Where it does not, Shift+Enter is indistinguishable from Enter and submits instead: configure the key to send `ESC [ 13 ; 2 u` (`\x1b[13;2u`), or paste multiline text, which preserves newlines without executing. Inside tmux this also needs `set -g extended-keys on`.

## Cluster topology

Open **cluster** or **peers**, or a collection's **shards**, **transfers** or **cluster details**. These show the configured REST node's observations, not independently verified peer health.

Topology may require permissions beyond point browsing. `--check` verifies collection access only. Without `rest_url`, point browsing still works.

Try `local_qdrant` in the [demo fixtures](../hack/README.md).
