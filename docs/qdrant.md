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

Use the [shared editor controls](ui.md#query-editor) for confirmation, cancellation and history.

## Cluster topology

Open **cluster** or **peers**, or a collection's **shards**, **transfers** or **cluster details**. These show the configured REST node's observations, not independently verified peer health.

Topology may require permissions beyond point browsing. `--check` verifies collection access only. Without `rest_url`, point browsing still works.

Try `local_qdrant` in the [demo fixtures](../hack/README.md).
