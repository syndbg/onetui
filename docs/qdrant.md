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

`url` must be the gRPC endpoint. Optional `rest_url` enables topology views and must address the same node. Both use the same API key. Use `onetui schema --datasource qdrant` for settings.

## Usage

Open a collection, choose Points, then select a point to inspect its payload or vectors. Collection metadata includes status and approximate counts. Press `e` on a collection for [filtered Scroll queries](queries.md#qdrant).

Similarity search, export and writes are not supported. See [shared controls](ui.md) for navigation and value display.

## Cluster topology

Open **cluster** or **peers**, or a collection's **shards**, **transfers** or **cluster details**. These show the configured REST node's observations, not independently verified peer health.

Topology may require permissions beyond point browsing. `--check` verifies collection access only. Without `rest_url`, point browsing still works.

Try `local_qdrant` in the [demo fixtures](../hack/README.md).
