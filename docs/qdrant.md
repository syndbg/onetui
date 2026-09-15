# Qdrant browsing

Qdrant uses the same connection picker, tables, local filter/sort, cached detail and worker as PostgreSQL. Its connector owns the resource descriptors, gRPC clients and continuation. The shell has no Qdrant-specific navigation code.

For server-side payload filtering, select or open a collection and press `e` or use `:query`. F5 runs the filtered Scroll JSON draft. Results remain point IDs with lazy payload/vector detail. See [supported query fields and examples](queries.md#qdrant); vector similarity queries remain planned.

## Usage

With your connection configured and its referenced secret exported:

```sh
onetui --config "$HOME/onetui.toml" --connection local_qdrant
onetui schema --datasource qdrant
```

For disposable local development, run `make dev-up`, then `make dev-run`. Select `local_qdrant` in the connection picker and press Enter; the script supplies both fixture credentials. Setup seeds five `demo_*` collections with 3,062 points and varied payload/vector shapes; see the [inventory](../hack/README.md#demo-data). `make dev-seed` adds them to existing fixtures without a reset. Qdrant integration tests create their own separate collections and remove those afterward.

| Screen | Enter opens |
| --- | --- |
| `qdrant.resources` | Collections, cluster status or peers |
| `qdrant.collections` | Selected collection's resource menu |
| `qdrant.collection` | Points, metadata, shards, transfers or full cluster details |
| `qdrant.cluster`, `qdrant.peers`, `qdrant.shards`, `qdrant.transfers`, `qdrant.collection_cluster` | Selected row's fields and full JSON |
| `qdrant.points` | Selected point's detail menu |
| `qdrant.point` | Payload only or vectors only |
| `qdrant.payload` | Already in the full cached value viewer; `j/k` scrolls and `v` changes display |
| `qdrant.metadata`, `qdrant.vectors` | All fields of the selected row; select a field, then Enter for its full value |

`Esc` returns to the retained parent. `n/p` moves between pages or cached detail chunks; `r` refreshes from the beginning. `/` filters displayed-page text and `s` sorts the selected field lexically. These do not fetch payloads or search unseen points. `m` remains PostgreSQL column metadata, not a Qdrant action. See [shared keys](postgres.md#usage).

Collection metadata shows status, segment count, approximate point count and approximate indexed-vector count. It does not show full collection configuration, optimizer diagnostics or payload index definitions, and never issues an exact count query.

Point pages request IDs without payload or vectors. Numeric IDs retain all 64 bits; UUIDs remain strings. Opening the point menu performs no read. Opening payload fetches only that point's payload as JSON; `{}` means an empty payload, while explicit JSON null values remain null. Arbitrary payload keys are data, not offline schema columns or path expressions. Opening vectors fetches only that point's vectors, displaying names, dense/sparse/multivector type, shape and full values within the limit. The empty vector name is shown as `(unnamed)`; a point without vectors has an empty result. Selecting the `values` field and pressing Enter opens cached detail.

Tables use the available column width and preview up to three visual lines. Payload retrieval opens the value viewer directly, so the full panel displays JSON rather than a truncated table row. Field detail supports text/JSON/hex/binary, with independent pretty-print, highlighting and wrapping controls through `v` or `:display`. JSON is serialized from the SDK response, not the original submitted document or wire bytes. Text chunks contain up to 4,096 characters without splitting graphemes; byte views use 256-byte chunks. Terminal controls and bidirectional overrides are escaped. See [display controls and limits](ui.md#value-display-controls). There is no export or similarity-query operation.

## Cluster topology

Set optional `rest_url` to the same node's REST endpoint, usually port 6333. `url` remains its gRPC endpoint, usually port 6334. Both use `api_key_env`. Plaintext is allowed only on loopback; HTTPS verifies platform trust and hostname. URLs cannot contain credentials, a path prefix, query or fragment. OneTUI never infers the REST port or follows redirects.

Open **cluster** for status and consensus state, or **peers** for peer IDs and addresses. In a collection, **shards** shows local/remote replicas, states, keys and local point counts. **transfers** shows active transfers. The `details` fields preserve full JSON; **cluster details** also includes resharding operations. Disabled distributed mode is shown explicitly.

These views read [`GET /cluster`](https://api.qdrant.tech/api-reference/distributed/cluster-status) and [`GET /collections/{name}/cluster`](https://api.qdrant.tech/api-reference/distributed/collection-cluster-info). They report the configured REST node's view, not independently verified peer health. Returned peer addresses are never contacted. There are no membership or shard-management writes.

REST responses are capped at 1 MiB. Lists display 100 rows per page and re-read on paging, without a shared snapshot. Credentials may deny topology access even when browsing works. `--check` still checks only gRPC collection access. Without `rest_url`, browsing works and topology reads report the missing setting.

## Paging, limits and failures

Point pages contain at most 100 IDs. Scroll uses the exact native continuation returned by Qdrant, bound to the executor and resource path. Refresh sends no continuation. Local sorting and filtering never change the native offset. Reads do not share a snapshot: concurrent writes can change later pages, and deleting a point before detail retrieval produces an explicit error.

`p` restores cached points or refetches an evicted page using its saved incoming Scroll token. The bookmark for page 1 has no token. The shell retains up to 4,096 previous-page bookmarks and 1 MiB of bookmark token text per view, separately from its three-page row cache. Hitting a bookmark limit stops forward paging without discarding history. Failed backward reads preserve the current page and bookmark. See [shared navigation](ui.md#navigation).

Qdrant's collection-list RPC has no pagination. Each collection page re-reads the list under the decoding limit, sorts names and displays at most 100. Concurrent collection changes can shift page boundaries. A list above the RPC limit is rejected, not silently truncated.

Every Collections/Points RPC has a 1 MiB decoding cap. Each retained page has a separate 1 MiB value budget, including column labels, targets and continuation. The shell keeps at most three pages per view and separately bounds derived display caches. Vector detail rejects more than 100 named vectors. Oversized data fails explicitly; the current page and continuation remain unchanged. OneTUI does not skip records, retry automatically or raise limits. These limits are not an RSS ceiling.

Successful RPCs reuse one lazy gRPC channel. Failure, cancellation or dropping an active operation discards it; a later explicit read reconnects. Shutdown drops the channel. The active deadline includes connection establishment and formatting; the default remains five seconds (`--timeout` accepts 1-300). No idle queries, periodic heartbeat, custom allocator, SIMD, GPU or extra runtime pool is added.

## Configuration and validation

Existing `kind = "qdrant"`, `url` and optional `api_key_env` settings work for checks and browsing. Add `rest_url` for topology. Use `onetui schema --datasource qdrant` for known settings, defaults, paths and columns. See [configuration](../README.md#configuration).

The offline catalog does not contact Qdrant or guarantee that the selected credentials can read a resource.

See the [Qdrant tests](../crates/qdrant/tests/) for paging, topology, value handling and CLI coverage. Use the checks in [CONTRIBUTING.md](../CONTRIBUTING.md).
