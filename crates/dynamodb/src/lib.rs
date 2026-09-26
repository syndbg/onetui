#![doc = include_str!("../README.md")]

mod api;
mod attributes;
mod browse;
mod config;
mod partiql;
mod provider;
mod query;
mod response;
mod streams;
pub use provider::{DynamoDbExecutor, DynamoDbProvider};

fn capabilities() -> serde_json::Value {
    let examples = serde_json::json!({"item":{"operation":"Scan","limit":100},"batch":{"operation":"BatchGetItem","keys":[{"pk":{"S":"demo"}}]},"transaction":{"operation":"TransactGetItems","items":[{"key":{"pk":{"S":"demo"}}}]},"vector":{"operation":"SearchVectors","index":"embedding","search_vector":[0.1,-0.2,0.3],"top_k":10},"partiql":{"operation":"ExecuteStatement","statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"demo"}],"limit":100},"partiql_batch":{"operation":"BatchExecuteStatement","statements":[{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"demo"}]}]},"partiql_transaction":{"operation":"ExecuteTransaction","statements":[{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"demo"}]}]},"stream":{"operation":"GetRecords","shard_id":"shard-id","sequence_number":"123","limit":100}});
    serde_json::json!({
        "operations":["check","fetch_page","query_page","follow_page"],
        "limits":{"page_rows":100,"page_bytes":1048576,"response_bytes":8388608,"bookmark_bytes":65536,"attribute_depth":32,"distinct_attributes_per_page":1024},
        "session":"Lazy AWS SDK client owned by the selected alias; native credential chain refresh and pooled HTTP connections. Native trust loading runs off async workers, with one blocking job at a time; request deadlines and cancellation remain active. Completed query failures keep the client; cancelled or timed-out operations discard it. No periodic application heartbeat.",
        "paging":"Explicit Scan or Query evaluates at most limit items (default 100). LastEvaluatedKey continues even after an empty filtered page. Bookmarks bind the alias session, resource and query. Independent reads, not snapshots; consumed capacity is returned.",
        "values":"Cells preserve DynamoDB AttributeValue JSON tags, decimal strings and base64-encoded binary data. An absent attribute differs from {NULL:true}.",
        "following":{"resource":"dynamodb.records","start":"latest on explicit f; browsing starts at trim horizon","poll_interval_ms":1000,"buffer_rows":100,"buffer_bytes":1048576,"bookmarks":"Last retained sequence and native iterator. Renew expired iterators only with a known sequence; otherwise return the native error. Closed shards retain their final batch and stop following. Child shards require explicit selection.","decoding":"Original record JSON, including keys and old/new images. SDK model decoding failures show a warning alongside unchanged JSON; missing attribute tags are never inferred."},
        "configuration":{
            "region":{"required":true,"type":"1..64 lowercase ASCII letters, digits or hyphens","purpose":"Explicit AWS region; no ambient region selection"},
            "profile":{"default":"AWS SDK default credential chain","type":"optional profile name, 1..256 bytes without controls","purpose":"Select the profile within the native AWS credential chain; environment credentials take precedence. Supports SSO, roles and container/instance credentials; credential_process is disabled. Refresh belongs to the SDK; unselected aliases do not load credentials."},
            "endpoint_url":{"default":"AWS regional endpoint","type":"optional HTTP(S) origin, at most 1024 bytes","purpose":"Explicit DynamoDB endpoint; overrides ambient endpoint settings. HTTP only for literal loopback, with explicit credential references. HTTPS uses verified native roots. No userinfo, path, query or fragment."},
            "streams_endpoint_url":{"default":"AWS regional Streams endpoint","type":"optional HTTP(S) origin, same validation as endpoint_url","purpose":"Explicit Streams endpoint; required for Streams reads when endpoint_url is set. Overrides ambient endpoint settings; never inferred from the database endpoint."},
            "access_key_id_env":{"default":"AWS SDK credential chain","type":"optional environment-variable name","purpose":"Explicit access key; requires secret_access_key_env; mutually exclusive with profile"},
            "secret_access_key_env":{"default":"AWS SDK credential chain","type":"optional environment-variable name","purpose":"Secret key paired with access_key_id_env"},
            "session_token_env":{"default":"none","type":"optional environment-variable name","purpose":"Session token with explicit key references. Referenced values must be nonblank, at most 64 KiB without controls; captured on selection, reconnect after rotation."}
        },
        "query_syntax":{
            "operations":["Scan","Query","GetItem","BatchGetItem","TransactGetItems","SearchVectors","ExecuteStatement","BatchExecuteStatement","ExecuteTransaction","GetRecords"],
            "scope":"selected table, or stream ARN for GetRecords; table/stream overrides rejected",
            "common_item_fields":["projection_expression","expression_attribute_names","consistent_read"],
            "scan_query_fields":["index","filter_expression","expression_attribute_values","limit"],
            "query_fields":{"key_condition_expression":"required string","scan_index_forward":"boolean, default true"},
            "get_item_fields":{"key":"required nonempty AttributeValue map"},
            "batch_get_item_fields":{"keys":"1..100 nonempty AttributeValue maps","projection_expression":"optional string","expression_attribute_names":"optional string map","consistent_read":"boolean, default false"},
            "transact_get_items_fields":{"items":"1..100 objects, each with key (nonempty AttributeValue map), optional projection_expression and expression_attribute_names; one atomic read"},
            "multi_item_results":"Complete native JSON response, bounded to 1 MiB; Enter inspects values. Capacity, unprocessed keys and missing-item positions are retained. Batch n retries only unprocessed keys; no automatic retry loop or stable result order. Transactions have no continuation. Reduce keys or projection when too large.",
            "search_vectors_fields":{"index":"required vector index name, 3..255 ASCII letters, digits, underscores, dots or hyphens","search_vector":"1..4096 JSON numbers within finite 32-bit float range; sent as native N attributes; the overall 16 KiB query-text limit still applies","top_k":"required integer, 1..100","search_condition_expression":"optional native condition string","projection_expression":"optional native projection string","expression_attribute_names":"optional string map","expression_attribute_values":"optional AttributeValue map"},
            "vector_results":"Complete native JSON within 1 MiB; SearchResults keep service order, scores, typed items and capacity. No continuation. VectorIndexes are included in index metadata. DynamoDB Local does not implement vector search.",
            "execute_statement_fields":{"statement":"required single SELECT, 1..8192 UTF-8 bytes, FROM the selected table and optional index","parameters":"optional nonempty array of native AttributeValue objects, bound to ? placeholders","consistent_read":"boolean, default false","limit":"1..100 evaluated items, default 100"},
            "batch_execute_statement_fields":{"statements":"1..25 objects with statement, optional parameters and optional consistent_read (default false); each SELECT must identify one item as required by DynamoDB"},
            "execute_transaction_fields":{"statements":"1..100 objects with statement and optional parameters; each SELECT identifies one item; consistent_read is not accepted because the read is transactional"},
            "partiql_guard":"All statements are checked before connecting. Only one SELECT per statement from the selected table is allowed; no joins, subqueries, writes or table overrides. Use doubled quotes and typed parameters; backtick literals, backslash-escaped quotes and nested comments are rejected. Expressions are validated by DynamoDB.",
            "partiql_results":"ExecuteStatement displays typed item rows and uses native NextToken bookmarks, including empty pages. LastEvaluatedKey without NextToken fails visibly instead of claiming completion. Responses above the requested row limit or 1 MiB are rejected, not truncated. Batch and transaction views retain complete native JSON, capacity, ordered responses and per-statement errors; they have no continuation or automatic retry loop.",
            "get_records_fields":{"shard_id":"required string, 1..240 bytes without controls","sequence_number":"required decimal string, 1..40 digits; exact value retained","after":"boolean, default false: start at sequence; true starts after sequence","limit":"1..100 records, default 100"},
            "defaults":{"consistent_read":false,"limit":100},
            "values":"Expression values and keys use DynamoDB AttributeValue JSON; unknown fields and operations rejected",
            "examples":examples,
            "editor":"Streams rows supply their selected shard and sequence; item views use Scan. Existing drafts take precedence."
        },
        "permissions":"Only invoked read operations are required. --check calls ListTables(limit=1). Browse operations can consume read capacity. No writes, administration, job creation or automatic whole-table traversal.",
        "paths": browse::RESOURCES.iter().map(|r|(r.id.to_owned(),match browse::depth(r.id) {0 => serde_json::json!([]), 2 if r.id == "dynamodb.index_insights" => serde_json::json!(["table name", "index name"]), 2 => serde_json::json!(["stream ARN", "shard ID"]), _ => serde_json::json!(["table name or resource ARN"])})).collect::<serde_json::Map<_,_>>()
    })
}
