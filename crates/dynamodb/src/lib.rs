#![doc = include_str!("../README.md")]

mod api;
mod attributes;
mod browse;
mod config;
mod provider;
mod query;
mod response;
pub use provider::{DynamoDbExecutor, DynamoDbProvider};

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "operations":["check","fetch_page","query_page"],
        "limits":{"page_rows":100,"page_bytes":1048576,"response_bytes":8388608,"bookmark_bytes":65536,"attribute_depth":32,"distinct_attributes_per_page":1024},
        "session":"Lazy AWS SDK client owned by the selected alias; native credential chain refresh and pooled HTTP connections. Failed/cancelled operations discard the client. No periodic application heartbeat.",
        "paging":"Explicit Scan or Query evaluates at most limit items (default 100). LastEvaluatedKey continues even after an empty filtered page. Bookmarks bind the alias session, resource and query. Independent reads, not snapshots; consumed capacity is returned.",
        "values":"Cells preserve DynamoDB AttributeValue JSON tags, decimal strings and base64-encoded binary data. An absent attribute differs from {NULL:true}.",
        "configuration":{
            "region":{"required":true,"type":"1..64 lowercase ASCII letters, digits or hyphens","purpose":"Explicit AWS region; no ambient region selection"},
            "profile":{"default":"AWS SDK default credential chain","type":"optional profile name, 1..256 bytes without controls","purpose":"Select the profile within the native AWS credential chain; environment credentials take precedence. Supports SSO, roles and container/instance credentials; credential_process is disabled. Refresh belongs to the SDK; unselected aliases do not load credentials."},
            "endpoint_url":{"default":"AWS regional endpoint","type":"optional HTTP(S) origin, at most 1024 bytes","purpose":"Explicit DynamoDB endpoint; overrides ambient endpoint settings. HTTP only for literal loopback, with explicit credential references. HTTPS uses verified native roots. No userinfo, path, query or fragment."},
            "streams_endpoint_url":{"default":"AWS regional Streams endpoint","type":"optional HTTP(S) origin, same validation as endpoint_url","purpose":"Reserved for pending Streams reads; no Streams requests in this build"},
            "access_key_id_env":{"default":"AWS SDK credential chain","type":"optional environment-variable name","purpose":"Explicit access key; requires secret_access_key_env; mutually exclusive with profile"},
            "secret_access_key_env":{"default":"AWS SDK credential chain","type":"optional environment-variable name","purpose":"Secret key paired with access_key_id_env"},
            "session_token_env":{"default":"none","type":"optional environment-variable name","purpose":"Session token with explicit key references. Referenced values must be nonblank, at most 64 KiB without controls; captured on selection, reconnect after rotation."}
        },
        "query_syntax":{"operations":["Scan","Query","GetItem"],"table":"selected resource path; table overrides rejected","common_fields":["projection_expression","expression_attribute_names","consistent_read"],"scan_query_fields":["index","filter_expression","expression_attribute_values","limit"],"query_fields":{"key_condition_expression":"required string","scan_index_forward":"boolean, default true"},"get_item_fields":{"key":"required nonempty AttributeValue map"},"defaults":{"consistent_read":false,"limit":100},"values":"Expression values and keys use DynamoDB AttributeValue JSON; unknown fields and operations rejected"},
        "permissions":"Only invoked read operations are required. --check calls ListTables(limit=1). Browse operations can consume read capacity. No writes, administration, job creation or automatic whole-table traversal.",
        "paths": browse::RESOURCES.iter().map(|r|(r.id.to_owned(),if browse::depth(r.id)==0 {serde_json::json!([])} else {serde_json::json!(["table name or resource ARN"])})).collect::<serde_json::Map<_,_>>()
    })
}
