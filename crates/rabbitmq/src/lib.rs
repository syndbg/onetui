mod browse;
mod config;
mod provider;
mod statement;

pub use provider::{RabbitMqExecutor, RabbitMqProvider};

#[must_use]
pub fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "configuration": {
            "url": {"required": true, "purpose": "Management HTTP API origin, without /api; HTTPS except loopback HTTP"},
            "username": {"required": "unless username_env is set", "purpose": "Literal management username"},
            "password": {"required": "unless password_env is set", "purpose": "Literal management password; protect the config file"},
            "username_env": {"required": "unless username is set", "purpose": "Environment variable containing the management username"},
            "password_env": {"required": "unless password is set", "purpose": "Environment variable containing the management password"},
            "ca_file": {"default": null, "purpose": "Optional PEM trust roots; otherwise use native system trust"}
        },
        "paths": {
            "rabbitmq.resources": [], "rabbitmq.overview": [], "rabbitmq.nodes": [], "rabbitmq.vhosts": [],
            "rabbitmq.vhost": ["vhost"],
            "rabbitmq.queues": "[] for all visible vhosts, or [vhost]",
            "rabbitmq.exchanges": "[] or [vhost]", "rabbitmq.bindings": "[] or [vhost]",
            "rabbitmq.connections": "[] or [vhost]", "rabbitmq.channels": "[] or [vhost]",
            "rabbitmq.consumers": "[] or [vhost]", "rabbitmq.policies": "[] or [vhost]",
            "rabbitmq.query": []
        },
        "permissions": "Management plugin and a monitoring user with access to the desired vhosts for browsing. Empty configure/write/read permission patterns suffice for metadata. Editor requests require the native permissions for their endpoint and operation. Native permission errors are retained.",
        "query_syntax": {
            "format": "VERB arguments on the first line, then a blank line and an optional body. Virtual hosts and names are literals: the default vhost is / and is encoded for the wire, never typed as %2F.",
            "verbs": {
                "PUBLISH": "PUBLISH vhost exchange routing_key, body is the payload. An empty routing key is written as \"\". Optional onetui-encoding header takes base64, hex or utf-8 for binary payloads.",
                "GET": "GET vhost queue [count] [requeue|ack]. Destructive read, default 1 message requeued. The count is sent as given; RabbitMQ rejects one it will not serve. Payloads are returned as bytes.",
                "DECLARE": "DECLARE object vhost name, or DECLARE vhost name. The object word is pluralised and sent without a client-side allowlist, so RabbitMQ reports an unsupported kind. JSON body is the definition; an empty body means defaults.",
                "DELETE": "DELETE object vhost name, or DELETE vhost name.",
                "PURGE": "PURGE vhost queue removes every ready message.",
                "RAW": "RAW METHOD /path with an optional JSON body, for endpoints the verbs above do not model such as users, permissions and node operations. Paths are native and include /api, so a literal / inside a name is percent-encoded as %2F here."
            },
            "example": "PUBLISH / amq.default demo\n\nhello",
            "result": "Reads return a page. Writes report applied or rejected with the native status and reason; a publish the broker routed nowhere says so. Responses are bounded to the display limit and have no continuation.",
            "safety": "Requests stay on the configured origin using its credentials and TLS settings. No redirects or automatic retries. A lost write response has an unknown outcome; inspect the target before retrying."
        },
        "limits": {"page_rows": 100, "response_and_page_bytes": 1048576},
        "paging": "Native pagination when returned, otherwise bounded local pages. Each page rereads current state, not a snapshot.",
        "lifecycle": "Lazy HTTP connection pool; cancellation drops the request and retires the pool. No background heartbeat or discovered-node connections.",
        "browsing": "GET metadata and metrics only. Editor requests can publish, retrieve messages or administer resources as permitted by RabbitMQ."
    })
}
