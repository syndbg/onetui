mod browse;
mod config;
mod provider;

pub use provider::{RabbitMqExecutor, RabbitMqProvider};

pub fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "configuration": {
            "url": {"required": true, "purpose": "Management HTTP API origin, without /api; HTTPS except loopback HTTP"},
            "username_env": {"required": true, "purpose": "Environment variable containing the management username"},
            "password_env": {"required": true, "purpose": "Environment variable containing the management password"},
            "ca_file": {"default": null, "purpose": "Optional PEM trust roots; otherwise use native system trust"}
        },
        "paths": {
            "rabbitmq.resources": [], "rabbitmq.overview": [], "rabbitmq.nodes": [], "rabbitmq.vhosts": [],
            "rabbitmq.vhost": ["vhost"],
            "rabbitmq.queues": "[] for all visible vhosts, or [vhost]",
            "rabbitmq.exchanges": "[] or [vhost]", "rabbitmq.bindings": "[] or [vhost]",
            "rabbitmq.connections": "[] or [vhost]", "rabbitmq.channels": "[] or [vhost]",
            "rabbitmq.consumers": "[] or [vhost]", "rabbitmq.policies": "[] or [vhost]"
        },
        "permissions": "Management plugin and a monitoring user with access to the desired vhosts. Empty configure/write/read permission patterns suffice for metadata. Native permission errors are retained.",
        "limits": {"page_rows": 100, "response_and_page_bytes": 1048576},
        "paging": "Native pagination when returned, otherwise bounded local pages. Each page rereads current state, not a snapshot.",
        "lifecycle": "Lazy HTTP connection pool; cancellation drops the request and retires the pool. No background heartbeat or discovered-node connections.",
        "read_only": "GET metadata and metrics only. No message get/requeue, consumers, ACKs, publishing or administration. Definitions and user password hashes are not requested."
    })
}
