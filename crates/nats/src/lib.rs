mod browse;
mod config;
mod provider;
pub use provider::{NatsExecutor, NatsProvider};

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "operations": ["check", "fetch_page", "follow_page"],
        "example_toml": "[connections.events]\nkind = 'nats'\nservers = ['tls://nats.example.net:4222']\nusername_env = 'NATS_USERNAME'\npassword_env = 'NATS_PASSWORD'\n",
        "configuration_behavior": "Unknown fields, wrong types and invalid entries fail even on unselected aliases. No URL/path expansion. No credentials when authentication fields are omitted. Selected secret references must resolve to nonblank Unicode without controls, max 65536 bytes. Names use nonempty ASCII letters, digits, underscores or hyphens. ca_file replaces native roots.",
        "session": "Lazy async-nats client retained for the selected alias; native PING/PONG every 15 seconds. Consecutive reconnect attempts capped at configured server count, TCP connection timeout 1 second. Failed/cancelled requests discard the client; later requests reconnect. Requests and shutdown are deadline bounded.",
        "limits": {"page_rows": 100, "page_bytes": 1048576, "response_bytes": 1048576, "client_queue": 16, "subscription_queue": 16, "rss_guarantee": false},
        "paths": {"nats.streams": [], "nats.messages": ["stream"], "nats.stream_info": ["stream"]},
        "paging": "Stream names use server offset pagination. Messages use stream sequences and a captured last-sequence boundary; sparse sequences are skipped by the server. Bookmarks bind executor, resource, stream creation time and window. Reads are not snapshots; retention or stream replacement can invalidate positions.",
        "values": "Message data and the original NATS header block are bytes; absent headers are null, empty payloads are empty bytes. Subject, sequence and stored timestamp are separate fields. No Protobuf/Avro decoding.",
        "following": {"start": "after the selected stream's current last sequence", "poll_interval_ms": 1000, "buffer_rows": 100, "buffer_bytes": 1048576, "overflow": "oldest displayed rows evicted with visible count", "stop": "f, Ctrl-C, navigation, inspection or error; explicit restart captures a new current end"},
        "configuration": {
            "kind": {"required": true, "values": ["nats"], "purpose": "Select the NATS JetStream connector"},
            "servers": {"required": true, "type": "array of 1..32 strings", "values": "nats://host:port or tls://host:port, max 1024 bytes each; no credentials or extra URL components", "purpose": "Explicit server allowlist; server-advertised addresses are ignored"},
            "tls": {"required": false, "default": true, "type": "boolean", "purpose": "Require verified TLS; false allows only loopback nats:// servers for development"},
            "ca_file": {"required": false, "default": "native trust roots", "type": "absolute PEM file path, max 4096 bytes; regular file max 1 MiB", "purpose": "Replace native trust roots with this CA bundle; requires TLS; read only when connecting"},
            "token_env": {"required": false, "type": "environment variable name", "purpose": "Token secret reference, resolved on selection; excludes username/password"},
            "username_env": {"required": "with password_env", "type": "environment variable name", "purpose": "Username reference, resolved on selection"},
            "password_env": {"required": "with username_env", "type": "environment variable name", "purpose": "Password reference, resolved on selection"}
        },
        "permissions": "Publish only $JS.API.INFO, $JS.API.STREAM.LIST, $JS.API.STREAM.INFO.<stream>, $JS.API.STREAM.MSG.GET.<stream>; subscribe to private _INBOX replies. No application-subject publish, consumer creation, pull, ACK or delete permissions needed.",
        "unsupported": ["Core NATS subscriptions", "consumer administration", "KV and object-store views", "native queries", "publishing", "NKEY/JWT credentials", "client TLS certificates", "custom JetStream domains/API prefixes"]
    })
}
