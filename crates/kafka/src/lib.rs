mod browse;
mod config;
mod provider;
pub use provider::{KafkaExecutor, KafkaProvider};

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "operations": ["check", "fetch_page", "follow_page"],
        "session": "One lazy native owner thread per process; client retained for the selected alias. Native cleanup retains its slot until complete, even after a foreground cancellation. No periodic metadata checks or consumer-group subscription.",
        "limits": {"page_rows": 100, "display_page_bytes": 1048576, "native_receive_bytes": 4194304, "prefetch_kib": 1024, "rss_guarantee": false},
        "paths": {"kafka.topics": [], "kafka.partitions": ["topic"], "kafka.records": ["topic", "partition number"]},
        "paging": "Metadata is re-read and locally paged; records read one partition from its earliest available offset to the native read-committed stable end. Open transactions and later records are outside that window. Continuations bind session, resource, partition, next offset and window end. Refresh or refetching the first page opens a new window. No retained snapshot; unavailable positions and request errors retain the displayed page and bookmark.",
        "values": "Key and value are nullable bytes; empty bytes differ from tombstones. Headers are ordered JSON entries with names and nullable byte arrays, preserving duplicates. No schema-registry decoding.",
        "following": {"start": "current read-committed stable end of the selected partition", "poll_interval_ms": 1000, "buffer_rows": 100, "buffer_bytes": 1048576, "overflow": "evict oldest displayed rows with a visible count; do not skip the broker cursor", "stop": "f, Ctrl-C, navigation or inspection; errors stop with retained data; restart explicitly from current end", "continuation": "follow-only, session/resource/partition scoped; empty batches retain a cursor; no commits or group subscription"},
        "configuration": {
            "kind": {"required": true, "values": ["kafka"], "purpose": "Select the Kafka connector"},
            "bootstrap_servers": {"required": true, "type": "array of strings", "values": "1..32 host:port addresses, max 255 bytes each, no URL scheme or credentials", "example": ["localhost:19092"], "purpose": "Initial broker discovery addresses; brokers may advertise other endpoints"},
            "security_protocol": {"required": false, "default": "SSL", "values": ["SSL", "SASL_SSL", "PLAINTEXT"], "purpose": "TLS or SASL over verified TLS; plaintext bootstrap addresses restricted to loopback for development"},
            "ca_file": {"required": false, "default": "librdkafka/OpenSSL trust discovery", "type": "absolute path, max 4096 bytes", "purpose": "CA certificates for broker TLS; not allowed with PLAINTEXT"},
            "sasl_mechanism": {"required": "with SASL_SSL", "values": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"], "purpose": "SASL mechanism; omitted for SSL/PLAINTEXT"},
            "username_env": {"required": "with SASL_SSL", "type": "nonempty ASCII environment variable name", "purpose": "Environment variable holding the SASL username; resolved only for selected alias"},
            "password_env": {"required": "with SASL_SSL", "type": "nonempty ASCII environment variable name", "purpose": "Environment variable holding the SASL password; resolved only for selected alias"}
        },
        "safety": "No auto commits, auto offset storage, topic auto-creation or group subscriptions. Explicit numeric assignment; auto.offset.reset=error; read_committed. Internal session group ID is not an application group. No generic native configuration overrides.",
        "permissions": "Topic Read and Describe; group Describe for the private onetui- prefix used by native coordinator lookup. No group Read permission is needed; the browser never joins or commits.",
        "unsupported": ["query_page", "cross-partition merging", "publishing from the app", "consumer-group administration", "schema registry", "client TLS certificates", "OAuth", "GSSAPI"]
    })
}
