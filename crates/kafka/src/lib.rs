mod browse;
mod buf;
mod catalog;
mod config;
mod decoding;
mod groups;
mod inspect;
mod oauth;
mod provider;
mod query;
mod registry;
#[cfg(test)]
#[path = "../tests/support/registry.rs"]
mod test_server;
mod topic;
pub use provider::{KafkaExecutor, KafkaProvider};

fn capabilities() -> serde_json::Value {
    serde_json::json!({
        "operations": ["check", "fetch_page", "follow_page", "query_page"],
        "diagnostics": {"recent_errors_per_request": provider::NATIVE_ERROR_COUNT, "bytes_per_error": provider::NATIVE_ERROR_BYTES, "behavior": "Retain distinct native errors through foreground timeout; redact secrets and escape terminal controls before retaining them. Oversized diagnostics end in an ellipsis."},
        "session": "One lazy native owner thread per process; client retained for the selected alias. Native cleanup retains its slot until complete, even after a foreground cancellation. No periodic metadata checks or consumer-group subscription.",
        "limits": {"page_rows": 100, "display_page_bytes": 1048576, "topic_partitions": 32, "topic_cursor_bytes": 4096, "native_receive_bytes": 4194304, "group_array_bytes": 4194304, "group_name_bytes": 1024, "prefetch_kib": 1024, "fetch_queue_backoff_ms": 100, "rss_guarantee": false},
        "paths": {"kafka.resources": [], "kafka.topics": [], "kafka.topic": ["topic"], "kafka.topic_config": ["topic"], "kafka.brokers": [], "kafka.broker_config": ["nonnegative i32 broker ID"], "kafka.groups": [], "kafka.group": ["group name, 1..1024 UTF-8 bytes without controls"], "kafka.members": ["group name, 1..1024 UTF-8 bytes without controls"], "kafka.offsets": ["group name, 1..1024 UTF-8 bytes without controls"], "kafka.partitions": ["topic"], "kafka.records": ["topic", "optional partition number; omission reads all partitions, up to 32"], "kafka.query": ["topic", "partition number"]},
        "paging": "Metadata is re-read and locally paged; records read a partition or all topic partitions from earliest available offsets to captured read-committed stable ends. Open transactions and later records are outside those windows. Continuations bind session, resource and each partition's next offset and window end. Topic-wide pages rotate partition batches, preserve offset order within partitions and add a partition column; no global time order. Partition-set changes fail until refresh/restart. Refresh or refetching the first page opens new windows. No retained snapshot; unavailable positions and request errors retain the displayed page and bookmark.",
        "values": "Key and value are nullable bytes; empty bytes differ from tombstones. Headers are ordered JSON entries with names and nullable byte arrays, preserving duplicates. Optional raw Protobuf/Avro or Confluent Avro/Protobuf registry bindings add decoded JSON, schema identity and per-value error columns; original fields remain unchanged.",
        "following": {"start": "current read-committed stable end of the selected partition, or independent ends of every topic partition", "poll_interval_ms": 1000, "buffer_rows": 100, "buffer_bytes": 1048576, "overflow": "evict oldest displayed rows with a visible count; do not skip the broker cursor", "stop": "f, Ctrl-C, navigation or inspection; errors stop with retained data; restart explicitly from current end", "continuation": "follow-only, session/resource/partition-set scoped with independent offsets; empty batches retain a cursor; no commits or group subscription"},
        "configuration": {
            "kind": {"required": true, "values": ["kafka"], "purpose": "Select the Kafka connector"},
            "bootstrap_servers": {"required": true, "type": "array of strings", "values": "1..32 host:port addresses, max 255 bytes each, no URL scheme or credentials", "example": ["localhost:19092"], "purpose": "Initial broker discovery addresses; brokers may advertise other endpoints"},
            "security_protocol": {"required": false, "default": "SSL", "values": ["SSL", "SASL_SSL", "PLAINTEXT"], "purpose": "TLS or SASL over verified TLS; plaintext bootstrap addresses restricted to loopback for development"},
            "ca_file": {"required": false, "default": "librdkafka/OpenSSL trust discovery", "type": "absolute path, max 4096 bytes", "purpose": "CA certificates for broker TLS; not allowed with PLAINTEXT"},
            "client_cert_file": {"required": "with client_key_file", "default": null, "type": "absolute path, max 4096 bytes without controls", "purpose": "PEM client certificate chain for broker mTLS; requires SSL or SASL_SSL; read on the first native request"},
            "client_key_file": {"required": "with client_cert_file", "default": null, "type": "absolute path, max 4096 bytes without controls", "purpose": "PEM private key matching client_cert_file; keep file permissions restricted; no tilde or environment expansion"},
            "client_key_password_env": {"required": false, "default": null, "type": "nonempty ASCII environment variable name", "purpose": "Password for an encrypted client key; requires client_key_file, resolved only for the selected alias and redacted from errors; omitted for unencrypted keys"},
            "sasl_mechanism": {"required": "with SASL_SSL", "values": ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"], "purpose": "SASL mechanism; omitted for SSL/PLAINTEXT"},
            "username_env": {"required": "with PLAIN/SCRAM; forbidden with OAUTHBEARER/GSSAPI", "type": "nonempty ASCII environment variable name", "purpose": "Environment variable holding the SASL username; resolved only for selected alias"},
            "password_env": {"required": "with PLAIN/SCRAM; forbidden with OAUTHBEARER/GSSAPI", "type": "nonempty ASCII environment variable name", "purpose": "Environment variable holding the SASL password; resolved only for selected alias"},
            "kerberos_principal": {"required": "with GSSAPI; forbidden otherwise", "type": "1..1024 UTF-8 bytes without whitespace or controls", "example": "reader@EXAMPLE.COM", "purpose": "SASL principal matching the caller's existing Kerberos ticket cache; does not obtain tickets or select a cache"},
            "kerberos_service_name": {"required": false, "default": "kafka", "values": "1..255 ASCII letters, digits, underscore, hyphen or dot; GSSAPI only", "purpose": "Broker Kerberos service name; native GSSAPI uses each advertised broker hostname"},
            "oauth": crate::oauth::capabilities(),
            "decoders": {
                "required": false, "default": [], "type": "array of tables, at most 32",
                "purpose": "Exact topic and key/value bindings within this connection; omitted bindings retain Auto. Duplicate topic/field pairs and unknown fields fail validation.",
                "fields": {
                    "topic": {"required": true, "type": "string", "values": "exact Kafka topic, 1..249 ASCII letters/digits/dot/underscore/hyphen; not dot or dot-dot; no wildcards"},
                    "field": {"required": true, "values": ["key", "value"]},
                    "format": {"required": true, "values": ["avro", "protobuf"]},
                    "framing": {"required": true, "values": ["raw", "confluent"], "purpose": "Raw requires exactly one of schema_file, catalog or buf; confluent requires registry, resolves the four-byte schema ID after version byte zero and Protobuf message indexes. No automatic detection."},
                    "schema_file": {"required": "raw without catalog or buf; forbidden with confluent, catalog or buf", "type": "absolute path, at most 4096 UTF-8 bytes without controls", "purpose": "Regular local writer-schema JSON or binary FileDescriptorSet with imports; at most 256 KiB. No environment expansion, tilde expansion or relative-path resolution."},
                    "message_name": {"required": "raw protobuf only; forbidden for avro and confluent", "type": "exact fully qualified string, 1..1024 UTF-8 bytes without controls"},
                    "registry": crate::registry::capabilities(),
                    "catalog": crate::catalog::capabilities(),
                    "buf": crate::buf::capabilities()
                },
                "example": {"topic": "events", "field": "value", "format": "avro", "framing": "raw", "schema_file": "/absolute/event.avsc"},
                "lifecycle": "Config parsing is offline. First relevant read resolves the schema on the native worker; --check loads local/Buf bindings and checks configured Confluent registries for the selected alias. Local-file, catalog and Buf successes/failures stay cached until reopening; Confluent caches retain eight IDs per binding. No renderer I/O.",
                "resources": ["kafka.records", "kafka.query"],
                "columns": ["key_decoded", "key_schema", "key_decode_error", "value_decoded", "value_schema", "value_decode_error"],
                "limits": {"schema_bytes_per_binding": 262144, "payload_bytes": 65536, "preview_bytes": 65536, "error_bytes": 512, "raw_page_reserve_bytes": 2048},
                "presentation": "Only bound fields add columns. Null payloads are not decoded; empty payloads are decoded. Errors preserve raw data and do not stop following. JSON is a projection, not a lossless native-type export. Combined page stays within 1 MiB; exhausted preview budget produces a per-value error or a page notice with null preview columns."
            }
        },
        "safety": "No auto commits, auto offset storage, topic auto-creation or group subscriptions. Explicit numeric assignment; auto.offset.reset=error; read_committed. Internal session group ID is not an application group. No generic native configuration overrides.",
        "permissions": "Topic Read and Describe; group Describe for the private onetui- prefix used by native coordinator lookup. No group Read permission is needed; the browser never joins or commits.",
        "group_inspection": "Read-only group state/protocol/member listing and stored offsets. Requires Describe on inspected groups; unauthorized groups may be omitted from listings. Metadata/assignments remain nullable protocol bytes. Offsets are sorted by topic/partition; watermarks are fetched only for the displayed page and require topic Describe. Lag is stable_end minus committed, not message count; no commit or positions outside the retained/stable window have null lag and an explicit status. Missing groups and groups without stored offsets both return an empty offsets view. Independent reads, not a snapshot.",
        "config_inspection": "Read-only topic/broker configuration, sorted by name and locally paged; requires DescribeConfigs on the topic or cluster respectively. Includes source, default/read-only/sensitive flags; sensitive and unavailable values are null. No config changes, synonyms or secret retrieval. Uses the existing native client and request deadline; adds no connection settings.",
        "replay": {"offset": "Optional nonnegative i64; omitted/null starts at earliest available offset", "timestamp_ms": "Optional nonnegative i64 Unix milliseconds; mutually exclusive with offset; resolves a starting offset, not a per-record time filter", "end_offset": "Optional nonnegative i64 exclusive end; omitted/null captures current read-committed end", "behavior": "JSON query editor for the selected partition; unknown fields rejected, no commits or writes"},
        "kerberos": "System SASL/GSSAPI libraries use the caller's ticket cache and Kerberos configuration (KRB5CCNAME/KRB5_CONFIG at process launch). Obtain and renew tickets externally. No kinit subprocess, keytab setting, cache creation or per-alias environment mutation. Restart after replacing expired tickets or changing caches: system GSSAPI may retain failed lookups across connection reopen. Native errors and request cancellation follow the existing owner lifecycle.",
        "unsupported": ["global timestamp ordering", "cross-topic browsing", "publishing from the app", "consumer-group administration", "Confluent header-GUID framing", "Avro reader-schema resolution", "native decoded-type inspector", "OAuth opaque tokens, interactive grants and SASL extensions", "Kerberos ticket acquisition, renewal and per-alias caches"]
    })
}
