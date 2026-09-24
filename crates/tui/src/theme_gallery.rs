use std::{fmt::Write, path::PathBuf};

use onetui_core::{
    Column, Page, Resource, Row, Value,
    catalog::{Action, ResourceDescriptor},
    config::Config,
    provider::{
        CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
        RequestContext, ShutdownContext,
    },
};
use onetui_theme::Theme;
use ratatui::{
    Terminal,
    backend::TestBackend,
    style::{Color, Modifier},
};
use unicode_width::UnicodeWidthStr;

const WIDTH: u16 = 144;
const HEIGHT: u16 = 32;

const KAFKA_RECORDS: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.records",
    description: "Kafka records",
    columns: &[
        "partition",
        "offset",
        "timestamp_ms",
        "key",
        "value",
        "headers",
    ],
    paging: true,
    actions: &[],
};
const KAFKA_GROUPS: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.groups",
    description: "Consumer group state and protocol; Enter chooses members or offsets",
    columns: &[
        "name",
        "state",
        "protocol_type",
        "protocol",
        "members",
        "broker_id",
        "broker_host",
        "broker_port",
    ],
    paging: true,
    actions: &[],
};
const KAFKA_GROUP: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.group",
    description: "Choose members or committed offsets without joining the group",
    columns: &["resource", "description"],
    paging: true,
    actions: &[],
};
const KAFKA_BROKER_CONFIG: ResourceDescriptor = ResourceDescriptor {
    id: "kafka.broker_config",
    description: "Broker settings; sensitive values are withheld",
    columns: &[
        "name",
        "value",
        "source",
        "is_default",
        "is_read_only",
        "is_sensitive",
        "synonyms",
    ],
    paging: true,
    actions: &[],
};
const POSTGRES_REPLICATION: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.replication",
    description: "Connected WAL senders from pg_stat_replication; columns follow the server version",
    columns: &[],
    paging: true,
    actions: &[],
};
const POSTGRES_WAL_RECEIVER: ResourceDescriptor = ResourceDescriptor {
    id: "postgres.wal_receiver",
    description: "Upstream WAL receiver from pg_stat_wal_receiver; columns follow the server version",
    columns: &[],
    paging: true,
    actions: &[],
};
const QDRANT_CLUSTER: ResourceDescriptor = ResourceDescriptor {
    id: "qdrant.cluster",
    description: "Cluster status and local consensus state",
    columns: &[
        "status",
        "peer_id",
        "leader",
        "role",
        "term",
        "commit",
        "pending_operations",
        "details",
    ],
    paging: true,
    actions: &[],
};
const DYNAMODB_ITEMS: ResourceDescriptor = ResourceDescriptor {
    id: "dynamodb.items",
    description: "Explicit bounded Scan; independent reads, not a snapshot",
    columns: &[],
    paging: true,
    actions: &[],
};
const DYNAMODB_SHARDS: ResourceDescriptor = ResourceDescriptor {
    id: "dynamodb.shards",
    description: "Shard descriptions and parent relationships",
    columns: &[],
    paging: true,
    actions: &[],
};
const DYNAMODB_RECORDS: ResourceDescriptor = ResourceDescriptor {
    id: "dynamodb.records",
    description: "Bounded shard records; original typed images retained",
    columns: &[],
    paging: true,
    actions: &[],
};
const RABBITMQ_QUEUES: ResourceDescriptor = ResourceDescriptor {
    id: "rabbitmq.queues",
    description: "Queues",
    columns: &[
        "name",
        "vhost",
        "type",
        "state",
        "messages_ready",
        "messages_unacknowledged",
        "consumers",
        "details",
    ],
    paging: true,
    actions: &[],
};
static KAFKA: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &["kafka.records"],
    query: None,
    kind: "kafka",
    entry_resource: Some("kafka.records"),
    browsing: "Kafka records",
    resources: &[
        &KAFKA_RECORDS,
        &KAFKA_GROUPS,
        &KAFKA_GROUP,
        &KAFKA_BROKER_CONFIG,
    ],
    documentation: empty_documentation,
};
static POSTGRES: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &[],
    query: None,
    kind: "postgres",
    entry_resource: Some("postgres.replication"),
    browsing: "connected replication processes",
    resources: &[&POSTGRES_REPLICATION, &POSTGRES_WAL_RECEIVER],
    documentation: empty_documentation,
};
static QDRANT: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &[],
    query: None,
    kind: "qdrant",
    entry_resource: Some("qdrant.cluster"),
    browsing: "Qdrant cluster",
    resources: &[&QDRANT_CLUSTER],
    documentation: empty_documentation,
};
static DYNAMODB: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &["dynamodb.records"],
    query: None,
    kind: "dynamodb",
    entry_resource: Some("dynamodb.items"),
    browsing: "DynamoDB items",
    resources: &[&DYNAMODB_ITEMS, &DYNAMODB_SHARDS, &DYNAMODB_RECORDS],
    documentation: empty_documentation,
};
static RABBITMQ: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &[],
    query: None,
    kind: "rabbitmq",
    entry_resource: Some("rabbitmq.queues"),
    browsing: "RabbitMQ queues",
    resources: &[&RABBITMQ_QUEUES],
    documentation: empty_documentation,
};

fn empty_documentation() -> serde_json::Value {
    serde_json::json!({})
}

struct GalleryProvider(&'static ProviderDescriptor);

impl Provider for GalleryProvider {
    type Executor = GalleryExecutor;

    fn descriptor(&self) -> &'static ProviderDescriptor {
        self.0
    }

    fn validate_config(&self, _: &toml::Table) -> anyhow::Result<()> {
        Ok(())
    }

    fn configure(
        &self,
        _: &toml::Table,
        _: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self::Executor> {
        Ok(GalleryExecutor(
            tokio::sync::watch::channel(ConnectionStatus::Configured).0,
        ))
    }
}

struct GalleryExecutor(tokio::sync::watch::Sender<ConnectionStatus>);

impl Executor for GalleryExecutor {
    fn status(&self) -> tokio::sync::watch::Receiver<ConnectionStatus> {
        self.0.subscribe()
    }

    async fn check(&self, _: RequestContext) -> anyhow::Result<CheckResult> {
        anyhow::bail!("gallery provider has no transport")
    }

    async fn fetch_page(&self, _: PageRequest, _: RequestContext) -> anyhow::Result<Page> {
        anyhow::bail!("gallery provider has no transport")
    }

    async fn shutdown(&mut self, _: ShutdownContext) -> anyhow::Result<()> {
        self.0.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}

fn app(theme: Theme) -> crate::App {
    let config = Config::parse(
        "[connections.demo_pg]\nkind='postgres'\nurl_env='UNUSED_GALLERY_DSN'",
        &[onetui_postgres::PostgresProvider],
    )
    .unwrap();
    let mut app = crate::App::new(config, Some("demo_pg"));
    app.config.theme = theme;
    let request = app.request.take().unwrap();
    app.view.resource = Resource::new("postgres.rows", vec!["demo".into(), "customers".into()]);
    app.complete(&request, Ok(Page {
        columns: [("id", "bigint"), ("name", "text"), ("country", "text"), ("profile", "jsonb")]
            .into_iter().map(|(name, datatype)| Column { name: name.into(), datatype: datatype.into() }).collect(),
        rows: [
            ("Mina", "JP", "starter"), ("Jose", "BR", "team"),
            ("Zoe", "BG", "enterprise"), ("Sam", "DE", "team"),
            ("Avery", "US", "starter"), ("Noor", "NL", "team"),
            ("Alex", "GB", "enterprise"), ("Robin", "SE", "starter"),
            ("Rene", "FR", "team"), ("Sasha", "CA", "starter"),
            ("Kai", "NZ", "enterprise"), ("Morgan", "AU", "team"),
        ].into_iter().enumerate().map(|(i, (name, country, plan))| Row {
            cells: vec![Some((i + 1).to_string().into()), Some(name.into()), Some(country.into()),
                if i == 4 { None } else { Some(Value::Json(serde_json::json!({"plan":plan,"active":i%3!=0,"tags":["demo","synthetic"]}).to_string())) }],
            target: None,
        }).collect(),
        notice: "Synthetic demo data | Independent reads, not a snapshot".into(),
        ..Page::default()
    }));
    app.view.selected = 3;
    app.connection_status = Some(ConnectionStatus::Connected);
    app
}

fn synthetic_app(
    alias: &str,
    provider: &'static ProviderDescriptor,
    resource: Resource,
    columns: Vec<Column>,
    rows: Vec<Row>,
) -> crate::App {
    let config = Config::parse(
        &format!("[connections.{alias}]\nkind='{}'", provider.kind),
        &[GalleryProvider(provider)],
    )
    .unwrap();
    let mut app = crate::App::new(config, Some(alias));
    app.config.theme = Theme::Catppuccin;
    let request = app.request.take().unwrap();
    app.view.resource = resource;
    app.complete(
        &request,
        Ok(Page {
            columns,
            rows,
            notice: "Synthetic demo data".into(),
            ..Page::default()
        }),
    );
    app.connection_status = Some(ConnectionStatus::Connected);
    app
}

fn decoded_message(format: &str) -> crate::App {
    let (
        topic,
        key_wire,
        key_decoded,
        key_native,
        key_schema,
        value_wire,
        value_decoded,
        value_native,
        value_schema,
    ) = if format == "avro" {
        (
            "demo_avro",
            vec![0, 0, 0, 0, 13, 14, 8, 77, 105, 110, 97],
            serde_json::json!({"id": 7, "name": "Mina"}),
            serde_json::json!({
                "type": "record",
                "fields": [
                    ["id", {"type": "long", "value": 7}],
                    ["name", {"type": "string", "value": "Mina"}]
                ]
            }),
            "confluent:https://registry.local#id=13",
            vec![0, 0, 0, 0, 14, 12, 65, 45, 49, 48, 52, 50],
            serde_json::json!({"id": 1042, "tags": ["demo", "avro"]}),
            serde_json::json!({
                "type": "record",
                "fields": [
                    ["id", {"type": "long", "value": 1042}],
                    ["tags", {"type": "array", "value": [
                        {"type": "string", "value": "demo"},
                        {"type": "string", "value": "avro"}
                    ]}]
                ]
            }),
            "confluent:https://registry.local#id=14",
        )
    } else {
        (
            "demo_protobuf",
            vec![0, 0, 0, 0, 20, 0, 8, 9, 18, 3, 90, 111, 101],
            serde_json::json!({"id": "9", "name": "Zoe"}),
            serde_json::json!({
                "type": "demo.Customer",
                "fields": [
                    {"name": "id", "number": 1, "datatype": "uint64", "extension": false, "value": {"type": "u64", "value": 9}},
                    {"name": "name", "number": 2, "datatype": "string", "extension": false, "value": {"type": "string", "value": "Zoe"}}
                ],
                "unknown_fields": []
            }),
            "confluent:https://registry.local#id=20&message=demo.Customer",
            vec![0, 0, 0, 0, 21, 0, 8, 128, 16],
            serde_json::json!({"id": "2048", "title": "Synthetic Protobuf event 2048"}),
            serde_json::json!({
                "type": "demo.Event",
                "fields": [
                    {"name": "id", "number": 1, "datatype": "uint64", "extension": false, "value": {"type": "u64", "value": 2048}},
                    {"name": "title", "number": 3, "datatype": "string", "extension": false, "value": {"type": "string", "value": "Synthetic Protobuf event 2048"}}
                ],
                "unknown_fields": []
            }),
            "confluent:https://registry.local#id=21&message=demo.Event",
        )
    };
    let mut columns = [
        ("offset", "integer"),
        ("timestamp_ms", "integer"),
        ("key", "bytes"),
        ("value", "bytes"),
        (
            "headers",
            "JSON (ordered header names and nullable byte arrays)",
        ),
    ]
    .into_iter()
    .map(|(name, datatype)| Column {
        name: name.into(),
        datatype: datatype.into(),
    })
    .collect::<Vec<_>>();
    for field in ["key", "value"] {
        columns.extend(
            [
                ("decoded", "JSON projection (not wire bytes)"),
                ("schema", "schema identity"),
                ("decode_error", "text"),
                ("native", "JSON typed inspection (not wire bytes)"),
                ("native_error", "text"),
            ]
            .into_iter()
            .map(|(suffix, datatype)| Column {
                name: format!("{field}_{suffix}"),
                datatype: datatype.into(),
            }),
        );
    }
    let mut app = synthetic_app(
        "demo_kafka",
        &KAFKA,
        Resource::new("kafka.records", vec![topic.into(), "0".into()]),
        columns,
        vec![Row {
            cells: vec![
                Some("1042".into()),
                Some("1789909200000".into()),
                Some(Value::Bytes(key_wire)),
                Some(Value::Bytes(value_wire)),
                Some(Value::Json(
                    serde_json::json!([{
                        "name": "content-type",
                        "value": b"application/octet-stream"
                    }])
                    .to_string(),
                )),
                Some(Value::Json(key_decoded.to_string())),
                Some(key_schema.into()),
                None,
                Some(Value::Json(key_native.to_string())),
                None,
                Some(Value::Json(value_decoded.to_string())),
                Some(value_schema.into()),
                None,
                Some(Value::Json(value_native.to_string())),
                None,
            ],
            target: None,
        }],
    );
    app.view.column = 6;
    app.act(Action::Open);
    app
}

fn postgres_replication() -> crate::App {
    let columns = [
        ("pid", "int4"),
        ("usesysid", "oid"),
        ("usename", "name"),
        ("application_name", "text"),
        ("client_addr", "inet"),
        ("client_hostname", "text"),
        ("client_port", "int4"),
        ("backend_start", "timestamptz"),
        ("backend_xmin", "xid"),
        ("state", "text"),
        ("sent_lsn", "pg_lsn"),
        ("write_lsn", "pg_lsn"),
        ("flush_lsn", "pg_lsn"),
        ("replay_lsn", "pg_lsn"),
        ("write_lag", "interval"),
        ("flush_lag", "interval"),
        ("replay_lag", "interval"),
        ("sync_priority", "int4"),
        ("sync_state", "text"),
        ("reply_time", "timestamptz"),
    ];
    let replica = |pid: &str,
                   application: &str,
                   address: &str,
                   state: &str,
                   lsn: &str,
                   sync_priority: &str,
                   sync_state: &str| Row {
        cells: [
            Some(pid),
            Some("16384"),
            Some("replicator"),
            Some(application),
            Some(address),
            None,
            Some("54324"),
            Some("2026-09-20 15:42:03+00"),
            None,
            Some(state),
            Some(lsn),
            Some(lsn),
            Some(lsn),
            Some(lsn),
            Some("00:00:00.001"),
            Some("00:00:00.002"),
            Some("00:00:00.003"),
            Some(sync_priority),
            Some(sync_state),
            Some("2026-09-20 18:42:03+00"),
        ]
        .into_iter()
        .map(|value| value.map(Value::from))
        .collect(),
        target: None,
    };
    let mut app = synthetic_app(
        "demo_pg",
        &POSTGRES,
        Resource::new("postgres.replication", vec![]),
        columns
            .into_iter()
            .map(|(name, datatype)| Column {
                name: name.into(),
                datatype: datatype.into(),
            })
            .collect(),
        vec![
            replica(
                "381",
                "orders-replica",
                "10.0.1.12",
                "streaming",
                "0/3A4F2C18",
                "1",
                "sync",
            ),
            replica(
                "407",
                "analytics-replica",
                "10.0.1.13",
                "streaming",
                "0/3A4F2B90",
                "0",
                "async",
            ),
        ],
    );
    app.view.page.notice = "Connected replication processes only, not HA membership. PostgreSQL may hide fields without pg_read_all_stats. Independent reads; no replication slots, promotion or configuration changes.".into();
    app.view.column = 9;
    app
}

fn postgres_wal_receiver() -> crate::App {
    let columns = [
        ("pid", "int4"),
        ("status", "text"),
        ("receive_start_lsn", "pg_lsn"),
        ("receive_start_tli", "int4"),
        ("written_lsn", "pg_lsn"),
        ("flushed_lsn", "pg_lsn"),
        ("received_tli", "int4"),
        ("last_msg_send_time", "timestamptz"),
        ("last_msg_receipt_time", "timestamptz"),
        ("latest_end_lsn", "pg_lsn"),
        ("latest_end_time", "timestamptz"),
        ("slot_name", "text"),
        ("sender_host", "text"),
        ("sender_port", "int4"),
        ("conninfo", "text"),
    ];
    let mut app = synthetic_app(
        "demo_pg",
        &POSTGRES,
        Resource::new("postgres.wal_receiver", vec![]),
        columns
            .into_iter()
            .map(|(name, datatype)| Column {
                name: name.into(),
                datatype: datatype.into(),
            })
            .collect(),
        vec![Row {
            cells: [
                Some("612"),
                Some("streaming"),
                Some("0/3A000000"),
                Some("1"),
                Some("0/3A4F2C18"),
                Some("0/3A4F2C18"),
                Some("1"),
                Some("2026-09-20 18:42:03+00"),
                Some("2026-09-20 18:42:03+00"),
                Some("0/3A4F2C18"),
                Some("2026-09-20 18:42:03+00"),
                Some("primary_slot"),
                Some("postgres-primary.internal"),
                Some("5432"),
                Some("user=replicator passfile=/run/secrets/pgpass sslmode=verify-full"),
            ]
            .into_iter()
            .map(|value| value.map(Value::from))
            .collect(),
            target: None,
        }],
    );
    app.view.page.notice = "Connected replication processes only, not HA membership. PostgreSQL may hide fields without pg_read_all_stats. Independent reads; no replication slots, promotion or configuration changes.".into();
    app.view.column = 1;
    app
}

fn kafka_groups() -> crate::App {
    let groups = [
        ("orders-api", "Stable", "consumer", "cooperative-sticky", 6),
        ("billing-workers", "Stable", "consumer", "range", 3),
        ("analytics-export", "Empty", "consumer", "", 0),
    ];
    let mut app = synthetic_app(
        "demo_kafka",
        &KAFKA,
        Resource::new("kafka.groups", vec![]),
        [
            ("name", "text"),
            ("state", "text"),
            ("protocol_type", "text"),
            ("protocol", "text"),
            ("members", "integer"),
            ("broker_id", "integer"),
            ("broker_host", "text"),
            ("broker_port", "integer"),
        ]
        .into_iter()
        .map(|(name, datatype)| Column {
            name: name.into(),
            datatype: datatype.into(),
        })
        .collect(),
        groups
            .into_iter()
            .map(|(name, state, protocol_type, protocol, members)| Row {
                cells: vec![
                    Some(name.into()),
                    Some(state.into()),
                    Some(protocol_type.into()),
                    Some(protocol.into()),
                    Some(members.to_string().into()),
                    Some("1".into()),
                    Some("127.0.0.1".into()),
                    Some("9092".into()),
                ],
                target: Some(Resource::new("kafka.group", vec![name.into()])),
            })
            .collect(),
    );
    app.view.page.notice = "Group metadata; assignments remain protocol bytes.".into();
    app
}

fn kafka_broker_config() -> crate::App {
    let row = |name: &str,
               value: Option<&str>,
               source: &str,
               is_default: bool,
               is_read_only: bool,
               is_sensitive: bool| Row {
        cells: vec![
            Some(name.into()),
            value.map(Value::from),
            Some(source.into()),
            Some(is_default.to_string().into()),
            Some(is_read_only.to_string().into()),
            Some(is_sensitive.to_string().into()),
            Some(Value::Json(
                serde_json::json!([{
                    "name": name,
                    "value": if is_sensitive { None } else { value },
                    "source": source
                }])
                .to_string(),
            )),
        ],
        target: None,
    };
    let mut app = synthetic_app(
        "demo_kafka",
        &KAFKA,
        Resource::new("kafka.broker_config", vec!["1".into()]),
        [
            ("name", "text"),
            ("value", "text"),
            ("source", "text"),
            ("is_default", "boolean"),
            ("is_read_only", "boolean"),
            ("is_sensitive", "boolean"),
            ("synonyms", "JSON (configuration precedence order)"),
        ]
        .into_iter()
        .map(|(name, datatype)| Column {
            name: name.into(),
            datatype: datatype.into(),
        })
        .collect(),
        vec![
            row(
                "advertised.listeners",
                Some("PLAINTEXT://broker:9092"),
                "STATIC_BROKER_CONFIG",
                false,
                true,
                false,
            ),
            row(
                "auto.create.topics.enable",
                Some("false"),
                "DEFAULT_CONFIG",
                true,
                false,
                false,
            ),
            row(
                "log.cleanup.policy",
                Some("delete"),
                "DEFAULT_CONFIG",
                true,
                false,
                false,
            ),
            row(
                "log.retention.hours",
                Some("168"),
                "DEFAULT_CONFIG",
                true,
                false,
                false,
            ),
            row(
                "num.partitions",
                Some("3"),
                "STATIC_BROKER_CONFIG",
                false,
                true,
                false,
            ),
            row(
                "ssl.keystore.password",
                None,
                "STATIC_BROKER_CONFIG",
                false,
                false,
                true,
            ),
        ],
    );
    app.view.page.notice = "Sensitive values withheld.".into();
    app.view.selected = 5;
    app
}

fn qdrant_consensus() -> crate::App {
    synthetic_app(
        "demo_qdrant",
        &QDRANT,
        Resource::new("qdrant.cluster", vec![]),
        [
            ("status", "text"),
            ("peer_id", "unsigned integer"),
            ("leader", "unsigned integer"),
            ("role", "text"),
            ("term", "unsigned integer"),
            ("commit", "unsigned integer"),
            ("pending_operations", "unsigned integer"),
            ("details", "JSON"),
        ]
        .into_iter()
        .map(|(name, datatype)| Column {
            name: name.into(),
            datatype: datatype.into(),
        })
        .collect(),
        vec![Row {
            cells: vec![
                Some("enabled".into()),
                Some("1".into()),
                Some("2".into()),
                Some("Follower".into()),
                Some("42".into()),
                Some("9007199254740993".into()),
                Some("0".into()),
                Some(Value::Json(
                    serde_json::json!({
                        "status": "enabled",
                        "peer_id": 1,
                        "peers": {
                            "1": {"uri": "http://qdrant-0:6335/"},
                            "2": {"uri": "http://qdrant-1:6335/"},
                            "3": {"uri": "http://qdrant-2:6335/"}
                        },
                        "raft_info": {
                            "term": 42,
                            "commit": 9007199254740993_u64,
                            "pending_operations": 0,
                            "leader": 2,
                            "role": "Follower"
                        }
                    })
                    .to_string(),
                )),
            ],
            target: None,
        }],
    )
}

fn dynamodb_items() -> crate::App {
    let mut app = synthetic_app(
        "demo_dynamodb",
        &DYNAMODB,
        Resource::new("dynamodb.items", vec!["demo_events".into()]),
        [
            "active", "binary", "customer", "pk", "precise", "sk", "tags",
        ]
        .map(|name| Column {
            name: name.into(),
            datatype: "DynamoDB AttributeValue (tagged JSON)".into(),
        })
        .into(),
        [
            ("customer-0", "0", "Mina", true, true),
            ("customer-0", "1", "Jose", true, false),
            ("customer-1", "0", "Zoe", false, true),
            ("customer-1", "1", "Sam", true, false),
        ]
        .into_iter()
        .map(|(pk, sk, name, active, binary)| Row {
            cells: vec![
                Some(Value::Json(serde_json::json!({"BOOL": active}).to_string())),
                binary.then(|| Value::Json(serde_json::json!({"B": "AP+AAA=="}).to_string())),
                Some(Value::Json(
                    serde_json::json!({"M": {"name": {"S": name}}}).to_string(),
                )),
                Some(Value::Json(serde_json::json!({"S": pk}).to_string())),
                Some(Value::Json(
                    serde_json::json!({"N": "12345678901234567890123456789012345678"}).to_string(),
                )),
                Some(Value::Json(serde_json::json!({"N": sk}).to_string())),
                Some(Value::Json(
                    serde_json::json!({"SS": ["demo", "priority"]}).to_string(),
                )),
            ],
            target: None,
        })
        .collect(),
    );
    app.view.page.notice = "{\"ConsumedCapacity\":null,\"Count\":4,\"ScannedCount\":4} | Independent reads, not a snapshot; attribute tags and decimal strings retained".into();
    app
}

fn dynamodb_stream_shards() -> crate::App {
    let stream =
        "arn:aws:dynamodb:eu-west-1:123456789012:table/demo_events/stream/2026-09-20T15:42:03.000";
    let mut app = synthetic_app(
        "demo_dynamodb",
        &DYNAMODB,
        Resource::new("dynamodb.shards", vec![stream.into()]),
        ["shard_id", "parent", "start_sequence", "end_sequence"]
            .map(|name| Column {
                name: name.into(),
                datatype: "text".into(),
            })
            .into(),
        [
            (
                "shardId-000000000001",
                None,
                "100000000000000000001",
                Some("199999999999999999999"),
            ),
            (
                "shardId-000000000002",
                Some("shardId-000000000001"),
                "200000000000000000000",
                None,
            ),
            (
                "shardId-000000000003",
                Some("shardId-000000000001"),
                "200000000000000000001",
                None,
            ),
        ]
        .into_iter()
        .map(|(id, parent, start, end)| Row {
            cells: vec![
                Some(id.into()),
                parent.map(Value::from),
                Some(start.into()),
                end.map(Value::from),
            ],
            target: Some(Resource::new(
                "dynamodb.records",
                vec![stream.into(), id.into()],
            )),
        })
        .collect(),
    );
    app.view.page.notice =
        "Independent Streams metadata reads; refresh to discover new shards".into();
    app
}

fn rabbitmq_queues() -> crate::App {
    let queues = [
        ("demo_quorum", "quorum", 128_u64, 4_u64, 3_u64),
        ("demo_stream", "stream", 2400, 0, 8),
        ("demo_records_000", "classic", 42, 2, 1),
        ("demo_records_001", "classic", 0, 0, 0),
    ];
    let mut app = synthetic_app(
        "demo_rabbitmq",
        &RABBITMQ,
        Resource::new("rabbitmq.queues", vec!["/".into()]),
        RABBITMQ_QUEUES
            .columns
            .iter()
            .map(|name| Column {
                name: (*name).into(),
                datatype: "JSON or text".into(),
            })
            .collect(),
        queues
            .into_iter()
            .map(|(name, kind, ready, unacknowledged, consumers)| {
                let details = serde_json::json!({
                    "name": name,
                    "vhost": "/",
                    "type": kind,
                    "state": "running",
                    "messages_ready": ready,
                    "messages_unacknowledged": unacknowledged,
                    "consumers": consumers,
                    "durable": true
                });
                Row {
                    cells: vec![
                        Some(name.into()),
                        Some("/".into()),
                        Some(kind.into()),
                        Some("running".into()),
                        Some(Value::Json(ready.to_string())),
                        Some(Value::Json(unacknowledged.to_string())),
                        Some(Value::Json(consumers.to_string())),
                        Some(Value::Json(details.to_string())),
                    ],
                    target: None,
                }
            })
            .collect(),
    );
    app.view.page.notice = "Management metadata and metrics only. Independent reads, not a snapshot. Enter shows all returned fields in details.".into();
    app.view.column = 4;
    app
}

fn render_app(app: &crate::App, label: &str, title: &str) -> String {
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
    terminal.draw(|frame| crate::ui::draw(frame, app)).unwrap();
    let theme = app.config.theme;
    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{}\" height=\"{}\" viewBox=\"0 0 {} {}\" role=\"img\" aria-label=\"{label}\">\n<title>{title}</title>\n<rect width=\"100%\" height=\"100%\" fill=\"{}\"/>\n<g font-family=\"'DejaVu Sans Mono',Consolas,monospace\" font-size=\"16\">\n",
        WIDTH * 10 + 32,
        HEIGHT * 22 + 32,
        WIDTH * 10 + 32,
        HEIGHT * 22 + 32,
        rgb(Color::Rgb(
            theme.palette().background[0],
            theme.palette().background[1],
            theme.palette().background[2]
        ))
    );
    for (y, row) in terminal
        .backend()
        .buffer()
        .content
        .chunks(WIDTH as usize)
        .enumerate()
    {
        let mut x = 0;
        for run in row.chunk_by(|a, b| a.fg == b.fg && a.bg == b.bg && a.modifier == b.modifier) {
            let cell = &run[0];
            // Fail if the sample or renderer needs SVG features this exporter does not support.
            assert!(
                (cell.modifier - (Modifier::BOLD | Modifier::REVERSED)).is_empty(),
                "unsupported SVG modifier: {:?}",
                cell.modifier
            );
            assert!(run.iter().all(|c| c.symbol().width() == 1));
            let text: String = run.iter().map(|c| c.symbol()).collect();
            let (left, top, width) = (16 + x * 10, 16 + y * 22, run.len() * 10);
            let (foreground, background) = if cell.modifier.contains(Modifier::REVERSED) {
                (cell.bg, cell.fg)
            } else {
                (cell.fg, cell.bg)
            };
            writeln!(
                svg,
                "<rect x=\"{left}\" y=\"{top}\" width=\"{width}\" height=\"22\" fill=\"{}\"/>",
                rgb(background)
            )
            .unwrap();
            if !text.trim().is_empty() {
                writeln!(svg, "<text x=\"{left}\" y=\"{}\" fill=\"{}\" font-weight=\"{}\" textLength=\"{width}\" lengthAdjust=\"spacingAndGlyphs\" xml:space=\"preserve\">{}</text>", top + 17, rgb(foreground), if cell.modifier.contains(Modifier::BOLD) { "bold" } else { "normal" }, escape(&text)).unwrap();
            }
            x += run.len();
        }
    }
    svg.push_str("</g>\n</svg>\n");
    svg
}

fn render(theme: Theme) -> String {
    let name = serde_json::to_value(theme)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    render_app(
        &app(theme),
        &format!("OneTUI {name} theme"),
        &format!("OneTUI {name} theme with synthetic customer data"),
    )
}

fn demos() -> [(String, &'static str); 13] {
    let browse = app(Theme::Catppuccin);
    let mut inspect = app(Theme::Catppuccin);
    inspect.act(Action::Open);
    let mut query = app(Theme::Catppuccin);
    query.act(Action::Query);
    query.query_editor = Some(crate::query::Editor::new(
        "SELECT id, name, country\nFROM demo.customers\nWHERE active\nORDER BY id".into(),
    ));
    let avro = decoded_message("avro");
    let protobuf = decoded_message("protobuf");
    let replication = postgres_replication();
    let wal_receiver = postgres_wal_receiver();
    let groups = kafka_groups();
    let broker_config = kafka_broker_config();
    let consensus = qdrant_consensus();
    let dynamodb = dynamodb_items();
    let shards = dynamodb_stream_shards();
    let rabbitmq = rabbitmq_queues();
    [
        (
            render_app(
                &browse,
                "OneTUI browsing rows",
                "Browse PostgreSQL rows in OneTUI",
            ),
            "browse.svg",
        ),
        (
            render_app(
                &inspect,
                "OneTUI inspecting a row",
                "Inspect row fields in OneTUI",
            ),
            "inspect.svg",
        ),
        (
            render_app(
                &query,
                "OneTUI query editor",
                "Edit a PostgreSQL query in OneTUI",
            ),
            "query.svg",
        ),
        (
            render_app(
                &avro,
                "OneTUI inspecting an Avro-decoded Kafka key",
                "Inspect an Avro-decoded Kafka key and its schema in OneTUI",
            ),
            "avro.svg",
        ),
        (
            render_app(
                &protobuf,
                "OneTUI inspecting a Protobuf-decoded Kafka key",
                "Inspect a Protobuf-decoded Kafka key and its schema in OneTUI",
            ),
            "protobuf.svg",
        ),
        (
            render_app(
                &replication,
                "OneTUI showing PostgreSQL replication state",
                "Inspect connected PostgreSQL WAL senders in OneTUI",
            ),
            "postgres-replication.svg",
        ),
        (
            render_app(
                &wal_receiver,
                "OneTUI showing the PostgreSQL WAL receiver",
                "Inspect the connected PostgreSQL WAL receiver in OneTUI",
            ),
            "postgres-wal-receiver.svg",
        ),
        (
            render_app(
                &groups,
                "OneTUI showing Kafka consumer groups",
                "Inspect Kafka consumer groups in OneTUI",
            ),
            "kafka-groups.svg",
        ),
        (
            render_app(
                &broker_config,
                "OneTUI showing Kafka broker configuration",
                "Inspect Kafka broker configuration in OneTUI",
            ),
            "kafka-broker-config.svg",
        ),
        (
            render_app(
                &consensus,
                "OneTUI showing Qdrant consensus state",
                "Inspect Qdrant consensus state in OneTUI",
            ),
            "qdrant-consensus.svg",
        ),
        (
            render_app(
                &dynamodb,
                "OneTUI browsing typed DynamoDB items",
                "Browse typed DynamoDB items in OneTUI",
            ),
            "dynamodb-items.svg",
        ),
        (
            render_app(
                &shards,
                "OneTUI showing DynamoDB stream shards",
                "Inspect DynamoDB stream shards in OneTUI",
            ),
            "dynamodb-stream-shards.svg",
        ),
        (
            render_app(
                &rabbitmq,
                "OneTUI showing RabbitMQ queue metrics",
                "Inspect RabbitMQ queue metrics in OneTUI",
            ),
            "rabbitmq-queues.svg",
        ),
    ]
}

fn rgb(color: Color) -> String {
    let Color::Rgb(r, g, b) = color else {
        panic!("Gallery requires explicit RGB colors: {color:?}")
    };
    format!("#{r:02x}{g:02x}{b:02x}")
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/assets/themes")
}

fn demo_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/assets/demo")
}

#[test]
fn snapshots_match_renderer() {
    assert_eq!(escape("a<&>b"), "a&lt;&amp;&gt;b");
    for theme in Theme::ALL {
        let name = serde_json::to_value(theme)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        let expected = std::fs::read_to_string(directory().join(format!("{name}.svg")))
            .expect("Missing theme preview. Run make theme-gallery");
        assert_eq!(
            expected,
            render(theme),
            "{name} preview is stale. Run make theme-gallery"
        );
    }
    for (expected, name) in demos() {
        let actual = std::fs::read_to_string(demo_directory().join(name))
            .expect("Missing demo screenshot. Run make theme-gallery");
        assert_eq!(actual, expected, "{name} is stale. Run make theme-gallery");
    }
}

#[test]
#[ignore = "regenerates committed theme previews. Run make theme-gallery"]
fn export() {
    std::fs::create_dir_all(directory()).unwrap();
    std::fs::create_dir_all(demo_directory()).unwrap();
    for theme in Theme::ALL {
        let name = serde_json::to_value(theme)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();
        std::fs::write(directory().join(format!("{name}.svg")), render(theme)).unwrap();
    }
    for (svg, name) in demos() {
        std::fs::write(demo_directory().join(name), svg).unwrap();
    }
}
