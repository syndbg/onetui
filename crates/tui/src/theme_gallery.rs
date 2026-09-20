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
static KAFKA: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &["kafka.records"],
    query: None,
    kind: "kafka",
    entry_resource: Some("kafka.records"),
    browsing: "Kafka records",
    resources: &[&KAFKA_RECORDS],
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
    let (topic, wire, decoded, native, schema) = if format == "avro" {
        (
            "demo_avro",
            vec![0, 0, 0, 0, 14, 12, 65, 45, 49, 48, 52, 50],
            serde_json::json!({"order_id": "A-1042"}),
            serde_json::json!({
                "type": "record",
                "fields": [["order_id", {"type": "string", "value": "A-1042"}]]
            }),
            "confluent:https://registry.local#id=14",
        )
    } else {
        (
            "demo_protobuf",
            vec![0, 0, 0, 0, 21, 0, 10, 6, 80, 45, 50, 48, 52, 56],
            serde_json::json!({"orderId": "P-2048"}),
            serde_json::json!({
                "type": "demo.Order",
                "fields": [{
                    "name": "order_id",
                    "number": 1,
                    "datatype": "string",
                    "extension": false,
                    "value": {"type": "string", "value": "P-2048"}
                }],
                "unknown_fields": []
            }),
            "confluent:https://registry.local#id=21&message=demo.Order",
        )
    };
    let mut app = synthetic_app(
        "demo_kafka",
        &KAFKA,
        Resource::new("kafka.records", vec![topic.into(), "0".into()]),
        [
            ("offset", "integer"),
            ("timestamp_ms", "integer"),
            ("key", "bytes"),
            ("value", "bytes"),
            (
                "headers",
                "JSON (ordered header names and nullable byte arrays)",
            ),
            ("value_decoded", "JSON projection (not wire bytes)"),
            ("value_schema", "schema identity"),
            ("value_decode_error", "text"),
            ("value_native", "JSON typed inspection (not wire bytes)"),
            ("value_native_error", "text"),
        ]
        .into_iter()
        .map(|(name, datatype)| Column {
            name: name.into(),
            datatype: datatype.into(),
        })
        .collect(),
        vec![Row {
            cells: vec![
                Some("1042".into()),
                Some("1789909200000".into()),
                Some(Value::Bytes(b"order-1042".to_vec())),
                Some(Value::Bytes(wire)),
                Some(Value::Json(
                    serde_json::json!([{
                        "name": "content-type",
                        "value": b"application/octet-stream"
                    }])
                    .to_string(),
                )),
                Some(Value::Json(decoded.to_string())),
                Some(schema.into()),
                None,
                Some(Value::Json(native.to_string())),
                None,
            ],
            target: None,
        }],
    );
    app.view.column = 5;
    app.act(Action::Open);
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

fn demos() -> [(String, &'static str); 6] {
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
    let consensus = qdrant_consensus();
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
                "OneTUI inspecting decoded Avro",
                "Inspect decoded Avro and its schema in OneTUI",
            ),
            "avro.svg",
        ),
        (
            render_app(
                &protobuf,
                "OneTUI inspecting decoded Protobuf",
                "Inspect decoded Protobuf and its schema in OneTUI",
            ),
            "protobuf.svg",
        ),
        (
            render_app(
                &consensus,
                "OneTUI showing Qdrant consensus state",
                "Inspect Qdrant consensus state in OneTUI",
            ),
            "qdrant-consensus.svg",
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
