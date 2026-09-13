use super::*;
use onetui_core::Row;
use serde_json::json;
use std::time::Duration;

#[path = "../../tests/support/schema_server.rs"]
mod http;
#[path = "../../examples/support/protobuf.rs"]
mod sample;

fn binding(value: serde_json::Value) -> Binding {
    let binding = serde_json::from_value(value).unwrap();
    validate(std::slice::from_ref(&binding)).unwrap();
    binding
}

fn page(messages: &[Vec<u8>]) -> Page {
    Page {
        columns: vec![
            Column {
                name: "subject".into(),
                datatype: "text".into(),
            },
            Column {
                name: "data".into(),
                datatype: "bytes".into(),
            },
        ],
        rows: messages
            .iter()
            .map(|raw| Row {
                cells: vec![Some("demo.event".into()), Some(Value::Bytes(raw.clone()))],
                target: None,
            })
            .collect(),
        ..Page::default()
    }
}

async fn project(cache: &Arc<Cache>, page: Page, check: bool) -> Page {
    cache
        .apply(
            page,
            check,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap()
}

#[test]
fn remote_bindings_are_strict_offline_and_secrets_are_selected_alias_only() {
    let registry = json!({"subject":"demo.event", "format":"protobuf", "framing":"confluent", "registry":{"url":"https://registry.invalid", "token_env":"REGISTRY_TOKEN"}});
    binding(registry.clone());
    for extra in [
        json!({"message_name":"demo.Event"}),
        json!({"schema_file":"/event.pb"}),
        json!({"reader_schema_file":"/reader.avsc"}),
        json!({"framing":"raw"}),
    ] {
        let mut invalid = registry.clone();
        invalid
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(validate(&[serde_json::from_value(invalid).unwrap()]).is_err());
    }
    let buf = json!({"subject":"demo.event", "format":"protobuf", "message_name":"demo.Event", "buf":{"url":"https://buf.build", "module":"demo/events", "label":"main", "token_env":"BUF_TOKEN"}});
    binding(buf.clone());
    let options: toml::Table = toml::from_str("servers=['nats://127.0.0.1:14222']\ntls=false\n[[decoders]]\nsubject='demo.event'\nformat='protobuf'\nframing='confluent'\nregistry={url='https://registry.invalid',token_env='REGISTRY_TOKEN'}\n[[decoders]]\nsubject='demo.buf'\nformat='protobuf'\nmessage_name='demo.Event'\nbuf={url='https://buf.build',module='demo/events',label='main',token_env='BUF_TOKEN'}").unwrap();
    use onetui_core::provider::Provider;
    crate::NatsProvider.validate_config(&options).unwrap();
    let names = Mutex::new(Vec::new());
    crate::NatsProvider
        .configure(&options, &|name| {
            names.lock().unwrap().push(name.to_owned());
            Some(format!("secret-{name}"))
        })
        .unwrap();
    assert_eq!(*names.lock().unwrap(), ["REGISTRY_TOKEN", "BUF_TOKEN"]);
    assert!(crate::NatsProvider.configure(&options, &|_| None).is_err());
}

#[tokio::test]
async fn registry_avro_versions_reader_raw_errors_and_cache() {
    let server = http::Server::start(false, |request| {
        match request.split_whitespace().nth(1).unwrap() {
            "/schemas/types" => (200, r#"["AVRO","PROTOBUF"]"#.into()),
            "/schemas/ids/1" => (200, json!({"schema":r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#}).to_string()),
            "/schemas/ids/2" => (200, json!({"schema":r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}"#}).to_string()),
            _ => (404, "unknown schema".into()),
        }
    });
    let reader = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(reader.path(), r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"email","type":"string","default":"unset"}]}"#).unwrap();
    let binding = binding(
        json!({"subject":"demo.event", "format":"avro", "framing":"confluent", "registry":{"url":server.url},"reader_schema_file":reader.path()}),
    );
    let cache = Cache::new(vec![binding.clone()]);
    project(&cache, Page::default(), true).await;
    let raw = vec![
        vec![0, 0, 0, 0, 1, 84],
        vec![0, 0, 0, 0, 2, 86],
        vec![0, 0, 0, 0, 99],
        vec![255],
    ];
    let displayed = project(&cache, page(&raw), false).await;
    for (row, bytes) in displayed.rows.iter().zip(&raw) {
        assert_eq!(row.cells[1], Some(Value::Bytes(bytes.clone())));
    }
    let value: serde_json::Value =
        serde_json::from_str(displayed.rows[0].cells[2].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(value, json!({"id":42,"email":"unset"}));
    assert!(
        displayed.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#id=1&reader=")
    );
    assert!(displayed.rows[1].cells[2].is_some());
    assert!(
        displayed.rows[2].cells[4]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("unknown schema")
    );
    assert!(displayed.rows[3].cells[4].is_some());
    assert!(displayed.rows[0].cells[5].is_some());
    project(&cache, page(&raw), false).await;
    assert_eq!(server.requests.lock().unwrap().len(), 4);
    project(&Cache::new(vec![binding]), page(&raw[..1]), false).await;
    assert_eq!(server.requests.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn registry_protobuf_imports_indexes_unknown_fields_and_raw_bytes() {
    let server = http::Server::start(false, |request| {
        match request.split_whitespace().nth(1).unwrap() {
        "/schemas/ids/7" => (200, json!({"schemaType":"PROTOBUF","schema":"syntax='proto3'; package demo; import 'child.proto'; message Event {message Nested {Child child=1;}}", "references":[{"name":"child.proto","subject":"child","version":3}]}).to_string()),
        "/subjects/child/versions/3" => (200, json!({"schemaType":"PROTOBUF","schema":"syntax='proto3'; package demo; message Child {int64 id=1;}"}).to_string()),
        _ => (404,"not found".into())
    }
    });
    let cache = Cache::new(vec![binding(
        json!({"subject":"demo.event","format":"protobuf","framing":"confluent","registry":{"url":server.url}}),
    )]);
    // Confluent indexes [0, 0] select Event.Nested, followed by field 99.
    let raw = vec![
        vec![0, 0, 0, 0, 7, 4, 0, 0, 10, 2, 8, 42, 0x98, 6, 123],
        vec![0, 0, 0, 0, 7, 0xff],
    ];
    let displayed = project(&cache, page(&raw), false).await;
    assert_eq!(
        displayed.rows[0].cells[1],
        Some(Value::Bytes(raw[0].clone()))
    );
    let json: serde_json::Value =
        serde_json::from_str(displayed.rows[0].cells[2].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(json["child"]["id"], "42");
    let native: serde_json::Value =
        serde_json::from_str(displayed.rows[0].cells[5].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(native["unknown_fields"][0]["number"], 99);
    assert!(displayed.rows[1].cells[4].is_some());
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn buf_label_pins_descriptor_and_caches_projection_until_reconnect() {
    const COMMIT: &str = "0123456789abcdef0123456789abcdef";
    let server = http::Server::start_bytes(false, |request| {
        let path = request.split_whitespace().nth(1).unwrap();
        if path == "/buf.registry.module.v1.CommitService/GetCommits" {
            assert!(request.contains("labelName"));
            (
                200,
                json!({"commits":[{"id":COMMIT}]}).to_string().into_bytes(),
            )
        } else {
            assert_eq!(path, format!("/demo/events/descriptor/{COMMIT}"));
            (200, sample::schema())
        }
    });
    let binding = binding(
        json!({"subject":"demo.event","format":"protobuf","message_name":"demo.Event","buf":{"url":server.url,"module":"demo/events","label":"main"}}),
    );
    let cache = Cache::new(vec![binding.clone()]);
    let raw = vec![sample::message(42), vec![255], vec![]];
    let displayed = project(&cache, page(&raw), false).await;
    assert_eq!(
        displayed.rows[0].cells[1],
        Some(Value::Bytes(raw[0].clone()))
    );
    assert!(
        displayed.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains(&format!("#commit={COMMIT}:protobuf:sha256:"))
    );
    assert!(displayed.rows[0].cells[2].is_some());
    assert!(displayed.rows[1].cells[4].is_some());
    assert!(displayed.rows[2].cells[2].is_some());
    project(&cache, Page::default(), true).await;
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    project(&Cache::new(vec![binding]), page(&raw), false).await;
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn remote_errors_redact_credentials_and_expired_requests_do_no_io() {
    let server = http::Server::start(true, |_| (403, "Denied secret-registry-token".into()));
    let mut binding = binding(
        json!({"subject":"demo.event","format":"avro","framing":"confluent","registry":{"url":server.url,"ca_file":server.ca_file,"token_env":"TOKEN"}}),
    );
    binding
        .registry
        .as_mut()
        .unwrap()
        .resolve(&|_| Some("secret-registry-token".into()))
        .unwrap();
    let cache = Cache::new(vec![binding]);
    let raw = vec![vec![0, 0, 0, 0, 1, 84]];
    assert!(
        cache
            .apply(page(&raw), false, tokio::time::Instant::now())
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
    let displayed = project(&cache, page(&raw), false).await;
    let error = displayed.rows[0].cells[4].as_ref().unwrap().text().unwrap();
    assert!(
        error.contains("403")
            && error.contains("Denied")
            && !error.contains("secret-registry-token"),
        "{error}"
    );
    assert_eq!(
        displayed.rows[0].cells[1],
        Some(Value::Bytes(raw[0].clone()))
    );
}
