use super::*;

#[tokio::test]
#[ignore = "reads the seeded NATS Protobuf registry demo and follows a disposable Core subject"]
async fn protobuf_registry_demo_and_core_follow() {
    let mut executor = demo_executor();
    let page = read(
        &executor,
        "nats.messages",
        &["DEMO_PROTOBUF_REGISTRY"],
        None,
        false,
    )
    .await
    .unwrap();
    assert_eq!(page.rows.len(), 100);
    let data = page.columns.iter().position(|c| c.name == "data").unwrap();
    let decoded = page
        .columns
        .iter()
        .position(|c| c.name == "data_decoded")
        .unwrap();
    let native = page
        .columns
        .iter()
        .position(|c| c.name == "data_native")
        .unwrap();
    let raw = page.rows[0].cells[data].as_ref().unwrap().bytes().to_vec();
    assert_eq!(&raw[6..], sample::message(1));
    let value: Json = serde_json::from_str(
        page.rows[0].cells[decoded]
            .as_ref()
            .unwrap()
            .text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["id"], "1");
    let inspection: Json =
        serde_json::from_str(page.rows[0].cells[native].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(inspection["unknown_fields"][0]["number"], 99);
    close(&mut executor).await;
    let subject = format!("demo.proto_registry_{}", std::process::id());
    let mut core = configured(
        &format!(
            "servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=[{subject:?}]\n[[decoders]]\nsubject={subject:?}\nformat='protobuf'\nframing='confluent'\nregistry={{url='http://127.0.0.1:18081'}}\n"
        ),
        "fixture-reader",
        "fixture-reader-only",
    );
    let start = read(&core, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let admin = admin().await;
    admin
        .publish(subject.clone(), raw.clone().into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut cursor = start.continuation;
    loop {
        let page = read(&core, "nats.core_messages", &[&subject], cursor, true)
            .await
            .unwrap();
        assert_eq!(
            page.columns
                .iter()
                .map(|column| &column.name)
                .collect::<Vec<_>>(),
            start
                .columns
                .iter()
                .map(|column| &column.name)
                .collect::<Vec<_>>()
        );
        if let Some(row) = page.rows.first() {
            assert_eq!(row.cells[2], Some(Value::Bytes(raw)));
            assert!(row.cells[6].is_some());
            break;
        }
        cursor = page.continuation;
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    close(&mut core).await;
}
use std::io::Write;
#[path = "../../examples/support/protobuf.rs"]
mod sample;

#[path = "../support/schema_server.rs"]
#[allow(dead_code)]
mod schema_server;

#[tokio::test]
#[ignore = "uses a disposable Core subject and local Buf protocol fixture"]
async fn buf_descriptor_core_follow_and_reopen_cache() {
    const COMMIT: &str = "0123456789abcdef0123456789abcdef";
    let server = schema_server::Server::start_bytes(false, |request| {
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-buf-token")
        );
        match request.split_whitespace().nth(1).unwrap() {
            "/buf.registry.module.v1.CommitService/GetCommits" => (
                200,
                json!({"commits":[{"id":COMMIT}]}).to_string().into_bytes(),
            ),
            path => {
                assert_eq!(path, format!("/demo/events/descriptor/{COMMIT}"));
                (200, sample::schema())
            }
        }
    });
    let subject = format!("demo.buf_{}", std::process::id());
    let options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=[{subject:?}]\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='protobuf'\nmessage_name='demo.Event'\nbuf={{url={:?},module='demo/events',label='main',token_env='BUF_TOKEN'}}",server.url)).unwrap();
    let configure = || {
        NatsProvider
            .configure(&options, &|name| match name {
                "U" => Some("fixture-reader".into()),
                "P" => Some("fixture-reader-only".into()),
                "BUF_TOKEN" => Some("fixture-buf-token".into()),
                _ => None,
            })
            .unwrap()
    };
    let mut executor = configure();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let admin = admin().await;
    let raw = sample::message(42);
    admin
        .publish(subject.clone(), raw.clone().into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut cursor = start.continuation;
    loop {
        let page = read(&executor, "nats.core_messages", &[&subject], cursor, true)
            .await
            .unwrap();
        if let Some(row) = page.rows.first() {
            assert_eq!(row.cells[2], Some(Value::Bytes(raw)));
            let value: Json =
                serde_json::from_str(row.cells[6].as_ref().unwrap().text().unwrap()).unwrap();
            assert_eq!(value["id"], "42");
            assert!(
                row.cells[7]
                    .as_ref()
                    .unwrap()
                    .text()
                    .unwrap()
                    .contains(&format!("#commit={COMMIT}:"))
            );
            break;
        }
        cursor = page.continuation;
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    close(&mut executor).await;
    let mut reopened = configure();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    reopened.check(context).await.unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 4);
    close(&mut reopened).await;
}

#[tokio::test]
#[ignore = "uses a disposable Core subject and local Protobuf catalog with imports"]
async fn protobuf_catalog_imports_core_follow_and_cached_schema_errors() {
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::Type,
    };
    let directory = tempfile::tempdir().unwrap();
    let descriptor = FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("common.proto".into()),
                package: Some("demo".into()),
                syntax: Some("proto3".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Child".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("id".into()),
                        number: Some(1),
                        r#type: Some(Type::Int64 as i32),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            FileDescriptorProto {
                name: Some("event.proto".into()),
                package: Some("demo".into()),
                syntax: Some("proto3".into()),
                dependency: vec!["common.proto".into()],
                message_type: vec![DescriptorProto {
                    name: Some("Event".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("child".into()),
                        number: Some(1),
                        r#type: Some(Type::Message as i32),
                        type_name: Some(".demo.Child".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
    };
    let schema = directory.path().join("event.pb");
    std::fs::write(&schema, descriptor.encode_to_vec()).unwrap();
    let subject = format!("demo.proto_catalog_{}", std::process::id());
    let options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=[{subject:?}]\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='protobuf'\nmessage_name='demo.Event'\ncatalog={{directory={:?},schema='event'}}", directory.path())).unwrap();
    let configure = || {
        NatsProvider
            .configure(&options, &|n| {
                Some(
                    if n == "U" {
                        "fixture-reader"
                    } else {
                        "fixture-reader-only"
                    }
                    .into(),
                )
            })
            .unwrap()
    };
    let mut executor = configure();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let mut without_import = descriptor.clone();
    without_import.file.remove(0);
    std::fs::write(&schema, without_import.encode_to_vec()).unwrap();
    let mut missing = configure();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    assert!(missing.check(context).await.is_err());
    close(&mut missing).await;
    let admin = admin().await;
    let raw = vec![10, 2, 8, 42, 0x98, 0x06, 0x7b];
    admin
        .publish(subject.clone(), raw.clone().into())
        .await
        .unwrap();
    admin
        .publish(subject.clone(), vec![255].into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut cursor = start.continuation;
    let mut rows = Vec::new();
    while rows.len() < 2 {
        let page = read(&executor, "nats.core_messages", &[&subject], cursor, true)
            .await
            .unwrap();
        assert_eq!(page.columns.len(), start.columns.len());
        cursor = page.continuation;
        rows.extend(page.rows);
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(rows[0].cells[2], Some(Value::Bytes(raw)));
    let value: Json =
        serde_json::from_str(rows[0].cells[6].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(value["child"]["id"], "42");
    assert!(
        rows[0].cells[7]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#schema=event:")
    );
    let native: Json =
        serde_json::from_str(rows[0].cells[9].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(native["unknown_fields"][0]["number"], 99);
    assert_eq!(rows[1].cells[2], Some(Value::Bytes(vec![255])));
    assert!(rows[1].cells[8].is_some());
    close(&mut executor).await;
    std::fs::write(&schema, descriptor.encode_to_vec()).unwrap();
    let mut reopened = configure();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    reopened.check(context).await.unwrap();
    close(&mut reopened).await;
}

#[tokio::test]
#[ignore = "uses an explicit disposable Core subject and local Protobuf descriptor; no JetStream writes"]
async fn protobuf_core_bytes_unknown_fields_empty_malformed_and_bounds() {
    let mut schema = tempfile::NamedTempFile::new().unwrap();
    schema.write_all(&sample::schema()).unwrap();
    let subject = format!("demo.protobuf_{}", std::process::id());
    let options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=[{subject:?}]\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='protobuf'\nschema_file={:?}\nmessage_name='demo.Event'", schema.path())).unwrap();
    let mut executor = NatsProvider
        .configure(&options, &|n| {
            Some(
                if n == "U" {
                    "fixture-reader"
                } else {
                    "fixture-reader-only"
                }
                .into(),
            )
        })
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let admin = admin().await;
    let messages = [sample::message(42), vec![], vec![255], vec![0; 65537]];
    for bytes in &messages {
        admin
            .publish(subject.clone(), bytes.clone().into())
            .await
            .unwrap();
    }
    admin.flush().await.unwrap();
    let mut rows = Vec::new();
    let mut cursor = start.continuation;
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while rows.len() < 4 {
        let page = read(&executor, "nats.core_messages", &[&subject], cursor, true)
            .await
            .unwrap();
        assert!(page.bytes() <= onetui_core::PAGE_BYTES);
        assert_eq!(page.columns.len(), start.columns.len());
        cursor = page.continuation;
        rows.extend(page.rows);
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (row, raw) in rows.iter().zip(&messages) {
        assert_eq!(row.cells[2], Some(Value::Bytes(raw.clone())));
    }
    let json: Json =
        serde_json::from_str(rows[0].cells[6].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(json["id"], "42");
    let native: Json =
        serde_json::from_str(rows[0].cells[9].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(native["unknown_fields"][0]["number"], 99);
    assert!(rows[1].cells[6].is_some());
    assert!(rows[2].cells[8].is_some());
    assert!(
        rows[3].cells[8]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("64 KiB")
    );
    close(&mut executor).await;
}
