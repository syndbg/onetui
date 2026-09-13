use super::*;

#[tokio::test]
#[ignore = "reads the seeded NATS Avro registry demo and appends one disposable live message"]
async fn avro_registry_demo_browse_replay_and_follow() {
    let mut executor = demo_executor();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    executor.check(context).await.unwrap();
    let page = read(
        &executor,
        "nats.messages",
        &["DEMO_AVRO_REGISTRY"],
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
    let raw = page.rows[0].cells[data].as_ref().unwrap().bytes().to_vec();
    assert_eq!(&raw[5..], sample::message(1));
    let value: Json = serde_json::from_str(
        page.rows[0].cells[decoded]
            .as_ref()
            .unwrap()
            .text()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["id"], 1);
    let second = read(
        &executor,
        "nats.messages",
        &["DEMO_AVRO_REGISTRY"],
        page.continuation,
        false,
    )
    .await
    .unwrap();
    assert_eq!(second.rows.len(), 100);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    let replay = executor
        .query_page(
            onetui_core::provider::QueryRequest {
                page: PageRequest {
                    resource: Resource::new("nats.query", vec!["DEMO_AVRO_REGISTRY".into()]),
                    continuation: None,
                },
                text: r#"{"subject":"demo.avro.registry","start_sequence":2,"end_sequence":3}"#
                    .into(),
            },
            context,
        )
        .await
        .unwrap();
    assert_eq!(replay.rows.len(), 1);
    assert!(replay.rows[0].cells[decoded].is_some());
    let tail = read(
        &executor,
        "nats.messages",
        &["DEMO_AVRO_REGISTRY"],
        None,
        true,
    )
    .await
    .unwrap();
    let admin = admin().await;
    async_nats::jetstream::new(admin)
        .publish("demo.avro.registry", raw.clone().into())
        .await
        .unwrap()
        .await
        .unwrap();
    let live = read(
        &executor,
        "nats.messages",
        &["DEMO_AVRO_REGISTRY"],
        tail.continuation,
        true,
    )
    .await
    .unwrap();
    assert_eq!(live.rows[0].cells[data], Some(Value::Bytes(raw)));
    assert!(live.rows[0].cells[decoded].is_some());
    close(&mut executor).await;
}
use std::io::Write;
#[path = "../../examples/support/avro.rs"]
mod sample;

#[tokio::test]
#[ignore = "creates/deletes a disposable Avro catalog stream and local schemas"]
async fn avro_catalog_references_paging_replay_follow_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let root = r#"{"type":"record","name":"Event","fields":[{"name":"child","type":"Child"}]}"#;
    let child = r#"{"type":"record","name":"Child","fields":[{"name":"id","type":"long"}]}"#;
    std::fs::write(directory.path().join("event.avsc"), root).unwrap();
    std::fs::write(directory.path().join("child.avsc"), child).unwrap();
    let reader = json!({"type":"record","name":"Event","fields":[
        {"name":"child","type":serde_json::from_str::<Json>(child).unwrap()},
        {"name":"extra","type":"string","default":"reader default"}
    ]});
    std::fs::write(directory.path().join("reader.avsc"), reader.to_string()).unwrap();
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("AVRO_CATALOG_{}", std::process::id());
    let subject = format!("avro.catalog.{}", std::process::id());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .unwrap();
    let name = stream.clone();
    let tested = tokio::spawn(async move {
        let options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='avro'\nreader_schema_file={:?}\ncatalog={{directory={:?},schema='event',references=['child']}}", directory.path().join("reader.avsc"), directory.path())).unwrap();
        let configure = || NatsProvider.configure(&options, &|n| Some(if n == "U" { "fixture-reader" } else { "fixture-reader-only" }.into())).unwrap();
        let mut executor = configure();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        for _ in 0..105 { js.publish(subject.clone(), vec![84].into()).await.unwrap().await.unwrap(); }
        let page = read(&executor, "nats.messages", &[&name], None, false).await.unwrap();
        assert_eq!(page.rows.len(), 100);
        assert_eq!(page.rows[0].cells[3], Some(Value::Bytes(vec![84])));
        let value: Json = serde_json::from_str(page.rows[0].cells[5].as_ref().unwrap().text().unwrap()).unwrap();
        assert_eq!(value["child"]["id"], 42);
        assert_eq!(value["extra"], "reader default");
        let identity = page.rows[0].cells[6].as_ref().unwrap().text().unwrap();
        assert!(identity.contains("directory:") && identity.contains("#schema=event:") && identity.contains("reader="));
        assert!(page.rows[0].cells[8].is_some());
        let next = read(&executor, "nats.messages", &[&name], page.continuation.clone(), false).await.unwrap();
        assert_eq!(next.rows.len(), 5);
        assert_eq!(read(&executor, "nats.messages", &[&name], page.continuation.clone(), false).await.unwrap().rows[0].cells, next.rows[0].cells);
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor.query_page(onetui_core::provider::QueryRequest {
            page: PageRequest { resource: Resource::new("nats.query", vec![name.clone()]), continuation: None },
            text: json!({"subject":subject,"start_sequence":1,"end_sequence":2}).to_string(),
        }, context).await.unwrap();
        assert_eq!(replay.rows[0].cells, page.rows[0].cells);
        let tail = read(&executor, "nats.messages", &[&name], None, true).await.unwrap();
        js.publish(subject.clone(), vec![255].into()).await.unwrap().await.unwrap();
        js.publish(subject.clone(), vec![84].into()).await.unwrap().await.unwrap();
        let live = read(&executor, "nats.messages", &[&name], tail.continuation, true).await.unwrap();
        assert_eq!(live.columns.len(), tail.columns.len());
        assert_eq!(live.rows[0].cells[3], Some(Value::Bytes(vec![255])));
        assert!(live.rows[0].cells[7].is_some() && live.rows[1].cells[5].is_some());
        std::fs::write(directory.path().join("child.avsc"), b"broken").unwrap();
        assert_eq!(read(&executor, "nats.messages", &[&name], None, false).await.unwrap().rows[0].cells, page.rows[0].cells);
        close(&mut executor).await;
        let mut missing = configure();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        assert!(missing.check(context).await.is_err());
        std::fs::write(directory.path().join("child.avsc"), child).unwrap();
        let failed = read(&missing, "nats.messages", &[&name], None, false).await.unwrap();
        assert_eq!(failed.rows[0].cells[3], Some(Value::Bytes(vec![84])));
        assert!(failed.rows[0].cells[7].is_some());
        close(&mut missing).await;
        let mut reopened = configure();
        assert_eq!(read(&reopened, "nats.messages", &[&name], None, false).await.unwrap().rows[0].cells, page.rows[0].cells);
        close(&mut reopened).await;
    }).await;
    api(&admin, &format!("STREAM.DELETE.{stream}"), json!({})).await;
    tested.unwrap();
}

#[tokio::test]
#[ignore = "creates/deletes disposable Avro stream; retains raw records and native inspection"]
async fn avro_browse_replay_follow_errors_and_reader_schema() {
    let mut schema = tempfile::NamedTempFile::new().unwrap();
    schema.write_all(sample::SCHEMA.as_bytes()).unwrap();
    let mut reader = tempfile::NamedTempFile::new().unwrap();
    let mut schema_json: Json = serde_json::from_str(sample::SCHEMA).unwrap();
    schema_json["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"extra","type":"string","default":"new default"}));
    reader
        .write_all(schema_json.to_string().as_bytes())
        .unwrap();
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("AVRO_{}", std::process::id());
    let subject = format!("avro.{}", std::process::id());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .unwrap();
    let name = stream.clone();
    let tested = tokio::spawn(async move {
        let mut options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='avro'\nschema_file={:?}\nreader_schema_file={:?}", schema.path(), reader.path())).unwrap();
        let mut executor = NatsProvider.configure(&options, &|n| Some(if n == "U" { "fixture-reader" } else { "fixture-reader-only" }.into())).unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        let raw = sample::message(42);
        js.publish(subject.clone(), raw.clone().into()).await.unwrap().await.unwrap();
        js.publish(subject.clone(), vec![255].into()).await.unwrap().await.unwrap();
        let page = read(&executor, "nats.messages", &[&name], None, false).await.unwrap();
        assert_eq!(page.rows[0].cells[3], Some(Value::Bytes(raw)));
        let decoded: Json = serde_json::from_str(page.rows[0].cells[5].as_ref().unwrap().text().unwrap()).unwrap();
        assert_eq!(decoded["id"], 42);
        assert_eq!(decoded["extra"], "new default");
        assert!(page.rows[0].cells[8].is_some());
        assert!(page.rows[1].cells[7].is_some());
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let replay = executor.query_page(onetui_core::provider::QueryRequest {
            page: PageRequest { resource: Resource::new("nats.query", vec![name.clone()]), continuation: None },
            text: json!({"subject":subject,"start_sequence":1,"end_sequence":2}).to_string(),
        }, context).await.unwrap();
        assert_eq!(replay.rows[0].cells, page.rows[0].cells);
        assert_eq!(page.rows[1].cells[3], Some(Value::Bytes(vec![255])));
        let tail = read(&executor, "nats.messages", &[&name], None, true).await.unwrap();
        js.publish(subject.clone(), sample::message(43).into()).await.unwrap().await.unwrap();
        let live = read(&executor, "nats.messages", &[&name], tail.continuation, true).await.unwrap();
        assert_eq!(tail.columns.len(), live.columns.len());
        let quiet = read(&executor, "nats.messages", &[&name], live.continuation.clone(), true).await.unwrap();
        assert_eq!(quiet.columns.len(), live.columns.len());
        assert!(live.rows[0].cells[5].is_some());
        close(&mut executor).await;
        options["decoders"][0]["schema_file"] = "/missing/onetui-fixture-schema".into();
        let mut missing = NatsProvider.configure(&options, &|n| Some(if n == "U" { "fixture-reader" } else { "fixture-reader-only" }.into())).unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        assert!(missing.check(context).await.is_err());
        let raw_page = read(&missing, "nats.messages", &[&name], None, false).await.unwrap();
        assert!(raw_page.rows[0].cells[3].is_some() && raw_page.rows[0].cells[7].is_some());
        close(&mut missing).await;
    }).await;
    api(&admin, &format!("STREAM.DELETE.{stream}"), json!({})).await;
    tested.unwrap();
}
