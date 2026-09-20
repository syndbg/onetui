use super::*;

#[tokio::test]
#[ignore = "requires seeded Redpanda Protobuf demo and generated local config"]
async fn demo_protobuf_config_browsing_and_live_traffic() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config: toml::Value =
        toml::from_str(&std::fs::read_to_string(root.join("target/demo-onetui.toml")).unwrap())
            .unwrap();
    let mut options = config["connections"]["local_redpanda"]
        .as_table()
        .unwrap()
        .clone();
    options.remove("kind");
    let mut executor = KafkaProvider.configure(&options, &|_| None).unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let registry = Resource::new("kafka.records", vec!["demo_protobuf".into(), "0".into()]);
    let catalog = Resource::new(
        "kafka.records",
        vec!["demo_protobuf_catalog".into(), "0".into()],
    );
    let first = fetch(&executor, registry.clone(), None).await;
    assert_eq!(first.rows.len(), 100);
    assert_eq!(first.columns.len(), 15);
    assert_eq!(first.columns[5].name, "key_decoded");
    assert_eq!(first.rows[1].cells[12], None);
    let decoded: serde_json::Value =
        serde_json::from_str(first.rows[1].cells[10].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(decoded["title"], "Synthetic Protobuf event 1");
    assert!(decoded["note"].is_string());
    let key: serde_json::Value =
        serde_json::from_str(first.rows[1].cells[5].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(key["id"], "1");
    assert_eq!(key["name"], "customer-1");
    assert!(
        first.rows[1].cells[6]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("&message=demo.Customer")
    );
    assert!(first.rows[1].cells[7].is_none());
    assert!(first.rows[1].cells[8].is_some());
    assert!(first.rows[1].cells[9].is_none());
    let raw = fetch(&executor, catalog.clone(), None).await;
    assert_eq!(raw.rows[1].cells[5], first.rows[1].cells[10]);
    assert_eq!(raw.rows[1].cells[7], None);
    assert!(
        raw.rows[1].cells[6]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#schema=protobuf_event:")
    );
    let tail = follow(&executor, registry.clone(), None).await;
    let raw_tail = follow(&executor, catalog.clone(), None).await;
    let output = std::process::Command::new(root.join("target/debug/examples/seed_redpanda"))
        .args(["--traffic", "--count", "1"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let live = follow(&executor, registry, tail.continuation).await;
    let raw_live = follow(&executor, catalog, raw_tail.continuation).await;
    assert_eq!(live.rows.len(), 1);
    assert_eq!(raw_live.rows.len(), 1);
    assert!(live.rows[0].cells[5].is_some());
    assert!(live.rows[0].cells[7].is_none());
    assert!(live.rows[0].cells[10].is_some());
    assert!(live.rows[0].cells[12].is_none());
    assert_eq!(raw_live.rows[0].cells[7], None);
    assert_eq!(live.rows[0].cells[10], raw_live.rows[0].cells[5]);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}
