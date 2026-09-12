use super::*;

#[tokio::test]
#[ignore = "requires seeded Redpanda Avro demo and generated local config"]
async fn demo_avro_config_browsing_and_live_traffic() {
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
    let registry = Resource::new("kafka.records", vec!["demo_avro".into(), "0".into()]);
    let catalog = Resource::new(
        "kafka.records",
        vec!["demo_avro_catalog".into(), "0".into()],
    );
    let first = fetch(&executor, registry.clone(), None).await;
    let raw = fetch(&executor, catalog.clone(), None).await;
    assert_eq!(first.rows.len(), 100);
    assert_eq!(raw.rows.len(), 100);
    assert_eq!(first.rows[2].cells[5], raw.rows[1].cells[5]);
    assert_eq!(raw.rows[1].cells[7], None);
    assert!(
        raw.rows[1].cells[6]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#schema=avro_event:")
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
    assert_eq!(live.rows[0].cells[7], None);
    assert_eq!(raw_live.rows[0].cells[7], None);
    assert_eq!(live.rows[0].cells[5], raw_live.rows[0].cells[5]);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}
