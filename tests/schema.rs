use std::process::Command;

fn dump(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_onetui"))
        .arg("schema")
        .args(args)
        .env_remove("HOME")
        .env_remove("XDG_CONFIG_HOME")
        .env_remove("ONETUI_POSTGRES_URL")
        .env_remove("ONETUI_QDRANT_API_KEY")
        .current_dir(tempfile::tempdir().unwrap().path())
        .output()
        .unwrap()
}

#[test]
fn catalog_is_offline_deterministic_and_reports_only_implemented_resources() {
    let output = dump(&[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, dump(&[]).stdout);
    let explicit = Command::new(env!("CARGO_BIN_EXE_onetui"))
        .args(["--config", "/nonexistent/onetui.toml", "schema"])
        .output()
        .unwrap();
    assert!(explicit.status.success());
    assert_eq!(output.stdout, explicit.stdout);
    let schema: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(schema["schema_format_version"], 1);
    assert_eq!(
        schema["datasources"][0]["resources"][0]["id"],
        "postgres.schemas"
    );
    assert_eq!(schema["datasources"][1]["resources"], serde_json::json!([]));
    assert_eq!(schema["shell"]["keybindings_configurable"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("qdrant.points"));
    let example = schema["configuration"]["example_toml"].as_str().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), example).unwrap();
    let config = onetui_core::config::Config::load(file.path()).unwrap();
    assert!(
        config
            .resolve("local_pg", |_| Some("host=localhost".into()))
            .is_ok()
    );
}

#[test]
fn datasource_filter_is_strict_and_schema_cannot_be_combined_with_check() {
    assert!(!dump(&["--datasource", "unknown"]).status.success());
    assert!(
        !Command::new(env!("CARGO_BIN_EXE_onetui"))
            .args(["--check", "--connection", "local_pg", "schema"])
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn postgres_catalog_filter() {
    let output = dump(&["--datasource", "postgres"]);
    assert!(output.status.success());
    let schema: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(schema["datasources"].as_array().unwrap().len(), 1);
    assert_eq!(schema["datasources"][0]["id"], "postgres");
}

#[test]
fn qdrant_catalog_filter() {
    let output = dump(&["--datasource", "qdrant"]);
    assert!(output.status.success());
    let schema: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(schema["datasources"].as_array().unwrap().len(), 1);
    assert_eq!(schema["datasources"][0]["id"], "qdrant");
}
