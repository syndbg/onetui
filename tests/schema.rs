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
        schema["configuration"]["display"]["defaults"],
        serde_json::to_value(onetui_core::value::DisplayOptions::default()).unwrap()
    );
    assert_eq!(
        schema["configuration"]["display"]["formats"],
        serde_json::to_value(onetui_core::value::FORMATS).unwrap()
    );
    assert_eq!(
        schema["configuration"]["display"]["fields"]["word_wrap"]["type"],
        "boolean"
    );
    assert_eq!(
        schema["datasources"][0]["resources"][0]["id"],
        "postgres.schemas"
    );
    assert_eq!(
        schema["datasources"][1]["resources"][0]["id"],
        "qdrant.collections"
    );
    assert_eq!(schema["shell"]["keybindings_configurable"], false);
    assert_eq!(schema["browsing_limits"]["page_bookmarks_per_view"], 4096);
    assert_eq!(
        schema["browsing_limits"]["page_bookmark_token_bytes_per_view"],
        1048576
    );
    assert_eq!(
        schema["configuration"]["theme"]["enum"],
        serde_json::json!(onetui_theme::Theme::ALL)
    );
    assert_eq!(schema["configuration"]["theme"]["default"], "catppuccin");
    assert_eq!(schema["configuration"]["theme"]["type"], "string");
    assert_eq!(schema["configuration"]["theme"]["required"], false);
    let themes = schema["shell"]["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|action| action["id"] == "themes")
        .expect("theme menu action");
    assert_eq!(themes["keys"], serde_json::json!(["T"]));
    for (id, key) in [
        ("page_up", "PageUp"),
        ("page_down", "PageDown"),
        ("half_page_up", "Ctrl-u"),
        ("half_page_down", "Ctrl-d"),
    ] {
        let action = schema["shell"]["actions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|action| action["id"] == id)
            .unwrap();
        assert_eq!(action["keys"], serde_json::json!([key]));
    }
    assert!(
        schema["configuration"]["theme"]["behavior"]
            .as_str()
            .unwrap()
            .contains("never writes configuration")
    );
    assert!(
        !schema["shell"]["action_context"]
            .as_str()
            .unwrap()
            .contains("Qdrant browsing is not implemented")
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("qdrant.points"));
    let example = schema["configuration"]["example_toml"].as_str().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), example).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_onetui"))
        .args([
            "--config",
            file.path().to_str().unwrap(),
            "--check",
            "--connection",
            "local_pg",
        ])
        .env_remove("ONETUI_POSTGRES_URL")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("ONETUI_POSTGRES_URL is missing"));
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
    assert_eq!(
        schema["datasources"][0]["entry_resource"],
        "postgres.schemas"
    );
    assert_eq!(
        schema["datasources"][0]["resources"][1]["actions"][0]["target"],
        "postgres.columns"
    );
    assert!(schema["datasources"][0]["session"].is_string());
    assert_eq!(
        schema["datasources"][0]["configuration"]["ca_file"]["max_bytes"],
        1048576
    );
}

#[test]
fn qdrant_catalog_filter() {
    let output = dump(&["--datasource", "qdrant"]);
    assert!(output.status.success());
    let schema: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(schema["datasources"].as_array().unwrap().len(), 1);
    assert_eq!(schema["datasources"][0]["id"], "qdrant");
    assert_eq!(
        schema["datasources"][0]["entry_resource"],
        "qdrant.collections"
    );
    assert_eq!(
        schema["datasources"][0]["resources"]
            .as_array()
            .unwrap()
            .len(),
        7
    );
    assert_eq!(schema["datasources"][0]["limits"]["rpc_bytes"], 1048576);
    assert!(schema["datasources"][0]["session"].is_string());
}
