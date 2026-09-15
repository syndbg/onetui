use std::process::Command;

#[test]
fn schema_registers_the_builtin_and_configuration_without_resolving_secrets() {
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
        });
    assert!(binary.is_file(), "Use make build first");
    let output = Command::new(binary)
        .args(["schema", "--datasource", "rabbitmq"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let schema: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let provider = &schema["datasources"][0];
    assert_eq!(schema["datasources"].as_array().unwrap().len(), 1);
    assert_eq!(provider["entry_resource"], "rabbitmq.resources");
    assert_eq!(provider["resources"].as_array().unwrap().len(), 12);
    assert!(provider["query"].is_null());
    assert_eq!(provider["follow_resources"], serde_json::json!([]));
    assert_eq!(provider["configuration"]["url"]["required"], true);
    assert_eq!(provider["configuration"]["username_env"]["required"], true);
    assert_eq!(provider["configuration"]["password_env"]["required"], true);
    assert!(provider["configuration"]["ca_file"]["default"].is_null());
}
