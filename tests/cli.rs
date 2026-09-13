use std::process::Command;

fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_onetui"))
}

#[test]
fn publishing_is_not_a_cli_command() {
    let output = binary().args(["write", "kafka.publish"]).output().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("unrecognized subcommand 'write'"), "{error}");
}

#[test]
fn help_version_and_nonterminal_error_work_without_configuration() {
    for arg in ["--help", "--version"] {
        let output = binary().arg(arg).output().unwrap();
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("onetui"), "{text}");
        if arg == "--version" {
            assert_eq!(text.trim(), concat!("onetui ", env!("CARGO_PKG_VERSION")));
        } else {
            assert!(text.contains("Qdrant collections and points"), "{text}");
            assert!(
                text.contains("Kafka topics, partitions and read-committed records"),
                "{text}"
            );
            assert!(
                text.contains("payloads and vectors loaded on demand"),
                "{text}"
            );
            assert!(!text.contains("not implemented"), "{text}");
        }
    }
    let output = binary().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires a terminal"));
    assert!(!binary().arg("--check").output().unwrap().status.success());
}

#[test]
fn headless_check_rejects_invalid_theme_before_resolving_connection() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), "theme='private-invalid-theme'\n[connections.local_pg]\nkind='postgres'\nurl_env='ONETUI_TEST_MISSING_SECRET'").unwrap();
    let output = binary()
        .args([
            "--config",
            file.path().to_str().unwrap(),
            "--check",
            "--connection",
            "local_pg",
        ])
        .env_remove("ONETUI_TEST_MISSING_SECRET")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("invalid config"), "{error}");
    assert!(!error.contains("private-invalid-theme"));
    assert!(!error.contains("ONETUI_TEST_MISSING_SECRET"));
}
