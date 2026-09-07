use std::io::Write;
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn binary() -> Command {
    let path = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
        });
    assert!(
        path.is_file(),
        "Build the CLI first with make build, or set ONETUI_TEST_BIN"
    );
    Command::new(path)
}

fn run_config(config: &str, alias: &str, env: &[(&str, &str)]) -> Output {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(config.as_bytes()).unwrap();
    binary()
        .args(["--check", "--connection", alias, "--config"])
        .arg(file.path())
        .args(["--timeout", "1"])
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

#[test]
fn qdrant_config_errors_are_redacted() {
    for (config, env) in [
        (
            "[connections.test]\nkind='qdrant'\nurl='https://user:fake-secret-do-not-print@localhost'",
            vec![],
        ),
        (
            "[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:6334'\napi_key_env='ONETUI_TEST_KEY'",
            vec![("ONETUI_TEST_KEY", "fake-secret-do-not-print\n")],
        ),
        (
            "[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:6334'\napi_key='fake-secret-do-not-print'",
            vec![],
        ),
    ] {
        let output = run_config(config, "test", &env);
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-secret-do-not-print"));
    }
}
#[test]
fn qdrant_stalled_server_has_an_overall_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let config = format!(
        "[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:{}'",
        listener.local_addr().unwrap().port()
    );
    let start = Instant::now();
    let output = run_config(&config, "test", &[]);
    assert!(!output.status.success());
    assert!(start.elapsed() < Duration::from_secs(4));
}
