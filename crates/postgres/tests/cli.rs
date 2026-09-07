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
fn postgres_dsn_errors_are_redacted() {
    let output = run_config(
        "[connections.test]\nkind='postgres'\nurl_env='ONETUI_TEST_DSN'",
        "test",
        &[("ONETUI_TEST_DSN", "fake-secret-do-not-print")],
    );
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-secret-do-not-print"));
}
#[test]
fn postgres_stalled_server_has_an_overall_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let dsn = format!(
        "host=127.0.0.1 port={} user=reader sslmode=disable",
        listener.local_addr().unwrap().port()
    );
    let start = Instant::now();
    let output = run_config(
        "[connections.test]\nkind='postgres'\nurl_env='ONETUI_TEST_DSN'",
        "test",
        &[("ONETUI_TEST_DSN", &dsn)],
    );
    assert!(!output.status.success());
    assert!(start.elapsed() < Duration::from_secs(4));
}
#[test]
#[cfg(unix)]
fn ctrl_c_interrupts_a_pending_check() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut config = tempfile::NamedTempFile::new().unwrap();
    writeln!(
        config,
        "[connections.test]\nkind='postgres'\nurl_env='ONETUI_TEST_DSN'"
    )
    .unwrap();
    let mut child = binary()
        .args(["--check", "--connection", "test", "--config"])
        .arg(config.path())
        .args(["--timeout", "30"])
        .env(
            "ONETUI_TEST_DSN",
            format!("host=127.0.0.1 port={port} user=reader sslmode=disable"),
        )
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let socket = loop {
        if let Ok((socket, _)) = listener.accept() {
            break socket;
        }
        if started.elapsed() > Duration::from_secs(4) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("check did not connect");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        Command::new("kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let interrupted = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if interrupted.elapsed() > Duration::from_secs(3) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("interruption did not stop the check");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(socket);
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cancelled"));
}
