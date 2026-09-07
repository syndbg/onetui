use std::io::Write;
use std::net::TcpListener;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

fn binary() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_bpearl"));
    cmd.env_remove("BPEARL_TEST_DSN")
        .env_remove("BPEARL_TEST_KEY");
    cmd
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
fn help_version_and_unsupported_ui_work_without_configuration() {
    for arg in ["--help", "--version"] {
        assert!(binary().arg(arg).output().unwrap().status.success());
    }
    let output = binary().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("TUI is not implemented"));
    assert!(!binary().arg("--check").output().unwrap().status.success());
}

#[test]
fn config_and_dsn_errors_do_not_expose_secrets() {
    for (config, env) in [
        (
            "[connections.test]\nkind='postgres'\nurl_env='BPEARL_TEST_DSN'",
            vec![("BPEARL_TEST_DSN", "fake-secret-do-not-print")],
        ),
        (
            "[connections.test]\nkind='qdrant'\nurl='https://user:fake-secret-do-not-print@localhost'",
            vec![],
        ),
        (
            "[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:6334'\napi_key_env='BPEARL_TEST_KEY'",
            vec![("BPEARL_TEST_KEY", "fake-secret-do-not-print\n")],
        ),
        (
            "[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:6334'\napi_key='fake-secret-do-not-print'",
            vec![],
        ),
    ] {
        let output = run_config(config, "test", &env);
        assert!(!output.status.success());
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!combined.contains("fake-secret-do-not-print"), "{combined}");
    }
}

#[test]
fn stalled_servers_are_bounded_by_the_overall_deadline() {
    // A listening socket that never completes either protocol, with no external server needed.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let pg = format!("host=127.0.0.1 port={port} user=reader sslmode=disable");
    let q = format!("[connections.test]\nkind='qdrant'\nurl='http://127.0.0.1:{port}'");
    for (config, env) in [
        (
            "[connections.test]\nkind='postgres'\nurl_env='BPEARL_TEST_DSN'",
            vec![("BPEARL_TEST_DSN", pg.as_str())],
        ),
        (q.as_str(), vec![]),
    ] {
        let start = Instant::now();
        let output = run_config(config, "test", &env);
        assert!(!output.status.success());
        assert!(start.elapsed() < Duration::from_secs(4));
    }
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
        "[connections.test]\nkind='postgres'\nurl_env='BPEARL_TEST_DSN'"
    )
    .unwrap();
    let mut child = binary()
        .args(["--check", "--connection", "test", "--config"])
        .arg(config.path())
        .args(["--timeout", "30"])
        .env(
            "BPEARL_TEST_DSN",
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
