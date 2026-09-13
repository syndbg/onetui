#![cfg(unix)]

use std::{
    io::Write,
    process::{Child, Command, Output, Stdio},
    time::{Duration, Instant},
};

fn certificate_child(timeout: u64) -> (tempfile::TempDir, Child) {
    let directory = tempfile::tempdir().unwrap();
    let fifo = directory.path().join("ca.pem");
    assert!(
        Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let mut config = std::fs::File::create(directory.path().join("onetui.toml")).unwrap();
    writeln!(config, "[connections.test]\nkind='dynamodb'\nregion='us-east-1'\nendpoint_url='https://127.0.0.1:9'\naccess_key_id_env='ONETUI_TEST_KEY'\nsecret_access_key_env='ONETUI_TEST_SECRET'").unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
        });
    assert!(binary.is_file(), "Use make build first");
    let child = Command::new(binary)
        .args(["--check", "--connection", "test", "--config"])
        .arg(directory.path().join("onetui.toml"))
        .args(["--timeout", &timeout.to_string()])
        .env("ONETUI_TEST_KEY", "fixture-access-only")
        .env("ONETUI_TEST_SECRET", "fixture-secret-only")
        .env("SSL_CERT_FILE", fifo)
        .env("SSL_CERT_DIR", directory.path())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    (directory, child)
}

fn bounded_output(mut child: Child) -> Output {
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native trust loading prevented bounded CLI exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn native_trust_loading_obeys_the_request_deadline() {
    let (_directory, child) = certificate_child(1);
    let output = bounded_output(child);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));
}

#[test]
fn native_trust_loading_allows_sigint_and_terminal_exit() {
    use std::os::unix::fs::OpenOptionsExt;
    let (directory, mut child) = certificate_child(30);
    let deadline = Instant::now() + Duration::from_secs(3);
    let _writer = loop {
        if let Ok(writer) = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(directory.path().join("ca.pem"))
        {
            break writer;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native trust loader did not start");
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
    let output = bounded_output(child);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cancelled"));
}
