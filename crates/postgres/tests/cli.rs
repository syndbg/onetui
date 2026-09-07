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

#[cfg(unix)]
fn certificate_child(explicit: bool, timeout: u64) -> (tempfile::TempDir, std::process::Child) {
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
    writeln!(
        config,
        "[connections.test]\nkind='postgres'\nurl_env='ONETUI_TEST_DSN'"
    )
    .unwrap();
    if explicit {
        writeln!(config, "ca_file='{}'", fifo.display()).unwrap();
    }
    let child = binary()
        .args(["--check", "--connection", "test", "--config"])
        .arg(directory.path().join("onetui.toml"))
        .args(["--timeout", &timeout.to_string()])
        .env("ONETUI_TEST_DSN", "host=localhost sslmode=require")
        .env("SSL_CERT_FILE", fifo)
        .env("SSL_CERT_DIR", directory.path())
        .stderr(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    (directory, child)
}

#[cfg(unix)]
fn bounded_output(mut child: std::process::Child) -> Output {
    let until = Instant::now() + Duration::from_secs(3);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= until {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("certificate loading prevented bounded CLI exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
#[cfg(unix)]
fn explicit_ca_rejects_fifo_without_waiting_for_a_writer() {
    let (_directory, child) = certificate_child(true, 30);
    let output = bounded_output(child);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("regular file"));
}

#[test]
#[cfg(unix)]
fn stalled_native_certificate_loading_obeys_request_deadline() {
    let (_directory, child) = certificate_child(false, 1);
    let output = bounded_output(child);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));
}

#[test]
#[cfg(unix)]
fn sigint_interrupts_stalled_native_certificate_loading() {
    use std::os::unix::fs::OpenOptionsExt;
    let (directory, mut child) = certificate_child(false, 30);
    let until = Instant::now() + Duration::from_secs(3);
    // A writer can open only once the real native-root loader is reading this FIFO.
    // Keep it open without bytes so the read stays blocked during SIGINT and exit.
    let _writer = loop {
        if let Ok(file) = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(nix::libc::O_NONBLOCK)
            .open(directory.path().join("ca.pem"))
        {
            break file;
        }
        if Instant::now() >= until {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("native trust loader did not open the FIFO");
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
