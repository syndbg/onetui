use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::pty::{Winsize, openpty};
use nix::sys::termios::{LocalFlags, tcgetattr};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Pty {
    child: Child,
    master: Option<std::fs::File>,
    output: Vec<u8>,
    width: u16,
}

impl Drop for Pty {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            // Closing our PTY handle lets macOS finish session teardown before reaping.
            self.master.take();
            let until = Instant::now() + Duration::from_secs(3);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

impl Pty {
    fn spawn(mut command: Command) -> (Self, std::fs::File) {
        let pair = openpty(
            &Winsize {
                ws_row: 40,
                ws_col: 180,
                ws_xpixel: 0,
                ws_ypixel: 0,
            },
            None,
        )
        .unwrap();
        let slave = std::fs::File::from(pair.slave);
        let master = std::fs::File::from(pair.master);
        let flags = OFlag::from_bits_truncate(fcntl(&master, FcntlArg::F_GETFL).unwrap());
        fcntl(&master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
        command
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()));
        // The child must own its controlling PTY, never the developer's terminal.
        unsafe {
            command.pre_exec(|| {
                if nix::libc::setsid() == -1
                    || nix::libc::ioctl(0, nix::libc::TIOCSCTTY as _, 0) == -1
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        (
            Self {
                child: command.spawn().unwrap(),
                master: Some(master),
                output: Vec::new(),
                width: 180,
            },
            slave,
        )
    }

    fn wait_token(&mut self, token: &str, timeout: Duration) {
        let until = Instant::now() + timeout;
        loop {
            self.read();
            if String::from_utf8_lossy(&self.output).contains(token) {
                return;
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "child exited before {token}"
            );
            assert!(
                Instant::now() < until,
                "child timed out before {token}: {}",
                String::from_utf8_lossy(&self.output)
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn send(&mut self, bytes: &[u8]) {
        self.output.clear();
        self.master.as_mut().unwrap().write_all(bytes).unwrap();
    }

    fn resize(&mut self) {
        // Real resize events force full frames, avoiding assertions against Ratatui diff fragments.
        self.width = if self.width == 180 { 181 } else { 180 };
        let size = Winsize {
            ws_row: 40,
            ws_col: self.width,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        assert_ne!(
            unsafe {
                nix::libc::ioctl(
                    self.master.as_ref().unwrap().as_raw_fd(),
                    nix::libc::TIOCSWINSZ as _,
                    &size,
                )
            },
            -1
        );
    }

    fn read(&mut self) {
        let mut buffer = [0; 16384];
        loop {
            match self.master.as_mut().unwrap().read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => self.output.extend_from_slice(&buffer[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) if error.raw_os_error() == Some(nix::libc::EIO) => break,
                Err(error) => panic!("PTY read failed: {error}"),
            }
        }
        assert!(self.output.len() <= 1024 * 1024, "unbounded PTY output");
    }

    fn wait(&mut self, expected: &[&str]) {
        let until = Instant::now() + Duration::from_secs(25);
        let mut redraw = Instant::now() + Duration::from_millis(150);
        loop {
            self.read();
            let raw = String::from_utf8_lossy(&self.output);
            let mut chars = raw.chars();
            let mut text = String::new();
            while let Some(c) = chars.next() {
                if c == '\x1b' {
                    if chars.next() == Some('[') {
                        for code in chars.by_ref() {
                            if ('@'..='~').contains(&code) {
                                break;
                            }
                        }
                    }
                } else if !c.is_whitespace() && !c.is_control() {
                    text.push(c);
                }
            }
            if expected.iter().all(|value| text.contains(value)) {
                return;
            }
            assert!(
                self.child.try_wait().unwrap().is_none(),
                "CLI exited before {expected:?}: {text}"
            );
            assert!(
                Instant::now() < until,
                "PTY timed out waiting for {expected:?}: {text}"
            );
            if Instant::now() >= redraw {
                self.output.clear();
                self.resize();
                redraw = Instant::now() + Duration::from_millis(150);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn open_filtered(&mut self, name: &str) {
        let mut keys = vec![b'/'];
        keys.extend([127; 64]);
        keys.extend_from_slice(name.as_bytes());
        keys.extend_from_slice(b"\r\r");
        self.send(&keys);
    }
}

pub(super) fn assert_avro_projection(mut options: toml::Table, topic: &str) {
    options.insert("kind".into(), "kafka".into());
    let mut config = tempfile::NamedTempFile::new().unwrap();
    let document = toml::Table::from_iter([(
        "connections".into(),
        toml::Value::Table(toml::Table::from_iter([(
            "kafka".into(),
            toml::Value::Table(options),
        )])),
    )]);
    write!(config, "{}", toml::to_string(&document).unwrap()).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/onetui"));
    let checked = Command::new(&binary)
        .arg("--config")
        .arg(config.path())
        .args(["--connection", "kafka", "--check"])
        .output()
        .unwrap();
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(config.path())
        .args(["--connection", "kafka"]);
    let (mut pty, _) = Pty::spawn(command);
    pty.wait(&["kafka.resources"]);
    pty.send(b"\r");
    pty.wait(&["kafka.topics"]);
    pty.open_filtered(topic);
    pty.wait(&["kafka.topic", "kafka.topic_config"]);
    pty.send(b"\r");
    pty.wait(&["kafka.partitions"]);
    pty.send(b"\r");
    pty.wait(&["kafka.records", "100shown/100loaded"]);
    pty.send(b"\r");
    pty.wait(&[
        "Rowdata",
        "10fields",
        "value_decoded",
        "avro:sha256:",
        "value_native",
    ]);
    pty.send(b"jjjjjjjj\r");
    pty.wait(&["writer", "reader", "long"]);
    pty.send(b"\x1b");
    pty.wait(&["Rowdata", "10fields"]);
    pty.send(b"kkkkk\r");
    pty.send(b":display format hex\r");
    pty.wait(&["00000000:0e"]);
    pty.send(b"q");
    pty.wait_token("\x1b[?1049l", Duration::from_secs(3));
    assert!(pty.child.wait().unwrap().success());
}

#[test]
#[ignore = "writes demo_live through the fixture producer; actual CLI follow and stop"]
fn actual_cli_kafka_live_follow_with_fixture_producer() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let producer_binary = root.join("target/debug/examples/produce_demo");
    let seed = Command::new(&producer_binary)
        .args(["--count", "1"])
        .output()
        .unwrap();
    assert!(
        seed.status.success(),
        "{}",
        String::from_utf8_lossy(&seed.stderr)
    );
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.kafka]\nkind='kafka'\nbootstrap_servers=['127.0.0.1:19092']\nsecurity_protocol='PLAINTEXT'").unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/onetui"));
    let mut command = Command::new(binary);
    command
        .arg("--config")
        .arg(config.path())
        .args(["--connection", "kafka"]);
    let (mut pty, _) = Pty::spawn(command);
    pty.wait(&[
        "kafka.resources",
        "kafka.topics",
        "kafka.brokers",
        "kafka.groups",
    ]);
    pty.send(b"\r");
    pty.wait(&["kafka.topics", "demo_live"]);
    pty.open_filtered("demo_live");
    pty.wait(&["kafka.topic", "kafka.topic_config"]);
    pty.send(b"\r");
    pty.wait(&["kafka.partitions", "1shown/1loaded"]);
    pty.send(b"\r");
    pty.wait(&["kafka.records", "Page1"]);
    pty.send(b"f");
    pty.wait(&["LIVE", "0retained"]);
    let started = Instant::now();
    let mut producer = Command::new(&producer_binary)
        .args(["--count", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pty.wait(&["LIVE", "2retained", "demo", "\"message\""]);
        assert!(
            started.elapsed() >= Duration::from_secs(15),
            "producer must not burst both records"
        );
        pty.send(b"\x03");
        pty.wait(&["Followingstopped", "2retained"]);
        pty.send(b"\r");
        pty.wait(&["Rowdata", "value", "headers", "demo", "\"message\""]);
        pty.send(b"jjj");
        pty.wait(&["Rowdata", "\"source\"", "onetuifixtureproducer"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.records", "Followingstopped"]);
        pty.send(b"r");
        pty.wait(&["Page1", "kafka.records"]);
        pty.send(b"f");
        pty.wait(&["LIVE", "0retained"]);
        pty.send(b"c");
        pty.wait(&["connections", "kafka"]);
        pty.send(b"q");
        pty.wait_token("\x1b[?1049l", Duration::from_secs(3));
    }));
    if result.is_err() {
        let _ = producer.kill();
    }
    let produced = producer.wait_with_output().unwrap();
    result.unwrap();
    assert!(
        produced.status.success(),
        "{}",
        String::from_utf8_lossy(&produced.stderr)
    );
    let output = String::from_utf8_lossy(&produced.stdout);
    assert_eq!(output.lines().count(), 2);
    assert!(output.contains("sequence=0") && output.contains("sequence=1"));
}

#[test]
#[ignore = "requires seeded Kafka and the built CLI; read-only, child-owned PTY"]
fn actual_cli_kafka_browsing_bookmarks_aliases_and_restore() {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.kafka]\nkind='kafka'\nbootstrap_servers=['127.0.0.1:19092']\nsecurity_protocol='PLAINTEXT'\n[connections.second]\nkind='kafka'\nbootstrap_servers=['127.0.0.1:19092']\nsecurity_protocol='PLAINTEXT'").unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
        });
    let mut command = Command::new(binary);
    command.arg("--config").arg(config.path());
    let (mut pty, slave) = Pty::spawn(command);
    pty.wait_token("\x1b[>1u", Duration::from_secs(3));
    pty.wait(&["connections", "kafka", "second"]);
    assert!(
        !tcgetattr(&slave)
            .unwrap()
            .local_flags
            .contains(LocalFlags::ICANON)
    );
    // Returning to the first alias must release the previous native owner.
    for alias in ["kafka", "second", "kafka"] {
        pty.open_filtered(alias);
        pty.wait(&[
            "kafka.resources",
            "kafka.topics",
            "kafka.brokers",
            "kafka.groups",
        ]);
        pty.send(b"j\r");
        pty.wait(&["kafka.brokers", "1shown/1loaded", "host", "port"]);
        pty.send(b"\r");
        pty.wait(&["kafka.broker_config", "source", "100shown/100loaded"]);
        pty.send(b"n");
        pty.wait(&["kafka.broker_config", "Page2"]);
        pty.send(b"p");
        pty.wait(&["kafka.broker_config", "Page1"]);
        pty.send(b"lllll");
        pty.wait(&["kafka.broker_config", "is_sensitive"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.brokers", "host", "port"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.resources", "kafka.groups"]);
        pty.send(b"k\r");
        pty.wait(&["kafka.topics", "demo_events"]);
        pty.open_filtered("demo_events");
        pty.wait(&["kafka.topic", "kafka.topic_config"]);
        pty.send(b"jj\r");
        pty.wait(&[
            "kafka.records",
            "partition",
            "Topic-wide",
            "3partitions",
            "100shown/100loaded",
        ]);
        for page in 2..=5 {
            pty.send(b"n");
            pty.wait(&["kafka.records", "Topic-wide", &format!("Page{page}")]);
        }
        for page in (1..5).rev() {
            pty.send(b"p");
            pty.wait(&["kafka.records", "Topic-wide", &format!("Page{page}")]);
        }
        pty.send(b"f");
        pty.wait(&["LIVE", "0retained", "3partitions"]);
        pty.send(b"\x03");
        pty.wait(&["Followingstopped"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.topic", "kafka.topic_config"]);
        pty.send(b"k");
        pty.send(b"\r");
        pty.wait(&["kafka.topic_config", "cleanup.policy", "source"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.topic", "kafka.partitions"]);
        pty.send(b"k\r");
        pty.wait(&["kafka.partitions", "3shown/3loaded"]);
        pty.send(b"\r");
        pty.wait(&["kafka.records", "100shown/100loaded", "Page1"]);
        for page in 2..=5 {
            pty.send(b"n");
            pty.wait(&["kafka.records", &format!("Page{page}")]);
        }
        for page in (1..5).rev() {
            pty.send(b"p");
            pty.wait(&["kafka.records", &format!("Page{page}")]);
        }
        pty.send(b"e");
        pty.wait(&["KafkareplayJSON", "retaineddata"]);
        pty.send(b"\x15{\"offset\":123,\"end_offset\":250}\r");
        pty.wait(&["kafka.query", "executed", "100shown/100loaded", "[123,250)"]);
        pty.send(b"n");
        pty.wait(&["kafka.query", "Page2", "27shown/27loaded", "[223,250)"]);
        pty.send(b"p");
        pty.wait(&["kafka.query", "Page1", "[123,250)"]);
        pty.send(b"e\x15{\"offset\":-1}\r");
        pty.wait(&["nonnegativesigned64-bitintegers", "retaineddata"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.query", "executed"]);
        pty.send(b"\x1b");
        pty.wait(&["kafka.records", "Page1"]);
        pty.send(b"\r");
        pty.wait(&["Rowdata", "timestamp_ms", "headers", "value"]);
        pty.send(b":display format hex\r");
        pty.wait(&["Rowdata", "00000000"]);
        pty.send(b"c");
        pty.wait(&["connections", "second"]);
    }
    pty.send(b"q");
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        pty.read();
        if let Some(status) = pty.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < until, "CLI failed to exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    pty.read();
    let output = String::from_utf8_lossy(&pty.output);
    for restored in ["\x1b[?1049l", "\x1b[?25h", "\x1b[?2004l", "\x1b[<1u"] {
        assert!(
            output.contains(restored),
            "terminal mode not restored: {restored:?}"
        );
    }
}
