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

#[test]
#[ignore = "seeded disposable NATS; actual CLI paging, byte inspection, live producer and terminal restoration"]
fn actual_cli_nats_browsing_and_following() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.nats]\nkind='nats'\nservers=['nats://127.0.0.1:14222']\ntls=false\nusername_env='NATS_USER'\npassword_env='NATS_PASS'\n[connections.second]\nkind='nats'\nservers=['nats://127.0.0.1:14222']\ntls=false\nusername_env='NATS_USER'\npassword_env='NATS_PASS'").unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/onetui"));
    // Keep the PTY owner alive until the parent checks restored terminal flags.
    let mut command = Command::new("sh");
    command
        .args(["-c", "\"$1\" --config \"$2\"; status=$?; printf '\\nONETUI_DONE\\n'; read -r finish; exit \"$status\"", "nats-pty"])
        .arg(binary)
        .arg(config.path())
        .env("NATS_USER", "fixture-reader")
        .env("NATS_PASS", "fixture-reader-only");
    let (mut pty, slave) = Pty::spawn(command);
    pty.wait(&["connections", "nats", "second"]);
    assert!(
        !tcgetattr(&slave)
            .unwrap()
            .local_flags
            .contains(LocalFlags::ICANON)
    );
    for alias in ["nats", "second"] {
        pty.open_filtered(alias);
        pty.wait(&["nats.resources", "Streams"]);
        pty.open_filtered("Streams");
        pty.wait(&["nats.streams", "DEMO_EVENTS"]);
        pty.open_filtered("DEMO_EVENTS");
        pty.wait(&["nats.messages", "100shown/100loaded", "Page1"]);
        for page in 2..=5 {
            pty.send(b"n");
            pty.wait(&["nats.messages", &format!("Page{page}")]);
        }
        for page in (1..5).rev() {
            pty.send(b"p");
            pty.wait(&["nats.messages", &format!("Page{page}")]);
        }
        pty.send(b"\r");
        pty.wait(&["Rowdata", "sequence", "subject", "data", "headers"]);
        pty.send(b"jjj\r");
        pty.wait(&["Field4/5", "category"]);
        pty.send(b":display format hex\r");
        pty.wait(&["00000000"]);
        pty.send(b"c");
        pty.wait(&["connections", "second"]);
    }
    pty.open_filtered("nats");
    pty.wait(&["nats.resources", "Streams"]);
    pty.open_filtered("Streams");
    pty.wait(&["nats.streams"]);
    pty.open_filtered("DEMO_LIVE");
    pty.wait(&["nats.messages", "Page1"]);
    pty.send(b"f");
    pty.wait(&["LIVE", "0retained"]);
    let started = Instant::now();
    let mut producer = Command::new(root.join("target/debug/examples/produce_nats"))
        .args(["--count", "2"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let tested = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pty.wait(&["LIVE", "2retained", "message"]);
        assert!(started.elapsed() >= Duration::from_secs(15));
        pty.send(b"\x03");
        pty.wait(&["Followingstopped", "2retained"]);
        pty.send(b"c");
        pty.wait(&["connections"]);
        pty.send(b"q");
        pty.wait_token("\x1b[?1049l", Duration::from_secs(3));
        pty.wait_token("ONETUI_DONE", Duration::from_secs(3));
    }));
    if tested.is_err() {
        let _ = producer.kill();
    }
    let produced = producer.wait_with_output().unwrap();
    tested.unwrap();
    assert!(
        produced.status.success(),
        "{}",
        String::from_utf8_lossy(&produced.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&produced.stdout).lines().count(), 2);
    assert!(
        tcgetattr(&slave)
            .unwrap()
            .local_flags
            .contains(LocalFlags::ICANON)
    );
    pty.send(b"\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "disposable Core subject; actual CLI subscribe, inspect, unsubscribe and terminal restoration"]
async fn actual_cli_core_subscription_stops_when_inspecting() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let subject = format!("core.pty.{}", std::process::id());
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.core]\nkind='nats'\nservers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=['{subject}']\nusername_env='NATS_USER'\npassword_env='NATS_PASS'").unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/onetui"));
    let mut command = Command::new("sh");
    command.args(["-c", "\"$1\" --config \"$2\"; status=$?; printf '\\nONETUI_DONE\\n'; read -r finish; exit \"$status\"", "core-pty"])
        .arg(binary).arg(config.path()).env("NATS_USER", "fixture-admin").env("NATS_PASS", "fixture-admin-only");
    let (mut pty, slave) = Pty::spawn(command);
    pty.wait(&["connections"]);
    pty.open_filtered("core");
    pty.wait(&["nats.resources", "Subjects"]);
    pty.open_filtered("Subjects");
    pty.wait(&["nats.subjects", &subject]);
    pty.send(b"\r");
    pty.wait(&["nats.core_messages", "0shown/0loaded"]);
    assert_eq!(super::core::subscriptions(&subject), 0);
    pty.send(b"f");
    pty.wait(&["LIVE", "0retained"]);
    assert_eq!(super::core::subscriptions(&subject), 1);
    let admin = super::admin().await;
    admin
        .publish(subject.clone(), r#"{"event":"core-inspect"}"#.into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    pty.wait(&["LIVE", "1retained", "core-inspect"]);
    pty.send(b"\r");
    pty.wait(&["Rowdata", "Followingstopped", "data"]);
    let deadline = Instant::now() + Duration::from_secs(3);
    while super::core::subscriptions(&subject) != 0 {
        assert!(Instant::now() < deadline, "inspect did not unsubscribe");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pty.send(b"q");
    pty.wait_token("\x1b[?1049l", Duration::from_secs(3));
    pty.wait_token("ONETUI_DONE", Duration::from_secs(3));
    assert!(
        tcgetattr(&slave)
            .unwrap()
            .local_flags
            .contains(LocalFlags::ICANON)
    );
    pty.send(b"\n");
}
