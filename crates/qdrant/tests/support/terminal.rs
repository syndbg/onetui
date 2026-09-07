//! Qdrant-only CLI journey; keep the PTY helper local to this package.
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
    before: nix::sys::termios::Termios,
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
        let before = tcgetattr(&slave).unwrap();
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
                before,
            },
            slave,
        )
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
        let until = Instant::now() + Duration::from_secs(10);
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

pub fn journey(collection: &str) {
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.qd]\nkind='qdrant'\nurl='http://127.0.0.1:16334'\napi_key_env='ONETUI_QDRANT_API_KEY'").unwrap();
    let mut command = super::binary();
    command
        .arg("--config")
        .arg(config.path())
        .args(["--connection", "qd"])
        .env("ONETUI_QDRANT_API_KEY", "fixture-reader-only");
    let (mut pty, slave) = Pty::spawn(command);
    pty.wait(&["qdrant.collections", collection]);
    assert!(pty.before.local_flags.contains(LocalFlags::ICANON));
    assert!(
        !tcgetattr(&slave)
            .unwrap()
            .local_flags
            .contains(LocalFlags::ICANON)
    );
    drop(slave);
    pty.open_filtered(collection);
    pty.wait(&["qdrant.collection", "metadata", "points"]);
    pty.send(b"\r");
    pty.wait(&["qdrant.points", "100items", "numeric"]);
    pty.send(b"n");
    pty.wait(&["Page2|", "uuid", "18446744073709551615"]);
    pty.send(b"p");
    pty.wait(&["Page1|", "100items"]);
    pty.send(b"r");
    pty.wait(&["Page1|", "100items"]);
    pty.send(b"\r");
    pty.wait(&["qdrant.point", "payload", "vectors"]);
    pty.send(b"\r");
    pty.wait(&["qdrant.payload", "point-1"]);
    pty.send(b"\r");
    pty.wait(&["Field1/1", "point-1"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.payload", "point-1"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.point", "vectors"]);
    pty.send(b"j\r");
    pty.wait(&["qdrant.vectors", "dense", "(unnamed)"]);
    pty.send(b"lll\r");
    pty.wait(&["Field4/4", "[1.0,2.0,3.0]"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.vectors", "dense"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.point", "payload"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.points", "100items"]);
    pty.send(b":back\r");
    pty.wait(&["qdrant.collection", "metadata"]);
    pty.send(b"j\r");
    pty.wait(&["qdrant.metadata", "points_count(approximate)"]);
    pty.send(b"q");
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        pty.read();
        if let Some(status) = pty.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < until, "CLI did not quit");
        std::thread::sleep(Duration::from_millis(10));
    }
    pty.read();
    let output = String::from_utf8_lossy(&pty.output);
    assert!(output.contains("\x1b[?1049l") && output.contains("\x1b[?25h"));
}
