use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::pty::{Winsize, openpty};
use nix::sys::termios::{LocalFlags, tcgetattr};

struct Terminal {
    child: Child,
    master: Option<std::fs::File>,
    output: Vec<u8>,
    width: u16,
}

impl Drop for Terminal {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            self.master.take();
            let until = Instant::now() + Duration::from_secs(3);
            while self.child.try_wait().ok().flatten().is_none() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

impl Terminal {
    fn read(&mut self) {
        let mut bytes = [0; 8192];
        while let Ok(size) = self.master.as_mut().unwrap().read(&mut bytes) {
            if size == 0 {
                break;
            }
            self.output.extend_from_slice(&bytes[..size]);
        }
    }
    fn send(&mut self, bytes: &[u8]) {
        self.output.clear();
        self.master.as_mut().unwrap().write_all(bytes).unwrap();
    }
    fn wait(&mut self, token: &str) {
        let until = Instant::now() + Duration::from_secs(5);
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
                "missing {token}: {}",
                String::from_utf8_lossy(&self.output)
            );
            // Force complete frames instead of matching fragments from Ratatui's diff output.
            self.width = if self.width == 180 { 181 } else { 180 };
            let size = Winsize {
                ws_row: 40,
                ws_col: self.width,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            unsafe {
                nix::libc::ioctl(
                    self.master.as_ref().unwrap().as_raw_fd(),
                    nix::libc::TIOCSWINSZ,
                    &size,
                );
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[test]
#[ignore = "requires disposable RabbitMQ fixture and built OneTUI binary"]
fn actual_cli_metadata_details_paging_and_terminal_restore() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(config, "[connections.rabbit]\nkind='rabbitmq'\nurl='http://127.0.0.1:15672'\nusername_env='ONETUI_FIXTURE_USER'\npassword_env='ONETUI_FIXTURE_PASS'\n").unwrap();
    let pair = openpty(
        Some(&Winsize {
            ws_row: 40,
            ws_col: 180,
            ws_xpixel: 0,
            ws_ypixel: 0,
        }),
        None,
    )
    .unwrap();
    let slave = std::fs::File::from(pair.slave);
    let before = tcgetattr(&slave).unwrap();
    let master = std::fs::File::from(pair.master);
    let flags = OFlag::from_bits_truncate(fcntl(&master, FcntlArg::F_GETFL).unwrap());
    fcntl(&master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
    let binary = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/onetui"));
    // Keep the PTY owner alive until restored terminal flags have been inspected.
    let mut command = Command::new("sh");
    command
        .args(["-c", "\"$1\" --config \"$2\"; status=$?; printf '\\nONETUI_DONE\\n'; read -r finish; exit \"$status\"", "rabbitmq-pty"])
        .arg(binary).arg(config.path())
        .env("TERM", "xterm-256color")
        .env("ONETUI_FIXTURE_USER", "fixture-reader")
        .env("ONETUI_FIXTURE_PASS", "fixture-reader-only")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave.try_clone().unwrap()));
    unsafe {
        command.pre_exec(|| {
            if nix::libc::setsid() == -1 || nix::libc::ioctl(0, nix::libc::TIOCSCTTY as _, 0) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut terminal = Terminal {
        child: command.spawn().unwrap(),
        master: Some(master),
        output: Vec::new(),
        width: 180,
    };
    terminal.wait("connections");
    terminal.send(b"\r");
    terminal.wait("rabbitmq.resources");
    terminal.send(b"/Queues\r");
    terminal.wait("1 shown");
    terminal.send(b"\r");
    terminal.wait("rabbitmq.queues");
    terminal.wait("100 loaded");
    terminal.send(b"n");
    terminal.wait("24 loaded");
    terminal.send(b"p");
    terminal.wait("100 loaded");
    terminal.send(b"\r");
    terminal.wait("Row data");
    terminal.wait("messages_ready");
    terminal.send(b"q");
    terminal.wait("Quit OneTUI?");
    terminal.send(b"y");
    terminal.wait("ONETUI_DONE");
    let after = tcgetattr(&slave).unwrap();
    assert_eq!(
        after.local_flags & !LocalFlags::PENDIN,
        before.local_flags & !LocalFlags::PENDIN
    );
    assert!(String::from_utf8_lossy(&terminal.output).contains("\x1b[?1049l"));
    assert!(String::from_utf8_lossy(&terminal.output).contains("\x1b[?25h"));
    terminal.send(b"\n");
    let until = Instant::now() + Duration::from_secs(3);
    loop {
        terminal.read();
        if let Some(status) = terminal.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    }
}
