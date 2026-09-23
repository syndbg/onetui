#![cfg(unix)]

use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::pty::{Winsize, openpty};
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[test]
fn first_run_opens_empty_picker_and_saves_without_connecting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("onetui/config.toml");
    let pair = openpty(
        &Winsize {
            ws_row: 30,
            ws_col: 120,
            ws_xpixel: 0,
            ws_ypixel: 0,
        },
        None,
    )
    .unwrap();
    let slave = std::fs::File::from(pair.slave);
    let mut master = std::fs::File::from(pair.master);
    let flags = OFlag::from_bits_truncate(fcntl(&master, FcntlArg::F_GETFL).unwrap());
    fcntl(&master, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_onetui"));
    command
        .env("XDG_CONFIG_HOME", dir.path())
        .env_remove("ONETUI_FIRST_RUN_TEST_SECRET")
        .stdin(Stdio::from(slave.try_clone().unwrap()))
        .stdout(Stdio::from(slave.try_clone().unwrap()))
        .stderr(Stdio::from(slave));
    // The test owns /dev/tty, so keyboard input cannot reach the developer's terminal.
    unsafe {
        command.pre_exec(|| {
            if nix::libc::setsid() == -1 || nix::libc::ioctl(0, nix::libc::TIOCSCTTY as _, 0) == -1
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = Child(command.spawn().unwrap());
    let started = Instant::now();
    let mut output = Vec::new();
    let mut stage = 0;
    loop {
        let mut buffer = [0; 16384];
        while let Ok(count) = master.read(&mut buffer) {
            if count == 0 {
                break;
            }
            output.extend_from_slice(&buffer[..count]);
        }
        assert!(output.len() < 1024 * 1024);
        let text = String::from_utf8_lossy(&output);
        if stage == 0 && text.contains("No connections yet") {
            assert!(!path.exists(), "opening the picker must not create a file");
            master.write_all(b"a").unwrap();
            stage = 1;
        } else if stage == 1 && text.contains("choose datasource") {
            master.write_all(b"\r").unwrap();
            stage = 2;
        } else if stage == 2 && text.contains("url_env") {
            // PostgreSQL is the first catalog entry. Save only a reference, not a DSN.
            master
                .write_all(b"first_run\tONETUI_FIRST_RUN_TEST_SECRET\x13")
                .unwrap();
            stage = 3;
        } else if stage == 3 && path.exists() {
            master.write_all(b"q").unwrap();
            stage = 4;
        } else if stage == 4 && text.contains("Quit OneTUI?") {
            master.write_all(b"y").unwrap();
            stage = 5;
        }
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success(), "{text}");
            assert_eq!(stage, 5, "{text}");
            assert!(
                !text.contains("referenced environment variable"),
                "must not connect: {text}"
            );
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "first-run TUI timed out at stage {stage}: {text}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let saved = std::fs::read_to_string(path).unwrap();
    assert!(saved.contains("[connections.first_run]"));
    assert!(saved.contains("ONETUI_FIRST_RUN_TEST_SECRET"));
}

#[test]
fn headless_missing_config_is_still_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_onetui"))
        .env("XDG_CONFIG_HOME", dir.path())
        .args(["--check", "--connection", "missing"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("config file not found"));
    assert!(!dir.path().join("onetui").exists());
}
