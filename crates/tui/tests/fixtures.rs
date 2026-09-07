//! Keyboard/request-state journey; PostgreSQL transport contracts stay in the PostgreSQL package.
use std::io::Write;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use onetui_core::config::Config;
use onetui_core::provider::{Executor, PageRequest, RequestContext, ShutdownContext};
use onetui_tui::App;

fn key(app: &mut App, code: KeyCode) {
    app.key(KeyEvent::new(code, KeyModifiers::NONE));
}

async fn complete(app: &mut App, executor: &onetui_postgres::PostgresExecutor) {
    let request = app.request.take().expect("keyboard action queued a read");
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let result = executor
        .fetch_page(
            PageRequest {
                resource: request.resource.clone(),
                continuation: request.continuation.clone(),
            },
            context,
        )
        .await;
    app.complete(&request, result);
}

fn select(app: &mut App, name: &str) {
    while app.view.selected > 0 {
        key(app, KeyCode::Char('k'));
    }
    let count = app.view.page.rows.len();
    for _ in 0..count {
        if app.view.page.rows[app.view.selected].cells[0].as_deref() == Some(name) {
            return;
        }
        key(app, KeyCode::Char('j'));
    }
    panic!("fixture resource missing: {name}");
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn keyboard_to_postgres_rows_detail_paging_metadata_and_failure() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    write!(
        file,
        "[connections.pg]\nkind='postgres'\nurl_env='FIXTURE_DSN'"
    )
    .unwrap();
    let catalog = &[onetui_postgres::PostgresProvider];
    let config = Config::load(file.path(), catalog).unwrap();
    let mut executor = config.configure("pg", catalog, &|_| Some("host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable".into())).unwrap();
    let mut app = App::new(config, None);
    key(&mut app, KeyCode::Enter);
    complete(&mut app, &executor).await;
    select(&mut app, "public");
    key(&mut app, KeyCode::Enter);
    complete(&mut app, &executor).await;
    select(&mut app, "browse_composite");
    key(&mut app, KeyCode::Enter);
    complete(&mut app, &executor).await;
    assert!(app.error.is_none(), "{:?}", app.error);
    assert_eq!(app.view.resource.id, "postgres.rows");
    assert_eq!(app.view.page.rows.len(), 100);
    let token = app.view.page.continuation.clone();
    key(&mut app, KeyCode::Enter);
    assert!(app.detail_text.contains("София"));
    assert!(app.detail_text.contains("\\n"));
    assert!(!app.detail_text.contains('\x1b'));
    key(&mut app, KeyCode::Esc);
    key(&mut app, KeyCode::Char('n'));
    complete(&mut app, &executor).await;
    assert_eq!(app.view.offset, 100);
    assert_eq!(app.view.page.rows[0].cells[1].as_deref(), Some("101"));
    key(&mut app, KeyCode::Char('p'));
    assert!(app.request.is_none());
    assert_eq!(app.view.page.continuation, token);
    key(&mut app, KeyCode::Char('m'));
    complete(&mut app, &executor).await;
    assert_eq!(app.view.resource.id, "postgres.columns");
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.view.resource.id, "postgres.rows");
    key(&mut app, KeyCode::Esc);
    select(&mut app, "restricted_rows");
    key(&mut app, KeyCode::Enter);
    complete(&mut app, &executor).await;
    assert!(app.error.as_ref().unwrap().contains("denied"));
    key(&mut app, KeyCode::Esc);
    select(&mut app, "browse_uuid");
    key(&mut app, KeyCode::Enter);
    complete(&mut app, &executor).await;
    assert!(app.view.page.rows.is_empty());
    assert!(app.error.is_none());
    key(&mut app, KeyCode::Char('q'));
    assert!(app.quit);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}

#[cfg(unix)]
mod terminal {
    use super::*;
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use nix::pty::{Winsize, openpty};
    use nix::sys::termios::{LocalFlags, tcgetattr};
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Command, Stdio};
    use std::time::Instant;

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

    #[test]
    #[ignore = "requires the disposable PostgreSQL fixture and built CLI"]
    fn actual_cli_terminal_worker_and_postgres_journey() {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        write!(
            config,
            "[connections.pg]\nkind='postgres'\nurl_env='ONETUI_LIVE_PTY_DSN'"
        )
        .unwrap();
        let binary = std::env::var_os("ONETUI_TEST_BIN")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
            });
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
        let mut command = Command::new(binary);
        command.arg("--config").arg(config.path()).args(["--connection", "pg"])
            .env("ONETUI_LIVE_PTY_DSN", "host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable")
            .stdin(Stdio::from(slave.try_clone().unwrap()))
            .stdout(Stdio::from(slave.try_clone().unwrap()))
            .stderr(Stdio::from(slave.try_clone().unwrap()));
        // Crossterm may open /dev/tty; it must resolve to this PTY, never the user's terminal.
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
        let mut pty = Pty {
            child: command.spawn().unwrap(),
            master: Some(master),
            output: Vec::new(),
            width: 180,
        };
        drop(command);
        pty.wait(&["postgres.schemas", "public"]);
        assert!(
            !tcgetattr(&slave)
                .unwrap()
                .local_flags
                .contains(LocalFlags::ICANON)
        );
        drop(slave);
        // A resize and input can become ready together; neither may strand the other.
        for _ in 0..10 {
            pty.resize();
            pty.send(b"?");
            pty.wait(&["Navigationactions"]);
            pty.resize();
            pty.send(b":back\r");
            pty.wait(&["postgres.schemas", "public"]);
        }
        pty.open_filtered("public");
        pty.wait(&["postgres.relations", "browse_composite"]);
        pty.open_filtered("browse_composite");
        pty.wait(&["postgres.rows", "Keyset", "София"]);
        pty.send(b"/value\r");
        pty.wait(&["Page-local:100/100shown", "filter:\"value\""]);
        pty.send(b"lss\r");
        pty.wait(&["Field2/3", "bigint", "non-nulltext", "lexicalsort:iddesc"]);
        pty.send(b":back\r");
        pty.wait(&["София", "Keyset"]);
        pty.send(b"n");
        pty.wait(&["Page2|", "200"]);
        pty.send(b"\r");
        pty.wait(&["Field2/3", "bigint", "200"]);
        pty.send(b":back\r");
        pty.wait(&["София", "Keyset"]);
        pty.send(b"p");
        pty.wait(&["Page1|", "100items"]);
        pty.send(b"m");
        pty.wait(&["postgres.columns", "bigint"]);
        pty.send(b":back\r");
        pty.wait(&["postgres.rows", "Keyset"]);
        pty.send(b":back\r");
        pty.wait(&["postgres.relations", "browse_composite"]);
        pty.open_filtered("restricted_rows");
        pty.wait(&["postgres.rows", "accessdenied"]);
        pty.send(b":back\r");
        pty.wait(&["postgres.relations", "restricted_rows"]);
        pty.open_filtered("browse_uuid");
        pty.wait(&["postgres.rows", "Emptyresult", "OFFSET"]);
        pty.send(b":back\r");
        pty.wait(&["postgres.relations", "browse_uuid"]);
        pty.open_filtered("browse_slow");
        pty.wait(&["postgres.rows", "Loading"]);
        pty.send(b"\x03");
        pty.wait(&["Requestcancelled"]);
        pty.send(b":back\r");
        pty.wait(&["postgres.relations", "browse_slow"]);
        pty.open_filtered("keyed_rows");
        pty.wait(&["postgres.rows", "Keyset", "9007199254740993"]);
        pty.output.clear();
        pty.master.as_mut().unwrap().write_all(b"q").unwrap();
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
        assert!(
            output.contains("\x1b[?1049l"),
            "alternate screen not restored"
        );
        assert!(output.contains("\x1b[?25h"), "cursor not restored");
    }
}
