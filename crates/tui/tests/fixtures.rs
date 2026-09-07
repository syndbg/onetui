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

    const PG_READER: &str = "host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable";
    const PG_OBSERVER: &str = "host=127.0.0.1 port=15432 user=onetui_fixture_admin password=fixture-admin-only dbname=onetui_fixture sslmode=disable";

    struct Observer {
        client: tokio_postgres::Client,
        driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    }
    impl Drop for Observer {
        fn drop(&mut self) {
            self.driver.abort();
        }
    }
    impl Observer {
        async fn connect() -> Self {
            let (client, connection) = tokio_postgres::connect(PG_OBSERVER, tokio_postgres::NoTls)
                .await
                .unwrap();
            Self {
                client,
                driver: tokio::spawn(connection),
            }
        }
        async fn sessions(&self) -> Vec<tokio_postgres::Row> {
            self.client.query("SELECT pid, wait_event FROM pg_stat_activity WHERE application_name='onetui-browse' AND usename='onetui_reader'", &[]).await.unwrap()
        }
        async fn wait_count(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(3), async {
                while self.sessions().await.len() != count {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("unexpected number of live browsing sessions");
        }
        async fn sleeping_pid(&self) -> i32 {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let sessions = self.sessions().await;
                    if let [row] = sessions.as_slice()
                        && row.get::<_, Option<String>>(1).as_deref() == Some("PgSleep")
                    {
                        return row.get(0);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("browser did not start its slow query")
        }
        async fn wait_gone(&self, pid: i32) {
            tokio::time::timeout(Duration::from_secs(3), async {
                while self
                    .sessions()
                    .await
                    .iter()
                    .any(|row| row.get::<_, i32>(0) == pid)
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("old browsing session survived shutdown");
        }
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Panic,
        Read,
        Shutdown,
    }
    struct FaultProvider(Fault);
    struct FaultExecutor {
        inner: onetui_postgres::PostgresExecutor,
        fault: Fault,
    }
    impl onetui_core::provider::Provider for FaultProvider {
        type Executor = FaultExecutor;
        fn descriptor(&self) -> &'static onetui_core::provider::ProviderDescriptor {
            onetui_core::provider::Provider::descriptor(&onetui_postgres::PostgresProvider)
        }
        fn validate_config(&self, options: &toml::Table) -> anyhow::Result<()> {
            onetui_core::provider::Provider::validate_config(
                &onetui_postgres::PostgresProvider,
                options,
            )
        }
        fn configure(
            &self,
            options: &toml::Table,
            env: &dyn Fn(&str) -> Option<String>,
        ) -> anyhow::Result<FaultExecutor> {
            Ok(FaultExecutor {
                inner: onetui_postgres::PostgresProvider.configure(options, env)?,
                fault: self.0,
            })
        }
    }
    impl Executor for FaultExecutor {
        fn status(&self) -> tokio::sync::watch::Receiver<onetui_core::provider::ConnectionStatus> {
            self.inner.status()
        }
        async fn check(
            &self,
            context: RequestContext,
        ) -> anyhow::Result<onetui_core::provider::CheckResult> {
            self.inner.check(context).await
        }
        async fn fetch_page(
            &self,
            request: PageRequest,
            context: RequestContext,
        ) -> anyhow::Result<onetui_core::Page> {
            let page = self.inner.fetch_page(request, context).await?;
            match self.fault {
                Fault::Panic => {
                    let mut trigger = tokio::signal::unix::signal(
                        tokio::signal::unix::SignalKind::user_defined1(),
                    )
                    .unwrap();
                    eprintln!("ONETUI_LIVE_CLIENT_READY");
                    trigger.recv().await;
                    panic!("intentional worker panic with a live PostgreSQL session");
                }
                Fault::Read => {
                    eprintln!("ONETUI_LIVE_CLIENT_READY");
                    std::future::pending().await
                }
                Fault::Shutdown => {
                    eprintln!("ONETUI_LIVE_CLIENT_READY");
                    Ok(page)
                }
            }
        }
        async fn shutdown(&mut self, context: ShutdownContext) -> anyhow::Result<()> {
            if matches!(self.fault, Fault::Shutdown) {
                std::future::pending::<()>().await;
            }
            self.inner.shutdown(context).await
        }
    }

    #[test]
    fn lifecycle_child() {
        let Ok(mode) = std::env::var("ONETUI_LIFECYCLE_TEST_MODE") else {
            return;
        };
        let fault = match mode.as_str() {
            "panic" => Fault::Panic,
            "read" | "quit_read" => Fault::Read,
            "shutdown" => Fault::Shutdown,
            _ => panic!("unknown test mode"),
        };
        let catalog = [FaultProvider(fault)];
        let config = Config::parse(
            "[connections.pg]\nkind='postgres'\nurl_env='ONETUI_LIFECYCLE_TEST_DSN'",
            &catalog,
        )
        .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let error = runtime
            .block_on(onetui_tui::run(
                App::new(config, Some("pg")),
                Duration::from_secs(30),
                &catalog,
            ))
            .unwrap_err();
        let expected = match fault {
            Fault::Panic => "browsing worker failed",
            Fault::Read => "browsing worker shutdown timed out",
            Fault::Shutdown => "session shutdown timed out",
        };
        assert!(error.to_string().contains(expected), "{error}");
        println!("ONETUI_LIFECYCLE_RESTORED");
        // Keep the process/runtime alive while the parent checks terminal modes and server PIDs.
        let mut acknowledgement = String::new();
        std::io::stdin().read_line(&mut acknowledgement).unwrap();
        drop(runtime);
    }

    #[tokio::test]
    #[ignore = "requires the disposable PostgreSQL fixture and child-owned PTYs"]
    async fn worker_faults_close_live_sessions_before_process_exit() {
        let observer = Observer::connect().await;
        observer.wait_count(0).await;
        for mode in ["panic", "read", "shutdown", "quit_read"] {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "terminal::lifecycle_child", "--nocapture"])
                .env("ONETUI_LIFECYCLE_TEST_MODE", mode)
                .env("ONETUI_LIFECYCLE_TEST_DSN", PG_READER);
            let (mut pty, slave) = Pty::spawn(command);
            pty.wait_token("ONETUI_LIVE_CLIENT_READY", Duration::from_secs(5));
            assert!(
                !tcgetattr(&slave)
                    .unwrap()
                    .local_flags
                    .contains(LocalFlags::ICANON)
            );
            observer.wait_count(1).await;
            let pid: i32 = observer.sessions().await[0].get(0);
            if mode == "panic" {
                assert!(
                    Command::new("kill")
                        .args(["-USR1", &pty.child.id().to_string()])
                        .status()
                        .unwrap()
                        .success()
                );
            } else if mode == "quit_read" {
                pty.send(b"q");
            } else {
                pty.send(b"c");
            }
            pty.wait_token("ONETUI_LIFECYCLE_RESTORED", Duration::from_secs(3));
            assert!(
                pty.child.try_wait().unwrap().is_none(),
                "child must remain alive for cleanup proof"
            );
            let after = tcgetattr(&slave).unwrap();
            assert_eq!(after.input_flags, pty.before.input_flags, "{mode}");
            assert_eq!(after.output_flags, pty.before.output_flags, "{mode}");
            assert_eq!(after.control_flags, pty.before.control_flags, "{mode}");
            assert_eq!(
                after.local_flags & !LocalFlags::PENDIN,
                pty.before.local_flags & !LocalFlags::PENDIN,
                "{mode}"
            );
            assert_eq!(after.control_chars, pty.before.control_chars, "{mode}");
            let output = String::from_utf8_lossy(&pty.output);
            assert!(
                output.contains("\x1b[?1049l"),
                "{mode}: alternate screen not restored"
            );
            assert!(output.contains("\x1b[?25h"), "{mode}: cursor not restored");
            observer.wait_gone(pid).await;
            observer.wait_count(0).await;
            pty.send(b"\n");
            let until = Instant::now() + Duration::from_secs(3);
            loop {
                pty.read();
                if let Some(status) = pty.child.try_wait().unwrap() {
                    assert!(status.success(), "{mode}");
                    break;
                }
                assert!(
                    Instant::now() < until,
                    "child did not exit after acknowledgement"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

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

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL/Qdrant fixtures and built CLI for connection switching"]
    async fn actual_cli_terminal_worker_and_datasource_switching() {
        use futures_util::FutureExt;
        use qdrant_client::qdrant::{
            CreateCollectionBuilder, Distance, PointStruct, UpsertPointsBuilder,
            VectorParamsBuilder,
        };

        // This client only seeds/cleans UI data; protocol assertions stay in the connector package.
        let fixture = qdrant_client::Qdrant::from_url("http://127.0.0.1:16334")
            .api_key("fixture-admin-only")
            .skip_compatibility_check()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let collection = format!("onetui_tui_{}", std::process::id());
        fixture
            .create_collection(
                CreateCollectionBuilder::new(&collection)
                    .vectors_config(VectorParamsBuilder::new(3, Distance::Dot)),
            )
            .await
            .unwrap();
        // Preserve a failed UI assertion while still removing this test's collection.
        let journey = std::panic::AssertUnwindSafe(async {
        let points: Vec<_> = (1_u64..=105).map(|id| {
            PointStruct::new(id, vec![1.0, 2.0, 3.0], [("title", format!("onetui-tui-point-{id}").into())])
        }).collect();
        fixture.upsert_points(UpsertPointsBuilder::new(&collection, points).wait(true)).await.unwrap();
        let mut config = tempfile::NamedTempFile::new().unwrap();
        write!(
            config,
            "[connections.pg]\nkind='postgres'\nurl_env='ONETUI_LIVE_PTY_DSN'\n[connections.pg_other]\nkind='postgres'\nurl_env='ONETUI_LIVE_PTY_DSN'\n[connections.qd]\nkind='qdrant'\nurl='http://127.0.0.1:16334'\napi_key_env='ONETUI_LIVE_PTY_KEY'"
        )
        .unwrap();
        let binary = std::env::var_os("ONETUI_TEST_BIN")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
            });
        let mut command = Command::new(binary);
        command
            .arg("--config")
            .arg(config.path())
            .args(["--connection", "pg"])
            .env("ONETUI_LIVE_PTY_DSN", PG_READER)
            .env("ONETUI_LIVE_PTY_KEY", "fixture-reader-only");
        let observer = Observer::connect().await;
        observer.wait_count(0).await;
        let (mut pty, slave) = Pty::spawn(command);
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
        for alias in ["pg_other", "pg", "pg_other"] {
            pty.send(b":back\r");
            pty.wait(&["postgres.relations", "keyed_rows"]);
            pty.open_filtered("browse_slow");
            pty.wait(&["postgres.rows", "Loading"]);
            let old_pid = observer.sleeping_pid().await;
            pty.send(b"c");
            pty.wait(&["connections", "pg_other"]);
            pty.open_filtered(alias);
            pty.wait(&[&format!("OneTUI|{alias}|"), "postgres.schemas", "public"]);
            observer.wait_gone(old_pid).await;
            observer.wait_count(1).await;
            pty.open_filtered("public");
            pty.wait(&["postgres.relations", "keyed_rows"]);
            pty.open_filtered("keyed_rows");
            pty.wait(&[
                &format!("OneTUI|{alias}|"),
                "postgres.rows",
                "Keyset",
                "9007199254740993",
            ]);
        }
        pty.send(b":back\r");
        pty.wait(&["postgres.relations", "keyed_rows"]);
        pty.open_filtered("browse_slow");
        pty.wait(&["postgres.rows", "Loading"]);
        let old_pid = observer.sleeping_pid().await;
        pty.send(b"c");
        pty.wait(&["connections", "qd"]);
        pty.open_filtered("qd");
        pty.wait(&["OneTUI|qd|read-only", "qdrant.collections", "Connected"]);
        observer.wait_gone(old_pid).await;
        observer.wait_count(0).await;
        // Allow cancelled PostgreSQL completion to reach the worker before another draw.
        tokio::time::sleep(Duration::from_millis(250)).await;
        pty.send(b"r");
        pty.wait(&["OneTUI|qd|read-only", "qdrant.collections", "Connected"]);
        assert!(!String::from_utf8_lossy(&pty.output).contains("9007199254740993"));
        assert!(!String::from_utf8_lossy(&pty.output).contains("Request cancelled"));
        pty.open_filtered(&collection);
        pty.wait(&["OneTUI|qd|", "qdrant.collection", "metadata", "points"]);
        pty.send(b"\r");
        pty.wait(&["qdrant.points", "100items", "numeric"]);
        pty.send(b"n");
        pty.wait(&["Page2|", "5items", "105"]);
        pty.send(b"p");
        pty.wait(&["Page1|", "100items"]);
        pty.send(b"r");
        pty.wait(&["Page1|", "100items"]);
        pty.send(b"\r");
        pty.wait(&["qdrant.point", "payload", "vectors"]);
        pty.send(b"\r");
        pty.wait(&["qdrant.payload", "onetui-tui-point-1"]);
        pty.send(b"\r");
        pty.wait(&["Field1/1", "onetui-tui-point-1"]);
        pty.send(b":back\r");
        pty.wait(&["qdrant.payload", "onetui-tui-point-1"]);
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
        observer.wait_count(0).await;

        pty.send(b"c");
        pty.wait(&["connections", "pg"]);
        pty.open_filtered("pg");
        pty.wait(&["OneTUI|pg|", "postgres.schemas", "public"]);
        pty.open_filtered("public");
        pty.wait(&["postgres.relations", "keyed_rows"]);
        pty.open_filtered("keyed_rows");
        pty.wait(&["OneTUI|pg|", "postgres.rows", "9007199254740993"]);
        observer.wait_count(1).await;
        assert!(!String::from_utf8_lossy(&pty.output).contains("onetui-tui-point-1"));
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
        observer.wait_count(0).await;
        }).catch_unwind().await;
        let cleanup = fixture.delete_collection(&collection).await;
        if let Err(panic) = journey {
            assert!(
                cleanup.is_ok(),
                "UI journey failed and fixture collection cleanup failed"
            );
            std::panic::resume_unwind(panic);
        }
        cleanup.unwrap();
    }
}
