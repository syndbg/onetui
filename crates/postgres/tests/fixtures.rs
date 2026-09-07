//! Connector experiments against the fixed disposable local PostgreSQL fixture.
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::io::Write;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

const PG: &str = "host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable";
const PG_ADMIN: &str = "host=127.0.0.1 port=15432 user=onetui_fixture_admin password=fixture-admin-only dbname=onetui_fixture sslmode=disable";

async fn browse(resource: onetui_core::Resource, offset: i64) -> anyhow::Result<onetui_core::Page> {
    let (_cancel, receiver) = tokio::sync::oneshot::channel();
    onetui_postgres::fetch(
        PG.into(),
        None,
        resource,
        offset,
        Duration::from_secs(5),
        receiver,
    )
    .await
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn metadata_browsing_pages_types_permissions_and_connection_cleanup() {
    use onetui_core::Resource;
    let schemas = browse(Resource::new("postgres.schemas", vec![]), 0)
        .await
        .unwrap();
    assert!(schemas.rows.iter().any(|row| row.cells[0] == "public"));
    let tables = browse(
        Resource::new("postgres.relations", vec!["public".into()]),
        0,
    )
    .await
    .unwrap();
    let sample = tables
        .rows
        .iter()
        .find(|row| row.cells[0] == "sample_rows")
        .unwrap();
    let columns = browse(sample.target.clone().unwrap(), 0).await.unwrap();
    assert!(
        columns
            .rows
            .iter()
            .any(|row| row.cells[0] == "amount" && row.cells[1] == "numeric(30,10)")
    );
    assert!(
        columns
            .rows
            .iter()
            .any(|row| row.cells[0] == "note" && row.cells[2] == "no")
    );
    let quoted = tables
        .rows
        .iter()
        .find(|row| row.cells[0] == "quoted'; -- relation")
        .unwrap();
    let columns = browse(quoted.target.clone().unwrap(), 0).await.unwrap();
    assert_eq!(columns.rows[0].cells[0], "odd\"column");
    let empty = browse(
        Resource::new("postgres.relations", vec!["empty_schema".into()]),
        0,
    )
    .await
    .unwrap();
    assert!(empty.rows.is_empty());
    let denied = browse(
        Resource::new(
            "postgres.columns",
            vec!["public".into(), "restricted_rows".into()],
        ),
        0,
    )
    .await
    .unwrap_err();
    assert!(denied.to_string().contains("denied"));
    let first = browse(
        Resource::new("postgres.relations", vec!["pg_catalog".into()]),
        0,
    )
    .await
    .unwrap();
    assert_eq!(first.rows.len(), 100);
    assert!(first.next);
    let second = browse(
        Resource::new("postgres.relations", vec!["pg_catalog".into()]),
        100,
    )
    .await
    .unwrap();
    assert!(!second.rows.is_empty());
    assert_ne!(first.rows[0].cells[0], second.rows[0].cells[0]);
    let observer = FixturePg::plain(PG_ADMIN).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = observer.client.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'onetui-browse'", &[]).await.unwrap();
            if row.get::<_, i64>(0) == 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("browse connection remained open after metadata read");
    assert!(
        !columns.rows.is_empty(),
        "cached metadata stays readable after disconnect"
    );
}
struct FixturePg {
    client: Client,
    driver: tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
}

impl FixturePg {
    async fn connect(dsn: &str, roots: rustls::RootCertStore) -> Self {
        let tls = MakeRustlsConnect::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let (client, connection) =
            tokio::time::timeout(Duration::from_secs(5), tokio_postgres::connect(dsn, tls))
                .await
                .expect("fixture connect deadline")
                .unwrap();
        let driver = tokio::spawn(connection);
        client
            .batch_execute("SET statement_timeout = '5s'")
            .await
            .unwrap();
        Self { client, driver }
    }

    async fn plain(dsn: &str) -> Self {
        Self::connect(dsn, rustls::RootCertStore::empty()).await
    }

    async fn pid(&self) -> i32 {
        self.client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0)
    }
}

impl Drop for FixturePg {
    fn drop(&mut self) {
        // Test assertions may panic; never leave a detached connection task behind.
        self.driver.abort();
    }
}

async fn wait_for_backend(observer: &Client, pid: i32, expected: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let row = observer.query_opt(
                "SELECT state, wait_event, xact_start IS NULL, backend_xmin IS NULL FROM pg_stat_activity WHERE pid = $1", &[&pid]
            ).await.unwrap();
            let matched = match (expected, row) {
                ("gone", None) => true,
                ("idle", Some(row)) => row.get::<_, String>(0) == "idle" && row.get::<_, bool>(2) && row.get::<_, bool>(3),
                ("sleep", Some(row)) => row.get::<_, String>(0) == "active" && row.get::<_, Option<String>>(1).as_deref() == Some("PgSleep"),
                _ => false,
            };
            if matched { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap_or_else(|_| panic!("backend {pid} never reached {expected}"));
}

#[tokio::test]
#[ignore = "requires the disposable Docker Compose fixtures; see README"]
async fn postgres_independent_offset_pages_preserve_types_without_idle_transactions() {
    let reader = FixturePg::plain(PG).await;
    let observer = FixturePg::plain(PG_ADMIN).await;
    let client = &reader.client;
    let pid = reader.pid().await;
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        // Accelerated guard: any transaction spanning this reading pause would be terminated.
        client
            .batch_execute("SET idle_in_transaction_session_timeout = '100ms'")
            .await
            .unwrap();
        let page = client
            .simple_query("SELECT * FROM public.sample_rows ORDER BY ordinal LIMIT 2 OFFSET 0")
            .await
            .unwrap();
        let rows: Vec<_> = page
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("note"), None);
        assert_eq!(rows[1].get("note"), Some(""));
        assert_eq!(
            rows[0].get("amount"),
            Some("12345678901234567890.1234567890")
        );
        assert_eq!(rows[0].get("bytes"), Some("\\x00ff"));
        assert_eq!(rows[0].get("tags"), Some("{a,b}"));
        assert_eq!(rows[0].get("state"), Some("paid"));
        assert_eq!(rows[0].get("price"), Some("12.34"));
        wait_for_backend(&observer.client, pid, "idle").await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        wait_for_backend(&observer.client, pid, "idle").await;
        assert_eq!(
            rows[0].get("amount"),
            Some("12345678901234567890.1234567890")
        );
        let next = client
            .simple_query("SELECT * FROM public.sample_rows ORDER BY ordinal LIMIT 2 OFFSET 2")
            .await
            .unwrap();
        let next: Vec<_> = next
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(next.len(), 2);
        assert_eq!(next[0].get("ordinal"), Some("3"));
        assert_eq!(next[0].get("note"), Some("NULL"));
        wait_for_backend(&observer.client, pid, "idle").await;
        // The extended-protocol inspection query itself occupies the unnamed portal.
        let row = client
            .query_one("SELECT count(*) FROM pg_cursors WHERE name <> ''", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        assert!(
            client
                .execute("DELETE FROM public.sample_rows", &[])
                .await
                .is_err()
        );
    })
    .await;
    drop(reader);
    wait_for_backend(&observer.client, pid, "gone").await;
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn postgres_keyset_uses_exact_single_and_composite_parameters() {
    let reader = FixturePg::plain(PG).await;
    let observer = FixturePg::plain(PG_ADMIN).await;
    let client = &reader.client;
    let pid = reader.pid().await;
    let first = client
        .query(
            "SELECT id, note::text FROM keyed_rows ORDER BY id LIMIT $1",
            &[&2_i64],
        )
        .await
        .unwrap();
    let anchor: i64 = first.last().unwrap().get(0);
    assert_eq!(anchor, 9_007_199_254_740_994);
    wait_for_backend(&observer.client, pid, "idle").await;
    let next = client
        .query(
            "SELECT id, note::text FROM keyed_rows WHERE id > $1 ORDER BY id LIMIT $2",
            &[&anchor, &2_i64],
        )
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].get::<_, i64>(0), anchor + 1);

    let first = client
        .query(
            "SELECT tenant, id FROM composite_rows ORDER BY tenant, id LIMIT $1",
            &[&3_i64],
        )
        .await
        .unwrap();
    let last = first.last().unwrap();
    let tenant: &str = last.get(0);
    let id: i64 = last.get(1);
    assert_eq!(tenant, "b'; DELETE FROM keyed_rows; --");
    wait_for_backend(&observer.client, pid, "idle").await;
    let next = client.query("SELECT tenant, id FROM composite_rows WHERE (tenant, id) > ($1, $2) ORDER BY tenant, id LIMIT $3", &[&tenant, &id, &3_i64]).await.unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].get::<_, String>(0), tenant);
    assert_eq!(next[0].get::<_, i64>(1), 2);
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM keyed_rows", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        3
    );
    wait_for_backend(&observer.client, pid, "idle").await;
}

#[tokio::test]
#[ignore = "mutates only changing_rows in the disposable PostgreSQL fixture"]
async fn postgres_independent_pages_do_not_claim_a_snapshot() {
    let reader = FixturePg::plain(PG).await;
    let writer = FixturePg::plain(PG_ADMIN).await;
    writer
        .client
        .batch_execute(
            "DELETE FROM changing_rows; INSERT INTO changing_rows VALUES (1), (2), (3), (4)",
        )
        .await
        .unwrap();
    let first = reader
        .client
        .query("SELECT id FROM changing_rows ORDER BY id LIMIT 2", &[])
        .await
        .unwrap();
    let anchor: i64 = first[1].get(0);
    writer
        .client
        .batch_execute(
            "DELETE FROM changing_rows WHERE id = 1; INSERT INTO changing_rows VALUES (5)",
        )
        .await
        .unwrap();
    let keyset = reader
        .client
        .query(
            "SELECT id FROM changing_rows WHERE id > $1 ORDER BY id LIMIT 2",
            &[&anchor],
        )
        .await
        .unwrap();
    let offset = reader
        .client
        .query(
            "SELECT id FROM changing_rows ORDER BY id LIMIT 2 OFFSET 2",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        keyset
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        [3, 4]
    );
    assert_eq!(
        offset
            .iter()
            .map(|r| r.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        [4, 5]
    );
    let tail = reader
        .client
        .query(
            "SELECT id FROM changing_rows WHERE id > $1 ORDER BY id LIMIT 2",
            &[&4_i64],
        )
        .await
        .unwrap();
    assert_eq!(tail[0].get::<_, i64>(0), 5);
    assert_eq!(first[0].get::<_, i64>(0), 1);
    writer
        .client
        .batch_execute(
            "DELETE FROM changing_rows; INSERT INTO changing_rows VALUES (1), (2), (3), (4)",
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL TLS fixture"]
async fn postgres_cancel_over_tls_finishes_before_connection_reuse() {
    let ca = fixture_ca("postgres", "/var/lib/postgresql/data/onetui-ca.crt");
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(ca.path()).unwrap() {
        roots.add(cert.unwrap()).unwrap();
    }
    let tls = MakeRustlsConnect::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_no_client_auth(),
    );
    let reader = FixturePg::connect(
        &PG.replace("127.0.0.1", "localhost")
            .replace("sslmode=disable", "sslmode=require"),
        roots,
    )
    .await;
    let observer = FixturePg::plain(PG_ADMIN).await;
    let pid = reader.pid().await;
    assert!(
        observer
            .client
            .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = $1", &[&pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    reader
        .client
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .unwrap();
    let cancel = reader.client.cancel_token();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(reader.client.query("SELECT pg_sleep(30)", &[]), async {
            wait_for_backend(&observer.client, pid, "sleep").await;
            cancel.cancel_query(tls).await.unwrap();
        })
    })
    .await
    .expect("cancel did not finish before the statement timeout");
    assert_eq!(
        result.unwrap_err().code(),
        Some(&tokio_postgres::error::SqlState::QUERY_CANCELED)
    );
    wait_for_backend(&observer.client, pid, "idle").await;
    // Both cancellation and the original query have finished before the next request starts.
    assert_eq!(
        reader
            .client
            .query("SELECT id FROM keyed_rows ORDER BY id LIMIT 2", &[])
            .await
            .unwrap()
            .len(),
        2
    );
    wait_for_backend(&observer.client, pid, "idle").await;
    drop(reader);
    wait_for_backend(&observer.client, pid, "gone").await;
}

#[tokio::test]
#[ignore = "terminates only its own reader session in the disposable PostgreSQL fixture"]
async fn postgres_connection_loss_preserves_cached_page_and_reconnects() {
    let reader = FixturePg::plain(PG).await;
    let observer = FixturePg::plain(PG_ADMIN).await;
    let pid = reader.pid().await;
    let cached = reader
        .client
        .query("SELECT id FROM keyed_rows ORDER BY id LIMIT 2", &[])
        .await
        .unwrap();
    reader
        .client
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .unwrap();
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(reader.client.query("SELECT pg_sleep(30)", &[]), async {
            wait_for_backend(&observer.client, pid, "sleep").await;
            assert!(
                observer
                    .client
                    .query_one("SELECT pg_terminate_backend($1)", &[&pid])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
        })
    })
    .await
    .unwrap();
    assert!(result.is_err());
    drop(reader);
    wait_for_backend(&observer.client, pid, "gone").await;
    assert_eq!(cached[0].get::<_, i64>(0), 9_007_199_254_740_993);
    let fresh = FixturePg::plain(PG).await;
    assert_eq!(
        fresh
            .client
            .query("SELECT id FROM keyed_rows ORDER BY id LIMIT 2", &[])
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn postgres_row_limit_does_not_limit_cell_bytes() {
    let reader = FixturePg::plain(PG).await;
    let observer = FixturePg::plain(PG_ADMIN).await;
    // A single row already exceeds a small display budget; LIMIT is not a byte cap.
    let result = reader
        .client
        .simple_query("SELECT repeat('🌊', 524288) AS value LIMIT 1")
        .await
        .unwrap();
    let row = result
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(row.get("value").unwrap().len(), 2 * 1024 * 1024);
    wait_for_backend(&observer.client, reader.pid().await, "idle").await;
    // Server-side projection can flag an oversized field without sending its full text.
    let row = reader.client.query_one(
        "SELECT CASE WHEN octet_length(value) <= $1 THEN value END, octet_length(value) > $1 FROM (SELECT repeat('🌊', 524288) AS value) s LIMIT 1",
        &[&1_048_576_i32]
    ).await.unwrap();
    assert_eq!(row.get::<_, Option<String>>(0), None);
    assert!(row.get::<_, bool>(1));
    wait_for_backend(&observer.client, reader.pid().await, "idle").await;
}
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
fn fixture_ca(service: &str, path: &str) -> tempfile::NamedTempFile {
    let cert = Command::new("docker")
        .args([
            "compose",
            "-f",
            "hack/compose.yaml",
            "exec",
            "-T",
            service,
            "cat",
            path,
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap();
    assert!(cert.status.success());
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&cert.stdout).unwrap();
    ca
}

fn check(dsn: &str) -> std::process::Output {
    binary()
        .args([
            "--check",
            "--config",
            "hack/connections.toml",
            "--connection",
            "local_pg",
        ])
        .env("ONETUI_POSTGRES_URL", dsn)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap()
}

#[test]
#[ignore = "requires the disposable PostgreSQL fixture"]
fn postgres_cli_check_and_auth_failure() {
    assert!(check(PG).status.success());
    let output = check(&PG.replace("fixture-reader-only", "fake-wrong-secret"));
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-wrong-secret"));
}
#[test]
#[ignore = "requires the disposable Docker Compose PostgreSQL TLS fixture"]
fn postgres_tls_checks_ca_and_hostname() {
    let ca = fixture_ca("postgres", "/var/lib/postgresql/data/onetui-ca.crt");
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(
        config,
        "[connections.tls]\nkind='postgres'\nurl_env='ONETUI_POSTGRES_URL'\nca_file='{}'\n",
        ca.path().display()
    )
    .unwrap();
    let tls = PG.replace("sslmode=disable", "sslmode=require");
    for (dsn, success) in [(tls.replace("127.0.0.1", "localhost"), true), (tls, false)] {
        let output = binary()
            .args(["--check", "--connection", "tls", "--config"])
            .arg(config.path())
            .env("ONETUI_POSTGRES_URL", dsn)
            .output()
            .unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    // Native roots must not trust our fixture's private CA, even with the correct hostname.
    let output = check(
        &PG.replace("sslmode=disable", "sslmode=require")
            .replace("127.0.0.1", "localhost"),
    );
    assert!(!output.status.success());
}
