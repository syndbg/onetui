//! Explicitly opted-in checks against fixtures/compose.yaml only; never production endpoints.
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, GetPointsBuilder, PointStruct, ScrollPointsBuilder,
    UpsertPointsBuilder, VectorParamsBuilder,
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

const PG: &str = "host=127.0.0.1 port=15432 user=bpearl_reader password=fixture-reader-only dbname=bpearl_fixture sslmode=disable";
const PG_ADMIN: &str = "host=127.0.0.1 port=15432 user=bpearl_fixture_admin password=fixture-admin-only dbname=bpearl_fixture sslmode=disable";
const QDRANT: &str = "http://127.0.0.1:16334";

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

fn check(alias: &str, pg: &str, key: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_bpearl"))
        .args([
            "--check",
            "--config",
            "fixtures/connections.toml",
            "--connection",
            alias,
        ])
        .env("BPEARL_POSTGRES_URL", pg)
        .env("BPEARL_QDRANT_API_KEY", key)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the disposable Docker Compose fixtures; see README"]
async fn both_cli_checks_and_auth_failures() {
    for alias in ["local_pg", "local_qdrant"] {
        let output = check(alias, PG, "fixture-reader-only");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).starts_with("OK "));
        let output = check(
            alias,
            &PG.replace("fixture-reader-only", "fake-wrong-secret"),
            "fake-wrong-secret",
        );
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-wrong-secret"));
    }
}

fn fixture_ca() -> tempfile::NamedTempFile {
    let cert = Command::new("docker")
        .args([
            "compose",
            "-f",
            "fixtures/compose.yaml",
            "exec",
            "-T",
            "postgres",
            "cat",
            "/var/lib/postgresql/data/bpearl-ca.crt",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(cert.status.success());
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&cert.stdout).unwrap();
    ca
}

#[test]
#[ignore = "requires the disposable Docker Compose PostgreSQL TLS fixture"]
fn postgres_tls_checks_ca_and_hostname() {
    let ca = fixture_ca();
    let mut config = tempfile::NamedTempFile::new().unwrap();
    write!(
        config,
        "[connections.tls]\nkind='postgres'\nurl_env='BPEARL_POSTGRES_URL'\nca_file='{}'\n",
        ca.path().display()
    )
    .unwrap();
    let tls = PG.replace("sslmode=disable", "sslmode=require");
    for (dsn, success) in [(tls.replace("127.0.0.1", "localhost"), true), (tls, false)] {
        let output = Command::new(env!("CARGO_BIN_EXE_bpearl"))
            .args(["--check", "--connection", "tls", "--config"])
            .arg(config.path())
            .env("BPEARL_POSTGRES_URL", dsn)
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
        "local_pg",
        &PG.replace("sslmode=disable", "sslmode=require")
            .replace("127.0.0.1", "localhost"),
        "fixture-reader-only",
    );
    assert!(!output.status.success());
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
    let ca = fixture_ca();
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
#[ignore = "creates a uniquely named collection ONLY in the disposable Qdrant fixture"]
async fn qdrant_scroll_and_lazy_details() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let name = format!("bpearl_fixture_{}", std::process::id());
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)),
        )
        .await
        .unwrap();
    let result = async {
        client
            .upsert_points(
                UpsertPointsBuilder::new(
                    &name,
                    vec![
                        PointStruct::new(1_u64, vec![1.0, 0.0, 0.0], [("title", "first".into())]),
                        PointStruct::new(2_u64, vec![0.0, 1.0, 0.0], [("title", "second".into())]),
                        PointStruct::new(
                            "550e8400-e29b-41d4-a716-446655440000",
                            vec![0.0, 0.0, 1.0],
                            [("title", "uuid".into())],
                        ),
                    ],
                )
                .wait(true),
            )
            .await?;
        let page = client
            .scroll(
                ScrollPointsBuilder::new(&name)
                    .limit(2)
                    .with_payload(false)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(page.result.len(), 2);
        assert!(
            page.result
                .iter()
                .all(|p| p.payload.is_empty() && p.vectors.is_none())
        );
        let next = client
            .scroll(
                ScrollPointsBuilder::new(&name)
                    .limit(2)
                    .offset(page.next_page_offset.unwrap())
                    .with_payload(false)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(next.result.len(), 1);
        assert!(next.next_page_offset.is_none());
        let id = next.result[0].id.clone().unwrap();
        assert!(matches!(
            id.point_id_options,
            Some(qdrant_client::qdrant::point_id::PointIdOptions::Uuid(_))
        ));
        let detail = client
            .get_points(
                GetPointsBuilder::new(&name, vec![id])
                    .with_payload(true)
                    .with_vectors(true),
            )
            .await?;
        assert_eq!(detail.result.len(), 1);
        assert!(!detail.result[0].payload.is_empty());
        assert!(detail.result[0].vectors.is_some());
        Ok::<_, qdrant_client::QdrantError>(())
    }
    .await;
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
}
