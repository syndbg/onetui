//! Explicitly opted-in checks against hack/compose.yaml only; never production endpoints.
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, DeletePointsBuilder, Distance, GetPointsBuilder,
    MultiVectorComparator, MultiVectorConfigBuilder, NamedVectors, PointStruct,
    ScrollPointsBuilder, SparseVectorConfig, SparseVectorParams, UpsertPointsBuilder, Vector,
    VectorParamsBuilder, VectorParamsMap, VectorsConfig, points_client::PointsClient,
    vector_output, vectors_config,
};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

const PG: &str = "host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable";
const PG_ADMIN: &str = "host=127.0.0.1 port=15432 user=onetui_fixture_admin password=fixture-admin-only dbname=onetui_fixture sslmode=disable";
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
    Command::new(env!("CARGO_BIN_EXE_onetui"))
        .args([
            "--check",
            "--config",
            "hack/connections.toml",
            "--connection",
            alias,
        ])
        .env("ONETUI_POSTGRES_URL", pg)
        .env("ONETUI_QDRANT_API_KEY", key)
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
        let output = Command::new(env!("CARGO_BIN_EXE_onetui"))
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
        "local_pg",
        &PG.replace("sslmode=disable", "sslmode=require")
            .replace("127.0.0.1", "localhost"),
        "fixture-reader-only",
    );
    assert!(!output.status.success());
}

#[test]
#[ignore = "requires the disposable Docker Compose Qdrant TLS fixture"]
fn qdrant_https_verifies_trust_hostname_and_authentication() {
    let ca = fixture_ca("qdrant-tls", "/qdrant/tls/ca.crt");
    let unrelated_ca = fixture_ca("postgres", "/var/lib/postgresql/data/onetui-ca.crt");
    let empty_cert_dir = tempfile::tempdir().unwrap();
    for (endpoint, trusted_ca, key, expected) in [
        (
            "https://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            None,
        ),
        (
            "https://127.0.0.1:16335",
            ca.path(),
            "fixture-reader-only",
            Some("TLS certificate trust"),
        ),
        (
            "https://localhost:16335",
            unrelated_ca.path(),
            "fixture-reader-only",
            Some("TLS certificate trust"),
        ),
        (
            "https://localhost:16335",
            ca.path(),
            "fake-wrong-secret",
            Some("authentication failed"),
        ),
        (
            "http://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            Some("Qdrant"),
        ),
        (
            "https://localhost:16334",
            ca.path(),
            "fixture-reader-only",
            Some("TLS certificate trust"),
        ),
        (
            "https://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            None,
        ),
    ] {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(config, "[connections.tls]\nkind='qdrant'\nurl='{endpoint}'\napi_key_env='ONETUI_QDRANT_API_KEY'").unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_onetui"))
            .args(["--check", "--connection", "tls", "--config"])
            .arg(config.path())
            .args(["--timeout", "2"])
            // rustls-native-certs reads these in the child only; the OS trust store is untouched.
            .env("SSL_CERT_FILE", trusted_ca)
            .env("SSL_CERT_DIR", empty_cert_dir.path())
            .env("ONETUI_QDRANT_API_KEY", key)
            .output()
            .unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.success(), expected.is_none(), "{error}");
        if let Some(expected) = expected {
            assert!(error.contains(expected), "{error}");
            assert!(output.stdout.is_empty());
        } else {
            assert!(String::from_utf8_lossy(&output.stdout).contains("OK tls (qdrant)"));
        }
        assert!(!error.contains("fake-wrong-secret"));
        assert!(!error.contains("fixture-reader-only"));
        assert!(!error.contains('\u{1b}'));
    }
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
#[ignore = "creates a uniquely named collection ONLY in the disposable Qdrant fixture"]
async fn qdrant_scroll_and_lazy_details() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let name = format!("onetui_fixture_{}", std::process::id());
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
        let numeric = client
            .get_points(
                GetPointsBuilder::new(&name, vec![1_u64.into()])
                    .with_payload(true)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(numeric.result.len(), 1);
        assert!(!numeric.result[0].payload.is_empty());
        assert!(numeric.result[0].vectors.is_none());
        client
            .delete_points(
                DeletePointsBuilder::new(&name)
                    .points(vec![qdrant_client::qdrant::PointId::from(1_u64)])
                    .wait(true),
            )
            .await?;
        let missing = client
            .get_points(GetPointsBuilder::new(&name, vec![1_u64.into()]))
            .await?;
        assert!(missing.result.is_empty());
        Ok::<_, qdrant_client::QdrantError>(())
    }
    .await;
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
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

#[tokio::test]
#[ignore = "creates only its own collection in the disposable Qdrant fixture"]
async fn qdrant_large_payload_and_vector_variants() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let name = format!("onetui_variants_{}", std::process::id());
    let params = std::collections::HashMap::from([
        (
            "dense".to_owned(),
            VectorParamsBuilder::new(3, Distance::Dot).build(),
        ),
        (
            "multi".to_owned(),
            VectorParamsBuilder::new(3, Distance::Dot)
                .multivector_config(MultiVectorConfigBuilder::new(MultiVectorComparator::MaxSim))
                .build(),
        ),
    ]);
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorsConfig {
                    config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                        map: params,
                    })),
                })
                .sparse_vectors_config(SparseVectorConfig {
                    map: std::collections::HashMap::from([(
                        "sparse".to_owned(),
                        SparseVectorParams::default(),
                    )]),
                }),
        )
        .await
        .unwrap();
    let result = async {
        let empty = client.scroll(ScrollPointsBuilder::new(&name).limit(2)).await?;
        assert!(empty.result.is_empty());
        assert!(empty.next_page_offset.is_none());
        let vectors = NamedVectors::default()
            .add_vector("dense", Vector::new_dense(vec![1.0, 2.0, 3.0]))
            .add_vector("sparse", Vector::new_sparse(vec![3, 1000], vec![0.5, 1.5]))
            .add_vector("multi", Vector::new_multi(vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]]));
        client.upsert_points(UpsertPointsBuilder::new(&name, vec![
            PointStruct::new(1_u64, vectors, [("large", "x".repeat(2 * 1024 * 1024).into())]),
        ]).wait(true)).await?;
        let page = client.scroll(ScrollPointsBuilder::new(&name).limit(2).with_payload(false).with_vectors(false)).await?;
        assert_eq!(page.result.len(), 1);
        assert!(page.result[0].payload.is_empty());
        assert!(page.result[0].vectors.is_none());

        let detail = client.get_points(GetPointsBuilder::new(&name, vec![1_u64.into()])
            .with_payload(false).with_vectors(true)).await?;
        let vectors = detail.result[0].vectors.as_ref().unwrap();
        assert!(matches!(vectors.get_vector_by_name("dense"), Some(vector_output::Vector::Dense(v)) if v.data == [1.0, 2.0, 3.0]));
        assert!(matches!(vectors.get_vector_by_name("sparse"), Some(vector_output::Vector::Sparse(v)) if v.indices == [3, 1000] && v.values == [0.5, 1.5]));
        assert!(matches!(vectors.get_vector_by_name("multi"), Some(vector_output::Vector::MultiDense(v)) if v.vectors.len() == 2 && v.vectors.iter().all(|v| v.data.len() == 3)));

        let channel = tonic::transport::Endpoint::from_static(QDRANT)
            .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(5)).connect().await.unwrap();
        let mut bounded = PointsClient::new(channel).max_decoding_message_size(1024 * 1024);
        let request: qdrant_client::qdrant::GetPoints = GetPointsBuilder::new(&name, vec![1_u64.into()])
            .with_payload(true).with_vectors(false).build();
        let mut request = tonic::Request::new(request);
        request.metadata_mut().insert("api-key", "fixture-reader-only".parse().unwrap());
        let error = bounded.get(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::OutOfRange);
        // A rejected detail request must not make an ID-only retry fail.
        let retry = client.scroll(ScrollPointsBuilder::new(&name).limit(2).with_payload(false).with_vectors(false)).await?;
        assert_eq!(retry.result[0].id, page.result[0].id);
        Ok::<_, qdrant_client::QdrantError>(())
    }.await;
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
}
