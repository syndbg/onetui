//! Explicitly opted-in checks against fixtures/compose.yaml only; never production endpoints.
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, GetPointsBuilder, PointStruct, ScrollPointsBuilder,
    UpsertPointsBuilder, VectorParamsBuilder,
};
use tokio_postgres::{NoTls, SimpleQueryMessage};

const PG: &str = "host=127.0.0.1 port=15432 user=bpearl_reader password=fixture-reader-only dbname=bpearl_fixture sslmode=disable";
const QDRANT: &str = "http://127.0.0.1:16334";

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

#[test]
#[ignore = "requires the disposable Docker Compose PostgreSQL TLS fixture"]
fn postgres_tls_checks_ca_and_hostname() {
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
async fn postgres_text_cursor_preserves_types_and_rolls_back() {
    let (client, connection) = tokio_postgres::connect(PG, NoTls).await.unwrap();
    let driver = tokio::spawn(connection);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        client.batch_execute("BEGIN READ ONLY; SET LOCAL statement_timeout = '5s'; DECLARE bpearl_probe NO SCROLL CURSOR FOR SELECT * FROM public.sample_rows ORDER BY ordinal").await.unwrap();
        let page = client.simple_query("FETCH FORWARD 2 FROM bpearl_probe").await.unwrap();
        let rows: Vec<_> = page.iter().filter_map(|m| match m { SimpleQueryMessage::Row(r) => Some(r), _ => None }).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("note"), None);
        assert_eq!(rows[1].get("note"), Some(""));
        assert_eq!(rows[0].get("amount"), Some("12345678901234567890.1234567890"));
        assert_eq!(rows[0].get("bytes"), Some("\\x00ff"));
        assert_eq!(rows[0].get("tags"), Some("{a,b}"));
        assert_eq!(rows[0].get("state"), Some("paid"));
        assert_eq!(rows[0].get("price"), Some("12.34"));
        let next = client.simple_query("FETCH FORWARD 2 FROM bpearl_probe").await.unwrap();
        let next: Vec<_> = next.iter().filter_map(|m| match m { SimpleQueryMessage::Row(r) => Some(r), _ => None }).collect();
        assert_eq!(next.len(), 2);
        assert_eq!(next[0].get("ordinal"), Some("3"));
        assert_eq!(next[0].get("note"), Some("NULL"));
        client.batch_execute("ROLLBACK").await.unwrap();
        let row = client.query_one("SELECT count(*) FROM pg_cursors WHERE name = 'bpearl_probe'", &[]).await.unwrap();
        assert_eq!(row.get::<_, i64>(0), 0);
        assert!(client.execute("DELETE FROM public.sample_rows", &[]).await.is_err());
    }).await;
    drop(client);
    driver.abort();
    let _ = driver.await;
    result.unwrap();
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
