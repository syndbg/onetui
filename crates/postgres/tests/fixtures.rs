//! Connector experiments against the fixed disposable local PostgreSQL fixture.
use onetui_core::provider::{Executor, PageRequest, Provider, RequestContext, ShutdownContext};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::io::Write;
use std::process::Command;
use std::time::Duration;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

const PG: &str = "host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable";
const PG_ADMIN: &str = "host=127.0.0.1 port=15432 user=onetui_fixture_admin password=fixture-admin-only dbname=onetui_fixture sslmode=disable";

#[tokio::test]
#[ignore = "requires make dev-seed in the local PostgreSQL fixture"]
async fn demo_data_browses_wide_typed_and_paged_rows() {
    let mut reader = provider();
    for (table, count, columns) in [
        ("customers", 2000, 16),
        ("events", 5000, 10),
        ("type_samples", 250, 40),
        ("wide_rows", 1500, 65),
        ("empty_rows", 0, 2),
    ] {
        let resource =
            onetui_core::Resource::new("postgres.rows", vec!["demo".into(), table.into()]);
        let mut token = None;
        let mut seen = std::collections::HashSet::new();
        loop {
            let page = browse(&reader, resource.clone(), token).await.unwrap();
            assert_eq!(page.columns.len(), columns, "{table}");
            assert!(page.rows.len() <= 100);
            for row in page.rows {
                assert_eq!(row.cells.len(), columns);
                assert!(seen.insert(row.cells[0].clone()), "duplicate in {table}");
            }
            if !page.next {
                break;
            }
            token = Some(page.continuation.expect("next page needs a token"));
        }
        assert_eq!(seen.len(), count, "{table}");
    }
    reader
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}

fn provider() -> onetui_postgres::PostgresExecutor {
    onetui_postgres::PostgresProvider
        .configure(&toml::from_str("url_env='DSN'").unwrap(), &|_| {
            Some(PG.into())
        })
        .unwrap()
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn native_error_preserves_sqlstate_detail_hint_and_redacts_password() {
    let admin = FixturePg::plain(PG_ADMIN).await;
    let schema = format!("onetui_diagnostic_{}", std::process::id());
    admin
        .client
        .batch_execute(&format!(
            r#"
        CREATE SCHEMA {schema};
        CREATE FUNCTION {schema}.fail() RETURNS integer LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION USING ERRCODE = 'P0001',
                MESSAGE = 'fixture rejection for fixture-reader-only',
                DETAIL = E'original detail\nsecond line\x1b[31m',
                HINT = 'original hint';
        END $$;
        CREATE VIEW {schema}.sample AS SELECT {schema}.fail() AS value;
        GRANT USAGE ON SCHEMA {schema} TO onetui_reader;
        GRANT SELECT ON {schema}.sample TO onetui_reader;
    "#
        ))
        .await
        .unwrap();
    let mut reader = provider();
    let result = browse(
        &reader,
        onetui_core::Resource::new("postgres.rows", vec![schema.clone(), "sample".into()]),
        None,
    )
    .await;
    reader
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    admin
        .client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    let error = result.unwrap_err().to_string();
    assert!(
        error.starts_with("PostgreSQL [P0001] ERROR: fixture rejection for [REDACTED]"),
        "{error}"
    );
    assert!(
        error.contains("DETAIL: original detail\\nsecond line\\u{1b}[31m"),
        "{error}"
    );
    assert!(error.contains("HINT: original hint"), "{error}");
    assert!(error.contains("CONTEXT:"), "{error}");
    assert!(!error.contains("fixture-reader-only"));
    assert!(!error.contains('\x1b'));
}

async fn browse(
    reader: &onetui_postgres::PostgresExecutor,
    resource: onetui_core::Resource,
    continuation: Option<String>,
) -> anyhow::Result<onetui_core::Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let resource_id = resource.id;
    let started = std::time::Instant::now();
    let result = reader
        .fetch_page(
            PageRequest {
                resource,
                continuation,
            },
            context,
        )
        .await;
    eprintln!(
        "postgres {resource_id} fetch/decode/format: {:?}",
        started.elapsed()
    );
    result
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn metadata_browsing_pages_types_permissions_and_connection_cleanup() {
    use onetui_core::Resource;
    let mut reader = provider();
    let schemas = browse(&reader, Resource::new("postgres.schemas", vec![]), None)
        .await
        .unwrap();
    assert!(
        schemas
            .rows
            .iter()
            .any(|row| row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("public"))
    );
    let tables = browse(
        &reader,
        Resource::new("postgres.relations", vec!["public".into()]),
        None,
    )
    .await
    .unwrap();
    let sample = tables
        .rows
        .iter()
        .find(|row| row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("sample_rows"))
        .unwrap();
    assert_eq!(sample.target.as_ref().unwrap().id, "postgres.rows");
    let columns = browse(
        &reader,
        Resource::new(
            "postgres.columns",
            sample.target.as_ref().unwrap().path.clone(),
        ),
        None,
    )
    .await
    .unwrap();
    assert!(columns.rows.iter().any(
        |row| row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("amount")
            && row.cells[1].as_ref().and_then(onetui_core::Value::text) == Some("numeric(30,10)")
    ));
    assert!(columns.rows.iter().any(
        |row| row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("note")
            && row.cells[2].as_ref().and_then(onetui_core::Value::text) == Some("no")
    ));
    let quoted = tables
        .rows
        .iter()
        .find(|row| {
            row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("quoted'; -- relation")
        })
        .unwrap();
    let columns = browse(
        &reader,
        Resource::new(
            "postgres.columns",
            quoted.target.as_ref().unwrap().path.clone(),
        ),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        columns.rows[0].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("odd\"column")
    );
    let empty = browse(
        &reader,
        Resource::new("postgres.relations", vec!["empty_schema".into()]),
        None,
    )
    .await
    .unwrap();
    assert!(empty.rows.is_empty());
    let denied = browse(
        &reader,
        Resource::new(
            "postgres.columns",
            vec!["public".into(), "restricted_rows".into()],
        ),
        None,
    )
    .await
    .unwrap_err();
    assert!(denied.to_string().contains("denied"));
    let first = browse(
        &reader,
        Resource::new("postgres.relations", vec!["pg_catalog".into()]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(first.rows.len(), 100);
    assert!(first.next);
    let second = browse(
        &reader,
        Resource::new("postgres.relations", vec!["pg_catalog".into()]),
        first.continuation.clone(),
    )
    .await
    .unwrap();
    assert!(!second.rows.is_empty());
    assert_ne!(first.rows[0].cells[0], second.rows[0].cells[0]);
    reader
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
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
async fn row_page(
    reader: &onetui_postgres::PostgresExecutor,
    relation: &str,
    token: Option<String>,
) -> anyhow::Result<onetui_core::Page> {
    browse(
        reader,
        onetui_core::Resource::new("postgres.rows", vec!["public".into(), relation.into()]),
        token,
    )
    .await
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn row_values_types_nulls_identifiers_and_fallback_selection() {
    let reader = provider();
    let page = row_page(&reader, "sample_rows", None).await.unwrap();
    assert!(page.notice.starts_with("OFFSET"));
    assert_eq!(page.rows.len(), 4);
    let first = page
        .rows
        .iter()
        .find(|row| row.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("1"))
        .unwrap();
    assert_eq!(first.cells[1], None);
    assert_eq!(
        first.cells[2].as_ref().and_then(onetui_core::Value::text),
        Some("12345678901234567890.1234567890")
    );
    assert_eq!(
        first.cells[3],
        Some(onetui_core::Value::Bytes(vec![0, 255]))
    );
    assert_eq!(
        first.cells[4].as_ref().and_then(onetui_core::Value::text),
        Some("{a,b}")
    );
    assert_eq!(
        first.cells[5].as_ref().and_then(onetui_core::Value::text),
        Some("paid")
    );
    assert_eq!(
        first.cells[6].as_ref().and_then(onetui_core::Value::text),
        Some("12.34")
    );
    assert_eq!(page.columns[2].datatype, "numeric(30,10)");
    assert_eq!(page.columns[6].datatype, "positive_amount");
    assert!(
        page.rows
            .iter()
            .any(|r| r.cells[1].as_ref().and_then(onetui_core::Value::text) == Some(""))
    );
    assert!(
        page.rows
            .iter()
            .any(|r| r.cells[1].as_ref().and_then(onetui_core::Value::text) == Some("NULL"))
    );
    let hostile = page
        .rows
        .iter()
        .find(|r| r.cells[0].as_ref().and_then(onetui_core::Value::text) == Some("4"))
        .unwrap()
        .cells[1]
        .as_ref()
        .unwrap()
        .text()
        .unwrap();
    assert!(hostile.contains("София 🌊"));
    assert!(hostile.contains('\n'));
    assert!(hostile.contains('\x1b'));
    let quoted = row_page(&reader, "quoted'; -- relation", None)
        .await
        .unwrap();
    assert_eq!(quoted.columns[0].name, "odd\"column");
    assert_eq!(
        quoted.rows[0].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("quoted value")
    );
    for relation in [
        "sample_view",
        "browse_nullable",
        "browse_partial",
        "browse_expression",
        "browse_uuid",
        "browse_parent",
    ] {
        assert!(
            row_page(&reader, relation, None)
                .await
                .unwrap()
                .notice
                .starts_with("OFFSET"),
            "{relation}"
        );
    }
    assert!(
        row_page(&reader, "browse_uuid", None)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(
        row_page(&reader, "restricted_rows", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("denied")
    );
    assert!(row_page(&reader, "missing'; --", None).await.is_err());
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn production_keysets_preserve_bigints_all_composite_components_and_raw_text() {
    let reader = provider();
    let mut offset_page = row_page(&reader, "browse_offset", None).await.unwrap();
    assert!(offset_page.notice.starts_with("OFFSET"));
    let mut offsets_seen = std::collections::BTreeSet::new();
    loop {
        offsets_seen.extend(offset_page.rows.iter().map(|r| r.cells[0].clone().unwrap()));
        if !offset_page.next {
            break;
        }
        assert_eq!(offset_page.rows.len(), 100);
        offset_page = row_page(&reader, "browse_offset", offset_page.continuation)
            .await
            .unwrap();
    }
    assert_eq!(offset_page.rows.len(), 5);
    assert_eq!(offsets_seen.len(), 205);
    let mut page = row_page(&reader, "browse_composite", None).await.unwrap();
    assert!(page.notice.starts_with("Keyset"));
    let mut expected = Vec::new();
    for tenant in ["a'; --\nСофия", "b"] {
        for id in 1..=205 {
            expected.push(vec![
                Some(tenant.into()),
                Some(id.to_string().into()),
                Some("value".into()),
            ]);
        }
    }
    let mut seen = Vec::new();
    loop {
        seen.extend(page.rows.iter().map(|r| r.cells.clone()));
        assert!(page.bytes() <= onetui_core::PAGE_BYTES);
        if !page.next {
            break;
        }
        page = row_page(&reader, "browse_composite", page.continuation)
            .await
            .unwrap();
    }
    assert_eq!(seen, expected);
    let first = row_page(&reader, "browse_bigint", None).await.unwrap();
    assert_eq!(
        first.rows[0].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("9007199254740993")
    );
    assert_eq!(
        first.rows[99].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("9007199254741092")
    );
    let second = row_page(&reader, "browse_bigint", first.continuation.clone())
        .await
        .unwrap();
    assert_eq!(
        second.rows[0].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("9007199254741093")
    );
    assert_eq!(
        row_page(&reader, "browse_bigint", None).await.unwrap().rows[0].cells[0],
        first.rows[0].cells[0]
    );
    assert!(
        row_page(&reader, "browse_composite", first.continuation.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("continuation")
    );
    let mut mismatched: serde_json::Value =
        serde_json::from_str(first.continuation.as_ref().unwrap()).unwrap();
    mismatched["offset"] = serde_json::json!(200);
    assert!(
        row_page(&reader, "browse_bigint", Some(mismatched.to_string()))
            .await
            .unwrap_err()
            .to_string()
            .contains("changed")
    );
    assert!(
        row_page(&reader, "browse_bigint", Some("not json".into()))
            .await
            .is_err()
    );
    let observer = FixturePg::plain(PG_ADMIN).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if observer.client.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'onetui-browse'", &[]).await.unwrap().get::<_, i64>(0) == 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("row browsing left a connection/transaction open");
    assert_eq!(
        second.rows.len(),
        100,
        "cached data survives disconnected reading time"
    );
    let last = row_page(&reader, "browse_bigint", second.continuation)
        .await
        .unwrap();
    assert_eq!(last.rows.len(), 5);
    assert!(!last.next);
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn production_row_limits_preserve_lookahead_position() {
    let reader = provider();
    assert!(
        row_page(&reader, "browse_zero", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("1..256 columns")
    );
    let admin = FixturePg::plain(PG_ADMIN).await;
    let columns = (0..257)
        .map(|i| format!("c{i} text"))
        .collect::<Vec<_>>()
        .join(", ");
    admin
        .client
        .batch_execute(&format!(
            "CREATE TABLE browse_wide ({columns}); GRANT SELECT ON browse_wide TO onetui_reader"
        ))
        .await
        .unwrap();
    assert!(
        row_page(&reader, "browse_wide", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("1..256 columns")
    );
    admin
        .client
        .batch_execute("DROP TABLE browse_wide")
        .await
        .unwrap();
    assert!(
        row_page(&reader, "browse_oversized", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("server text limit")
    );
    assert!(
        row_page(&reader, "browse_page_limit", None)
            .await
            .unwrap_err()
            .to_string()
            .contains("display limit")
    );
    let first = row_page(&reader, "browse_lookahead", None).await.unwrap();
    assert_eq!(first.rows.len(), 100);
    assert!(first.next);
    let token = first.continuation.clone();
    assert!(
        row_page(&reader, "browse_lookahead", token.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("server text limit")
    );
    assert_eq!(first.continuation, token);
    assert_eq!(
        first.rows[99].cells[0]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("100")
    );
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL fixture"]
async fn production_row_cancel_discards_active_connection_and_allows_new_read() {
    let observer = FixturePg::plain(PG_ADMIN).await;
    let (cancel, receiver) = tokio::sync::oneshot::channel();
    let reader = provider();
    let task = tokio::spawn(async move {
        reader
            .fetch_page(
                PageRequest {
                    resource: onetui_core::Resource::new(
                        "postgres.rows",
                        vec!["public".into(), "browse_slow".into()],
                    ),
                    continuation: None,
                },
                RequestContext {
                    deadline: tokio::time::Instant::now() + Duration::from_secs(60),
                    cancel: receiver,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if observer.client.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'onetui-browse' AND wait_event = 'PgSleep'", &[]).await.unwrap().get::<_, i64>(0) == 1 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("row request never reached server sleep");
    let started = std::time::Instant::now();
    cancel.send(()).unwrap();
    let error = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    eprintln!("postgres active-read cancellation: {:?}", started.elapsed());
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if observer.client.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'onetui-browse'", &[]).await.unwrap().get::<_, i64>(0) == 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("cancelled row connection remained open");
    let reader = provider();
    assert_eq!(
        row_page(&reader, "keyed_rows", None)
            .await
            .unwrap()
            .rows
            .len(),
        3
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
#[ignore = "requires the disposable PostgreSQL TLS fixture"]
async fn provider_reuses_idle_tls_session_retires_cancel_and_reconnects_explicitly() {
    use onetui_core::Resource;
    use onetui_core::provider::{
        ConnectionStatus, Executor, PageRequest, Provider, RequestContext, ShutdownContext,
    };
    let ca = fixture_ca("postgres", "/var/lib/postgresql/data/onetui-ca.crt");
    let options =
        toml::from_str(&format!("url_env='DSN'\nca_file='{}'", ca.path().display())).unwrap();
    let dsn = PG
        .replace("127.0.0.1", "localhost")
        .replace("sslmode=disable", "sslmode=require");
    let mut executor = onetui_postgres::PostgresProvider
        .configure(&options, &|_| Some(dsn.clone()))
        .unwrap();
    let mut status = executor.status();
    assert_eq!(*status.borrow(), ConnectionStatus::Configured);
    let observer = FixturePg::plain(PG_ADMIN).await;
    let resource = Resource::new(
        "postgres.rows",
        vec!["public".into(), "browse_composite".into()],
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let first = executor
        .fetch_page(
            PageRequest {
                resource: resource.clone(),
                continuation: None,
            },
            context,
        )
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 100);
    let pid: i32 = observer
        .client
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name = 'onetui-browse'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        observer
            .client
            .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid=$1", &[&pid])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    wait_for_backend(&observer.client, pid, "idle").await;
    let last_query: String = observer
        .client
        .query_one(
            "SELECT query_start::text FROM pg_stat_activity WHERE pid=$1",
            &[&pid],
        )
        .await
        .unwrap()
        .get(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        observer
            .client
            .query_one(
                "SELECT query_start::text FROM pg_stat_activity WHERE pid=$1",
                &[&pid]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        last_query,
        "idle browsing issued a background query"
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let second = executor
        .fetch_page(
            PageRequest {
                resource: resource.clone(),
                continuation: first.continuation.clone(),
            },
            context,
        )
        .await
        .unwrap();
    assert_eq!(
        second.rows[0].cells[1]
            .as_ref()
            .and_then(onetui_core::Value::text),
        Some("101")
    );
    assert_eq!(
        observer
            .client
            .query_one(
                "SELECT pid FROM pg_stat_activity WHERE application_name='onetui-browse'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i32>(0),
        pid
    );
    wait_for_backend(&observer.client, pid, "idle").await;

    let (cancel, context) = RequestContext::new(Duration::from_secs(60));
    let (result, ()) = tokio::join!(
        executor.fetch_page(
            PageRequest {
                resource: Resource::new(
                    "postgres.rows",
                    vec!["public".into(), "browse_slow".into()]
                ),
                continuation: None
            },
            context
        ),
        async {
            wait_for_backend(&observer.client, pid, "sleep").await;
            cancel.send(()).unwrap();
        }
    );
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    wait_for_backend(&observer.client, pid, "gone").await;
    assert_eq!(*status.borrow_and_update(), ConnectionStatus::Disconnected);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .fetch_page(
            PageRequest {
                resource: resource.clone(),
                continuation: first.continuation.clone(),
            },
            context,
        )
        .await
        .unwrap();
    let replacement: i32 = observer
        .client
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name='onetui-browse'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(pid, replacement);
    wait_for_backend(&observer.client, replacement, "idle").await;

    observer
        .client
        .query_one("SELECT pg_terminate_backend($1)", &[&replacement])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while *status.borrow_and_update() != ConnectionStatus::Disconnected {
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(first.rows.len(), 100);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .fetch_page(
            PageRequest {
                resource,
                continuation: None,
            },
            context,
        )
        .await
        .unwrap();
    let final_pid: i32 = observer
        .client
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name='onetui-browse'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(replacement, final_pid);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    let (result, ()) = tokio::join!(
        executor.fetch_page(
            PageRequest {
                resource: Resource::new(
                    "postgres.rows",
                    vec!["public".into(), "browse_slow".into()]
                ),
                continuation: None,
            },
            context,
        ),
        wait_for_backend(&observer.client, final_pid, "sleep")
    );
    assert!(result.is_err());
    wait_for_backend(&observer.client, final_pid, "gone").await;
    assert_eq!(*status.borrow(), ConnectionStatus::Disconnected);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let check_pid: i32 = observer
        .client
        .query_one(
            "SELECT pid FROM pg_stat_activity WHERE application_name='onetui-check'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_ne!(final_pid, check_pid);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*status.borrow(), ConnectionStatus::Closed);
    wait_for_backend(&observer.client, check_pid, "gone").await;
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    assert!(executor.check(context).await.is_err());
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
