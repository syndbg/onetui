use std::time::Duration;

use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, QueryExecution, QueryRequest,
    RequestContext, ShutdownContext, WriteOutcome,
};
use onetui_core::{Page, Resource, Value};
use onetui_cql::{CqlExecutor, CqlProvider};

const SCYLLA: &str = "127.0.0.1:19042";
const CASSANDRA: &str = "127.0.0.1:19043";

fn executor(node: &str, credentials: Option<(&str, &str)>) -> CqlExecutor {
    let mut options = toml::from_str::<toml::Table>(&format!("nodes=['{node}']")).unwrap();
    if credentials.is_some() {
        options.insert("username_env".into(), "USER".into());
        options.insert("password_env".into(), "PASS".into());
    }
    CqlProvider
        .configure(&options, &|name| {
            let (user, password) = credentials?;
            Some(if name == "USER" { user } else { password }.into())
        })
        .unwrap()
}

fn reader() -> CqlExecutor {
    executor(SCYLLA, Some(("fixture_reader", "fixture-reader-only")))
}

// Each helper keeps its cancel sender alive: dropping it reads as a cancellation.
async fn check(e: &CqlExecutor) -> anyhow::Result<String> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    Ok(e.check(context).await?.summary)
}

async fn fetch(
    e: &CqlExecutor,
    id: &'static str,
    path: &[&str],
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    e.fetch_page(
        PageRequest {
            resource: Resource::new(id, path.iter().map(|p| (*p).into()).collect()),
            continuation,
        },
        context,
    )
    .await
}

async fn run(
    e: &CqlExecutor,
    text: &str,
    continuation: Option<String>,
) -> anyhow::Result<QueryExecution> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    e.execute_query(
        QueryRequest {
            page: PageRequest {
                resource: Resource::new("cql.query", vec![]),
                continuation,
            },
            text: text.into(),
        },
        context,
    )
    .await
}

fn page(execution: QueryExecution) -> Page {
    match execution {
        QueryExecution::Page(page) => page,
        QueryExecution::Write(result) => panic!("expected rows, got {}", result.summary),
    }
}

fn write(execution: QueryExecution) -> (WriteOutcome, String) {
    match execution {
        QueryExecution::Write(result) => (result.outcome, result.summary),
        QueryExecution::Page(page) => panic!("expected an outcome, got {} rows", page.rows.len()),
    }
}

fn names(page: &Page) -> Vec<String> {
    page.rows
        .iter()
        .filter_map(|row| row.cells[0].as_ref()?.text().map(Into::into))
        .collect()
}

/// The cell under a named column, since `SELECT *` column order is the server's.
fn cell<'a>(page: &'a Page, row: usize, column: &str) -> Option<&'a Value> {
    let index = page
        .columns
        .iter()
        .position(|c| c.name == column)
        .unwrap_or_else(|| panic!("no column {column}"));
    page.rows[row].cells[index].as_ref()
}

/// Read every page of a resource, following continuations.
async fn all_rows(e: &CqlExecutor, id: &'static str, path: &[&str]) -> Vec<Page> {
    let mut pages = vec![fetch(e, id, path, None).await.unwrap()];
    while let Some(token) = pages.last().unwrap().continuation.clone() {
        pages.push(fetch(e, id, path, Some(token)).await.unwrap());
        assert!(pages.len() < 10, "paging did not terminate");
    }
    pages
}

#[tokio::test]
#[ignore = "requires the disposable ScyllaDB fixture"]
async fn scylla_browses_metadata_pages_and_renders_values_exactly() {
    let mut e = reader();
    let summary = check(&e).await.unwrap();
    assert!(summary.contains("release"), "{summary}");
    assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);

    let keyspaces = fetch(&e, "cql.keyspaces", &[], None).await.unwrap();
    let demo = keyspaces
        .rows
        .iter()
        .find(|row| row.cells[0] == Some("onetui_demo".into()))
        .expect("demo keyspace");
    assert_eq!(
        demo.target,
        Some(Resource::new("cql.tables", vec!["onetui_demo".into()]))
    );
    assert!(names(&keyspaces).contains(&"system_schema".into()));

    let tables = fetch(&e, "cql.tables", &["onetui_demo"], None)
        .await
        .unwrap();
    for table in ["events", "profiles", "CaseSensitive"] {
        assert!(names(&tables).contains(&table.into()), "{table}");
    }

    let columns = fetch(&e, "cql.columns", &["onetui_demo", "events"], None)
        .await
        .unwrap();
    let amount = columns
        .rows
        .iter()
        .find(|row| row.cells[0] == Some("amount".into()))
        .unwrap();
    assert_eq!(amount.cells[3], Some("decimal".into()));

    // 250 rows in one partition and one in another: pages of 100, 100 and 51.
    let pages = all_rows(&e, "cql.rows", &["onetui_demo", "events"]).await;
    assert_eq!(
        pages.iter().map(|p| p.rows.len()).sum::<usize>(),
        251,
        "{:?}",
        pages.iter().map(|p| p.rows.len()).collect::<Vec<_>>()
    );
    assert!(pages[0].rows.len() == 100 && pages[0].next);
    assert!(!pages.last().unwrap().next);

    let first = &pages[0];
    let row = (0..first.rows.len())
        .find(|&i| {
            cell(first, i, "seq") == Some(&"5".into())
                && cell(first, i, "bucket") == Some(&"0".into())
        })
        .expect("seq 5 on the first page");
    assert_eq!(cell(first, row, "amount"), Some(&"12.34".into()));
    assert_eq!(
        cell(first, row, "big"),
        Some(&"1000000000000000000000".into())
    );
    assert_eq!(
        cell(first, row, "payload"),
        Some(&Value::Bytes(vec![0, 255]))
    );
    assert_eq!(
        cell(first, row, "at"),
        Some(&"2023-11-14 22:13:20.005Z".into())
    );
    assert_eq!(cell(first, row, "day"), Some(&"2000-02-29".into()));
    assert_eq!(
        cell(first, row, "id"),
        Some(&"00000000-0000-0000-0000-000000000005".into())
    );
    assert_eq!(
        cell(first, row, "tags"),
        Some(&Value::Json(r#"["a","b"]"#.into()))
    );
    assert_eq!(
        cell(first, row, "attrs"),
        Some(&Value::Json(r#"[{"key":"k","value":"5"}]"#.into()))
    );
    // The row written with only a key keeps its unset cells null.
    let null_row = pages
        .iter()
        .flat_map(|p| (0..p.rows.len()).map(move |i| (p, i)))
        .find(|(p, i)| cell(p, *i, "bucket") == Some(&"1".into()))
        .unwrap();
    assert!(cell(null_row.0, null_row.1, "note").is_none());
    assert_eq!(
        first
            .columns
            .iter()
            .find(|c| c.name == "attrs")
            .unwrap()
            .datatype,
        "map<text, int>"
    );

    // A case-sensitive name is quoted, not folded.
    let quoted = fetch(&e, "cql.rows", &["onetui_demo", "CaseSensitive"], None)
        .await
        .unwrap();
    assert_eq!(cell(&quoted, 0, "Value"), Some(&"quoted".into()));

    let profiles = fetch(&e, "cql.rows", &["onetui_demo", "profiles"], None)
        .await
        .unwrap();
    assert_eq!(
        cell(&profiles, 0, "home"),
        Some(&Value::Json(
            r#"{"fields":{"city":"Sofia","zip":null},"type":"onetui_demo.address"}"#.into()
        ))
    );
    assert_eq!(
        cell(&profiles, 0, "pair"),
        Some(&Value::Json(r#"["7","seven"]"#.into()))
    );

    // A continuation from one executor cannot resume another's read.
    let other = reader();
    assert!(
        fetch(
            &other,
            "cql.rows",
            &["onetui_demo", "events"],
            first.continuation.clone()
        )
        .await
        .is_err()
    );

    e.shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*e.status().borrow(), ConnectionStatus::Closed);
}

#[tokio::test]
#[ignore = "requires the disposable ScyllaDB fixture"]
async fn scylla_editor_pages_reads_and_reports_write_outcomes() {
    let e = reader();
    let text = "SELECT * FROM onetui_demo.events WHERE bucket = 0";
    let first = page(run(&e, text, None).await.unwrap());
    assert_eq!(first.rows.len(), 100);
    let second = page(run(&e, text, first.continuation.clone()).await.unwrap());
    assert_eq!(second.rows.len(), 100);
    assert_ne!(first.rows[0].cells, second.rows[0].cells);
    // A continuation belongs to the statement that produced it.
    assert!(
        run(&e, "SELECT * FROM onetui_demo.events", first.continuation)
            .await
            .is_err()
    );

    // The server refuses what the role may not do, and what it cannot parse.
    let (outcome, summary) = write(
        run(
            &e,
            "INSERT INTO onetui_writable.notes (id, body) VALUES (1, 'x')",
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(outcome, WriteOutcome::Rejected, "{summary}");
    let (outcome, summary) = write(run(&e, "SELEC nothing", None).await.unwrap());
    assert_eq!(outcome, WriteOutcome::Rejected, "{summary}");

    let writer = executor(SCYLLA, Some(("fixture_writer", "fixture-writer-only")));
    let table = format!("onetui_writable.editor_{}", std::process::id());
    for statement in [
        format!("CREATE TABLE IF NOT EXISTS {table} (id int PRIMARY KEY, body text)"),
        format!("INSERT INTO {table} (id, body) VALUES (1, 'written')"),
    ] {
        let (outcome, summary) = write(run(&writer, &statement, None).await.unwrap());
        assert_eq!(outcome, WriteOutcome::Applied, "{statement}: {summary}");
    }
    let read = page(
        run(
            &writer,
            &format!("SELECT body FROM {table} WHERE id = 1"),
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(read.rows[0].cells[0], Some("written".into()));
    let (outcome, summary) = write(
        run(&writer, &format!("DROP TABLE {table}"), None)
            .await
            .unwrap(),
    );
    assert_eq!(outcome, WriteOutcome::Applied, "{summary}");

    // A wrong password fails without echoing it.
    let wrong = executor(SCYLLA, Some(("fixture_reader", "not-the-fixture-password")));
    let error = check(&wrong).await.unwrap_err().to_string();
    assert!(!error.contains("not-the-fixture-password"), "{error}");
}

#[tokio::test]
#[ignore = "requires the disposable Cassandra fixture"]
async fn cassandra_speaks_the_same_protocol() {
    let e = executor(CASSANDRA, None);
    let summary = check(&e).await.unwrap();
    assert!(summary.contains("release 5."), "{summary}");

    let tables = fetch(&e, "cql.tables", &["onetui_demo"], None)
        .await
        .unwrap();
    assert!(names(&tables).contains(&"events".into()));
    let pages = all_rows(&e, "cql.rows", &["onetui_demo", "events"]).await;
    assert_eq!(pages.iter().map(|p| p.rows.len()).sum::<usize>(), 251);

    let first = &pages[0];
    let row = (0..first.rows.len())
        .find(|&i| cell(first, i, "bucket") == Some(&"0".into()))
        .unwrap();
    assert_eq!(cell(first, row, "amount"), Some(&"12.34".into()));
    assert_eq!(
        cell(first, row, "payload"),
        Some(&Value::Bytes(vec![0, 255]))
    );

    let id = std::process::id() as i32;
    let (outcome, summary) = write(
        run(
            &e,
            &format!("INSERT INTO onetui_writable.notes (id, body) VALUES ({id}, 'cassandra')"),
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(outcome, WriteOutcome::Applied, "{summary}");
    let read = page(
        run(
            &e,
            &format!("SELECT body FROM onetui_writable.notes WHERE id = {id}"),
            None,
        )
        .await
        .unwrap(),
    );
    assert_eq!(read.rows[0].cells[0], Some("cassandra".into()));
}
