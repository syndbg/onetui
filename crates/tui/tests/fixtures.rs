//! Keyboard/request-state journey; PostgreSQL transport contracts stay in the PostgreSQL package.
use std::io::Write;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use onetui_core::config::{Config, ResolvedConnection};
use onetui_tui::App;

fn key(app: &mut App, code: KeyCode) {
    app.key(KeyEvent::new(code, KeyModifiers::NONE));
}

async fn complete(app: &mut App) {
    let request = app.request.take().expect("keyboard action queued a read");
    let ResolvedConnection::Postgres { url, ca_file } = app.config.resolve(
        &request.alias,
        |_| Some("host=127.0.0.1 port=15432 user=onetui_reader password=fixture-reader-only dbname=onetui_fixture sslmode=disable".into()),
    ).unwrap() else { panic!("PostgreSQL fixture only") };
    let (_cancel, receiver) = tokio::sync::oneshot::channel();
    let result = onetui_postgres::fetch(
        url,
        ca_file,
        request.resource.clone(),
        request.offset,
        request.continuation.clone(),
        Duration::from_secs(5),
        receiver,
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
    let mut app = App::new(Config::load(file.path()).unwrap(), None);
    key(&mut app, KeyCode::Enter);
    complete(&mut app).await;
    select(&mut app, "public");
    key(&mut app, KeyCode::Enter);
    complete(&mut app).await;
    select(&mut app, "browse_composite");
    key(&mut app, KeyCode::Enter);
    complete(&mut app).await;
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
    complete(&mut app).await;
    assert_eq!(app.view.offset, 100);
    assert_eq!(app.view.page.rows[0].cells[1].as_deref(), Some("101"));
    key(&mut app, KeyCode::Char('p'));
    assert!(app.request.is_none());
    assert_eq!(app.view.page.continuation, token);
    key(&mut app, KeyCode::Char('m'));
    complete(&mut app).await;
    assert_eq!(app.view.resource.id, "postgres.columns");
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.view.resource.id, "postgres.rows");
    key(&mut app, KeyCode::Esc);
    select(&mut app, "restricted_rows");
    key(&mut app, KeyCode::Enter);
    complete(&mut app).await;
    assert!(app.error.as_ref().unwrap().contains("denied"));
    key(&mut app, KeyCode::Esc);
    select(&mut app, "browse_uuid");
    key(&mut app, KeyCode::Enter);
    complete(&mut app).await;
    assert!(app.view.page.rows.is_empty());
    assert!(app.error.is_none());
    key(&mut app, KeyCode::Char('q'));
    assert!(app.quit);
}
