use super::*;
use onetui_core::{Page, Resource, Value};

fn reader(dsn: &str) -> onetui_postgres::PostgresExecutor {
    onetui_postgres::PostgresProvider
        .configure(&toml::from_str("url_env='DSN'").unwrap(), &|_| {
            Some(dsn.into())
        })
        .unwrap()
}

fn field<'a>(page: &'a Page, row: usize, name: &str) -> Option<&'a str> {
    let index = page
        .columns
        .iter()
        .position(|column| column.name == name)
        .unwrap_or_else(|| panic!("column {name} absent: {:?}", page.columns));
    page.rows[row].cells[index].as_ref().and_then(Value::text)
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL primary and streaming replica"]
async fn primary_and_standby_replication_views_preserve_native_fields_and_permissions() {
    let mut primary = reader(PG);
    let replicas = browse(
        &primary,
        Resource::new("postgres.replication", vec![]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(replicas.rows.len(), 1);
    assert_eq!(
        field(&replicas, 0, "application_name"),
        Some("onetui-fixture-standby")
    );
    assert_eq!(field(&replicas, 0, "state"), Some("streaming"));
    assert!(field(&replicas, 0, "sent_lsn").unwrap().contains('/'));
    assert!(replicas.notice.contains("not HA membership"));
    assert!(
        browse(
            &primary,
            Resource::new("postgres.wal_receiver", vec![]),
            None
        )
        .await
        .unwrap()
        .rows
        .is_empty()
    );
    let mut standby = reader(&PG.replace("15432", "15433"));
    let receiver = browse(
        &standby,
        Resource::new("postgres.wal_receiver", vec![]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(receiver.rows.len(), 1);
    assert_eq!(field(&receiver, 0, "status"), Some("streaming"));
    assert_eq!(field(&receiver, 0, "sender_host"), Some("postgres"));
    assert_eq!(field(&receiver, 0, "sender_port"), Some("5432"));
    assert!(
        !field(&receiver, 0, "conninfo")
            .unwrap()
            .contains("fixture-replica-only")
    );
    assert!(
        browse(
            &standby,
            Resource::new("postgres.replication", vec![]),
            None
        )
        .await
        .unwrap()
        .rows
        .is_empty()
    );
    let mut limited = reader(
        &PG.replace("onetui_reader", "onetui_limited_stats")
            .replace("fixture-reader-only", "fixture-limited-only"),
    );
    let restricted = browse(
        &limited,
        Resource::new("postgres.replication", vec![]),
        None,
    )
    .await
    .unwrap();
    assert_eq!(restricted.rows.len(), 1);
    assert!(field(&restricted, 0, "pid").is_some());
    assert_eq!(field(&restricted, 0, "state"), None);
    assert_eq!(field(&restricted, 0, "sent_lsn"), None);
    let result = sql_query(
        &primary,
        "SELECT current_setting('transaction_read_only') AS mode",
        None,
    )
    .await
    .unwrap();
    assert_eq!(field(&result, 0, "mode"), Some("on"));
    primary
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    standby
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    limited
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}
