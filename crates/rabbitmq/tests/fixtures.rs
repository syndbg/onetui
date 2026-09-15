use std::collections::BTreeSet;
use std::io::Write;
use std::process::Command;
use std::time::Duration;

use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, RequestContext, ShutdownContext,
};
use onetui_core::{Page, Resource, Value};
use onetui_rabbitmq::{RabbitMqExecutor, RabbitMqProvider};

const URL: &str = "http://127.0.0.1:15672";

fn executor(url: &str, username: &str, password: &str, ca: Option<&str>) -> RabbitMqExecutor {
    let mut options = toml::from_str::<toml::Table>(&format!(
        "url='{url}'\nusername_env='USER'\npassword_env='PASS'"
    ))
    .unwrap();
    if let Some(ca) = ca {
        options.insert("ca_file".into(), ca.into());
    }
    RabbitMqProvider
        .configure(&options, &|name| {
            Some(if name == "USER" { username } else { password }.into())
        })
        .unwrap()
}

async fn fetch(
    executor: &RabbitMqExecutor,
    id: &'static str,
    path: Vec<String>,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .fetch_page(
            PageRequest {
                resource: Resource::new(id, path),
                continuation,
            },
            context,
        )
        .await
}

#[tokio::test]
#[ignore = "requires disposable RabbitMQ management and traffic fixtures"]
async fn native_metadata_metrics_paging_and_scoped_names_are_read_only() {
    let mut executor = executor(URL, "fixture-reader", "fixture-reader-only", None);
    assert_eq!(
        fetch(&executor, "rabbitmq.resources", vec![], None)
            .await
            .unwrap()
            .rows
            .len(),
        10
    );
    let overview = fetch(&executor, "rabbitmq.overview", vec![], None)
        .await
        .unwrap();
    assert_eq!(overview.rows.len(), 1);
    assert!(matches!(&overview.rows[0].cells[4], Some(Value::Json(_))));
    let nodes = fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    assert_eq!(nodes.rows[0].cells[0], Some("rabbit@rabbitmq".into()));
    assert_eq!(nodes.rows[0].cells[1], Some(Value::Json("true".into())));
    let vhosts = fetch(&executor, "rabbitmq.vhosts", vec![], None)
        .await
        .unwrap();
    assert!(vhosts.rows.iter().any(|r| r.target == Some(Resource::new("rabbitmq.vhost", vec!["demo / София".into()]))));
    let first = fetch(&executor, "rabbitmq.queues", vec!["/".into()], None)
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 100);
    let second = fetch(
        &executor,
        "rabbitmq.queues",
        vec!["/".into()],
        first.continuation.clone(),
    )
    .await
    .unwrap();
    assert_eq!(second.rows.len(), 23);
    assert!(!second.next);
    let names = first
        .rows
        .iter()
        .chain(&second.rows)
        .map(|r| r.cells[0].as_ref().unwrap().text().unwrap().to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), 123);
    assert!(names.contains("demo_quorum") && names.contains("demo_stream"));
    let repeated = fetch(
        &executor,
        "rabbitmq.queues",
        vec!["/".into()],
        first.continuation,
    )
    .await
    .unwrap();
    assert_eq!(repeated.rows[0].cells[0], second.rows[0].cells[0]);
    assert_eq!(
        fetch(
            &executor,
            "rabbitmq.queues",
            vec!["demo / София".into()],
            None
        )
        .await
        .unwrap()
        .rows
        .len(),
        1
    );
    assert!(
        !fetch(&executor, "rabbitmq.exchanges", vec!["/".into()], None)
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        fetch(&executor, "rabbitmq.policies", vec!["/".into()], None)
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    let bindings = fetch(&executor, "rabbitmq.bindings", vec!["/".into()], None)
        .await
        .unwrap();
    assert_eq!(bindings.rows.len(), 100);
    assert!(bindings.next);
    for id in [
        "rabbitmq.connections",
        "rabbitmq.channels",
        "rabbitmq.consumers",
    ] {
        let page = fetch(&executor, id, vec!["/".into()], None).await.unwrap();
        assert!(!page.rows.is_empty(), "{id}: fixture traffic is missing");
        assert!(matches!(
            page.rows[0].cells.last(),
            Some(Some(Value::Json(_)))
        ));
    }
    let all = fetch(&executor, "rabbitmq.queues", vec![], None)
        .await
        .unwrap();
    assert_eq!(all.rows.len(), 100);
    assert!(all.next);
    let error = fetch(
        &executor,
        "rabbitmq.queues",
        vec!["not-present".into()],
        None,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("404") && error.contains("\"error\":\"Object Not Found\""),
        "{error}"
    );
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
}

#[tokio::test]
#[ignore = "requires disposable RabbitMQ management TLS fixture"]
async fn native_tls_trust_hostname_credentials_and_reopen() {
    let certificate = Command::new("docker")
        .args(["exec", "onetui-fixtures-rabbitmq-1", "cat", "/tls/ca.crt"])
        .output()
        .unwrap();
    assert!(certificate.status.success());
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&certificate.stdout).unwrap();
    let ca_path = ca.path().to_str().unwrap();
    let mut trusted = executor(
        "https://localhost:15671",
        "fixture-reader",
        "fixture-reader-only",
        Some(ca_path),
    );
    fetch(&trusted, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    fetch(&trusted, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    trusted.check(context).await.unwrap();
    for denied in [
        executor(
            "https://127.0.0.1:15671",
            "fixture-reader",
            "fixture-reader-only",
            Some(ca_path),
        ),
        executor(
            "https://localhost:15671",
            "fixture-reader",
            "fixture-reader-only",
            None,
        ),
    ] {
        let error = fetch(&denied, "rabbitmq.nodes", vec![], None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.to_lowercase().contains("certificate"), "{error}");
    }
    let wrong = executor(URL, "fixture-reader", "wrong-secret", None);
    let error = fetch(&wrong, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("401") && !error.contains("wrong-secret"),
        "{error}"
    );
    let denied = executor(URL, "fixture-denied", "fixture-denied-only", None);
    let error = fetch(&denied, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("401") || error.contains("403"), "{error}");
    assert!(!error.contains("fixture-denied-only"));
    trusted
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(
        fetch(&trusted, "rabbitmq.nodes", vec![], None)
            .await
            .is_err()
    );
}

#[cfg(unix)]
#[path = "fixtures/terminal.rs"]
mod terminal;
