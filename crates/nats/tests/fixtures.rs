use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, RequestContext, ShutdownContext,
};
use onetui_core::{Page, Resource, Value};
use onetui_nats::{NatsExecutor, NatsProvider};
use serde_json::{Value as Json, json};
use std::time::Duration;

#[cfg(unix)]
#[path = "fixtures/terminal.rs"]
mod terminal;

#[path = "fixtures/core.rs"]
mod core;

#[path = "fixtures/metadata.rs"]
mod metadata;

#[path = "fixtures/replay.rs"]
mod replay;

#[path = "fixtures/auth.rs"]
mod auth;

#[path = "fixtures/avro.rs"]
mod avro;

#[path = "fixtures/protobuf.rs"]
mod protobuf;

fn executor() -> NatsExecutor {
    configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-reader",
        "fixture-reader-only",
    )
}
fn configured(connection: &str, username: &str, password: &str) -> NatsExecutor {
    NatsProvider
        .configure(
            &toml::from_str(&format!(
                "{connection}\nusername_env='USER'\npassword_env='PASS'"
            ))
            .unwrap(),
            &|name| Some(if name == "USER" { username } else { password }.into()),
        )
        .unwrap()
}
async fn read(
    executor: &NatsExecutor,
    id: &'static str,
    path: &[&str],
    token: Option<String>,
    live: bool,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    let request = PageRequest {
        resource: Resource::new(id, path.iter().map(|s| (*s).into()).collect()),
        continuation: token,
    };
    if live {
        executor.follow_page(request, context).await
    } else {
        executor.fetch_page(request, context).await
    }
}
async fn close(executor: &mut NatsExecutor) {
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(3)))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
}
async fn admin() -> async_nats::Client {
    async_nats::ConnectOptions::new()
        .user_and_password("fixture-admin".into(), "fixture-admin-only".into())
        .max_reconnects(1)
        .connect("nats://127.0.0.1:14222")
        .await
        .unwrap()
}
async fn api(client: &async_nats::Client, op: &str, body: Json) -> Json {
    let response = client
        .request(
            format!("$JS.API.{op}"),
            serde_json::to_vec(&body).unwrap().into(),
        )
        .await
        .unwrap();
    let value: Json = serde_json::from_slice(&response.payload).unwrap();
    assert!(value.get("error").is_none(), "{value}");
    value
}
fn docker(args: &[&str]) -> std::process::Output {
    let output = std::process::Command::new("docker")
        .args([
            "compose",
            "--project-name",
            "onetui-fixtures",
            "--env-file",
            "/dev/null",
            "-f",
            "hack/compose.yaml",
        ])
        .args(args)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn reader_connections() -> usize {
    let output = docker(&[
        "exec",
        "-T",
        "nats",
        "wget",
        "-qO-",
        "http://127.0.0.1:8222/connz",
    ]);
    let connections: Json = serde_json::from_slice(&output.stdout).unwrap();
    connections["connections"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["name"] == "onetui")
        .count()
}

#[tokio::test]
#[ignore = "requires seeded disposable NATS; read-only browser credentials"]
async fn browsing_bytes_metadata_bookmarks_and_session_binding() {
    let mut executor = executor();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
    let streams = read(&executor, "nats.streams", &[], None, false)
        .await
        .unwrap();
    assert!(
        streams
            .rows
            .iter()
            .any(|r| r.target.as_ref().unwrap().path == ["DEMO_EVENTS"])
    );
    let info = read(&executor, "nats.stream_info", &["DEMO_EVENTS"], None, false)
        .await
        .unwrap();
    assert!(matches!(&info.rows[0].cells[0], Some(Value::Json(_))));
    let first = read(&executor, "nats.messages", &["DEMO_EVENTS"], None, false)
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 100);
    let bookmark = first.continuation.clone();
    let mut token = first.continuation;
    let mut count = 100;
    while let Some(next) = token {
        let page = read(
            &executor,
            "nats.messages",
            &["DEMO_EVENTS"],
            Some(next),
            false,
        )
        .await
        .unwrap();
        assert!(!page.rows.is_empty());
        count += page.rows.len();
        token = page.continuation;
    }
    assert_eq!(count, 1205);
    let replay = read(
        &executor,
        "nats.messages",
        &["DEMO_EVENTS"],
        bookmark.clone(),
        false,
    )
    .await
    .unwrap();
    assert_eq!(replay.rows[0].cells[0].as_ref().unwrap().bytes(), b"101");
    assert!(
        read(
            &executor,
            "nats.messages",
            &["DEMO_BINARY"],
            bookmark.clone(),
            false
        )
        .await
        .is_err()
    );
    let mut other = self::executor();
    assert!(
        read(
            &other,
            "nats.messages",
            &["DEMO_EVENTS"],
            bookmark.clone(),
            false
        )
        .await
        .is_err()
    );
    assert!(
        read(&executor, "nats.messages", &["DEMO_EVENTS"], bookmark, true)
            .await
            .is_err()
    );
    let binary = read(&executor, "nats.messages", &["DEMO_BINARY"], None, false)
        .await
        .unwrap();
    assert_eq!(
        binary.rows[0].cells[3],
        Some(Value::Bytes(vec![0, 255, 128, 27, 10]))
    );
    assert_eq!(binary.rows[3].cells[3], Some(Value::Bytes(vec![])));
    let headers = binary.rows[0].cells[4].as_ref().unwrap().bytes();
    assert!(
        headers
            .windows(b"X-Demo:".len())
            .filter(|s| *s == b"X-Demo:")
            .count()
            == 2
    );
    let empty = read(&executor, "nats.messages", &["DEMO_EMPTY"], None, false)
        .await
        .unwrap();
    assert!(empty.rows.is_empty() && !empty.next);
    close(&mut other).await;
    close(&mut executor).await;
}

#[tokio::test]
#[ignore = "creates and removes only test-owned disposable metadata streams"]
async fn server_sized_metadata_batches_are_paged_without_skips() {
    let admin = admin().await;
    let prefix = format!("META_{}_", std::process::id());
    for i in 0..110 {
        let name = format!("{prefix}{i:03}");
        api(
            &admin,
            &format!("STREAM.CREATE.{name}"),
            json!({"name": name, "storage": "memory"}),
        )
        .await;
    }
    let owned_prefix = prefix.clone();
    let tested = tokio::spawn(async move {
        let mut executor = executor();
        let mut token = None;
        let mut names = std::collections::BTreeSet::new();
        loop {
            let page = read(&executor, "nats.streams", &[], token, false)
                .await
                .unwrap();
            assert!(page.rows.len() <= 100);
            for row in page.rows {
                let name = row.target.unwrap().path[0].clone();
                assert!(names.insert(name), "metadata page repeated a stream");
            }
            token = page.continuation;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(
            names
                .iter()
                .filter(|name| name.starts_with(&owned_prefix))
                .count(),
            110
        );
        close(&mut executor).await;
    })
    .await;
    for i in 0..110 {
        api(&admin, &format!("STREAM.DELETE.{prefix}{i:03}"), json!({})).await;
    }
    tested.unwrap();
}

#[tokio::test]
#[ignore = "writes only a test-owned disposable stream; read-only connector must not touch consumers"]
async fn following_sparse_sequences_retention_and_byte_limits_without_consumer_changes() {
    let admin = admin().await;
    let name = format!("TEST_{}", std::process::id());
    let subject = format!("test.{}", std::process::id());
    api(
        &admin,
        &format!("STREAM.CREATE.{name}"),
        json!({"name": name, "subjects": [subject], "storage": "memory"}),
    )
    .await;
    let stream_name = name.clone();
    let test_admin = admin.clone();
    let tested = tokio::spawn(async move {
        let name = stream_name;
        let admin = test_admin;
        let js = async_nats::jetstream::new(admin.clone());
        api(&admin, &format!("CONSUMER.DURABLE.CREATE.{name}.application"), json!({
            "stream_name": name, "config": {"durable_name": "application", "ack_policy": "explicit", "deliver_policy": "all"}
        })).await;
        let before = api(&admin, &format!("CONSUMER.INFO.{name}.application"), json!({})).await;
        let mut executor = executor();
        let tail = read(&executor, "nats.messages", &[&name], None, true).await.unwrap();
        assert!(tail.rows.is_empty() && tail.continuation.is_some());
        for i in 0..205 {
            js.publish(subject.clone(), format!("message-{i}").into()).await.unwrap().await.unwrap();
        }
        api(&admin, &format!("STREAM.MSG.DELETE.{name}"), json!({"seq": 2})).await;
        let first = read(&executor, "nats.messages", &[&name], tail.continuation.clone(), true).await.unwrap();
        assert_eq!(first.rows.len(), 100);
        assert_eq!(first.rows[1].cells[0].as_ref().unwrap().bytes(), b"3");
        let second = read(&executor, "nats.messages", &[&name], first.continuation, true).await.unwrap();
        let third = read(&executor, "nats.messages", &[&name], second.continuation, true).await.unwrap();
        assert_eq!(third.rows.len(), 4);
        let quiet = read(&executor, "nats.messages", &[&name], third.continuation, true).await.unwrap();
        assert!(quiet.rows.is_empty());
        let large = vec![255; 400_000];
        for _ in 0..2 {
            js.publish(subject.clone(), large.clone().into()).await.unwrap().await.unwrap();
        }
        let large_first = read(&executor, "nats.messages", &[&name], quiet.continuation, true).await.unwrap();
        let large_second = read(&executor, "nats.messages", &[&name], large_first.continuation, true).await.unwrap();
        assert_eq!(large_first.rows.len(), 1);
        assert_eq!(large_second.rows.len(), 1);
        assert_eq!(large_second.rows[0].cells[3], Some(Value::Bytes(large)));
        assert_ne!(large_first.rows[0].cells[0], large_second.rows[0].cells[0]);
        js.publish(subject, vec![0; 800_000].into()).await.unwrap().await.unwrap();
        let error = read(&executor, "nats.messages", &[&name], large_second.continuation, true).await.unwrap_err();
        assert!(error.to_string().contains("1 MiB"), "{error:#}");
        let after = api(&admin, &format!("CONSUMER.INFO.{name}.application"), json!({})).await;
        assert_eq!(before["delivered"], after["delivered"]);
        assert_eq!(before["ack_floor"], after["ack_floor"]);
        let info = api(&admin, &format!("STREAM.INFO.{name}"), json!({})).await;
        assert_eq!(info["state"]["consumer_count"], 1);
        api(&admin, &format!("STREAM.PURGE.{name}"), json!({"seq": 200})).await;
        assert!(read(&executor, "nats.messages", &[&name], tail.continuation.clone(), true).await.unwrap_err().to_string().contains("unavailable"));
        api(&admin, &format!("STREAM.DELETE.{name}"), json!({})).await;
        api(&admin, &format!("STREAM.CREATE.{name}"), json!({"name": name, "storage": "memory"})).await;
        assert!(read(&executor, "nats.messages", &[&name], tail.continuation, true).await.is_err());
        close(&mut executor).await;
    }).await;
    api(&admin, &format!("STREAM.DELETE.{name}"), json!({})).await;
    tested.unwrap();
}

#[tokio::test]
#[ignore = "disposable NATS TLS/auth fixture; checks native errors and trust failures"]
async fn verified_tls_authentication_permissions_and_native_errors() {
    use std::io::Write;
    let cert = docker(&["exec", "-T", "nats-tls", "cat", "/tls/ca.crt"]);
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&cert.stdout).unwrap();
    let connection = format!(
        "servers=['tls://127.0.0.1:14223']\nca_file={:?}",
        ca.path().to_str().unwrap()
    );
    let mut valid = configured(&connection, "fixture-reader", "fixture-reader-only");
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    valid.check(context).await.unwrap();
    close(&mut valid).await;
    let mut untrusted = configured(
        "servers=['tls://127.0.0.1:14223']",
        "fixture-reader",
        "fixture-reader-only",
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let error = untrusted.check(context).await.err().unwrap();
    assert!(
        error.to_string().to_lowercase().contains("certificate"),
        "{error:#}"
    );
    close(&mut untrusted).await;
    let mut bad = configured(&connection, "fixture-reader", "wrong-fixture-secret");
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let error = bad.check(context).await.err().unwrap();
    assert!(!error.to_string().contains("wrong-fixture-secret"));
    assert!(
        error.to_string().to_lowercase().contains("authorization"),
        "{error:#}"
    );
    close(&mut bad).await;
    let mut denied = configured(&connection, "fixture-denied", "fixture-denied-only");
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let error = denied.check(context).await.err().unwrap();
    assert!(
        error
            .to_string()
            .to_lowercase()
            .contains("permissions violation"),
        "{error:#}"
    );
    close(&mut denied).await;
    let mut valid = executor();
    let error = read(&valid, "nats.messages", &["DOES_NOT_EXIST"], None, false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("10059"), "{error:#}");
    close(&mut valid).await;
}

#[tokio::test]
#[ignore = "pauses only the disposable NATS broker; cancellation, deadline and recovery"]
async fn cancellation_deadline_recovery_and_shutdown() {
    let baseline = reader_connections();
    let mut current = executor();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    current.check(context).await.unwrap();
    assert_eq!(reader_connections(), baseline + 1);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    current.check(context).await.unwrap();
    assert_eq!(
        reader_connections(),
        baseline + 1,
        "reuse the selected transport"
    );
    docker(&["pause", "nats"]);
    let tested = tokio::spawn(async move {
        let (cancel, context) = RequestContext::new(Duration::from_secs(5));
        let (result, ()) = tokio::join!(current.check(context), async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            let _ = cancel.send(());
        });
        assert!(result.err().unwrap().to_string().contains("cancelled"));
        let (_cancel, context) = RequestContext::new(Duration::from_millis(100));
        assert!(
            current
                .check(context)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("timed out")
        );
        current
    })
    .await;
    docker(&["unpause", "nats"]);
    current = tested.unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    current.check(context).await.unwrap();
    close(&mut current).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while reader_connections() != baseline {
        assert!(
            tokio::time::Instant::now() < deadline,
            "reader connection leaked after shutdown"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    assert!(current.check(context).await.is_err());
}
