use super::*;

pub(super) fn subscriptions(subject: &str) -> usize {
    let output = docker(&[
        "exec",
        "-T",
        "nats",
        "wget",
        "-qO-",
        "http://127.0.0.1:8222/connz?subs=1",
    ]);
    let connections: Json = serde_json::from_slice(&output.stdout).unwrap();
    connections["connections"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| {
            c["name"] == "onetui"
                && c["subscriptions_list"]
                    .as_array()
                    .is_some_and(|list| list.iter().any(|s| s == subject))
        })
        .count()
}

#[tokio::test]
#[ignore = "disposable Core NATS subjects only; no JetStream consumer or application acknowledgements"]
async fn core_subscriptions_bytes_limits_restart_and_unsubscribe() {
    let subject = format!("core.{}", std::process::id());
    let options = format!(
        "servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=['{subject}']"
    );
    let mut executor = configured(&options, "fixture-admin", "fixture-admin-only");
    let admin = admin().await;
    let (_cancel, context) = RequestContext::new(Duration::from_secs(3));
    assert!(
        executor
            .check(context)
            .await
            .unwrap()
            .summary
            .contains("Core")
    );
    let menu = read(&executor, "nats.resources", &[], None, false)
        .await
        .unwrap();
    assert_eq!(menu.rows.len(), 1);
    assert!(
        read(&executor, "nats.streams", &[], None, false)
            .await
            .is_err()
    );
    let idle = read(&executor, "nats.core_messages", &[&subject], None, false)
        .await
        .unwrap();
    assert!(idle.rows.is_empty() && idle.continuation.is_none());
    assert_eq!(subscriptions(&subject), 0);
    assert!(
        read(
            &executor,
            "nats.core_messages",
            &["unconfigured"],
            None,
            true
        )
        .await
        .is_err()
    );
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    assert_eq!(subscriptions(&subject), 1);
    let mut headers = async_nats::HeaderMap::new();
    headers.append("X-Demo", "one");
    headers.append("X-Demo", "two");
    admin
        .publish_with_headers(subject.clone(), headers, vec![0, 255, 128, 27].into())
        .await
        .unwrap();
    admin
        .publish(subject.clone(), Vec::new().into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    let mut token = start.continuation.clone();
    let mut rows = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while rows.len() < 2 {
        assert!(tokio::time::Instant::now() < deadline);
        let page = read(&executor, "nats.core_messages", &[&subject], token, true)
            .await
            .unwrap();
        token = page.continuation;
        rows.extend(page.rows);
        tokio::task::yield_now().await;
    }
    assert_eq!(rows[0].cells[2], Some(Value::Bytes(vec![0, 255, 128, 27])));
    assert_eq!(rows[1].cells[2], Some(Value::Bytes(Vec::new())));
    let headers: Json = serde_json::from_slice(rows[0].cells[3].as_ref().unwrap().bytes()).unwrap();
    assert_eq!(headers[0]["values"], json!(["one", "two"]));
    executor
        .stop_follow(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
    assert_eq!(subscriptions(&subject), 0);
    assert!(
        read(&executor, "nats.core_messages", &[&subject], token, true)
            .await
            .is_err()
    );
    admin
        .publish(subject.clone(), "missed while stopped".into())
        .await
        .unwrap();
    admin.flush().await.unwrap();
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let quiet = read(
        &executor,
        "nats.core_messages",
        &[&subject],
        start.continuation,
        true,
    )
    .await
    .unwrap();
    assert!(quiet.rows.is_empty());
    // A burst can still be in transit when a busy runner reads the next page.
    let error = tokio::time::timeout(Duration::from_secs(3), async {
        let mut token = quiet.continuation;
        loop {
            for _ in 0..100 {
                admin
                    .publish(subject.clone(), "overflow".into())
                    .await
                    .unwrap();
            }
            admin.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            match read(&executor, "nats.core_messages", &[&subject], token, true).await {
                Err(error) => break error,
                Ok(page) => token = page.continuation,
            }
        }
    })
    .await
    .expect("Core subscription did not overflow");
    assert!(error.to_string().contains("overflow"), "{error:#}");
    close(&mut executor).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while subscriptions(&subject) != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Core subscription leaked"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
