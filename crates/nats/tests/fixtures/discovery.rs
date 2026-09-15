use super::*;

#[tokio::test]
#[ignore = "requires disposable two-server NATS system-account fixtures"]
async fn native_system_discovery_observes_both_servers_and_rejects_denied_permissions() {
    let config =
        "servers=['nats://127.0.0.1:14226']\ntls=false\njetstream=false\nsystem_discovery=true";
    let mut executor = configured(config, "fixture-reader", "fixture-reader-only");
    let menu = read(&executor, "nats.resources", &[], None, false)
        .await
        .unwrap();
    assert!(
        menu.rows
            .iter()
            .any(|row| row.target == Some(Resource::new("nats.servers", vec![])))
    );
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    let page = loop {
        let page = read(&executor, "nats.servers", &[], None, false)
            .await
            .unwrap();
        if page.rows.len() == 2 {
            break page;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "second server did not respond: {page:?}"
        );
    };
    let mut names = page
        .rows
        .iter()
        .map(|row| row.cells[1].as_ref().unwrap().bytes().to_vec())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [b"system-a".to_vec(), b"system-b".to_vec()]);
    assert!(
        page.rows
            .iter()
            .all(|row| row.cells[3] == Some("onetui-system".into()))
    );
    assert!(page.notice.contains("not complete membership"));
    assert!(!page.next);
    assert!(
        matches!(&page.rows[0].cells[7], Some(Value::Json(raw)) if serde_json::from_str::<Json>(raw).unwrap()["statsz"].is_object())
    );
    assert!(
        read(
            &executor,
            "nats.servers",
            &[],
            Some("invalid".into()),
            false
        )
        .await
        .is_err()
    );
    assert!(
        read(&executor, "nats.servers", &[], None, true)
            .await
            .is_err()
    );
    close(&mut executor).await;

    let mut denied = configured(config, "fixture-denied", "fixture-denied-only");
    let error = read(&denied, "nats.servers", &[], None, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.to_lowercase().contains("permissions violation"),
        "{error}"
    );
    assert!(!error.contains("fixture-denied-only"));
    close(&mut denied).await;
}

#[tokio::test]
#[ignore = "requires disposable NATS system-account fixture"]
async fn discovery_deadline_cancellation_and_reopen_release_the_session() {
    let mut executor = configured(
        "servers=['nats://127.0.0.1:14226']\ntls=false\njetstream=false\nsystem_discovery=true",
        "fixture-reader",
        "fixture-reader-only",
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(3));
    executor.check(context).await.unwrap();
    let request = || PageRequest {
        resource: Resource::new("nats.servers", vec![]),
        continuation: None,
    };
    let (_cancel, context) = RequestContext::new(Duration::from_millis(100));
    assert!(
        executor
            .fetch_page(request(), context)
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    assert_eq!(
        read(&executor, "nats.servers", &[], None, false)
            .await
            .unwrap()
            .rows
            .len(),
        2
    );
    let (cancel, context) = RequestContext::new(Duration::from_secs(3));
    let (result, _) = tokio::join!(executor.fetch_page(request(), context), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = cancel.send(());
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    assert_eq!(
        read(&executor, "nats.servers", &[], None, false)
            .await
            .unwrap()
            .rows
            .len(),
        2
    );
    close(&mut executor).await;
}

#[tokio::test]
async fn disabled_discovery_and_invalid_paths_fail_without_connecting() {
    let mut executor = configured(
        "servers=['nats://127.0.0.1:1']\ntls=false\njetstream=false",
        "user",
        "secret",
    );
    let error = read(&executor, "nats.servers", &[], None, false)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("system_discovery=true"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
    assert!(
        read(&executor, "nats.servers", &["other"], None, false)
            .await
            .is_err()
    );
    close(&mut executor).await;
}
