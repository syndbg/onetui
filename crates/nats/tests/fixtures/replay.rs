use super::*;
use onetui_core::provider::QueryRequest;

async fn query(
    executor: &NatsExecutor,
    stream: &str,
    text: &str,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    executor
        .query_page(
            QueryRequest {
                page: PageRequest {
                    resource: Resource::new("nats.query", vec![stream.into()]),
                    continuation,
                },
                text: text.into(),
            },
            context,
        )
        .await
}

#[tokio::test]
#[ignore = "creates/deletes one disposable stream; replay reader has no consumer permissions"]
async fn subject_sequence_time_empty_pages_and_bookmark_binding() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("REPLAY_{}", std::process::id());
    let prefix = format!("replay.{}", std::process::id());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![format!("{prefix}.*")],
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .unwrap();
    let name = stream.clone();
    let tested = tokio::spawn(async move {
        for i in 0..250 {
            js.publish(format!("{prefix}.{}", i % 2), format!("{i}").into())
                .await
                .unwrap()
                .await
                .unwrap();
        }
        let mut executor = executor();
        let text = json!({"subject":format!("{prefix}.0"),"start_sequence":1,"end_sequence":240})
            .to_string();
        let first = query(&executor, &name, &text, None).await.unwrap();
        assert_eq!(first.rows.len(), 100);
        assert_eq!(first.rows[0].cells[0], Some("1".into()));
        let bookmark = first.continuation;
        let next = query(&executor, &name, &text, bookmark.clone())
            .await
            .unwrap();
        assert_eq!(next.rows.len(), 20);
        assert!(!next.next);
        let again = query(&executor, &name, &text, bookmark.clone())
            .await
            .unwrap();
        assert_eq!(again.rows[0].cells, next.rows[0].cells);
        assert!(query(&executor, &name, "{}", bookmark).await.is_err());
        let future = r#"{"start_time":"9999-01-01T00:00:00Z"}"#;
        let empty = query(&executor, &name, future, None).await.unwrap();
        assert!(empty.rows.is_empty() && empty.next);
        let empty = query(&executor, &name, future, empty.continuation)
            .await
            .unwrap();
        assert!(empty.rows.is_empty() && empty.next);
        let empty = query(&executor, &name, future, empty.continuation)
            .await
            .unwrap();
        assert!(empty.rows.is_empty() && !empty.next);
        let ordinary = read(&executor, "nats.messages", &[&name], None, false)
            .await
            .unwrap();
        let cutoff = ordinary.rows[50].cells[2].as_ref().unwrap().text().unwrap();
        let timed = query(
            &executor,
            &name,
            &json!({"start_time":cutoff}).to_string(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(timed.rows[0].cells[0], Some("51".into()));
        assert_eq!(timed.rows.len(), 50);
        assert!(timed.next);
        assert!(
            read(&executor, "nats.query", &[&name], None, false)
                .await
                .is_err()
        );
        close(&mut executor).await;
    })
    .await;
    let info = api(&admin, &format!("STREAM.INFO.{stream}"), json!({})).await;
    assert_eq!(info["state"]["consumer_count"], 0);
    api(&admin, &format!("STREAM.DELETE.{stream}"), json!({})).await;
    tested.unwrap();
}
