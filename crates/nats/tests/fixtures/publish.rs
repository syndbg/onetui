use super::*;
use futures_util::StreamExt;
use onetui_core::provider::{QueryExecution, QueryRequest, WriteOutcome};

fn request(stream: &str, text: String) -> QueryRequest {
    QueryRequest {
        page: PageRequest {
            resource: Resource::new("nats.query", vec![stream.into()]),
            continuation: None,
        },
        text,
    }
}

#[tokio::test]
async fn invalid_publish_does_not_connect() {
    let executor = configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-admin",
        "fixture-admin-only",
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    let result = executor
        .execute_query(request("DEMO_LIVE", "PRODUCE DEMO_LIVE".into()), context)
        .await;
    assert!(result.is_err());
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    let result = executor
        .execute_query(
            request("DEMO_LIVE", "PRODUCE BAD/STREAM demo.live\n\nhello".into()),
            context,
        )
        .await;
    assert!(result.is_err());
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
}

#[tokio::test]
#[ignore = "creates and deletes only its own NATS JetStream streams"]
async fn publish_uses_the_selected_stream_and_waits_for_ack() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("ONETUI_PUBLISH_{}", std::process::id());
    let other = format!("ONETUI_OTHER_{}", std::process::id());
    let subject = format!("onetui.publish.{}", std::process::id());
    let other_subject = format!("onetui.other.{}", std::process::id());
    let _ = js.delete_stream(&stream).await;
    let _ = js.delete_stream(&other).await;
    let target = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    let other_stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: other.clone(),
            subjects: vec![other_subject.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut core_subscriber = admin.subscribe(other_subject.clone()).await.unwrap();
    admin.flush().await.unwrap();
    let executor = configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-admin",
        "fixture-admin-only",
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    let result = executor
        .execute_query(
            request(
                "OTHER_SELECTED_STREAM",
                format!("PRODUCE {stream} {subject}\n\nhello\nworld"),
            ),
            context,
        )
        .await
        .unwrap();
    assert!(
        matches!(result, QueryExecution::Write(write) if write.outcome == WriteOutcome::Applied && write.summary.contains(&stream) && write.summary.contains("sequence 1"))
    );
    assert_eq!(
        target.get_raw_message(1).await.unwrap().payload.as_ref(),
        b"hello\nworld"
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    let result = executor
        .execute_query(
            request(
                &stream,
                format!("PRODUCE {stream} {other_subject}\n\nwrong"),
            ),
            context,
        )
        .await
        .unwrap();
    assert!(
        matches!(result, QueryExecution::Write(write) if write.outcome == WriteOutcome::Unknown)
    );
    assert!(other_stream.get_raw_message(1).await.is_err());
    let delivered = tokio::time::timeout(Duration::from_secs(1), core_subscriber.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.payload.as_ref(), b"wrong");
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
    js.delete_stream(&stream).await.unwrap();
    js.delete_stream(&other).await.unwrap();
}

#[tokio::test]
#[ignore = "creates and deletes only its own NATS JetStream streams"]
async fn cancelling_a_publish_keeps_the_connection() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("ONETUI_CANCEL_{}", std::process::id());
    let subject = format!("onetui.cancel.{}", std::process::id());
    let _ = js.delete_stream(&stream).await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    })
    .await
    .unwrap();
    let executor = configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-admin",
        "fixture-admin-only",
    );
    // A cancellation that lands before dispatch is an ordinary error, and the session
    // stays usable either way. The dispatched case cannot be forced against a local
    // server, which always acknowledges first; its classification is covered by the
    // publish_failure unit test in crates/nats/src/provider.rs.
    let (cancel, context) = RequestContext::new(Duration::from_secs(10));
    cancel.send(()).unwrap();
    let result = executor
        .execute_query(
            request(&stream, format!("PRODUCE {stream} {subject}\n\nlate")),
            context,
        )
        .await;
    assert!(
        result.is_err(),
        "a cancelled request does not report a write"
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    executor.check(context).await.unwrap();
    js.delete_stream(&stream).await.unwrap();
}

#[tokio::test]
#[ignore = "creates and deletes only its own NATS JetStream streams"]
async fn a_payload_the_client_refuses_is_an_error_not_an_unknown_outcome() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("ONETUI_TOOBIG_{}", std::process::id());
    let subject = format!("onetui.toobig.{}", std::process::id());
    let _ = js.delete_stream(&stream).await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    })
    .await
    .unwrap();
    let executor = configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-admin",
        "fixture-admin-only",
    );
    // The client rejects an oversized payload before anything reaches the wire, so this
    // is an ordinary failure. Reporting it as unknown would tell the user to go and
    // inspect a stream that cannot have changed.
    let payload = "x".repeat(2 * 1024 * 1024);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    match executor
        .execute_query(
            request(&stream, format!("PRODUCE {stream} {subject}\n\n{payload}")),
            context,
        )
        .await
    {
        Err(error) => assert!(
            !error.to_string().contains("outcome unknown"),
            "a locally refused payload never reached the server: {error}"
        ),
        Ok(QueryExecution::Write(write)) => assert_ne!(
            write.outcome,
            WriteOutcome::Unknown,
            "a locally refused payload never reached the server: {}",
            write.summary
        ),
        Ok(QueryExecution::Page(_)) => panic!("a publish reports a write result"),
    }
    js.delete_stream(&stream).await.unwrap();
}

#[tokio::test]
#[ignore = "creates and deletes only its own NATS JetStream streams"]
async fn headers_and_encoded_payloads_reach_the_stream() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let stream = format!("ONETUI_HEADERS_{}", std::process::id());
    let subject = format!("onetui.headers.{}", std::process::id());
    let _ = js.delete_stream(&stream).await;
    let target = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    let executor = configured(
        "servers=['nats://127.0.0.1:14222']\ntls=false",
        "fixture-admin",
        "fixture-admin-only",
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(10));
    let published = executor
        .execute_query(
            request(
                &stream,
                format!("PRODUCE {stream} {subject}\nsrc: onetui\nOnetui-Encoding: base64\n\n//48"),
            ),
            context,
        )
        .await
        .unwrap();
    assert!(
        matches!(&published, QueryExecution::Write(write) if write.outcome == WriteOutcome::Applied)
    );
    let stored = target.get_raw_message(1).await.unwrap();
    // Base64 decoded to bytes that are not valid UTF-8, so a literal publish could not
    // have produced them.
    assert_eq!(stored.payload.as_ref(), &[255u8, 254, 60]);
    let headers = &stored.headers;
    assert_eq!(headers.get("src").map(|v| v.as_str()), Some("onetui"));
    // The encoding line selects decoding; it is never sent as a header.
    assert!(headers.get("Onetui-Encoding").is_none());
    js.delete_stream(&stream).await.unwrap();
}
