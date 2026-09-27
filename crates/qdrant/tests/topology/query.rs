use super::*;
use onetui_core::provider::{QueryExecution, QueryRequest, WriteOutcome};

fn request(text: &str) -> QueryRequest {
    QueryRequest {
        page: PageRequest {
            resource: Resource::new("qdrant.query", vec![]),
            continuation: None,
        },
        text: text.into(),
    }
}

#[tokio::test]
async fn editor_forwards_native_writes_and_keeps_complete_error_responses() {
    let server = Server::new(json!({"operation_id":42,"status":"acknowledged"})).await;
    let e = server.executor();
    for status in [200, 400, 403, 500] {
        server.reply.lock().unwrap().status = status;
        let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
        let QueryExecution::Page(page) = e
            .execute_query(
                request("PUT /collections/demo/points?wait=false\n\n{native body}"),
                context,
            )
            .await
            .unwrap()
        else {
            panic!("Expected the complete native HTTP response");
        };
        assert!(text(&page, 0, 0).unwrap().starts_with(&status.to_string()));
        assert_eq!(
            page.rows[0].cells[1],
            Some(onetui_core::Value::Bytes(
                server.reply.lock().unwrap().body.clone()
            ))
        );
        assert!(!page.next);
        assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests.iter().all(|r| {
        r.starts_with("PUT /collections/demo/points?wait=false HTTP/1.1")
            && r.ends_with("{native body}")
            && r.contains("api-key: fixture-secret")
    }));
}

#[tokio::test]
async fn editor_cancellation_after_dispatch_is_unknown_and_reconnects() {
    let server = Server::new(json!({})).await;
    server.reply.lock().unwrap().delay = Duration::from_millis(300);
    let e = server.executor();
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    let execution = e.execute_query(request("DELETE /collections/demo"), context);
    let cancellation = async {
        while server.requests.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        cancel.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(execution, cancellation)
    })
    .await
    .expect("request must reach the server before cancellation");
    let QueryExecution::Write(result) = result.unwrap() else {
        panic!("Expected unknown outcome");
    };
    assert_eq!(result.outcome, WriteOutcome::Unknown);
    assert!(result.summary.contains("cancelled"));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Disconnected);
    server.reply.lock().unwrap().delay = Duration::ZERO;
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    assert!(matches!(
        e.execute_query(request("GET /collections"), context)
            .await
            .unwrap(),
        QueryExecution::Page(_)
    ));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn oversized_write_responses_have_unknown_outcomes_without_retries() {
    let server = Server::new(json!({})).await;
    let e = server.executor();
    for chunked in [false, true] {
        {
            let mut reply = server.reply.lock().unwrap();
            reply.body = vec![b'x'; PAGE_BYTES];
            reply.chunked = chunked;
        }
        let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
        let QueryExecution::Write(result) = e
            .execute_query(request("DELETE /collections/demo"), context)
            .await
            .unwrap()
        else {
            panic!("Expected unknown outcome");
        };
        assert_eq!(result.outcome, WriteOutcome::Unknown);
        assert!(result.summary.contains("display limit"));
    }
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}
