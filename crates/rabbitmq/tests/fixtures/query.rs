use super::*;
use onetui_core::provider::{QueryExecution, QueryRequest, WriteOutcome};
use serde_json::Value as Json;

struct Queue(String);

impl Drop for Queue {
    fn drop(&mut self) {
        // Remove only this test's queue, including when an assertion fails.
        let _ = Command::new("curl")
            .args([
                "--silent",
                "--max-time",
                "5",
                "-u",
                "fixture-admin:fixture-admin-only",
                "-X",
                "DELETE",
            ])
            .arg(format!("{URL}/api/queues/%2F/{}", self.0))
            .output();
    }
}

/// A read returns a page of messages; the fixture asserts on its decoded payload.
async fn read(e: &RabbitMqExecutor, text: String) -> Page {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let QueryExecution::Page(page) = e
        .execute_query(
            QueryRequest {
                page: PageRequest {
                    resource: Resource::new("rabbitmq.query", vec![]),
                    continuation: None,
                },
                text,
            },
            context,
        )
        .await
        .unwrap()
    else {
        panic!("Expected a page");
    };
    page
}

/// A write reports an outcome and a summary carrying the native status.
async fn write(e: &RabbitMqExecutor, text: String) -> (WriteOutcome, String) {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let QueryExecution::Write(result) = e
        .execute_query(
            QueryRequest {
                page: PageRequest {
                    resource: Resource::new("rabbitmq.query", vec![]),
                    continuation: None,
                },
                text,
            },
            context,
        )
        .await
        .unwrap()
    else {
        panic!("Expected a write outcome");
    };
    (result.outcome, result.summary)
}

async fn raw(e: &RabbitMqExecutor, text: String) -> (u16, Json) {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let QueryExecution::Page(page) = e
        .execute_query(
            QueryRequest {
                page: PageRequest {
                    resource: Resource::new("rabbitmq.query", vec![]),
                    continuation: None,
                },
                text,
            },
            context,
        )
        .await
        .unwrap()
    else {
        panic!("Expected native HTTP response");
    };
    let status = page.rows[0].cells[0].as_ref().unwrap().text().unwrap()[..3]
        .parse()
        .unwrap();
    let Some(Value::Bytes(body)) = &page.rows[0].cells[1] else {
        panic!("Expected response bytes");
    };
    (
        status,
        if body.is_empty() {
            Json::Null
        } else {
            serde_json::from_slice(body).unwrap()
        },
    )
}

#[tokio::test]
#[ignore = "requires disposable RabbitMQ management fixture"]
async fn verbs_declare_publish_read_and_delete_with_permission_errors() {
    let queue = Queue(format!(
        "onetui-write-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = executor(URL, "fixture-admin", "fixture-admin-only", None);
    let reader = executor(URL, "fixture-reader", "fixture-reader-only", None);
    let declare = format!(
        "DECLARE queue / {}\n\n{{\"durable\":true,\"auto_delete\":false,\"arguments\":{{}}}}",
        queue.0
    );

    // A reader lacks the native permission, which is a rejection carrying the reason.
    let (outcome, summary) = write(&reader, declare.clone()).await;
    assert_eq!(outcome, WriteOutcome::Rejected, "{summary}");
    assert!(
        summary.contains("401") || summary.contains("403"),
        "{summary}"
    );
    assert_eq!(*reader.status().borrow(), ConnectionStatus::Connected);

    let (outcome, summary) = write(&admin, declare).await;
    assert_eq!(outcome, WriteOutcome::Applied, "{summary}");

    // A routing key matching the queue is routed; a missing one is applied and says so.
    for (key, routed) in [
        (queue.0.clone(), true),
        (format!("{}-missing", queue.0), false),
    ] {
        let (outcome, summary) = write(
            &admin,
            // Hex carries bytes that are not valid UTF-8 through the editor.
            format!("PUBLISH / amq.default {key}\nOnetui-Encoding: hex\n\n00ff"),
        )
        .await;
        assert_eq!(outcome, WriteOutcome::Applied, "{summary}");
        assert_eq!(
            !summary.contains("no queue matched the routing key"),
            routed,
            "{summary}"
        );
    }

    // The destructive read returns the published bytes intact.
    let page = read(&admin, format!("GET / {} 1 ack", queue.0)).await;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].cells[4], Some(Value::Bytes(vec![0, 255])));

    let (outcome, summary) = write(&admin, format!("PURGE / {}", queue.0)).await;
    assert_eq!(outcome, WriteOutcome::Applied, "{summary}");

    let (outcome, summary) = write(&admin, format!("DELETE queue / {}", queue.0)).await;
    assert_eq!(outcome, WriteOutcome::Applied, "{summary}");

    // RAW reaches what the verbs do not model, and confirms the queue is gone.
    let (status, _) = raw(&admin, format!("RAW GET /api/queues/%2F/{}", queue.0)).await;
    assert_eq!(status, 404);
}
