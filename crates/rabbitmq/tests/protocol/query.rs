use super::*;
use onetui_core::provider::{QueryExecution, QueryRequest, WriteOutcome};

fn request(text: &str) -> QueryRequest {
    QueryRequest {
        page: PageRequest {
            resource: Resource::new("rabbitmq.query", vec![]),
            continuation: None,
        },
        text: text.into(),
    }
}

async fn execute(e: &RabbitMqExecutor, text: &str) -> anyhow::Result<QueryExecution> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    e.execute_query(request(text), context).await
}

#[tokio::test]
async fn verbs_build_native_paths_and_report_write_outcomes() {
    let server = Server::start(vec![
        response("200 OK", r#"{"routed":false}"#),
        response("200 OK", r#"{"routed":true}"#),
        response(
            "400 Bad Request",
            r#"{"error":"bad_request","reason":"native syntax error"}"#,
        ),
        response("403 Forbidden", r#"{"error":"not_authorised"}"#),
        response("204 No Content", ""),
        response("204 No Content", ""),
        response("201 Created", ""),
    ]);
    let e = executor(&server.url);
    for (text, outcome, expected) in [
        // A publish the broker accepted but routed nowhere is applied and says so.
        (
            "PUBLISH / amq.default demo\n\nhello",
            WriteOutcome::Applied,
            "no queue matched the routing key",
        ),
        (
            "PUBLISH / amq.default demo\n\nhello",
            WriteOutcome::Applied,
            "Published to amq.default on / with routing key demo",
        ),
        (
            "DECLARE queue / demo\n\n{\"durable\":true}",
            WriteOutcome::Rejected,
            "native syntax error",
        ),
        (
            "DELETE queue / demo",
            WriteOutcome::Rejected,
            "not_authorised",
        ),
        (
            "DELETE queue / demo",
            WriteOutcome::Applied,
            "Deleted queue demo on /",
        ),
        ("PURGE / demo", WriteOutcome::Applied, "Purged demo on /"),
        (
            "DECLARE vhost staging",
            WriteOutcome::Applied,
            "Declared vhost staging",
        ),
    ] {
        let QueryExecution::Write(result) = execute(&e, text).await.unwrap() else {
            panic!("{text}: a write must report an outcome, not a page");
        };
        assert_eq!(result.outcome, outcome, "{text}: {}", result.summary);
        assert!(
            result.summary.contains(expected),
            "{text}: {}",
            result.summary
        );
        assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 7);
    // The default vhost is typed as / and encoded here, which is the whole point.
    assert!(requests[0].starts_with("POST /api/exchanges/%2F/amq.default/publish HTTP/1.1"));
    // The payload is carried as base64 so any bytes survive the trip.
    assert!(requests[0].ends_with(
        r#"{"payload":"aGVsbG8=","payload_encoding":"base64","properties":{},"routing_key":"demo"}"#
    ));
    assert!(requests[0].to_lowercase().contains("authorization: basic "));
    assert!(requests[2].starts_with("PUT /api/queues/%2F/demo HTTP/1.1"));
    assert!(requests[2].ends_with(r#"{"durable":true}"#));
    assert!(requests[3].starts_with("DELETE /api/queues/%2F/demo HTTP/1.1"));
    assert!(requests[5].starts_with("DELETE /api/queues/%2F/demo/contents HTTP/1.1"));
    // A vhost is addressed by name alone.
    assert!(requests[6].starts_with("PUT /api/vhosts/staging HTTP/1.1"));
}

#[tokio::test]
async fn get_decodes_payload_bytes_and_raw_reaches_unmodelled_endpoints() {
    let server = Server::start(vec![
        response(
            "200 OK",
            r#"[{"routing_key":"demo","exchange":"","redelivered":false,"properties":{},"payload":"AP8="}]"#,
        ),
        response("200 OK", r#"{"name":"rabbit@host"}"#),
    ]);
    let e = executor(&server.url);
    let QueryExecution::Page(page) = execute(&e, "GET / demo 5 ack").await.unwrap() else {
        panic!("a read must return a page");
    };
    // Base64 on the wire becomes bytes on screen, so binary stays binary.
    assert_eq!(page.rows[0].cells[4], Some(Value::Bytes(vec![0, 255])));
    assert_eq!(page.rows[0].cells[0], Some(Value::Text("demo".into())));
    assert!(!page.next && page.continuation.is_none());
    assert!(page.notice.contains("Destructive read"));

    // RAW has no known shape, so its body is displayed as-is.
    let QueryExecution::Page(page) = execute(&e, "RAW GET /api/nodes/rabbit%40host")
        .await
        .unwrap()
    else {
        panic!("a raw read must return a page");
    };
    // A RAW read keeps its native status alongside the body, so an error stays readable.
    assert_eq!(page.rows[0].cells[0], Some(Value::Text("200 OK".into())));
    assert_eq!(
        page.rows[0].cells[1],
        Some(Value::Bytes(br#"{"name":"rabbit@host"}"#.to_vec()))
    );
    let requests = server.requests.lock().unwrap();
    assert!(requests[0].starts_with("POST /api/queues/%2F/demo/get HTTP/1.1"));
    assert!(
        requests[0].ends_with(r#"{"ackmode":"ack_requeue_false","count":5,"encoding":"base64"}"#)
    );
    assert!(requests[1].starts_with("GET /api/nodes/rabbit%40host HTTP/1.1"));
}

#[tokio::test]
async fn query_scope_cancellation_and_shutdown_fail_before_sending() {
    let mut e = executor("http://127.0.0.1:1");
    for text in [
        "",
        "GET / demo extra-and-not-a-count",
        "FLUSH / demo",
        "PUBLISH / amq.default demo\npayload",
        "RAW GET https://other.example/api/overview",
        "RAW GET //other.example/",
        "RAW GET /api/overview#fragment",
        "RAW GET /api/queues///demo",
    ] {
        assert!(execute(&e, text).await.is_err(), "{text}");
    }
    for (path, continuation) in [(vec!["/".into()], None), (vec![], Some("old-page".into()))] {
        let mut input = request("GET / demo");
        input.page.resource.path = path;
        input.page.continuation = continuation;
        let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
        assert!(e.execute_query(input, context).await.is_err());
    }
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    cancel.send(()).unwrap();
    let error = e
        .execute_query(request("DELETE queue / demo"), context)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Configured);
    assert!(fetch(&e, "rabbitmq.query", vec![], None).await.is_err());
    e.shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(
        execute(&e, "GET / demo")
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("closed")
    );
}

#[tokio::test]
async fn cancelled_publish_is_unknown_and_a_later_request_can_reconnect() {
    let mut delayed = response("200 OK", r#"{"routed":true}"#);
    delayed.1 = Duration::from_millis(300);
    let server = Server::start(vec![delayed, response("200 OK", "{}")]);
    let e = executor(&server.url);
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    let execution = e.execute_query(request("PUBLISH / amq.default demo\n\nhello"), context);
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
    assert!(matches!(
        execute(&e, "RAW GET /api/overview").await.unwrap(),
        QueryExecution::Page(_)
    ));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn query_redirects_are_displayed_without_forwarding_credentials() {
    let elsewhere = Server::start(vec![]);
    let reply = format!(
        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {}/api/overview\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        elsewhere.url
    );
    let server = Server::start(vec![(reply, Duration::ZERO)]);
    let QueryExecution::Write(result) =
        execute(&executor(&server.url), "DECLARE queue / demo\n\n{}")
            .await
            .unwrap()
    else {
        panic!("Expected a write outcome for the redirect");
    };
    // A redirect is not success, so the write is rejected rather than followed.
    assert_eq!(result.outcome, WriteOutcome::Rejected);
    assert!(result.summary.contains("307"), "{}", result.summary);
    assert!(elsewhere.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_editor_opens_from_every_view_and_prefills_the_open_vhost() {
    // The descriptor is what the TUI consults, so assert the wiring rather than the
    // watermark function alone: a contextual watermark left unwired prefills nothing.
    let query = RabbitMqProvider
        .descriptor()
        .query
        .expect("RabbitMQ declares a query");
    let row = onetui_core::Row {
        cells: vec![Some("orders".into())],
        target: None,
    };
    // Depth 0 and no scope list keep the editor reachable from the root, where a session
    // starts, as well as from inside a vhost.
    for resource in [
        Resource::new("rabbitmq.resources", vec![]),
        Resource::new("rabbitmq.overview", vec![]),
        Resource::new("rabbitmq.queues", vec!["demo".into()]),
    ] {
        assert!(query.accepts(&resource), "{}", resource.id);
    }
    assert_eq!(
        query.watermark(
            &Resource::new("rabbitmq.queues", vec!["demo".into()]),
            Some(&row)
        ),
        "GET demo orders 1 requeue"
    );
    assert_eq!(
        query.watermark(&Resource::new("rabbitmq.resources", vec![]), None),
        "PUBLISH / amq.default demo\n\nhello"
    );

    // Browsing into the query resource stays closed: it holds results, not navigation.
    let e = executor("http://127.0.0.1:1");
    assert!(
        fetch(&e, "rabbitmq.query", vec![], None)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("execute_query")
    );
    let menu = fetch(&e, "rabbitmq.resources", vec![], None).await.unwrap();
    assert!(
        menu.rows
            .iter()
            .all(|row| row.cells[1] != Some(Value::Text("rabbitmq.query".into()))),
        "the resource menu must not offer the query resource"
    );
}

#[tokio::test]
async fn raw_reads_keep_native_error_statuses_inspectable() {
    // A RAW read is the escape hatch, so an error status is data to look at rather than a
    // failure: confirming a deleted object returns 404 and that must reach the screen.
    let server = Server::start(vec![
        response(
            "404 Not Found",
            r#"{"error":"Object Not Found","reason":"Not Found"}"#,
        ),
        response("403 Forbidden", r#"{"error":"not_authorised"}"#),
        response("500 Internal Server Error", ""),
    ]);
    let e = executor(&server.url);
    for (status, body) in [
        (
            "404 Not Found",
            r#"{"error":"Object Not Found","reason":"Not Found"}"#,
        ),
        ("403 Forbidden", r#"{"error":"not_authorised"}"#),
        ("500 Internal Server Error", ""),
    ] {
        let QueryExecution::Page(page) = execute(&e, "RAW GET /api/queues/%2F/gone").await.unwrap()
        else {
            panic!("{status}: a RAW read must stay inspectable");
        };
        assert_eq!(page.rows[0].cells[0], Some(Value::Text(status.into())));
        assert_eq!(
            page.rows[0].cells[1],
            Some(Value::Bytes(body.as_bytes().to_vec()))
        );
        assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    }
}

#[tokio::test]
async fn get_errors_stay_errors_because_it_promises_decoded_messages() {
    // GET renders an array of messages; an error body is not one, so it cannot be shown
    // as a page and must surface as an error carrying the native reason.
    let server = Server::start(vec![response(
        "404 Not Found",
        r#"{"error":"Object Not Found","reason":"Not Found"}"#,
    )]);
    let error = execute(&executor(&server.url), "GET / missing")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("404"), "{error}");
    assert!(error.contains("Object Not Found"), "{error}");
}
