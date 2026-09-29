use super::*;
use onetui_core::provider::{QueryExecution, WriteOutcome};

fn statement_request(text: &str) -> QueryRequest {
    QueryRequest {
        page: request("dynamodb.query", &["demo"], None),
        text: text.into(),
    }
}

#[tokio::test]
async fn editor_preserves_native_results_and_reports_rejected_or_unknown_outcomes() {
    let batch_response = json!({"Responses":[{"TableName":"demo"},
        {"Error":{"Code":"DuplicateItem","Message":"already exists"}}]});
    let server = Server::start(vec![
        (200, batch_response.to_string()),
        (
            400,
            json!({"__type":"ValidationException","message":"native syntax error fixture-secret"})
                .to_string(),
        ),
        (
            503,
            json!({"__type":"ServiceUnavailable","message":"try later"}).to_string(),
        ),
        (200, "not-json".into()),
    ]);
    let e = server.executor();
    let batch = r#"{"operation":"BatchExecuteStatement","statements":[{"statement":"INSERT INTO demo VALUE {'pk':'a'}"},{"statement":"INSERT INTO demo VALUE {'pk':'b'}"}]}"#;
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let QueryExecution::Page(page) = e
        .execute_query(statement_request(batch), context)
        .await
        .unwrap()
    else {
        panic!("A partial batch must retain its native response");
    };
    assert_eq!(cell(&page, 0, "response"), Some(batch_response));
    for (outcome, message) in [
        (WriteOutcome::Rejected, "native syntax error [REDACTED]"),
        (WriteOutcome::Unknown, "outcome unknown"),
        (
            WriteOutcome::Unknown,
            "request completed, but its response could not be read",
        ),
    ] {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let QueryExecution::Write(result) = e.execute_query(statement_request(
            r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk='a'"}"#), context).await.unwrap() else {
            panic!("Expected a shared write outcome");
        };
        assert_eq!(result.outcome, outcome);
        assert!(result.summary.contains(message), "{}", result.summary);
        assert!(!result.summary.contains("fixture-secret"));
        assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    }
    assert_eq!(server.finish().len(), 4);
}

#[tokio::test]
async fn editor_cancellation_before_dispatch_is_an_error_without_sending() {
    let server = Server::start(vec![]);
    let e = server.executor();
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    cancel.send(()).unwrap();
    let error = e
        .execute_query(
            statement_request(
                r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk='a'"}"#,
            ),
            context,
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancelled"));
    assert!(!error.to_string().contains("outcome unknown"));
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn editor_cancellation_after_dispatch_is_unknown_without_retrying() {
    let server = Server::delayed(vec![(200, "{}".into())], Duration::from_millis(300));
    let e = server.executor();
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    let execution = e.execute_query(
        statement_request(
            r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk='a'"}"#,
        ),
        context,
    );
    let cancel_after_dispatch = async {
        loop {
            match server.requests.try_recv() {
                Ok((_, body)) => {
                    assert_eq!(body["Statement"], "DELETE FROM demo WHERE pk='a'");
                    cancel.send(()).unwrap();
                    break;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(error) => panic!("{error}"),
            }
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(execution, cancel_after_dispatch)
    })
    .await
    .expect("request must reach the server before cancellation");
    let QueryExecution::Write(result) = result.unwrap() else {
        panic!("Expected an unknown write outcome");
    };
    assert_eq!(result.outcome, WriteOutcome::Unknown);
    assert!(result.summary.contains("cancelled"));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Disconnected);
    assert!(server.finish().is_empty());
}

#[tokio::test]
async fn select_parameters_empty_pages_and_bookmarks_use_native_requests() {
    let text = r#"{"operation":"ExecuteStatement","statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"B":"AP8="}],"consistent_read":true,"limit":2}"#;
    let item = json!({"pk":{"B":"AP8="},"value":{"N":"12345678901234567890123456789012345678"}});
    let server = Server::start(vec![
        (
            200,
            json!({"Items":[],"NextToken":"next-page","ConsumedCapacity":{"CapacityUnits":1}})
                .to_string(),
        ),
        (200, json!({"Items":[item]}).to_string()),
        (200, json!({"Items":[item]}).to_string()),
    ]);
    let e = server.executor();
    let first = query(&e, text, None).await.unwrap();
    assert!(first.rows.is_empty() && first.next);
    assert!(first.notice.contains("CapacityUnits"));
    let bookmark = first.continuation.unwrap();
    assert!(
        query(&e, &text.replace("true", "false"), Some(bookmark.clone()))
            .await
            .is_err()
    );
    let second = query(&e, text, Some(bookmark.clone())).await.unwrap();
    assert_eq!(cell(&second, 0, "value"), Some(item["value"].clone()));
    assert!(!second.next);
    let replay = query(&e, text, Some(bookmark)).await.unwrap();
    assert_eq!(cell(&replay, 0, "pk"), Some(item["pk"].clone()));
    let calls = server.finish();
    assert!(
        calls
            .iter()
            .all(|(headers, _)| headers.contains("DynamoDB_20120810.ExecuteStatement"))
    );
    assert_eq!(calls[0].1["Parameters"], json!([{"B":"AP8="}]));
    assert_eq!(calls[0].1["ConsistentRead"], true);
    assert_eq!(calls[0].1["Limit"], 2);
    assert_eq!(calls[0].1["ReturnConsumedCapacity"], "INDEXES");
    assert!(calls[0].1.get("NextToken").is_none());
    assert_eq!(calls[1].1["NextToken"], "next-page");
    assert_eq!(calls[1].1, calls[2].1);
}

#[tokio::test]
async fn batch_errors_and_transaction_positions_remain_inspectable() {
    let batch_response = json!({"Responses":[{"Item":{"pk":{"S":"a"}},"TableName":"demo"},
        {"Error":{"Code":"ValidationError","Message":"key must match one item"},"TableName":"demo"}],
        "ConsumedCapacity":[{"TableName":"demo","CapacityUnits":1}]});
    let transaction_response = json!({"Responses":[{}, {"Item":{"pk":{"S":"a"}}}],"ConsumedCapacity":[{"CapacityUnits":4}]});
    let server = Server::start(vec![
        (200, batch_response.to_string()),
        (200, transaction_response.to_string()),
    ]);
    let e = server.executor();
    let statements = json!([{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"a"}],"consistent_read":true},{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"b"}]}]);
    let text = json!({"operation":"BatchExecuteStatement","statements":statements}).to_string();
    let page = query(&e, &text, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(batch_response));
    assert!(!page.next);
    let text = r#"{"operation":"ExecuteTransaction","statements":[{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"missing"}]},{"statement":"SELECT * FROM demo WHERE pk=?","parameters":[{"S":"a"}]}]}"#;
    let page = query(&e, text, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(transaction_response));
    assert!(!page.next && page.notice.contains("atomic operation"));
    let calls = server.finish();
    assert!(
        calls[0]
            .0
            .contains("DynamoDB_20120810.BatchExecuteStatement")
    );
    assert_eq!(calls[0].1["Statements"][0]["ConsistentRead"], true);
    assert_eq!(
        calls[0].1["Statements"][0]["Parameters"],
        json!([{"S":"a"}])
    );
    assert!(calls[1].0.contains("DynamoDB_20120810.ExecuteTransaction"));
    assert_eq!(
        calls[1].1["TransactStatements"][0]["Parameters"],
        json!([{"S":"missing"}])
    );
    assert_eq!(calls[1].1["ReturnConsumedCapacity"], "TOTAL");
}

#[tokio::test]
async fn partiql_statements_reach_dynamodb_and_show_native_results() {
    let server = Server::start(vec![
        (
            200,
            json!({"ConsumedCapacity":{"CapacityUnits":1}}).to_string(),
        ),
        (
            400,
            json!({"__type":"ValidationException","message":"native syntax error"}).to_string(),
        ),
        (
            503,
            json!({"__type":"ServiceUnavailable","message":"try later"}).to_string(),
        ),
    ]);
    let e = server.executor();
    let insert = r#"{"operation":"ExecuteStatement","statement":"INSERT INTO other VALUE {'pk': ?}","parameters":[{"S":"a"}]}"#;
    let page = query(&e, insert, None).await.unwrap();
    assert!(!page.next);
    assert_eq!(
        cell(&page, 0, "response"),
        Some(json!({"ConsumedCapacity":{"CapacityUnits":1}}))
    );
    assert!(page.notice.contains("statement completed"));
    let invalid = r#"{"operation":"ExecuteStatement","statement":"server parses this"}"#;
    assert!(
        query(&e, invalid, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("native syntax error")
    );
    assert_eq!(*e.status().borrow(), ConnectionStatus::Connected);
    let delete = r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk=?","parameters":[{"S":"a"}]}"#;
    assert!(
        query(&e, delete, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("outcome unknown")
    );
    let calls = server.finish();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].1["Statement"], "INSERT INTO other VALUE {'pk': ?}");
    assert_eq!(calls[0].1["Parameters"], json!([{"S":"a"}]));
    assert_eq!(calls[1].1["Statement"], "server parses this");
    assert_eq!(calls[2].1["Statement"], "DELETE FROM demo WHERE pk=?");
}

#[tokio::test]
async fn partiql_batch_and_transaction_writes_reach_native_operations() {
    let batch_response = json!({"Responses":[{"TableName":"other"}]});
    let transaction_response = json!({"Responses":[{}]});
    let server = Server::start(vec![
        (200, batch_response.to_string()),
        (200, transaction_response.to_string()),
    ]);
    let e = server.executor();
    let batch = r#"{"operation":"BatchExecuteStatement","statements":[{"statement":"INSERT INTO other VALUE {'pk':'a'}"}]}"#;
    let page = query(&e, batch, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(batch_response));
    let transaction = r#"{"operation":"ExecuteTransaction","statements":[{"statement":"DELETE FROM other WHERE pk='a'"}]}"#;
    let page = query(&e, transaction, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(transaction_response));
    let calls = server.finish();
    assert_eq!(calls.len(), 2);
    assert!(
        calls[0]
            .0
            .contains("DynamoDB_20120810.BatchExecuteStatement")
    );
    assert_eq!(
        calls[0].1["Statements"][0]["Statement"],
        "INSERT INTO other VALUE {'pk':'a'}"
    );
    assert!(calls[1].0.contains("DynamoDB_20120810.ExecuteTransaction"));
    assert_eq!(
        calls[1].1["TransactStatements"][0]["Statement"],
        "DELETE FROM other WHERE pk='a'"
    );
}

#[tokio::test]
async fn completed_transaction_reports_an_unrenderable_response() {
    let response = json!({"Responses":[{"Item":{"payload":{"S":"x".repeat(1024 * 1024)}}}]});
    let server = Server::start(vec![(200, response.to_string())]);
    let e = server.executor();
    let transaction = r#"{"operation":"ExecuteTransaction","statements":[{"statement":"DELETE FROM demo WHERE pk='a'"}]}"#;
    let error = query(&e, transaction, None).await.unwrap_err().to_string();
    assert!(error.contains("request completed"), "{error}");
    assert!(error.contains("could not be displayed"), "{error}");
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn completed_statement_reports_an_unreadable_response() {
    let server = Server::start(vec![(200, "not-json".into())]);
    let e = server.executor();
    let statement =
        r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk='a'"}"#;
    let error = query(&e, statement, None).await.unwrap_err().to_string();
    assert!(error.contains("request completed"), "{error}");
    assert!(error.contains("could not be read"), "{error}");
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn unsupported_continuation_and_ignored_limits_fail_without_partial_results() {
    let server = Server::start(vec![
        (
            200,
            json!({"Items":[],"LastEvaluatedKey":{"pk":{"S":"a"}}}).to_string(),
        ),
        (
            200,
            json!({"Items":[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]}).to_string(),
        ),
        (
            400,
            json!({"__type":"ValidationException","message":"native expression error"}).to_string(),
        ),
    ]);
    let e = server.executor();
    let text = r#"{"operation":"ExecuteStatement","statement":"SELECT * FROM demo","limit":1}"#;
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("without NextToken")
    );
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("requested item limit")
    );
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("native expression error")
    );
    assert_eq!(server.finish().len(), 3);
}
