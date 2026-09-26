use super::*;

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
