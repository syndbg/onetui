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
    assert!(!page.next && page.notice.contains("atomic read"));
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
async fn writes_and_other_tables_fail_before_client_initialization() {
    let server = Server::start(vec![]);
    let e = server.executor();
    for text in [
        r#"{"operation":"ExecuteStatement","statement":"DELETE FROM demo WHERE pk=?","parameters":[{"S":"a"}]}"#,
        r#"{"operation":"ExecuteStatement","statement":"SELECT * FROM other"}"#,
        r#"{"operation":"BatchExecuteStatement","statements":[{"statement":"SELECT * FROM demo WHERE pk='a'"},{"statement":"DELETE FROM demo"}]}"#,
        r#"{"operation":"ExecuteTransaction","statements":[{"statement":"SELECT * FROM demo WHERE pk='a'"},{"statement":"SELECT * FROM other WHERE pk='a'"}]}"#,
    ] {
        assert!(query(&e, text, None).await.is_err(), "{text}");
        assert_eq!(*e.status().borrow(), ConnectionStatus::Configured);
    }
    assert!(server.finish().is_empty());
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
