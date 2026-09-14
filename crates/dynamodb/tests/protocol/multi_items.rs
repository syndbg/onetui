use super::*;

#[tokio::test]
async fn batch_retries_only_unprocessed_keys_and_retains_the_native_response() {
    let key = json!({"pk":{"B":"AP8="},"sk":{"N":"12345678901234567890123456789012345678"}});
    let processed = json!({"pk":{"B":"AA=="},"sk":{"N":"1"}});
    let text = json!({"operation":"BatchGetItem","keys":[processed,key],"consistent_read":true,
        "projection_expression":"pk, sk, #v","expression_attribute_names":{"#v":"value"}})
    .to_string();
    let response = json!({"Responses":{"demo":[processed]},"UnprocessedKeys":{"demo":{"Keys":[key]}},
        "ConsumedCapacity":[{"TableName":"demo","CapacityUnits":1}],"FutureMetadata":{"retained":true}});
    let final_response = json!({"Responses":{"demo":[{"pk":{"B":"AP8="},"sk":{"N":"12345678901234567890123456789012345678"},"value":{"NULL":true}}]},"UnprocessedKeys":{}});
    let server = Server::start(vec![
        (200, response.to_string()),
        (200, final_response.to_string()),
        (200, final_response.to_string()),
    ]);
    let e = server.executor();
    let first = query(&e, &text, None).await.unwrap();
    assert_eq!(cell(&first, 0, "response"), Some(response));
    assert!(first.next);
    let bookmark = first.continuation.unwrap();
    assert!(
        query(&e, &text.replace("true", "false"), Some(bookmark.clone()))
            .await
            .is_err()
    );
    let second = query(&e, &text, Some(bookmark.clone())).await.unwrap();
    assert_eq!(cell(&second, 0, "response"), Some(final_response.clone()));
    assert!(!second.next);
    let replay = query(&e, &text, Some(bookmark)).await.unwrap();
    assert_eq!(cell(&replay, 0, "response"), Some(final_response));
    let calls = server.finish();
    assert_eq!(calls.len(), 3);
    for (index, (headers, request)) in calls.into_iter().enumerate() {
        assert!(headers.contains("DynamoDB_20120810.BatchGetItem"));
        assert_eq!(
            request["RequestItems"]["demo"]["Keys"],
            if index == 0 {
                json!([processed, key])
            } else {
                json!([key])
            }
        );
        assert_eq!(request["RequestItems"]["demo"]["ConsistentRead"], true);
        assert_eq!(
            request["RequestItems"]["demo"]["ProjectionExpression"],
            "pk, sk, #v"
        );
        assert_eq!(
            request["RequestItems"]["demo"]["ExpressionAttributeNames"]["#v"],
            "value"
        );
        assert_eq!(request["ReturnConsumedCapacity"], "INDEXES");
    }
}

#[tokio::test]
async fn oversized_batch_fails_without_truncating_or_advancing() {
    let text = r#"{"operation":"BatchGetItem","keys":[{"pk":{"S":"x"}}]}"#;
    let oversized =
        json!({"Responses":{"demo":[{"value":{"S":"x".repeat(onetui_core::PAGE_BYTES)}}]}});
    let server = Server::start(vec![
        (200, oversized.to_string()),
        (
            200,
            json!({"Responses":{},"UnprocessedKeys":{"demo":{"Keys":[{"pk":{"S":"x"}}]}}})
                .to_string(),
        ),
    ]);
    let e = server.executor();
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("1 MiB")
    );
    let retry = query(&e, text, None).await.unwrap();
    assert!(retry.next, "an all-unprocessed batch must remain retryable");
    let calls = server.finish();
    assert_eq!(calls[0].1, calls[1].1);
}

#[tokio::test]
async fn transaction_retains_order_missing_items_capacity_and_cancellation_reasons() {
    let text = r##"{"operation":"TransactGetItems","items":[{"key":{"pk":{"S":"missing"}}},{"key":{"pk":{"S":"found"}},"projection_expression":"#v","expression_attribute_names":{"#v":"value"}}]}"##;
    let response = json!({"Responses":[{}, {"Item":{"value":{"N":"1.0000000000000000001"}}}],
        "ConsumedCapacity":[{"TableName":"demo","CapacityUnits":4}]});
    let server = Server::start(vec![(200,response.to_string()),(400,json!({"__type":"TransactionCanceledException","Message":"Transaction conflict","CancellationReasons":[{"Code":"TransactionConflict","Message":"retry later"}]}).to_string())]);
    let e = server.executor();
    let page = query(&e, text, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(response));
    assert!(!page.next && page.continuation.is_none());
    assert!(page.notice.contains("atomic read"));
    let error = query(&e, text, None).await.unwrap_err().to_string();
    assert!(
        error.contains("TransactionConflict") && error.contains("retry later"),
        "{error}"
    );
    let calls = server.finish();
    assert!(calls[0].0.contains("DynamoDB_20120810.TransactGetItems"));
    assert_eq!(
        calls[0].1["TransactItems"][0]["Get"]["Key"]["pk"]["S"],
        "missing"
    );
    assert_eq!(calls[0].1["TransactItems"][1]["Get"]["TableName"], "demo");
    assert_eq!(
        calls[0].1["TransactItems"][1]["Get"]["ProjectionExpression"],
        "#v"
    );
    assert_eq!(calls[0].1["ReturnConsumedCapacity"], "TOTAL");
}
