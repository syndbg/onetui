use super::*;

#[tokio::test]
async fn vector_search_preserves_native_ranking_values_capacity_and_errors() {
    let text = r##"{"operation":"SearchVectors","index":"embedding","search_vector":[0.125,-2,3.5],"top_k":2,"search_condition_expression":"#category = :category","projection_expression":"pk, value","expression_attribute_names":{"#category":"category"},"expression_attribute_values":{":category":{"S":"demo"}}}"##;
    let response = json!({"SearchResults":[
        {"Score":-0.25,"Item":{"pk":{"S":"a"},"value":{"N":"12345678901234567890123456789012345678"}},"FutureResult":"kept"},
        {"Score":-1.5,"Item":{"pk":{"S":"b"},"bytes":{"B":"AP8="}}}],
        "ConsumedCapacity":{"VectorSearchRequestBytes":120},"FutureMetadata":true});
    let server = Server::start(vec![
        (200,response.to_string()),
        (200,json!({"SearchResults":[],"ConsumedCapacity":{"VectorSearchRequestBytes":80}}).to_string()),
        (400,json!({"__type":"ResourceNotFoundException","message":"vector index embedding is not ACTIVE"}).to_string()),
    ]);
    let e = server.executor();
    let page = query(&e, text, None).await.unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(response));
    assert!(!page.next && page.continuation.is_none());
    let empty = query(&e, text, None).await.unwrap();
    assert_eq!(
        cell(&empty, 0, "response").unwrap()["SearchResults"],
        json!([])
    );
    let error = query(&e, text, None).await.unwrap_err().to_string();
    assert!(
        error.contains("ResourceNotFoundException") && error.contains("not ACTIVE"),
        "{error}"
    );
    let calls = server.finish();
    assert_eq!(calls.len(), 3);
    let (headers, body) = &calls[0];
    assert!(headers.contains("DynamoDB_20120810.SearchVectors"));
    assert_eq!(body["TableName"], "demo");
    assert_eq!(body["IndexName"], "embedding");
    assert_eq!(
        body["SearchVector"],
        json!([{"N":"0.125"},{"N":"-2"},{"N":"3.5"}])
    );
    assert_eq!(body["TopK"], 2);
    assert_eq!(body["SearchConditionExpression"], "#category = :category");
    assert_eq!(body["ProjectionExpression"], "pk, value");
    assert_eq!(
        body["ExpressionAttributeNames"],
        json!({"#category":"category"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"],
        json!({":category":{"S":"demo"}})
    );
    assert_eq!(body["ReturnConsumedCapacity"], "INDEXES");
}

#[tokio::test]
async fn vector_index_metadata_is_included_without_searching() {
    let index = json!({"IndexName":"embedding","Dimensions":3,"DistanceFunction":"DOT_PRODUCT",
        "IndexStatus":"ACTIVE","Backfilling":true,"ItemCount":42,"FutureMetadata":true});
    let server = Server::start(vec![(
        200,
        json!({"Table":{"TableName":"demo","VectorIndexes":[index]}}).to_string(),
    )]);
    let page = fetch(
        &server.executor(),
        request("dynamodb.indexes", &["demo"], None),
    )
    .await
    .unwrap();
    assert_eq!(
        cell(&page, 0, "response").unwrap()["VectorIndexes"],
        json!([index])
    );
    let calls = server.finish();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].0.contains("DynamoDB_20120810.DescribeTable"));
}

#[tokio::test]
async fn vector_results_cannot_exceed_requested_count_or_page_bytes() {
    let text = r#"{"operation":"SearchVectors","index":"embedding","search_vector":[0],"top_k":1}"#;
    let result = json!({"Score":0,"Item":{"pk":{"S":"demo"}}});
    let server = Server::start(vec![
        (200,json!({"SearchResults":[result,result]}).to_string()),
        (200,json!({"SearchResults":[{"Score":0,"Item":{"value":{"S":"x".repeat(onetui_core::PAGE_BYTES)}}}]}).to_string()),
        (200,json!({"SearchResults":[result]}).to_string()),
    ]);
    let e = server.executor();
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("top_k")
    );
    assert!(
        query(&e, text, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("1 MiB")
    );
    let page = query(&e, text, None).await.unwrap();
    assert_eq!(
        cell(&page, 0, "response").unwrap()["SearchResults"],
        json!([result])
    );
    assert!(!page.next);
    assert_eq!(server.finish().len(), 3);
}
