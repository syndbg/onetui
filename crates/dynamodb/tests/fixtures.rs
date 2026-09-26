use onetui_core::{
    Page, Resource, Value,
    provider::{Executor, PageRequest, Provider, QueryRequest, RequestContext},
};
use onetui_dynamodb::{DynamoDbExecutor, DynamoDbProvider};
use serde_json::{Value as Json, json};
use std::time::Duration;

mod support;

fn executor() -> DynamoDbExecutor {
    DynamoDbProvider.configure(&toml::from_str("region='us-east-1'\nendpoint_url='http://127.0.0.1:18000'\nstreams_endpoint_url='http://127.0.0.1:18000'\naccess_key_id_env='KEY'\nsecret_access_key_env='SECRET'").unwrap(),&|name|Some(if name=="KEY" {"onetuiFixtureOnly"} else {"fixture-secret-only"}.into())).unwrap()
}
async fn fetch(e: &DynamoDbExecutor, id: &'static str, path: &[&str]) -> Page {
    let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
    e.fetch_page(
        PageRequest {
            resource: Resource::new(id, path.iter().map(|p| (*p).into()).collect()),
            continuation: None,
        },
        ctx,
    )
    .await
    .unwrap()
}
async fn query(
    e: &DynamoDbExecutor,
    table: &str,
    text: &str,
    continuation: Option<String>,
) -> Page {
    let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
    e.query_page(
        QueryRequest {
            page: PageRequest {
                resource: Resource::new("dynamodb.query", vec![table.into()]),
                continuation,
            },
            text: text.into(),
        },
        ctx,
    )
    .await
    .unwrap()
}
fn cell(page: &Page, row: usize, name: &str) -> Json {
    let column = page.columns.iter().position(|c| c.name == name).unwrap();
    match page.rows[row].cells[column].as_ref().unwrap() {
        Value::Json(text) => serde_json::from_str(text).unwrap(),
        _ => panic!("JSON expected"),
    }
}

#[tokio::test]
#[ignore = "creates and deletes only its own DynamoDB Local table"]
async fn fixture_partiql_writes_are_visible_and_can_be_reversed() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };

    let client = support::client();
    let name = format!(
        "onetui_partiql_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    client
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let table = name.clone();
    let result = tokio::spawn(async move {
        let executor = executor();
        for statement in [
            format!("INSERT INTO \"{table}\" VALUE {{'pk': 'owned', 'value': 'before'}}"),
            format!("UPDATE \"{table}\" SET \"value\"='after' WHERE pk='owned'"),
        ] {
            let text = json!({"operation":"ExecuteStatement","statement":statement});
            let page = query(&executor, &table, &text.to_string(), None).await;
            assert!(page.notice.contains("statement completed"));
        }
        let item = support::client()
            .get_item()
            .table_name(&table)
            .key("pk", AttributeValue::S("owned".into()))
            .send()
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item["value"], AttributeValue::S("after".into()));

        let text = json!({"operation":"ExecuteStatement","statement":format!("DELETE FROM \"{table}\" WHERE pk='owned'")});
        let page = query(&executor, &table, &text.to_string(), None).await;
        assert!(page.notice.contains("statement completed"));
        let item = support::client()
            .get_item()
            .table_name(&table)
            .key("pk", AttributeValue::S("owned".into()))
            .send()
            .await
            .unwrap();
        assert!(item.item.is_none());
    })
    .await;

    client
        .delete_table()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires seeded DynamoDB Local Streams; read-only"]
async fn fixture_streams_preserve_seed_images_and_sequence_bookmarks() {
    let e = executor();
    let streams = fetch(&e, "dynamodb.table_streams", &["demo_events"]).await;
    assert!(!streams.rows.is_empty());
    let stream = streams.rows[0].target.as_ref().unwrap();
    let shards = fetch(&e, "dynamodb.shards", &[&stream.path[0]]).await;
    assert!(!shards.rows.is_empty());
    let resource = shards.rows[0].target.clone().unwrap();
    assert_eq!(
        shards.rows[0].cells[0],
        Some(resource.path[1].clone().into())
    );
    let details = fetch(&e, "dynamodb.shard_details", &[&stream.path[0]]).await;
    assert!(details.rows[0].target.is_none());
    let description = cell(&details, 0, "description");
    assert_eq!(description["ShardId"], resource.path[1]);
    assert_eq!(
        details.rows[0].cells[2].as_ref().and_then(Value::text),
        description["SequenceNumberRange"]["StartingSequenceNumber"].as_str()
    );
    let mut continuation = None;
    let mut count = 0;
    let mut replay = None;
    for _ in 0..40 {
        let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
        let page = e
            .fetch_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: continuation.clone(),
                },
                ctx,
            )
            .await
            .unwrap();
        if !page.rows.is_empty() {
            let record = cell(&page, 0, "record");
            assert!(record["dynamodb"]["Keys"].is_object());
            assert!(record["dynamodb"]["NewImage"].is_object());
            if continuation.is_some() && replay.is_none() {
                replay = Some((continuation.clone(), record));
            }
        }
        count += page.rows.len();
        continuation = page.continuation;
        if !page.next || count >= 1205 {
            break;
        }
    }
    assert_eq!(count, 1205);
    let (continuation, expected) = replay.unwrap();
    let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
    let page = e
        .fetch_page(
            PageRequest {
                resource: resource.clone(),
                continuation,
            },
            ctx,
        )
        .await
        .unwrap();
    assert_eq!(cell(&page, 0, "record"), expected);
    let sequence = expected["dynamodb"]["SequenceNumber"].as_str().unwrap();
    let mut read = json!({"operation":"GetRecords","shard_id":resource.path[1],"sequence_number":sequence,"limit":2});
    let inclusive = query(&e, &resource.path[0], &read.to_string(), None).await;
    assert_eq!(cell(&inclusive, 0, "record"), expected);
    assert_eq!(inclusive.rows.len(), 2);
    read["after"] = json!(true);
    let exclusive = query(&e, &resource.path[0], &read.to_string(), None).await;
    assert_eq!(cell(&exclusive, 0, "record"), cell(&inclusive, 1, "record"));
}

#[tokio::test]
#[ignore = "requires seeded DynamoDB Local; read-only"]
async fn fixture_batch_and_transaction_preserve_missing_items_and_projection() {
    let e = executor();
    let found = json!({"pk":{"S":"customer-0"},"sk":{"N":"0"}});
    let missing = json!({"pk":{"S":"not-in-fixtures"},"sk":{"N":"-1"}});
    let batch = json!({"operation":"BatchGetItem","keys":[missing,found],"consistent_read":true,"projection_expression":"pk, sk"});
    let page = query(&e, "demo_events", &batch.to_string(), None).await;
    assert!(!page.next);
    let response = cell(&page, 0, "response");
    assert_eq!(response["Responses"]["demo_events"], json!([found]));
    assert!(response["ConsumedCapacity"].is_array());
    let transaction = json!({"operation":"TransactGetItems","items":[{"key":missing},{"key":found,"projection_expression":"pk, sk"}]});
    let page = query(&e, "demo_events", &transaction.to_string(), None).await;
    let response = cell(&page, 0, "response");
    assert_eq!(response["Responses"], json!([{}, {"Item":found}]));
    assert!(response["ConsumedCapacity"].is_array());
    assert!(!page.next);
}

#[tokio::test]
#[ignore = "requires seeded DynamoDB Local; read-only"]
async fn fixture_partiql_select_batch_and_transaction_read_existing_items() {
    let e = executor();
    let statement = "SELECT pk, sk FROM \"demo_events\" WHERE pk=? AND sk=?";
    let parameters = json!([{"S":"customer-0"},{"N":"0"}]);
    let text = json!({"operation":"ExecuteStatement","statement":statement,"parameters":parameters,"consistent_read":true,"limit":1});
    let page = query(&e, "demo_events", &text.to_string(), None).await;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(cell(&page, 0, "pk"), json!({"S":"customer-0"}));
    assert_eq!(cell(&page, 0, "sk"), json!({"N":"0"}));
    if let Some(bookmark) = page.continuation {
        let end = query(&e, "demo_events", &text.to_string(), Some(bookmark)).await;
        assert!(end.rows.is_empty() && !end.next);
    }
    let statements = json!([
        {"statement":statement,"parameters":parameters},
        {"statement":statement,"parameters":[{"S":"not-in-fixtures"},{"N":"-1"}]}
    ]);
    let batch = json!({"operation":"BatchExecuteStatement","statements":statements});
    let page = query(&e, "demo_events", &batch.to_string(), None).await;
    let response = cell(&page, 0, "response");
    assert_eq!(response["Responses"].as_array().unwrap().len(), 2);
    assert_eq!(
        response["Responses"][0]["Item"]["pk"],
        json!({"S":"customer-0"})
    );
    assert!(
        response["Responses"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r.get("Error").is_none())
    );
    let transaction = json!({"operation":"ExecuteTransaction","statements":statements});
    let page = query(&e, "demo_events", &transaction.to_string(), None).await;
    let response = cell(&page, 0, "response");
    assert_eq!(response["Responses"].as_array().unwrap().len(), 2);
    assert_eq!(
        response["Responses"][0]["Item"]["pk"],
        json!({"S":"customer-0"})
    );
    assert_eq!(response["Responses"][1], json!({}));
    assert!(!page.next);
}

#[tokio::test]
#[ignore = "creates and deletes only its own DynamoDB Local table"]
async fn fixture_follow_observes_insert_modify_remove_without_consuming_records() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue as A, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType,
    };
    let client = support::client();
    let name = format!(
        "onetui_stream_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let table = client
        .create_table()
        .table_name(&name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let arn = table.table_description.unwrap().latest_stream_arn.unwrap();
    let owned_table = name.clone();
    let result = tokio::spawn(async move {
        let e = executor();
        let shards = fetch(&e, "dynamodb.shards", &[&arn]).await;
        let resource = shards.rows[0].target.clone().unwrap();
        let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
        let initial = e
            .follow_page(
                PageRequest {
                    resource: resource.clone(),
                    continuation: None,
                },
                ctx,
            )
            .await
            .unwrap();
        assert!(initial.rows.is_empty());
        let writer = support::client();
        let table = owned_table.as_str();
        writer
            .put_item()
            .table_name(table)
            .item("pk", A::S("owned-test-key".into()))
            .item("value", A::S("before".into()))
            .send()
            .await
            .unwrap();
        writer
            .put_item()
            .table_name(table)
            .item("pk", A::S("owned-test-key".into()))
            .item("value", A::S("after".into()))
            .send()
            .await
            .unwrap();
        writer
            .delete_item()
            .table_name(table)
            .key("pk", A::S("owned-test-key".into()))
            .send()
            .await
            .unwrap();
        let mut bookmark = initial.continuation;
        let mut records = Vec::new();
        for _ in 0..20 {
            let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
            let page = e
                .follow_page(
                    PageRequest {
                        resource: resource.clone(),
                        continuation: bookmark,
                    },
                    ctx,
                )
                .await
                .unwrap();
            records.extend((0..page.rows.len()).map(|row| cell(&page, row, "record")));
            bookmark = page.continuation;
            if records.len() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["eventName"], "INSERT");
        assert_eq!(records[1]["eventName"], "MODIFY");
        assert_eq!(records[1]["dynamodb"]["OldImage"]["value"]["S"], "before");
        assert_eq!(records[1]["dynamodb"]["NewImage"]["value"]["S"], "after");
        assert_eq!(records[2]["eventName"], "REMOVE");
        let (_cancel, ctx) = RequestContext::new(Duration::from_secs(5));
        let retained = e
            .fetch_page(
                PageRequest {
                    resource,
                    continuation: None,
                },
                ctx,
            )
            .await
            .unwrap();
        assert_eq!(retained.rows.len(), 3);
    })
    .await;
    client
        .delete_table()
        .table_name(&name)
        .send()
        .await
        .unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable DynamoDB Local fixture"]
async fn fixture_metadata_and_all_attribute_types() {
    let e = executor();
    let tables = fetch(&e, "dynamodb.tables", &[]).await;
    assert!(
        tables
            .rows
            .iter()
            .any(|r| r.target.as_ref().is_some_and(|t| t.path == ["demo_events"]))
    );
    let indexes = fetch(&e, "dynamodb.indexes", &["demo_events"]).await;
    let indexes = cell(&indexes, 0, "response");
    assert_eq!(
        indexes["GlobalSecondaryIndexes"][0]["IndexName"],
        "by_status"
    );
    assert_eq!(
        indexes["LocalSecondaryIndexes"][0]["IndexName"],
        "by_created"
    );
    let page=query(&e,"demo_events",r#"{"operation":"GetItem","key":{"pk":{"S":"customer-0"},"sk":{"N":"0"}},"consistent_read":true}"#,None).await;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(
        cell(&page, 0, "precise"),
        json!({"N":"12345678901234567890123456789012345678"})
    );
    assert_eq!(cell(&page, 0, "binary"), json!({"B":"AP+AAA=="}));
    assert_eq!(cell(&page, 0, "null"), json!({"NULL":true}));
    assert_eq!(cell(&page, 0, "empty_map"), json!({"M":{}}));
    assert_eq!(
        cell(&page, 0, "string_set")["SS"].as_array().unwrap().len(),
        2
    );
    assert_eq!(
        cell(&page, 0, "number_set")["NS"].as_array().unwrap().len(),
        3
    );
    assert_eq!(
        cell(&page, 0, "binary_set")["BS"].as_array().unwrap().len(),
        2
    );
    assert!(!page.columns.iter().any(|c| c.name == "optional"));
    assert!(
        fetch(&e, "dynamodb.items", &["demo_wide"])
            .await
            .columns
            .len()
            > 65
    );
    assert!(
        fetch(&e, "dynamodb.items", &["demo_empty"])
            .await
            .rows
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires disposable DynamoDB Local fixture"]
async fn fixture_key_query_paging_replay_and_empty_filtered_pages() {
    let e = executor();
    let text = r#"{"operation":"Query","key_condition_expression":"pk=:p","expression_attribute_values":{":p":{"S":"customer-0"}},"limit":100,"consistent_read":true}"#;
    let first = query(&e, "demo_events", text, None).await;
    assert_eq!(first.rows.len(), 100);
    assert_eq!(cell(&first, 0, "sk"), json!({"N":"0"}));
    let bookmark = first.continuation.clone();
    let second = query(&e, "demo_events", text, bookmark.clone()).await;
    assert_eq!(cell(&second, 0, "sk"), json!({"N":"500"}));
    let third = query(&e, "demo_events", text, second.continuation.clone()).await;
    assert_eq!(third.rows.len(), 41);
    assert!(!third.next);
    let previous = query(&e, "demo_events", text, bookmark).await;
    assert_eq!(cell(&previous, 0, "sk"), cell(&second, 0, "sk"));
    let filtered = query(
        &e,
        "demo_events",
        r#"{"operation":"Scan","limit":1,"filter_expression":"attribute_not_exists(pk)"}"#,
        None,
    )
    .await;
    assert!(filtered.rows.is_empty() && filtered.next);
    let gsi=query(&e,"demo_events",r##"{"operation":"Query","index":"by_status","key_condition_expression":"#s=:s","expression_attribute_names":{"#s":"status"},"expression_attribute_values":{":s":{"S":"open"}},"limit":10}"##,None).await;
    assert_eq!(gsi.rows.len(), 10);
    let lsi=query(&e,"demo_events",r#"{"operation":"Query","index":"by_created","key_condition_expression":"pk=:p","expression_attribute_values":{":p":{"S":"customer-0"}},"consistent_read":true,"limit":10}"#,None).await;
    assert_eq!(lsi.rows.len(), 10);
}
