use onetui_core::{
    Page, Resource, Value,
    provider::{Executor, PageRequest, Provider, QueryRequest, RequestContext},
};
use onetui_dynamodb::{DynamoDbExecutor, DynamoDbProvider};
use serde_json::{Value as Json, json};
use std::time::Duration;

fn executor() -> DynamoDbExecutor {
    DynamoDbProvider.configure(&toml::from_str("region='us-east-1'\nendpoint_url='http://127.0.0.1:18000'\naccess_key_id_env='KEY'\nsecret_access_key_env='SECRET'").unwrap(),&|name|Some(if name=="KEY" {"onetuiFixtureOnly"} else {"fixture-secret-only"}.into())).unwrap()
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
