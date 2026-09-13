use anyhow::{Result, ensure};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue as A, BillingMode, GlobalSecondaryIndex,
        KeySchemaElement, KeyType, LocalSecondaryIndex, Projection, ProjectionType, PutRequest,
        ScalarAttributeType, StreamSpecification, StreamViewType, WriteRequest,
    },
};
use std::collections::HashMap;

#[path = "../tests/support/mod.rs"]
mod support;

fn key(name: &str, kind: KeyType) -> KeySchemaElement {
    KeySchemaElement::builder()
        .attribute_name(name)
        .key_type(kind)
        .build()
        .unwrap()
}
fn attribute(name: &str, kind: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(kind)
        .build()
        .unwrap()
}

async fn create(client: &Client, name: &str, indexes: bool) -> Result<()> {
    let mut request = client
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(attribute("pk", ScalarAttributeType::S))
        .attribute_definitions(attribute("sk", ScalarAttributeType::N))
        .key_schema(key("pk", KeyType::Hash))
        .key_schema(key("sk", KeyType::Range))
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()?,
        );
    if indexes {
        let projection = Projection::builder()
            .projection_type(ProjectionType::All)
            .build();
        request = request
            .attribute_definitions(attribute("status", ScalarAttributeType::S))
            .attribute_definitions(attribute("created", ScalarAttributeType::N))
            .global_secondary_indexes(
                GlobalSecondaryIndex::builder()
                    .index_name("by_status")
                    .key_schema(key("status", KeyType::Hash))
                    .key_schema(key("sk", KeyType::Range))
                    .projection(projection.clone())
                    .build()?,
            )
            .local_secondary_indexes(
                LocalSecondaryIndex::builder()
                    .index_name("by_created")
                    .key_schema(key("pk", KeyType::Hash))
                    .key_schema(key("created", KeyType::Range))
                    .projection(projection)
                    .build()?,
            );
    }
    if let Err(error) = request.send().await
        && !error
            .as_service_error()
            .is_some_and(|e| e.is_resource_in_use_exception())
    {
        return Err(error.into());
    }
    Ok(())
}

fn item(i: usize, wide: bool) -> HashMap<String, A> {
    let mut item = HashMap::from([
        ("pk".into(), A::S(format!("customer-{}", i % 5))),
        ("sk".into(), A::N(i.to_string())),
        (
            "status".into(),
            A::S(
                if i.is_multiple_of(2) {
                    "open"
                } else {
                    "closed"
                }
                .into(),
            ),
        ),
        ("created".into(), A::N((1750000000 + i).to_string())),
        (
            "title".into(),
            A::S(format!("Synthetic item {i}: София / 東京 / São Paulo")),
        ),
        (
            "precise".into(),
            A::N("12345678901234567890123456789012345678".into()),
        ),
        ("fraction".into(), A::N("-0.00000000000000000012345".into())),
        (
            "binary".into(),
            A::B(vec![0, 255, 128, (i % 256) as u8].into()),
        ),
        (
            "string_set".into(),
            A::Ss(vec!["blue".into(), "green".into()]),
        ),
        (
            "number_set".into(),
            A::Ns(vec!["1".into(), "2.5".into(), "-3".into()]),
        ),
        (
            "binary_set".into(),
            A::Bs(vec![vec![0, 255].into(), vec![128, 1].into()]),
        ),
        ("active".into(), A::Bool(i.is_multiple_of(2))),
        ("null".into(), A::Null(true)),
        ("empty_text".into(), A::S(String::new())),
        ("empty_binary".into(), A::B(Vec::new().into())),
        ("empty_list".into(), A::L(vec![])),
        ("empty_map".into(), A::M(HashMap::new())),
        (
            "nested".into(),
            A::M(HashMap::from([(
                "mixed".into(),
                A::L(vec![
                    A::N("1".into()),
                    A::S("two".into()),
                    A::Null(true),
                    A::Bool(false),
                ]),
            )])),
        ),
    ]);
    if !i.is_multiple_of(3) {
        item.insert("optional".into(), A::S("present".into()));
    }
    if wide {
        for field in 0..65 {
            item.insert(
                format!("field_{field:02}"),
                A::S(format!("value {i}/{field}")),
            );
        }
        item.insert(
            "long_text".into(),
            A::S("line\ncontrol:\u{1b}[31m\t".repeat(200)),
        );
    }
    item
}

async fn seed(client: &Client, table: &str, count: usize, wide: bool) -> Result<()> {
    for start in (0..count).step_by(25) {
        let writes = (start..(start + 25).min(count))
            .map(|i| {
                Ok(WriteRequest::builder()
                    .put_request(
                        PutRequest::builder()
                            .set_item(Some(item(i, wide)))
                            .build()?,
                    )
                    .build())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut pending = Some(HashMap::from([(table.into(), writes)]));
        for _ in 0..3 {
            let result = client
                .batch_write_item()
                .set_request_items(pending)
                .send()
                .await?;
            pending = result.unprocessed_items.filter(|items| !items.is_empty());
            if pending.is_none() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        ensure!(pending.is_none(), "Fixture seeding left unprocessed items");
    }
    println!("{table}: {count} synthetic items");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    ensure!(
        std::env::args().len() == 1,
        "No endpoint overrides: seed only the fixed disposable fixture"
    );
    let client = support::client();
    create(&client, "demo_events", true).await?;
    create(&client, "demo_wide", false).await?;
    create(&client, "demo_empty", false).await?;
    seed(&client, "demo_events", 1205, false).await?;
    seed(&client, "demo_wide", 32, true).await?;
    Ok(())
}
