use super::broker;
use anyhow::Result;
use prost::Message;
use serde_json::json;
use std::path::Path;

const CUSTOMER: &str = include_str!("../../../../hack/fixtures/schemas/customer.proto");
const EVENT: &str = include_str!("../../../../hack/fixtures/schemas/event.proto");

#[derive(Clone, PartialEq, Message)]
struct Customer {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct Event {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(message, optional, tag = "2")]
    customer: Option<Customer>,
    #[prost(string, tag = "3")]
    title: String,
    #[prost(bool, tag = "4")]
    active: bool,
    #[prost(double, tag = "5")]
    score: f64,
    #[prost(string, repeated, tag = "6")]
    tags: Vec<String>,
    #[prost(bytes = "vec", tag = "7")]
    attachment: Vec<u8>,
    #[prost(string, optional, tag = "8")]
    note: Option<String>,
}

pub fn prepare(directory: &Path) -> Result<()> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../hack/fixtures/schemas");
    let descriptors = protox::compile([source.join("event.proto")], [source])?;
    std::fs::write(
        directory.join("protobuf_event.pb"),
        descriptors.encode_to_vec(),
    )?;
    Ok(())
}

pub fn schemas(topic: &str) -> Result<[u32; 2]> {
    let subject = format!("{topic}_customer");
    broker::register(
        &subject,
        json!({"schemaType":"PROTOBUF", "schema":CUSTOMER}),
    )?;
    let mut ids = [0; 2];
    for (version, id) in ids.iter_mut().enumerate() {
        let source = if version == 0 {
            EVENT.replace("  optional string note = 8;\n", "")
        } else {
            EVENT.into()
        };
        *id = broker::register(
            &format!("{topic}_value"),
            json!({"schemaType":"PROTOBUF", "schema":source,"references":[{"name":"customer.proto","subject":subject,"version":1}]}),
        )?;
    }
    Ok(ids)
}

pub fn key_schema(topic: &str) -> Result<u32> {
    broker::register(
        &format!("{topic}_key"),
        json!({"schemaType":"PROTOBUF", "schema":CUSTOMER}),
    )
}

pub fn key_raw(n: u32) -> Vec<u8> {
    Customer {
        id: u64::from(n % 23),
        name: format!("customer-{n}"),
    }
    .encode_to_vec()
}

pub fn key_message(n: u32, id: u32) -> Vec<u8> {
    let mut framed = vec![0];
    framed.extend(id.to_be_bytes());
    framed.push(0);
    framed.extend(key_raw(n));
    framed
}

pub fn raw(n: u32) -> Vec<u8> {
    Event {
        id: u64::from(n),
        customer: Some(Customer {
            id: u64::from(n % 23),
            name: format!("София / 東京 / São Paulo {n}"),
        }),
        title: format!("Synthetic Protobuf event {n}"),
        active: n.is_multiple_of(2),
        score: f64::from(n) / 10.0,
        tags: vec!["demo".into(), "protobuf".into()],
        attachment: vec![0, 0xff, (n % 256) as u8],
        note: (n % 2 == 1).then(|| "Added in writer version 2\nSafe terminal text".into()),
    }
    .encode_to_vec()
}

pub fn message(n: u32, ids: [u32; 2]) -> Vec<u8> {
    let mut framed = vec![0];
    framed.extend(ids[(n % 2) as usize].to_be_bytes());
    framed.push(0); // Confluent's shortcut message-index path [0].
    framed.extend(raw(n));
    framed
}

pub async fn seed(topic: &str, count: u32) -> Result<()> {
    let ids = schemas(topic)?;
    let key_id = key_schema(topic)?;
    broker::seed_keyed(topic, count, |n| {
        Ok((key_message(n, key_id), message(n, ids)))
    })
    .await
}
