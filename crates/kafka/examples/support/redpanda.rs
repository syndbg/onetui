use super::broker;
use anyhow::{Result, ensure};
use apache_avro::{Schema, types::Value};
use serde_json::json;
use std::path::Path;
const CUSTOMER: &str = r#"{"type":"record","name":"Customer","namespace":"demo","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;

pub fn prepare(directory: &Path) -> Result<()> {
    std::fs::write(directory.join("avro_customer.avsc"), CUSTOMER)?;
    std::fs::write(directory.join("avro_event.avsc"), writer(0))?;
    Ok(())
}

fn writer(version: usize) -> String {
    let mut fields = vec![
        json!({"name":"id","type":"long"}),
        json!({"name":"customer","type":"demo.Customer"}),
        json!({"name":"tags","type":{"type":"array","items":"string"}}),
    ];
    if version == 1 {
        fields.push(json!({"name":"note","type":["null","string"],"default":null}));
    }
    json!({"type":"record","name":"Event","namespace":"demo","fields":fields}).to_string()
}

pub fn schemas(topic: &str) -> Result<[u32; 2]> {
    // These fixture-only names become URL segments, never an arbitrary endpoint.
    ensure!(
        !topic.is_empty()
            && topic
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
        "Invalid fixture topic"
    );
    let subject = format!("{topic}_customer");
    broker::register(&subject, json!({"schemaType":"AVRO","schema":CUSTOMER}))?;
    let mut ids = [0; 2];
    for (version, id) in ids.iter_mut().enumerate() {
        *id = broker::register(
            &format!("{topic}_value"),
            json!({"schemaType":"AVRO","schema":writer(version),"references":[{"name":"demo.Customer","subject":subject,"version":1}]}),
        )?;
    }
    Ok(ids)
}

pub fn message(n: u32, ids: [u32; 2]) -> Result<Vec<u8>> {
    let version = (n % 2) as usize;
    let root = writer(version);
    let schemas = Schema::parse_list([CUSTOMER, &root])?;
    let mut fields = vec![
        ("id".into(), Value::Long(i64::from(n))),
        (
            "customer".into(),
            Value::Record(vec![
                ("id".into(), Value::Long(i64::from(n % 23))),
                (
                    "name".into(),
                    Value::String(format!("София / 東京 / São Paulo {n}")),
                ),
            ]),
        ),
        (
            "tags".into(),
            Value::Array(vec![
                Value::String("synthetic".into()),
                Value::String("registry".into()),
            ]),
        ),
    ];
    if version == 1 {
        fields.push((
            "note".into(),
            Value::Union(
                1,
                Box::new(Value::String(
                    "Added in writer version 2\nSafe terminal text".into(),
                )),
            ),
        ));
    }
    let mut raw = vec![0];
    raw.extend(ids[version].to_be_bytes());
    raw.extend(
        apache_avro::writer::datum::GenericDatumWriter::builder(&schemas[1])
            .schemata(schemas.iter().collect())?
            .build()?
            .write_value_to_vec(Value::Record(fields))?,
    );
    Ok(raw)
}

pub async fn seed(topic: &str, count: u32) -> Result<()> {
    let ids = schemas(topic)?;
    broker::seed(topic, count, |n| message(n, ids)).await
}

pub fn raw(n: u32) -> Result<Vec<u8>> {
    // The catalog binds writer version 1; framing and registry IDs are absent.
    Ok(message(n.saturating_mul(2), [0, 0])?[5..].to_vec())
}
