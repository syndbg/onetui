//! Seed only the fixed disposable NATS fixture.
use anyhow::Result;
use async_nats::jetstream::stream::{Config, StorageType};
use serde_json::json;

#[path = "support/avro.rs"]
mod avro;
#[path = "support/protobuf.rs"]
mod protobuf;

fn prepare() -> Result<()> {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/nats-schemas");
    std::fs::create_dir_all(directory.join("avro"))?;
    std::fs::create_dir_all(directory.join("protobuf"))?;
    std::fs::write(directory.join("avro/event.avsc"), avro::SCHEMA)?;
    std::fs::write(directory.join("protobuf/event.pb"), protobuf::schema())?;
    Ok(())
}

fn register(subject: &str, kind: &str, schema: &str) -> Result<u32> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .proxy(None)
        .max_redirects(0)
        .timeout_global(Some(std::time::Duration::from_secs(5)))
        .build()
        .into();
    let mut response = agent
        .post(format!(
            "http://127.0.0.1:18081/subjects/{subject}/versions"
        ))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .send(serde_json::to_vec(
            &json!({"schemaType":kind,"schema":schema}),
        )?)?;
    let bytes = response
        .body_mut()
        .with_config()
        .limit(65536)
        .read_to_vec()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let id = value["id"]
        .as_u64()
        .filter(|id| *id > 0 && *id <= i32::MAX as u64)
        .ok_or_else(|| anyhow::anyhow!("Invalid fixture schema ID: {value}"))?;
    Ok(id as u32)
}

fn framed(id: u32, protobuf: bool, raw: Vec<u8>) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend(id.to_be_bytes());
    if protobuf {
        bytes.push(0);
    }
    bytes.extend(raw);
    bytes
}

#[tokio::main]
async fn main() -> Result<()> {
    prepare()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--prepare"] {
        return Ok(());
    }
    anyhow::ensure!(args.is_empty(), "Usage: seed_nats [--prepare]");
    let avro_id = register("onetui-nats-avro", "AVRO", avro::SCHEMA)?;
    let protobuf_id = register(
        "onetui-nats-protobuf",
        "PROTOBUF",
        "syntax='proto3'; package demo; message Event {int64 id=1; string city=2; bytes payload=3;}",
    )?;
    let client = async_nats::ConnectOptions::new()
        .user_and_password("fixture-admin".into(), "fixture-admin-only".into())
        .max_reconnects(1)
        .connection_timeout(std::time::Duration::from_secs(1))
        .connect("nats://127.0.0.1:14222")
        .await?;
    let js = async_nats::jetstream::new(client.clone());
    for (name, subject, count) in [
        ("DEMO_EVENTS", "demo.events", 1205),
        ("DEMO_BINARY", "demo.binary", 260),
        ("DEMO_WIDE", "demo.wide", 110),
        ("DEMO_EMPTY", "demo.empty", 0),
        ("DEMO_LIVE", "demo.live", 0),
        ("DEMO_AVRO", "demo.avro", 250),
        ("DEMO_PROTOBUF", "demo.protobuf", 250),
        ("DEMO_AVRO_REGISTRY", "demo.avro.registry", 250),
        ("DEMO_PROTOBUF_REGISTRY", "demo.protobuf.registry", 250),
    ] {
        let stream = js
            .get_or_create_stream(Config {
                name: name.into(),
                subjects: vec![subject.into()],
                storage: StorageType::Memory,
                max_bytes: 8 * 1024 * 1024,
                ..Default::default()
            })
            .await?;
        if stream.cached_info().state.messages != 0 {
            continue;
        }
        for i in 1..=count {
            let payload = if name == "DEMO_AVRO_REGISTRY" {
                framed(avro_id, false, avro::message(i as u64))
            } else if name == "DEMO_PROTOBUF_REGISTRY" {
                framed(protobuf_id, true, protobuf::message(i as u64))
            } else if name == "DEMO_AVRO" {
                avro::message(i as u64)
            } else if name == "DEMO_PROTOBUF" {
                protobuf::message(i as u64)
            } else if name == "DEMO_BINARY" {
                match i % 4 {
                    0 => vec![],
                    1 => vec![0, 255, 128, 27, 10],
                    2 => "София / 東京 / São Paulo 🌊".as_bytes().to_vec(),
                    _ => br#"{"active":true,"optional":null,"items":[1,"two",false]}"#.to_vec(),
                }
            } else if name == "DEMO_WIDE" {
                serde_json::to_vec(&(0..40).map(|field| (format!("field_{field:02}"), json!({
                    "ordinal": i, "text": "wide value ".repeat(30), "nested": [true, null, {"score": 1.25}]
                }))).collect::<serde_json::Map<_, _>>())?
            } else {
                serde_json::to_vec(&json!({
                    "id": i, "category": (["search", "order", "login"][i % 3]),
                    "active": i % 2 == 0, "price": -1.25, "optional": null,
                    "empty_text": "", "empty_list": [], "empty_object": {},
                    "unicode": "София / 東京 / São Paulo 🌊", "controls": "\n\t\u{1b}[31m",
                    "tags": ["demo", "synthetic"], "nested": {"customer": {"id": i, "rating": 0.5}}
                }))?
            };
            let mut headers = async_nats::HeaderMap::new();
            headers.append("X-Demo", "synthetic");
            headers.append("X-Demo", "repeat");
            js.publish_with_headers(subject, headers, payload.into())
                .await?
                .await?;
        }
        println!("{name}: {count} synthetic messages");
    }
    let stream = js.get_stream("DEMO_EVENTS").await?;
    stream
        .get_or_create_consumer(
            "demo_reader",
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some("demo_reader".into()),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await?;
    if js.get_key_value("DEMO_SETTINGS").await.is_err() {
        let kv = js
            .create_key_value(async_nats::jetstream::kv::Config {
                bucket: "DEMO_SETTINGS".into(),
                history: 8,
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await?;
        for i in 0..125 {
            kv.put(
                format!("service.{i:03}"),
                serde_json::to_vec(
                    &json!({"enabled": i % 2 == 0, "region": "София", "retries": i % 5}),
                )?
                .into(),
            )
            .await?;
        }
        kv.put("history", "first".into()).await?;
        kv.put("history", vec![0, 255, 128].into()).await?;
        kv.delete("history").await?;
        kv.put("empty", Vec::new().into()).await?;
        kv.put("purged", "old".into()).await?;
        kv.purge("purged").await?;
        println!("DEMO_SETTINGS: keys, revisions, binary, empty and deletion markers");
    }
    if js.get_object_store("DEMO_FILES").await.is_err() {
        let objects = js
            .create_object_store(async_nats::jetstream::object_store::Config {
                bucket: "DEMO_FILES".into(),
                storage: StorageType::Memory,
                ..Default::default()
            })
            .await?;
        for i in 0..110 {
            let data = serde_json::to_vec(&json!({"id": i, "city": "東京", "optional": null}))?;
            objects
                .put(
                    format!("reports/{i:03}.json").as_str(),
                    &mut data.as_slice(),
                )
                .await?;
        }
        let bytes: Vec<u8> = (0..1_200_000).map(|i| (i % 256) as u8).collect();
        objects
            .put("София / 東京.bin", &mut bytes.as_slice())
            .await?;
        objects.put("empty", &mut &[][..]).await?;
        objects.put("deleted", &mut &b"removed"[..]).await?;
        objects.delete("deleted").await?;
        println!("DEMO_FILES: JSON objects, multi-page binary, empty and deleted objects");
    }
    client.flush().await?;
    Ok(())
}
