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
    std::fs::create_dir_all(&directory)?;
    std::fs::write(directory.join("nats-event.avsc"), avro::SCHEMA)?;
    std::fs::write(directory.join("nats-event.pb"), protobuf::schema())?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    prepare()?;
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--prepare"] {
        return Ok(());
    }
    anyhow::ensure!(args.is_empty(), "Usage: seed_nats [--prepare]");
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
    ] {
        let stream = js
            .get_or_create_stream(Config {
                name: name.into(),
                subjects: vec![subject.into()],
                storage: StorageType::Memory,
                max_bytes: 32 * 1024 * 1024,
                ..Default::default()
            })
            .await?;
        if stream.cached_info().state.messages != 0 {
            continue;
        }
        for i in 1..=count {
            let payload = if name == "DEMO_AVRO" {
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
