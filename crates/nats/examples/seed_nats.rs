//! Seed only the fixed disposable NATS fixture.
use anyhow::Result;
use async_nats::jetstream::stream::{Config, StorageType};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
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
            let payload = if name == "DEMO_BINARY" {
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
    client.flush().await?;
    Ok(())
}
