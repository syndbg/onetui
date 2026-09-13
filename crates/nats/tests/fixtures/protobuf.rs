use super::*;
use std::io::Write;
#[path = "../../examples/support/protobuf.rs"]
mod sample;

#[tokio::test]
#[ignore = "uses an explicit disposable Core subject and local Protobuf descriptor; no JetStream writes"]
async fn protobuf_core_bytes_unknown_fields_empty_malformed_and_bounds() {
    let mut schema = tempfile::NamedTempFile::new().unwrap();
    schema.write_all(&sample::schema()).unwrap();
    let subject = format!("demo.protobuf_{}", std::process::id());
    let options: toml::Table = toml::from_str(&format!("servers=['nats://127.0.0.1:14222']\ntls=false\njetstream=false\nsubjects=[{subject:?}]\nusername_env='U'\npassword_env='P'\n[[decoders]]\nsubject={subject:?}\nformat='protobuf'\nschema_file={:?}\nmessage_name='demo.Event'", schema.path())).unwrap();
    let mut executor = NatsProvider
        .configure(&options, &|n| {
            Some(
                if n == "U" {
                    "fixture-reader"
                } else {
                    "fixture-reader-only"
                }
                .into(),
            )
        })
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let start = read(&executor, "nats.core_messages", &[&subject], None, true)
        .await
        .unwrap();
    let admin = admin().await;
    let messages = [sample::message(42), vec![], vec![255], vec![0; 65537]];
    for bytes in &messages {
        admin
            .publish(subject.clone(), bytes.clone().into())
            .await
            .unwrap();
    }
    admin.flush().await.unwrap();
    let mut rows = Vec::new();
    let mut cursor = start.continuation;
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while rows.len() < 4 {
        let page = read(&executor, "nats.core_messages", &[&subject], cursor, true)
            .await
            .unwrap();
        assert!(page.bytes() <= onetui_core::PAGE_BYTES);
        assert_eq!(page.columns.len(), start.columns.len());
        cursor = page.continuation;
        rows.extend(page.rows);
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    for (row, raw) in rows.iter().zip(&messages) {
        assert_eq!(row.cells[2], Some(Value::Bytes(raw.clone())));
    }
    let json: Json =
        serde_json::from_str(rows[0].cells[6].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(json["id"], "42");
    let native: Json =
        serde_json::from_str(rows[0].cells[9].as_ref().unwrap().text().unwrap()).unwrap();
    assert_eq!(native["unknown_fields"][0]["number"], 99);
    assert!(rows[1].cells[6].is_some());
    assert!(rows[2].cells[8].is_some());
    assert!(
        rows[3].cells[8]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("64 KiB")
    );
    close(&mut executor).await;
}
