#[path = "../examples/support/broker.rs"]
#[allow(dead_code)]
mod broker;
#[path = "../examples/support/protobuf.rs"]
#[allow(dead_code)]
mod fixture;

#[test]
fn demo_protobuf_descriptors_match_producer_and_registry_framing() {
    let dir = tempfile::tempdir().unwrap();
    fixture::prepare(dir.path()).unwrap();
    let descriptors = std::fs::read(dir.path().join("protobuf_event.pb")).unwrap();
    let decoder = onetui_protobuf::Decoder::new(&descriptors, "demo.Event").unwrap();
    for n in [0, 1, 999, 1000] {
        let raw = fixture::raw(n);
        let json: serde_json::Value =
            serde_json::from_str(&decoder.decode(&raw).unwrap().json().unwrap()).unwrap();
        assert_eq!(json["title"], format!("Synthetic Protobuf event {n}"));
        assert_eq!(
            json["customer"]["name"],
            format!("София / 東京 / São Paulo {n}")
        );
        assert_eq!(json["tags"], serde_json::json!(["demo", "protobuf"]));
        assert!(json["attachment"].is_string());
        assert_eq!(json.get("note").is_some(), n % 2 == 1);
        let framed = fixture::message(n, [42, 43]);
        assert_eq!(&framed[..6], &[0, 0, 0, 0, 42 + (n % 2) as u8, 0]);
        assert_eq!(&framed[6..], raw);
    }
}
