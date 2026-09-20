#[path = "../examples/support/broker.rs"]
#[allow(dead_code)]
mod broker;
#[path = "../examples/support/redpanda.rs"]
#[allow(dead_code)]
mod fixture;

#[test]
fn demo_avro_catalog_matches_producer_and_references() {
    let dir = tempfile::tempdir().unwrap();
    fixture::prepare(dir.path()).unwrap();
    let root = std::fs::read_to_string(dir.path().join("avro_event.avsc")).unwrap();
    let child = std::fs::read_to_string(dir.path().join("avro_customer.avsc")).unwrap();
    let decoder = onetui_avro::Decoder::with_references(&root, &[&child]).unwrap();
    let key_decoder = onetui_avro::Decoder::new(&child).unwrap();
    for n in [0, 1, 999, 1000] {
        let raw = fixture::raw(n).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&decoder.decode(&raw).unwrap().json().unwrap()).unwrap();
        assert_eq!(json["id"], n * 2);
        assert_eq!(
            json["customer"]["name"],
            format!("София / 東京 / São Paulo {}", n * 2)
        );
        assert!(json["tags"].is_array());
        let key: serde_json::Value = serde_json::from_str(
            &key_decoder
                .decode(&fixture::key_raw(n).unwrap())
                .unwrap()
                .json()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(key["id"], n % 23);
        assert_eq!(key["name"], format!("customer-{n}"));
        let framed = fixture::key_message(n, 42).unwrap();
        assert_eq!(&framed[..5], &[0, 0, 0, 0, 42]);
    }
}
