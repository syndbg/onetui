use onetui_avro::{Decoder, MAX_SCHEMA_BYTES, ReaderSchema};

const WRITER: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}"#;

#[test]
fn explicit_reader_defaults_promotions_and_writer_inspection() {
    let reader = ReaderSchema::new(
        r#"{"type":"record","name":"Event","fields":[
        {"name":"email","type":["null","string"],"default":null},
        {"name":"id","type":"long"}
    ]}"#,
    )
    .unwrap();
    let identity = reader.schema_id().to_owned();
    let decoder = Decoder::new(WRITER).unwrap().with_reader(reader);
    assert_eq!(decoder.reader_schema_id(), Some(identity.as_str()));
    let decoded = decoder.decode(&[14]).unwrap();
    assert_eq!(decoded.reader_schema_id(), Some(identity.as_str()));
    assert_eq!(decoded.raw(), [14]);
    let json: serde_json::Value = serde_json::from_str(&decoded.json().unwrap()).unwrap();
    assert_eq!(json, serde_json::json!({"id":7,"email":null}));
    let native: serde_json::Value = serde_json::from_str(&decoded.native().unwrap()).unwrap();
    assert_eq!(native["writer"]["fields"][0][1]["type"], "int");
    assert_eq!(native["reader"]["fields"][1][1]["type"], "long");
    assert!(native["reader_error"].is_null());
    let plain = Decoder::new(WRITER).unwrap().decode(&[14]).unwrap();
    assert_eq!(plain.json().unwrap(), r#"{"id":7}"#);
    assert_eq!(plain.value(), decoded.value());
}

#[test]
fn incompatible_reader_keeps_native_writer_and_raw_bytes() {
    let reader = ReaderSchema::new(
        r#"{"type":"record","name":"Event","fields":[{"name":"missing","type":"string"}]}"#,
    )
    .unwrap();
    let decoded = Decoder::new(WRITER)
        .unwrap()
        .with_reader(reader)
        .decode(&[14])
        .unwrap();
    assert!(decoded.json().is_err());
    let native: serde_json::Value = serde_json::from_str(&decoded.native().unwrap()).unwrap();
    assert_eq!(native["writer"]["fields"][0][1]["value"], 7);
    assert!(native["reader_error"].is_string());
    assert_eq!(decoded.raw(), [14]);
    assert!(ReaderSchema::new(&" ".repeat(MAX_SCHEMA_BYTES + 1)).is_err());
    assert!(ReaderSchema::new("\"Unresolved\"").is_err());
}

#[test]
fn large_reader_errors_do_not_hide_small_writer_values() {
    let schema = serde_json::json!({"type":"record","name":"R","fields":[
        {"name":"x".repeat(8192),"type":"string"}
    ]})
    .to_string();
    let decoded = Decoder::new("\"int\"")
        .unwrap()
        .with_reader(ReaderSchema::new(&schema).unwrap())
        .decode(&[14])
        .unwrap();
    let native: serde_json::Value = serde_json::from_str(&decoded.native().unwrap()).unwrap();
    assert_eq!(native["writer"]["value"], 7);
    assert!(native["reader_error"].as_str().unwrap().len() <= onetui_avro::MAX_ERROR_BYTES);
    assert!(decoded.native().unwrap().len() < 1024);
}

#[test]
fn default_expansion_and_recursive_defaults_are_bounded_before_resolution() {
    let writer = r#"{"type":"array","items":{"type":"record","name":"R","fields":[]}}"#;
    let reader = serde_json::json!({"type":"array","items":{"type":"record","name":"R","fields":[
        {"name":"pad","type":"string","default":"x".repeat(4096)}
    ]}})
    .to_string();
    let decoder = Decoder::new(writer)
        .unwrap()
        .with_reader(ReaderSchema::new(&reader).unwrap());
    // 100 empty records, then end of array.
    let decoded = decoder.decode(&[200, 1, 0]).unwrap();
    assert!(decoded.json().unwrap_err().to_string().contains("256 KiB"));
    assert_eq!(decoded.raw(), [200, 1, 0]);

    let reader =
        r#"{"type":"record","name":"R","fields":[{"name":"next","type":"R","default":{}}]}"#;
    assert!(ReaderSchema::new(reader).is_err());
}

#[test]
fn named_recursive_readers_and_field_removal_work() {
    let schema = r#"{"type":"record","name":"Node","namespace":"demo","fields":[{"name":"next","type":["null","Node"]}]}"#;
    let decoder = Decoder::new(schema)
        .unwrap()
        .with_reader(ReaderSchema::new(schema).unwrap());
    assert_eq!(
        decoder.decode(&[2, 0]).unwrap().json().unwrap(),
        r#"{"next":{"next":null}}"#
    );
    let reader = ReaderSchema::new(r#"{"type":"record","name":"Event","fields":[]}"#).unwrap();
    assert_eq!(
        Decoder::new(WRITER)
            .unwrap()
            .with_reader(reader)
            .decode(&[14])
            .unwrap()
            .json()
            .unwrap(),
        "{}"
    );
}
