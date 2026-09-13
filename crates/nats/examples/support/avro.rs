pub const SCHEMA: &str = r#"{"type":"record","name":"Event","namespace":"demo","fields":[{"name":"id","type":"long"},{"name":"city","type":"string"},{"name":"payload","type":"bytes"},{"name":"labels","type":["null",{"type":"array","items":"string"}]}]}"#;

pub fn message(id: u64) -> Vec<u8> {
    use apache_avro::types::Value;
    apache_avro::writer::datum::GenericDatumWriter::builder(
        &apache_avro::Schema::parse_str(SCHEMA).unwrap(),
    )
    .build()
    .unwrap()
    .write_value_to_vec(Value::Record(vec![
        ("id".into(), Value::Long(id as i64)),
        ("city".into(), Value::String("София / 東京".into())),
        ("payload".into(), Value::Bytes(vec![0, 255, 128])),
        (
            "labels".into(),
            Value::Union(
                1,
                Box::new(Value::Array(vec![Value::String("synthetic".into())])),
            ),
        ),
    ]))
    .unwrap()
}
