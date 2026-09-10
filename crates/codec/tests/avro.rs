use apache_avro::types::Value;
use onetui_codec::{Decoder, MAX_NODES, MAX_PAYLOAD_BYTES, MAX_SCHEMA_BYTES, TypedValue};

fn long(n: i64) -> Vec<u8> {
    let mut bytes = Vec::new();
    prost::encoding::encode_varint(((n as u64) << 1) ^ ((n >> 63) as u64), &mut bytes);
    bytes
}

#[test]
fn writer_schema_preserves_union_bytes_logical_types_and_provenance() {
    let schema = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"long"}, {"name":"data","type":"bytes"},
        {"name":"note","type":["null","string"]},
        {"name":"day","type":{"type":"int","logicalType":"date"}},
        {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
    ]}"#;
    let decoder = Decoder::avro(schema).unwrap();
    let raw = [14, 4, 255, 0, 2, 4, b'H', b'i', 2, 2, 123];
    let decoded = decoder.decode(&raw).unwrap();
    assert_eq!(decoded.raw(), raw);
    assert_eq!(decoded.schema_id(), decoder.schema_id());
    assert!(decoded.schema_id().starts_with("avro:sha256:"));
    let TypedValue::Avro(Value::Record(fields)) = decoded.value() else {
        panic!("wrong type")
    };
    assert_eq!(fields[0].1, Value::Long(7));
    assert_eq!(fields[1].1, Value::Bytes(vec![255, 0]));
    assert_eq!(
        fields[2].1,
        Value::Union(1, Box::new(Value::String("Hi".into())))
    );
    assert_eq!(fields[3].1, Value::Date(1));
    assert!(matches!(fields[4].1, Value::Decimal(_)));
    let json: serde_json::Value = serde_json::from_str(&decoded.json().unwrap()).unwrap();
    assert_eq!(json["data"], serde_json::json!([255, 0]));
    assert_eq!(json["note"], "Hi");
    assert_ne!(
        decoder.schema_id(),
        Decoder::avro(&format!("{schema} ")).unwrap().schema_id()
    );
}

#[test]
fn empty_values_null_truncation_and_trailing_bytes_remain_distinct() {
    let null = Decoder::avro("\"null\"").unwrap();
    assert!(matches!(
        null.decode(&[]).unwrap().value(),
        TypedValue::Avro(Value::Null)
    ));
    assert!(null.decode(&[0]).is_err());
    let string = Decoder::avro("\"string\"").unwrap();
    assert!(
        matches!(string.decode(&[0]).unwrap().value(), TypedValue::Avro(Value::String(s)) if s.is_empty())
    );
    for raw in [
        &[][..],
        &[4, b'a'][..],
        &[2, 255][..],
        &[0, 0][..],
        &[1][..],
    ] {
        assert!(string.decode(raw).is_err());
    }
    assert!(Decoder::avro("\"boolean\"").unwrap().decode(&[]).is_err());
    assert!(
        Decoder::avro("[\"null\",\"string\"]")
            .unwrap()
            .decode(&[])
            .is_err()
    );
    assert!(string.decode(&vec![0; MAX_PAYLOAD_BYTES + 1]).is_err());
    assert!(Decoder::avro(&" ".repeat(MAX_SCHEMA_BYTES + 1)).is_err());
    assert!(Decoder::avro("\"missing\"").is_err());
    assert!(Decoder::avro("{").is_err());
}

#[test]
fn nested_collection_bombs_and_zero_width_values_fail_before_native_decode() {
    let nulls = Decoder::avro(r#"{"type":"array","items":"null"}"#).unwrap();
    let mut bomb = long(200_000_000);
    bomb.push(0);
    assert!(
        nulls
            .decode(&bomb)
            .err()
            .unwrap()
            .to_string()
            .contains("budget")
    );
    let nested =
        Decoder::avro(r#"{"type":"array","items":{"type":"array","items":"null"}}"#).unwrap();
    let mut raw = long(2);
    for _ in 0..2 {
        raw.extend(long((MAX_NODES / 2) as i64));
        raw.push(0);
    }
    raw.push(0);
    assert!(
        nested
            .decode(&raw)
            .err()
            .unwrap()
            .to_string()
            .contains("budget")
    );
    let small = nulls.decode(&[6, 0]).unwrap();
    assert!(matches!(small.value(), TypedValue::Avro(Value::Array(values)) if values.len()==3));
    let fixed = Decoder::avro(r#"{"type":"fixed","name":"Huge","size":2147483647}"#).unwrap();
    assert!(fixed.decode(&[]).is_err());
}

#[test]
fn named_recursion_and_block_sizes_are_checked() {
    let schema = r#"{"type":"record","name":"Node","namespace":"demo","fields":[{"name":"next","type":["null","Node"]}]}"#;
    let decoder = Decoder::avro(schema).unwrap();
    assert!(decoder.decode(&[2, 0]).is_ok());
    let mut deep = vec![2; 100];
    deep.push(0);
    assert!(
        decoder
            .decode(&deep)
            .err()
            .unwrap()
            .to_string()
            .contains("depth")
    );
    let array = Decoder::avro(r#"{"type":"array","items":"long"}"#).unwrap();
    assert!(array.decode(&[3, 4, 2, 4, 0]).is_ok()); // -2 items, 2 bytes, [1,2], end
    assert!(array.decode(&[3, 6, 2, 4, 0]).is_err());
    let mut overflow = long(i64::MIN);
    overflow.push(0);
    assert!(array.decode(&overflow).is_err());
    let map = Decoder::avro(r#"{"type":"map","values":"long"}"#).unwrap();
    assert!(map.decode(&[2, 2, b'x', 14, 0]).is_ok());
}

#[test]
fn unsupported_decimal_and_json_failure_keep_raw_access() {
    let big = Decoder::avro(r#"{"type":"bytes","logicalType":"big-decimal"}"#).unwrap();
    assert!(
        big.decode(&[0])
            .err()
            .unwrap()
            .to_string()
            .contains("not supported")
    );
    let double = Decoder::avro("\"double\"").unwrap();
    let raw = f64::NAN.to_le_bytes();
    let decoded = double.decode(&raw).unwrap();
    assert!(decoded.json().is_err());
    assert_eq!(decoded.raw(), raw);
    assert!(matches!(decoded.value(), TypedValue::Avro(Value::Double(v)) if v.is_nan()));
}
