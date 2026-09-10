use apache_avro::types::Value;
use onetui_avro::{Decoder, MAX_NODES, MAX_PAYLOAD_BYTES, MAX_SCHEMA_BYTES};

fn long(n: i64) -> Vec<u8> {
    apache_avro::writer::datum::GenericDatumWriter::builder(&apache_avro::Schema::Long)
        .build()
        .unwrap()
        .write_value_to_vec(Value::Long(n))
        .unwrap()
}

#[test]
fn writer_schema_preserves_union_bytes_logical_types_and_provenance() {
    let schema = r#"{"type":"record","name":"Event","fields":[
        {"name":"id","type":"long"}, {"name":"data","type":"bytes"},
        {"name":"note","type":["null","string"]},
        {"name":"day","type":{"type":"int","logicalType":"date"}},
        {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}}
    ]}"#;
    let decoder = Decoder::new(schema).unwrap();
    let raw = [14, 4, 255, 0, 2, 4, b'H', b'i', 2, 2, 123];
    let decoded = decoder.decode(&raw).unwrap();
    assert_eq!(decoded.raw(), raw);
    assert_eq!(decoded.schema_id(), decoder.schema_id());
    assert!(decoded.schema_id().starts_with("avro:sha256:"));
    let Value::Record(fields) = decoded.value() else {
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
        Decoder::new(&format!("{schema} ")).unwrap().schema_id()
    );
}

#[test]
fn empty_values_null_truncation_and_trailing_bytes_remain_distinct() {
    let null = Decoder::new("\"null\"").unwrap();
    assert!(matches!(null.decode(&[]).unwrap().value(), Value::Null));
    assert!(null.decode(&[0]).is_err());
    let string = Decoder::new("\"string\"").unwrap();
    assert!(matches!(string.decode(&[0]).unwrap().value(), Value::String(s) if s.is_empty()));
    for raw in [
        &[][..],
        &[4, b'a'][..],
        &[2, 255][..],
        &[0, 0][..],
        &[1][..],
    ] {
        assert!(string.decode(raw).is_err());
    }
    assert!(Decoder::new("\"boolean\"").unwrap().decode(&[]).is_err());
    assert!(
        Decoder::new("[\"null\",\"string\"]")
            .unwrap()
            .decode(&[])
            .is_err()
    );
    assert!(string.decode(&vec![0; MAX_PAYLOAD_BYTES + 1]).is_err());
    assert!(Decoder::new(&" ".repeat(MAX_SCHEMA_BYTES + 1)).is_err());
    assert!(Decoder::new("\"missing\"").is_err());
    assert!(Decoder::new("{").is_err());
}

#[test]
fn nested_collection_bombs_and_zero_width_values_fail_before_native_decode() {
    let nulls = Decoder::new(r#"{"type":"array","items":"null"}"#).unwrap();
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
        Decoder::new(r#"{"type":"array","items":{"type":"array","items":"null"}}"#).unwrap();
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
    assert!(matches!(small.value(), Value::Array(values) if values.len()==3));
    let fixed = Decoder::new(r#"{"type":"fixed","name":"Huge","size":2147483647}"#).unwrap();
    assert!(fixed.decode(&[]).is_err());
}

#[test]
fn named_recursion_and_block_sizes_are_checked() {
    let schema = r#"{"type":"record","name":"Node","namespace":"demo","fields":[{"name":"next","type":["null","Node"]}]}"#;
    let decoder = Decoder::new(schema).unwrap();
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
    let array = Decoder::new(r#"{"type":"array","items":"long"}"#).unwrap();
    assert!(array.decode(&[3, 4, 2, 4, 0]).is_ok()); // -2 items, 2 bytes, [1,2], end
    assert!(array.decode(&[3, 6, 2, 4, 0]).is_err());
    let mut overflow = long(i64::MIN);
    overflow.push(0);
    assert!(array.decode(&overflow).is_err());
    let map = Decoder::new(r#"{"type":"map","values":"long"}"#).unwrap();
    assert!(map.decode(&[2, 2, b'x', 14, 0]).is_ok());
}

#[test]
fn unsupported_decimal_and_json_failure_keep_raw_access() {
    let big = Decoder::new(r#"{"type":"bytes","logicalType":"big-decimal"}"#).unwrap();
    assert!(
        big.decode(&[0])
            .err()
            .unwrap()
            .to_string()
            .contains("not supported")
    );
    let double = Decoder::new("\"double\"").unwrap();
    let raw = f64::NAN.to_le_bytes();
    let decoded = double.decode(&raw).unwrap();
    assert!(decoded.json().is_err());
    assert_eq!(decoded.raw(), raw);
    assert!(matches!(decoded.value(), Value::Double(v) if v.is_nan()));
}

#[test]
fn long_boundaries_truncation_and_overflow() {
    let decoder = Decoder::new(r#""long""#).unwrap();
    for n in [
        i64::MIN,
        i64::MAX,
        -8193,
        -8192,
        -65,
        -64,
        -1,
        0,
        1,
        63,
        64,
        8191,
        8192,
    ] {
        let raw = long(n);
        let decoded = decoder.decode(&raw).unwrap();
        assert_eq!(decoded.value(), &Value::Long(n));
        assert_eq!(decoded.raw(), raw);
        for end in 0..raw.len() {
            assert!(decoder.decode(&raw[..end]).is_err());
        }
    }
    for last in [2, 127, 128, 255] {
        let mut raw = vec![128; 9];
        raw.push(last);
        assert!(decoder.decode(&raw).is_err());
    }
    assert!(decoder.decode(&[128; 11]).is_err());
}
