use onetui_avro::Decoder;

#[test]
fn named_writer_references_keep_bounds_and_raw_bytes() {
    let root = r#"{"type":"record","name":"Root","fields":[{"name":"child","type":"Child"}]}"#;
    let child = r#"{"type":"record","name":"Child","fields":[{"name":"id","type":"long"}]}"#;
    let decoder = Decoder::with_references(root, &[child]).unwrap();
    let value = decoder.decode(&[14]).unwrap();
    assert_eq!(value.raw(), &[14]);
    assert_eq!(value.json().unwrap(), r#"{"child":{"id":7}}"#);
    assert!(Decoder::new(root).is_err());
    assert!(Decoder::with_references(root, &[child, child]).is_err());
    assert!(Decoder::with_references(root, &vec![child; 33]).is_err());
    assert!(decoder.decode(&[14, 0]).is_err());
    assert!(decoder.decode(&[255]).is_err());
    assert!(Decoder::with_references(root, &[&"x".repeat(262144)]).is_err());
}
