use onetui_protobuf::{Decoder, MAX_DEPTH, MAX_NODES, MAX_PAYLOAD_BYTES, MAX_SCHEMA_BYTES};
use prost::Message;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};

fn field(
    name: &str,
    number: i32,
    kind: Type,
    repeated: bool,
    type_name: Option<&str>,
) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(kind as i32),
        label: Some(if repeated {
            Label::Repeated
        } else {
            Label::Optional
        } as i32),
        type_name: type_name.map(str::to_owned),
        ..Default::default()
    }
}

fn schema() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Event".into()),
                field: vec![
                    field("id", 1, Type::Int64, false, None),
                    field("name", 2, Type::String, false, None),
                    field("data", 3, Type::Bytes, false, None),
                    field("state", 4, Type::Enum, false, Some(".demo.State")),
                    field("scores", 5, Type::Int32, true, None),
                    field("child", 6, Type::Message, false, Some(".demo.Event")),
                ],
                ..Default::default()
            }],
            enum_type: vec![EnumDescriptorProto {
                name: Some("State".into()),
                value: vec![EnumValueDescriptorProto {
                    name: Some("UNKNOWN".into()),
                    number: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[test]
fn raw_typed_fields_unknown_enum_and_unknown_fields_survive() {
    let decoder = Decoder::new(&schema(), "demo.Event").unwrap();
    let mut raw = vec![
        8, 7, 18, 2, b'H', b'i', 26, 2, 255, 0, 32, 99, 42, 3, 1, 2, 3,
    ];
    prost::encoding::encode_key(99, prost::encoding::WireType::Varint, &mut raw);
    prost::encoding::encode_varint(123, &mut raw);
    let decoded = decoder.decode(&raw).unwrap();
    assert_eq!(decoded.raw(), raw);
    assert_eq!(decoded.schema_id(), decoder.schema_id());
    assert!(decoded.schema_id().starts_with("protobuf:sha256:"));
    let message = decoded.value();
    assert_eq!(
        message
            .get_field_by_name("data")
            .unwrap()
            .as_bytes()
            .unwrap()
            .as_ref(),
        &[255, 0]
    );
    assert_eq!(
        message.get_field_by_name("state").unwrap().as_enum_number(),
        Some(99)
    );
    assert_eq!(message.unknown_fields().count(), 1);
    let json: serde_json::Value = serde_json::from_str(&decoded.json().unwrap()).unwrap();
    assert_eq!(json["id"], "7");
    assert_eq!(json["data"], "/wA=");
    assert_eq!(json["state"], 99);
    assert_eq!(json["scores"], serde_json::json!([1, 2, 3]));
}

#[test]
fn exact_schema_names_missing_imports_empty_and_malformed_input() {
    let bytes = schema();
    assert!(Decoder::new(&bytes, "Event").is_err());
    assert!(Decoder::new(&bytes, "").is_err());
    assert!(Decoder::new(&[255], "demo.Event").is_err());
    assert!(Decoder::new(&vec![0; MAX_SCHEMA_BYTES + 1], "demo.Event").is_err());
    let mut missing = FileDescriptorSet::decode(bytes.as_slice()).unwrap();
    missing.file[0].dependency.push("missing.proto".into());
    assert!(Decoder::new(&missing.encode_to_vec(), "demo.Event").is_err());
    let decoder = Decoder::new(&bytes, "demo.Event").unwrap();
    assert_eq!(decoder.decode(&[]).unwrap().json().unwrap(), "{}");
    for raw in [
        &[8][..],
        &[0][..],
        &[18, 2, b'a'][..],
        &[18, 1, 255][..],
        &[8, 7, 8][..],
        &[0x3c][..],
    ] {
        assert!(decoder.decode(raw).is_err(), "{raw:?}");
    }
    assert!(decoder.decode(&vec![0; MAX_PAYLOAD_BYTES + 1]).is_err());
    // Protobuf concatenation is valid merge semantics, not detectable framing.
    assert!(decoder.decode(&[8, 1, 8, 2]).is_ok());
}

#[test]
fn preflight_bounds_recursive_messages_unknown_groups_and_packed_values() {
    let decoder = Decoder::new(&schema(), "demo.Event").unwrap();
    let mut raw = Vec::new();
    for _ in 0..MAX_DEPTH + 1 {
        let mut outer = vec![50];
        prost::encoding::encode_varint(raw.len() as u64, &mut outer);
        outer.extend(raw);
        raw = outer;
    }
    assert!(
        decoder
            .decode(&raw)
            .err()
            .unwrap()
            .to_string()
            .contains("depth")
    );
    let mut groups = vec![0x3b; MAX_DEPTH + 1];
    groups.extend(vec![0x3c; MAX_DEPTH + 1]);
    assert!(
        decoder
            .decode(&groups)
            .err()
            .unwrap()
            .to_string()
            .contains("depth")
    );
    let mut packed = vec![42];
    prost::encoding::encode_varint(MAX_NODES as u64, &mut packed);
    packed.extend(vec![0; MAX_NODES]);
    assert!(
        decoder
            .decode(&packed)
            .err()
            .unwrap()
            .to_string()
            .contains("node")
    );
    let repeated: Vec<_> = (0..MAX_NODES).flat_map(|_| [8, 0]).collect();
    assert!(
        decoder
            .decode(&repeated)
            .err()
            .unwrap()
            .to_string()
            .contains("node")
    );
}

#[test]
fn any_unpacked_json_is_an_error_without_losing_typed_or_raw_data() {
    let descriptor = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("any.proto".into()),
            package: Some("google.protobuf".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Any".into()),
                field: vec![
                    field("type_url", 1, Type::String, false, None),
                    field("value", 2, Type::Bytes, false, None),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let decoder = Decoder::new(&descriptor, "google.protobuf.Any").unwrap();
    let raw = [18, 2, 255, 0];
    let decoded = decoder.decode(&raw).unwrap();
    assert!(decoded.json().unwrap_err().to_string().contains("Any JSON"));
    assert_eq!(decoded.raw(), raw);
    assert_eq!(
        decoded
            .value()
            .get_field_by_name("value")
            .unwrap()
            .as_bytes()
            .unwrap()
            .as_ref(),
        &[255, 0]
    );
}

#[test]
fn json_output_expansion_is_bounded_before_allocation() {
    let mut descriptor = FileDescriptorSet::decode(schema().as_slice()).unwrap();
    descriptor.file[0].message_type[0].field[1].name = Some("x".repeat(2000));
    descriptor.file[0].message_type[0].field[5].label = Some(Label::Repeated as i32);
    let decoder = Decoder::new(&descriptor.encode_to_vec(), "demo.Event").unwrap();
    let raw: Vec<_> = (0..1000).flat_map(|_| [50, 3, 18, 1, b'a']).collect();
    let decoded = decoder.decode(&raw).unwrap();
    assert!(decoded.json().unwrap_err().to_string().contains("1 MiB"));
    assert_eq!(decoded.raw(), raw);
}
