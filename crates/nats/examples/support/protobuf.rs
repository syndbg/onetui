use prost::Message;

pub fn schema() -> Vec<u8> {
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    };
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Event".into()),
                field: [
                    ("id", 1, Type::Int64),
                    ("city", 2, Type::String),
                    ("payload", 3, Type::Bytes),
                ]
                .into_iter()
                .map(|(name, number, kind)| FieldDescriptorProto {
                    name: Some(name.into()),
                    number: Some(number),
                    r#type: Some(kind as i32),
                    label: Some(Label::Optional as i32),
                    ..Default::default()
                })
                .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[derive(Clone, PartialEq, Message)]
struct Event {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    city: String,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
}

pub fn message(id: u64) -> Vec<u8> {
    let mut bytes = Event {
        id: id as i64,
        city: "София / 東京".into(),
        payload: vec![0, 255, 128],
    }
    .encode_to_vec();
    bytes.extend([0x98, 0x06, 0x7b]); // Unknown field 99 remains visible in native inspection.
    bytes
}
