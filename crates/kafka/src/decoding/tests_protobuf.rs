use super::*;
use onetui_core::Row;
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

#[test]
fn protobuf_key_binding_uses_exact_message_and_preserves_raw_value() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("event.pb");
    let schema = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("event.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Event".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("id".into()),
                    number: Some(1),
                    r#type: Some(3),
                    label: Some(1),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    std::fs::write(&path, schema).unwrap();
    let binding = Binding {
        topic: "events".into(),
        field: Field::Key,
        format: Format::Protobuf,
        framing: Framing::Raw,
        schema_file: Some(path.to_str().unwrap().into()),
        message_name: Some("demo.Event".into()),
        registry: None,
    };
    validate(std::slice::from_ref(&binding)).unwrap();
    let mut invalid = binding.clone();
    invalid.message_name = None;
    assert!(validate(&[invalid]).is_err());
    let mut wrong = binding.clone();
    wrong.message_name = Some("Event".into());
    assert!(Bindings::new(vec![wrong]).check(|| Ok(())).is_err());
    let mut bindings = Bindings::new(vec![binding]);
    let mut page = Page {
        columns: vec![
            Column {
                name: "key".into(),
                datatype: "bytes".into(),
            },
            Column {
                name: "value".into(),
                datatype: "bytes".into(),
            },
        ],
        rows: [vec![8, 7], vec![8], vec![]]
            .into_iter()
            .map(|raw| Row {
                cells: vec![
                    Some(Value::Bytes(raw)),
                    Some(Value::Bytes(b"untouched".to_vec())),
                ],
                target: None,
            })
            .collect(),
        ..Page::default()
    };
    bindings.project("events", &mut page, || Ok(())).unwrap();
    assert_eq!(page.columns[2].name, "key_decoded");
    assert_eq!(
        page.rows[0].cells[2],
        Some(Value::Json(r#"{"id":"7"}"#.into()))
    );
    assert!(
        page.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .ends_with(":demo.Event")
    );
    assert!(page.rows[1].cells[2].is_none() && page.rows[1].cells[4].is_some());
    assert_eq!(page.rows[2].cells[2], Some(Value::Json("{}".into())));
    assert_eq!(page.rows[0].cells[0], Some(Value::Bytes(vec![8, 7])));
    for row in page.rows {
        assert_eq!(row.cells[1], Some(Value::Bytes(b"untouched".to_vec())));
    }
}
