use super::*;
use onetui_core::Row;
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

#[test]
fn protobuf_key_binding_uses_exact_message_and_preserves_raw_value() {
    protobuf_binding(false);
}

#[cfg(unix)]
#[test]
fn protobuf_catalog_uses_exact_message_and_preserves_raw_value() {
    protobuf_binding(true);
}

fn event_schema() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("event.proto".into()),
                package: Some("demo".into()),
                syntax: Some("proto3".into()),
                dependency: vec!["common.proto".into()],
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
            },
            FileDescriptorProto {
                name: Some("common.proto".into()),
                syntax: Some("proto3".into()),
                ..Default::default()
            },
        ],
    }
    .encode_to_vec()
}

fn event_binding(catalog: bool, path: &std::path::Path, dir: &std::path::Path) -> Binding {
    let mut binding = Binding {
        reader_schema_file: None,
        topic: "events".into(),
        field: Field::Key,
        format: Format::Protobuf,
        framing: Framing::Raw,
        schema_file: Some(path.to_str().unwrap().into()),
        message_name: Some("demo.Event".into()),
        registry: None,
        catalog: None,
        buf: None,
    };
    if catalog {
        binding.schema_file = None;
        binding.catalog = Some(crate::catalog::Config {
            directory: dir.to_str().unwrap().into(),
            schema: "event".into(),
            references: vec![],
        });
    }
    validate(std::slice::from_ref(&binding)).unwrap();
    let mut invalid = binding.clone();
    invalid.message_name = None;
    assert!(validate(&[invalid]).is_err());
    let mut wrong = binding.clone();
    wrong.message_name = Some("Event".into());
    assert!(Bindings::new(vec![wrong]).check(|| Ok(())).is_err());
    binding
}

fn raw_page() -> Page {
    Page {
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
    }
}

fn assert_binding_validation(binding: &Binding) {
    validate(std::slice::from_ref(binding)).unwrap();
    let mut invalid = binding.clone();
    invalid.message_name = None;
    assert!(validate(&[invalid]).is_err());
    let mut wrong = binding.clone();
    wrong.message_name = Some("Event".into());
    assert!(Bindings::new(vec![wrong]).check(|| Ok(())).is_err());
}

fn assert_projection(page: &Page) {
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
    for row in &page.rows {
        assert_eq!(row.cells[1], Some(Value::Bytes(b"untouched".to_vec())));
    }
}

fn assert_catalog_survives_import_removal(
    page: &Page,
    bindings: &mut Bindings,
    schema: &[u8],
    path: &std::path::Path,
) {
    assert!(
        page.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#schema=event:protobuf:sha256:")
    );
    let mut missing_import = FileDescriptorSet::decode(schema).unwrap();
    missing_import.file.pop();
    std::fs::write(path, missing_import.encode_to_vec()).unwrap();
    bindings.check(|| Ok(())).unwrap();
    let mut reopened = Bindings::new(vec![bindings.0[0].binding.clone()]);
    assert!(reopened.check(|| Ok(())).is_err());
    assert_eq!(
        page.rows[0].cells[2],
        Some(Value::Json(r#"{"id":"7"}"#.into()))
    );
}

fn protobuf_binding(catalog: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("event.pb");
    let schema = event_schema();
    std::fs::write(&path, &schema).unwrap();
    let binding = event_binding(catalog, &path, dir.path());
    assert_binding_validation(&binding);
    let mut bindings = Bindings::new(vec![binding]);
    let mut page = raw_page();
    bindings.project("events", &mut page, || Ok(())).unwrap();
    assert_projection(&page);
    if catalog {
        assert_catalog_survives_import_removal(&page, &mut bindings, &schema, &path);
    }
}
