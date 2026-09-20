use super::*;
use onetui_core::Row;
use std::path::Path;

fn binding(path: &Path) -> Binding {
    Binding {
        topic: "events".into(),
        field: Field::Value,
        format: Format::Avro,
        framing: Framing::Raw,
        schema_file: Some(path.to_str().unwrap().into()),
        reader_schema_file: None,
        message_name: None,
        registry: None,
        catalog: None,
        buf: None,
    }
}

fn page(values: Vec<Option<Value>>) -> Page {
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
        rows: values
            .into_iter()
            .map(|value| Row {
                cells: vec![Some("key".into()), value],
                target: None,
            })
            .collect(),
        continuation: Some("unchanged-position".into()),
        next: true,
        ..Page::default()
    }
}

#[test]
fn avro_binding_is_strict_offline_and_scoped() {
    let catalog = crate::capabilities();
    let settings = &catalog["configuration"]["decoders"];
    assert_eq!(settings["default"], serde_json::json!([]));
    assert_eq!(
        settings["fields"]["framing"]["values"],
        serde_json::json!(["raw", "confluent"])
    );
    assert_eq!(settings["limits"]["schema_bytes_per_binding"], SCHEMA_BYTES);
    assert_eq!(settings["fields"]["catalog"]["limits"]["schemas"], 64);
    let example: toml::Value =
        toml::from_str(include_str!("../../../../hack/kafka-decoders.toml.example")).unwrap();
    let mut options = example["connections"]["local_kafka"]
        .as_table()
        .unwrap()
        .clone();
    options.remove("kind");
    crate::config::Config::parse(&options).unwrap();
    let example: Binding = serde_json::from_value(settings["example"].clone()).unwrap();
    validate(&[example]).unwrap();
    let valid = binding(Path::new("/not-read-during-validation/event.avsc"));
    validate(std::slice::from_ref(&valid)).unwrap();
    assert!(validate(&[valid.clone(), valid.clone()]).is_err());
    assert!(validate(&vec![valid.clone(); 33]).is_err());
    for field in ["schema_file", "topic", "message_name"] {
        let mut invalid = valid.clone();
        match field {
            "schema_file" => invalid.schema_file = Some("relative.avsc".into()),
            "topic" => invalid.topic = "*".into(),
            _ => invalid.message_name = Some("not-avro".into()),
        }
        assert!(validate(&[invalid]).is_err());
    }
    let prefix = "bootstrap_servers=['localhost:9092']\n";
    for bad in [
        "field='header'\nformat='avro'\nframing='raw'",
        "field='value'\nformat='avro'\nframing='confluent'",
        "field='value'\nformat='avro'",
        "field='value'\nformat='avro'\nframing='raw'\nreader_schema='ignored'",
    ] {
        let options = toml::from_str(&format!(
            "{prefix}[[decoders]]\ntopic='events'\nschema_file='/tmp/schema'\n{bad}"
        ))
        .unwrap();
        assert!(crate::config::Config::parse(&options).is_err());
    }
    let mut bindings = Bindings::new(vec![valid]);
    let mut other = page(vec![Some(Value::Bytes(vec![14]))]);
    bindings.project("other", &mut other, || Ok(())).unwrap();
    assert_eq!(other.columns.len(), 2);
    assert_eq!(bindings.raw_page_limit("other"), PAGE_BYTES);
}

#[test]
fn avro_projection_retains_raw_null_errors_and_session_schema() {
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("event.avsc");
    std::fs::write(&schema, r#""long""#).unwrap();
    let mut bindings = Bindings::new(vec![binding(&schema)]);
    let mut data = page(vec![
        Some(Value::Bytes(vec![14])),
        None,
        Some(Value::Bytes(vec![255])),
        Some(Value::Bytes(vec![])),
        Some(Value::Bytes(vec![16])),
    ]);
    let original = data
        .rows
        .iter()
        .map(|r| r.cells.clone())
        .collect::<Vec<_>>();
    bindings.project("events", &mut data, || Ok(())).unwrap();
    for (row, original) in data.rows.iter().zip(original) {
        assert_eq!(row.cells[..2], original);
    }
    assert_eq!(data.rows[0].cells[2], Some(Value::Json("7".into())));
    let native: serde_json::Value =
        serde_json::from_slice(data.rows[0].cells[5].as_ref().unwrap().bytes()).unwrap();
    assert_eq!(native, serde_json::json!({"type":"long","value":7}));
    assert!(data.rows[0].cells[6].is_none());
    assert!(
        data.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .starts_with("avro:sha256:")
    );
    assert!(data.rows[0].cells[4].is_none());
    assert!(data.rows[1].cells[2].is_none() && data.rows[1].cells[4].is_none());
    assert!(data.rows[2].cells[4].is_some() && data.rows[3].cells[4].is_some());
    assert_eq!(data.rows[4].cells[2], Some(Value::Json("8".into())));
    assert_eq!(data.continuation.as_deref(), Some("unchanged-position"));
    assert!(data.next);
    std::fs::write(&schema, r#""string""#).unwrap();
    let mut again = page(vec![Some(Value::Bytes(vec![14]))]);
    bindings.project("events", &mut again, || Ok(())).unwrap();
    assert_eq!(again.rows[0].cells[2], Some(Value::Json("7".into())));
    assert_eq!(again.rows[0].cells[3], data.rows[0].cells[3]);
    assert!(bindings.check(|| anyhow::bail!("cancelled")).is_err());
}

#[test]
fn key_projection_precedes_value_projection_regardless_of_config_order() {
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("event.avsc");
    std::fs::write(&schema, r#""long""#).unwrap();
    let value = binding(&schema);
    let mut key = value.clone();
    key.field = Field::Key;
    let mut bindings = Bindings::new(vec![value, key]);
    let mut data = page(vec![Some(Value::Bytes(vec![16]))]);
    data.rows[0].cells[0] = Some(Value::Bytes(vec![14]));

    bindings.project("events", &mut data, || Ok(())).unwrap();

    assert_eq!(data.columns[2].name, "key_decoded");
    assert_eq!(data.columns[7].name, "value_decoded");
    assert_eq!(data.rows[0].cells[2], Some(Value::Json("7".into())));
    assert_eq!(data.rows[0].cells[7], Some(Value::Json("8".into())));
}

#[test]
fn avro_missing_files_limits_and_live_columns_are_stable() {
    let dir = tempfile::tempdir().unwrap();
    let schema = dir.path().join("event.avsc");
    let mut missing = Bindings::new(vec![binding(&schema)]);
    assert!(missing.check(|| Ok(())).is_err());
    std::fs::write(&schema, r#""string""#).unwrap();
    assert!(missing.check(|| Ok(())).is_err()); // Cached failure; a new session reloads.
    let mut bindings = Bindings::new(vec![binding(&schema)]);
    bindings.check(|| Ok(())).unwrap();
    let mut small = page(vec![Some(Value::Bytes(vec![0]))]);
    bindings.project("events", &mut small, || Ok(())).unwrap();
    assert_eq!(small.rows[0].cells[2], Some(Value::Json("\"\"".into())));
    let mut large = page(vec![Some(Value::Bytes(vec![
        0;
        bindings
            .raw_page_limit("events")
            - 100
    ]))]);
    bindings.project("events", &mut large, || Ok(())).unwrap();
    assert_eq!(
        small.columns.iter().map(|c| &c.name).collect::<Vec<_>>(),
        large.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
    assert!(large.bytes() <= PAGE_BYTES);
    assert!(large.rows[0].cells[4].is_some() || large.notice.contains("omitted"));
    let mut full = page(vec![Some(Value::Bytes(vec![0; 10_000])); 100]);
    let raw = full
        .rows
        .iter()
        .map(|r| r.cells.clone())
        .collect::<Vec<_>>();
    assert!(full.bytes() <= bindings.raw_page_limit("events"));
    bindings.project("events", &mut full, || Ok(())).unwrap();
    assert!(full.notice.contains("Decoder previews omitted"));
    assert!(full.bytes() <= PAGE_BYTES);
    assert_eq!(full.continuation.as_deref(), Some("unchanged-position"));
    assert!(full.next);
    assert_eq!(
        small
            .columns
            .iter()
            .map(|c| (&c.name, &c.datatype))
            .collect::<Vec<_>>(),
        full.columns
            .iter()
            .map(|c| (&c.name, &c.datatype))
            .collect::<Vec<_>>()
    );
    for (row, original) in full.rows.iter().zip(raw) {
        assert_eq!(row.cells[..2], original);
        assert!(row.cells[2..].iter().all(Option::is_none));
    }
    std::fs::write(&schema, vec![0; SCHEMA_BYTES + 1]).unwrap();
    assert!(
        Bindings::new(vec![binding(&schema)])
            .check(|| Ok(()))
            .is_err()
    );
    assert!(
        Bindings::new(vec![binding(dir.path())])
            .check(|| Ok(()))
            .is_err()
    );
    #[cfg(unix)]
    {
        use nix::{sys::stat::Mode, unistd::mkfifo};
        let fifo = dir.path().join("fifo");
        mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        assert!(
            Bindings::new(vec![binding(&fifo)])
                .check(|| Ok(()))
                .is_err()
        );
    }
}

#[cfg(unix)]
#[test]
fn avro_catalog_resolves_references_and_reloads_only_when_reopened() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("event.avsc");
    let child = dir.path().join("child.avsc");
    std::fs::write(
        &root,
        r#"{"type":"record","name":"Event","fields":[{"name":"child","type":"Child"}]}"#,
    )
    .unwrap();
    let child_schema = r#"{"type":"record","name":"Child","fields":[{"name":"id","type":"long"}]}"#;
    std::fs::write(&child, child_schema).unwrap();
    let mut configured = binding(&root);
    configured.schema_file = None;
    configured.catalog = Some(crate::catalog::Config {
        directory: dir.path().to_str().unwrap().into(),
        schema: "event".into(),
        references: vec!["child".into()],
    });
    validate(std::slice::from_ref(&configured)).unwrap();
    let mut invalid = configured.clone();
    invalid.schema_file = Some(root.to_str().unwrap().into());
    assert!(validate(&[invalid]).is_err());
    let mut invalid = configured.clone();
    invalid.framing = Framing::Confluent;
    assert!(validate(&[invalid]).is_err());
    let mut bindings = Bindings::new(vec![configured.clone()]);
    bindings.check(|| Ok(())).unwrap();
    let mut data = page(vec![
        Some(Value::Bytes(vec![14])),
        Some(Value::Bytes(vec![255])),
        None,
    ]);
    bindings.project("events", &mut data, || Ok(())).unwrap();
    assert_eq!(
        data.rows[0].cells[2],
        Some(Value::Json(r#"{"child":{"id":7}}"#.into()))
    );
    assert_eq!(data.rows[0].cells[1], Some(Value::Bytes(vec![14])));
    assert!(
        data.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("#schema=event:avro:sha256:")
    );
    assert!(data.rows[1].cells[4].is_some());
    assert!(data.rows[2].cells[2].is_none());
    std::fs::write(&child, child_schema.replace("\"id\"", "\"renamed\"")).unwrap();
    let mut again = page(vec![Some(Value::Bytes(vec![14]))]);
    bindings.project("events", &mut again, || Ok(())).unwrap();
    assert_eq!(again.rows[0].cells, data.rows[0].cells);
    let mut reopened = Bindings::new(vec![configured.clone()]);
    let mut reloaded = page(vec![Some(Value::Bytes(vec![14]))]);
    reopened
        .project("events", &mut reloaded, || Ok(()))
        .unwrap();
    assert_eq!(
        reloaded.rows[0].cells[2],
        Some(Value::Json(r#"{"child":{"renamed":7}}"#.into()))
    );
    assert_ne!(reloaded.rows[0].cells[3], data.rows[0].cells[3]);
    std::fs::remove_file(&child).unwrap();
    let mut missing = Bindings::new(vec![configured.clone()]);
    assert!(missing.check(|| Ok(())).is_err());
    std::fs::write(&child, child_schema).unwrap();
    assert!(missing.check(|| Ok(())).is_err());
    Bindings::new(vec![configured]).check(|| Ok(())).unwrap();
}

#[test]
fn reader_files_are_explicit_bounded_and_cached_per_binding() {
    let dir = tempfile::tempdir().unwrap();
    let writer = dir.path().join("writer.avsc");
    let reader = dir.path().join("reader.avsc");
    std::fs::write(
        &writer,
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"int"}]}"#,
    )
    .unwrap();
    std::fs::write(&reader, r#"{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"added","type":"string","default":"new"}]}"#).unwrap();
    let mut config = binding(&writer);
    config.reader_schema_file = Some(reader.to_str().unwrap().into());
    validate(std::slice::from_ref(&config)).unwrap();
    let mut invalid = config.clone();
    invalid.format = Format::Protobuf;
    assert!(validate(&[invalid]).is_err());
    let mut invalid = config.clone();
    invalid.reader_schema_file = Some("relative.avsc".into());
    assert!(validate(&[invalid]).is_err());
    let mut bindings = Bindings::new(vec![config.clone()]);
    bindings.check(|| Ok(())).unwrap();
    let mut data = page(vec![Some(Value::Bytes(vec![14]))]);
    bindings.project("events", &mut data, || Ok(())).unwrap();
    assert_eq!(
        data.rows[0].cells[2],
        Some(Value::Json(r#"{"added":"new","id":7}"#.into()))
    );
    assert!(
        data.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("&reader=avro:sha256:")
    );
    let native: serde_json::Value =
        serde_json::from_slice(data.rows[0].cells[5].as_ref().unwrap().bytes()).unwrap();
    assert_eq!(native["writer"]["fields"][0][1]["type"], "int");
    assert_eq!(native["reader"]["fields"][0][1]["type"], "long");
    std::fs::write(&reader, "invalid").unwrap();
    bindings.check(|| Ok(())).unwrap();
    let mut again = page(vec![Some(Value::Bytes(vec![14]))]);
    bindings.project("events", &mut again, || Ok(())).unwrap();
    assert_eq!(again.rows[0].cells, data.rows[0].cells);
    assert!(
        Bindings::new(vec![config.clone()])
            .check(|| Ok(()))
            .is_err()
    );
    std::fs::write(&reader, vec![b'x'; SCHEMA_BYTES + 1]).unwrap();
    assert!(Bindings::new(vec![config]).check(|| Ok(())).is_err());
}
