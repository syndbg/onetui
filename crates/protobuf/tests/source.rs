use onetui_protobuf::{MAX_SCHEMA_BYTES, SourceSchema};

#[test]
fn sources_imports_and_nested_declaration_indexes() {
    let root = r#"syntax = "proto3"; package demo; import "child.proto";
        message First { int32 id = 1; }
        message Outer { message Inner { Child child = 1; } }"#;
    let schema = SourceSchema::compile(
        root,
        &[(
            "child.proto",
            r#"syntax = "proto3"; package demo; message Child { string name = 1; }"#,
        )],
    )
    .unwrap();
    assert_eq!(
        schema
            .decoder(&[0])
            .unwrap()
            .decode(&[8, 7])
            .unwrap()
            .json()
            .unwrap(),
        r#"{"id":7}"#
    );
    let decoder = schema.decoder(&[1, 0]).unwrap();
    assert!(decoder.schema_id().ends_with(":demo.Outer.Inner"));
    let raw = [10, 4, 10, 2, b'H', b'i'];
    let decoded = decoder.decode(&raw).unwrap();
    assert_eq!(decoded.raw(), raw);
    assert_eq!(decoded.json().unwrap(), r#"{"child":{"name":"Hi"}}"#);
    for indexes in [&[][..], &[2], &[1, 1], &[0, 0], &[0; 33]] {
        assert!(schema.decoder(indexes).is_err());
    }
}

#[test]
fn source_bounds_and_no_ambient_imports() {
    let empty = "syntax = \"proto3\"; message Empty {}";
    assert!(SourceSchema::compile(&"x".repeat(MAX_SCHEMA_BYTES + 1), &[]).is_err());
    assert!(SourceSchema::compile(empty, &vec![("x.proto", empty); 33]).is_err());
    for name in [
        "../x.proto",
        "/x.proto",
        "x//y.proto",
        "x\\y.proto",
        "__onetui_root.proto",
        "google/protobuf/any.proto",
    ] {
        assert!(
            SourceSchema::compile(empty, &[(name, empty)]).is_err(),
            "{name}"
        );
    }
    assert!(SourceSchema::compile(empty, &[("x.proto", empty), ("x.proto", empty)]).is_err());
    assert!(SourceSchema::compile("import \"/etc/passwd\"; message Empty {}", &[]).is_err());
    assert!(SourceSchema::compile("import \"missing.proto\"; message Empty {}", &[]).is_err());
    assert!(
        SourceSchema::compile(
            "import \"cycle.proto\"; message Empty {}",
            &[("cycle.proto", "import \"cycle.proto\";")]
        )
        .is_err()
    );
    assert!(
        SourceSchema::compile(
            &format!("{}{}", "message M {".repeat(40), "}".repeat(40)),
            &[]
        )
        .is_err()
    );
    assert!(
        SourceSchema::compile(
            &format!(
                "message M {{ {} }}",
                (1..5000)
                    .map(|i| format!("optional int32 f{i} = {i};"))
                    .collect::<String>()
            ),
            &[]
        )
        .is_err()
    );
    assert!(SourceSchema::compile("message {", &[]).is_err());
    let harmless = format!("/* {} */ // {}\n{empty}", "{".repeat(50), "}".repeat(50));
    assert!(SourceSchema::compile(&harmless, &[]).is_ok());
    assert!(SourceSchema::compile(r#"syntax="proto3"; import "google/protobuf/timestamp.proto"; message Event { google.protobuf.Timestamp at=1; }"#, &[]).is_ok());
}
