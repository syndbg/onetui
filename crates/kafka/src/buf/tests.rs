use super::*;
use crate::decoding::{Binding, Bindings};
use crate::test_server::Server;
use anyhow::ensure;
use onetui_core::{Column, Page, Row, Value};
use prost::Message;
use std::time::{Duration, Instant};

const COMMIT: &str = "0123456789abcdef0123456789abcdef";

fn binding(server: &Server) -> Binding {
    let cfg = serde_json::json!({
        "topic": "events", "field": "value", "format": "protobuf", "framing": "raw", "message_name": "demo.Event",
        "buf": {"url": server.url, "module": "demo/events", "revision": COMMIT, "ca_file": server.ca_file}
    });
    let binding: Binding = serde_json::from_value(cfg).unwrap();
    crate::decoding::validate(std::slice::from_ref(&binding)).unwrap();
    binding
}

fn descriptors() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("common.proto"),
        "syntax='proto3'; package demo; message Child {int32 id=1;}",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("event.proto"),
        "syntax='proto3'; package demo; import 'common.proto'; message Event {Child child=1;}",
    )
    .unwrap();
    protox::compile([dir.path().join("event.proto")], [dir.path()])
        .unwrap()
        .encode_to_vec()
}

fn page() -> Page {
    Page {
        columns: vec![Column {
            name: "value".into(),
            datatype: "bytes".into(),
        }],
        rows: [Some(vec![10, 2, 8, 7]), Some(vec![255]), Some(vec![]), None]
            .into_iter()
            .map(|raw| Row {
                cells: vec![raw.map(Value::Bytes)],
                target: None,
            })
            .collect(),
        ..Page::default()
    }
}

#[test]
fn pinned_imports_projection_cache_reopen_and_raw_errors() {
    let bytes = descriptors();
    let server = Server::start_bytes(false, move |request| {
        assert_eq!(
            request.split_whitespace().nth(1).unwrap(),
            format!("/demo/events/descriptor/{COMMIT}")
        );
        assert!(!request.to_ascii_lowercase().contains("authorization:"));
        (200, bytes.clone())
    });
    let cfg = binding(&server);
    let mut bindings = Bindings::new(vec![cfg.clone()]);
    bindings.check(|| Ok(())).unwrap();
    let mut displayed = page();
    bindings
        .project("events", &mut displayed, || Ok(()))
        .unwrap();
    assert_eq!(displayed.rows[0].cells[0], page().rows[0].cells[0]);
    assert_eq!(
        displayed.rows[0].cells[1],
        Some(Value::Json(r#"{"child":{"id":7}}"#.into()))
    );
    let identity = displayed.rows[0].cells[2].as_ref().unwrap().text().unwrap();
    assert!(
        identity.contains(&format!("/demo/events#commit={COMMIT}:protobuf:sha256:"))
            && identity.ends_with(":demo.Event"),
        "{identity}"
    );
    assert!(displayed.rows[1].cells[1].is_none() && displayed.rows[1].cells[3].is_some());
    assert_eq!(displayed.rows[2].cells[1], Some(Value::Json("{}".into())));
    assert!(displayed.rows[3].cells[1].is_none() && displayed.rows[3].cells[3].is_none());
    bindings.check(|| Ok(())).unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 1);
    Bindings::new(vec![cfg.clone()]).check(|| Ok(())).unwrap();
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    let mut wrong = cfg;
    wrong.message_name = Some("Other".into());
    assert!(Bindings::new(vec![wrong]).check(|| Ok(())).is_err());

    let mut missing = prost_types::FileDescriptorSet::decode(descriptors().as_slice()).unwrap();
    missing
        .file
        .retain(|file| file.name.as_deref() != Some("common.proto"));
    let server = Server::start_bytes(false, move |_| (200, missing.encode_to_vec()));
    assert!(
        Bindings::new(vec![binding(&server)])
            .check(|| Ok(()))
            .is_err()
    );
}

#[test]
fn private_token_tls_redaction_and_binding_isolation() {
    let bytes = descriptors();
    let server = Server::start_bytes(true, move |request| {
        if request
            .to_ascii_lowercase()
            .contains("authorization: bearer private-buf-token")
        {
            (200, bytes.clone())
        } else {
            (401, b"private module denied".to_vec())
        }
    });
    let mut cfg = binding(&server);
    cfg.buf.as_mut().unwrap().token_env = Some("BUF_TOKEN".into());
    let secrets = cfg
        .buf
        .as_mut()
        .unwrap()
        .resolve(&|name| {
            assert_eq!(name, "BUF_TOKEN");
            Some("private-buf-token".into())
        })
        .unwrap();
    assert!(secrets.contains(&"private-buf-token".into()));
    Bindings::new(vec![cfg.clone()]).check(|| Ok(())).unwrap();
    assert!(
        Bindings::new(vec![binding(&server)])
            .check(|| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("401")
    );
    let remote = cfg.buf.as_mut().unwrap();
    remote.ca_file = None;
    remote
        .resolve(&|_| Some("private-buf-token".into()))
        .unwrap();
    assert!(Bindings::new(vec![cfg]).check(|| Ok(())).is_err());
    let denied = Server::start(true, |_| {
        (
            403,
            "Denied private-buf-token Bearer private-buf-token".into(),
        )
    });
    let mut cfg = binding(&denied);
    let remote = cfg.buf.as_mut().unwrap();
    remote.token_env = Some("BUF_TOKEN".into());
    remote
        .resolve(&|_| Some("private-buf-token".into()))
        .unwrap();
    let mut bindings = Bindings::new(vec![cfg]);
    let mut displayed = page();
    bindings
        .project("events", &mut displayed, || Ok(()))
        .unwrap();
    let error = displayed.rows[0].cells[3].as_ref().unwrap().text().unwrap();
    assert!(
        error.contains("403") && error.contains("Denied") && !error.contains("private-buf-token"),
        "{error}"
    );
    assert_eq!(displayed.rows[0].cells[0], page().rows[0].cells[0]);
    assert_eq!(denied.requests.lock().unwrap().len(), 1);
}

#[test]
fn response_bounds_redirect_timeout_cancellation_and_failure_cache() {
    for (status, body) in [
        (302, b"redirect".to_vec()),
        (404, b"commit not found".to_vec()),
        (200, vec![0; 262145]),
        (200, vec![255]),
    ] {
        let server = Server::start_bytes(false, move |_| (status, body.clone()));
        let mut bindings = Bindings::new(vec![binding(&server)]);
        assert!(bindings.check(|| Ok(())).is_err());
        assert!(bindings.check(|| Ok(())).is_err());
        let mut displayed = page();
        bindings
            .project("events", &mut displayed, || Ok(()))
            .unwrap();
        assert_eq!(displayed.rows[0].cells[0], page().rows[0].cells[0]);
        assert!(displayed.rows[0].cells[1].is_none() && displayed.rows[0].cells[3].is_some());
        assert!(
            displayed.rows[0].cells[2]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains(&format!("#commit={COMMIT}:message=demo.Event"))
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    let bytes = descriptors();
    let server = Server::start_bytes(false, move |_| (200, bytes.clone()));
    let mut bindings = Bindings::new(vec![binding(&server)]);
    assert!(bindings.check(|| anyhow::bail!("cancelled")).is_err());
    assert!(server.requests.lock().unwrap().is_empty());
    bindings.check(|| Ok(())).unwrap();
    let stalled = Server::start(false, |_| {
        std::thread::sleep(Duration::from_millis(2400));
        (200, String::new())
    });
    let started = Instant::now();
    assert!(
        Bindings::new(vec![binding(&stalled)])
            .check(|| Ok(()))
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_millis(2300));
}

#[test]
fn configuration_is_offline_strict_and_validates_commit_ids() {
    let valid =
        serde_json::json!({"url":"https://buf.build", "module":"demo/events", "revision":COMMIT});
    serde_json::from_value::<Config>(valid.clone())
        .unwrap()
        .validate()
        .unwrap();
    for (key, value) in [
        ("revision", "main"),
        ("revision", "latest"),
        ("revision", ""),
        ("revision", "0123456789ABCDEF0123456789ABCDEF"),
        ("module", "../events"),
        ("module", "demo/events/extra"),
        ("module", "demo/%2e%2e"),
        ("module", "demo/-events"),
        ("url", "http://buf.build"),
        ("url", "https://buf.build/path"),
        ("url", "https://user:secret@buf.build"),
        ("url", "https://buf.build?secret=value"),
        ("token_env", "not a variable"),
    ] {
        let mut invalid = valid.clone();
        invalid[key] = value.into();
        assert!(
            serde_json::from_value::<Config>(invalid)
                .unwrap()
                .validate()
                .is_err(),
            "{key}={value}"
        );
    }
    for unknown in ["authorization", "remote", "username_env", "imports"] {
        let mut invalid = valid.clone();
        invalid[unknown] = "unaccepted".into();
        assert!(serde_json::from_value::<Config>(invalid).is_err());
    }
    let cfg = serde_json::json!({"topic":"events", "field":"value", "format":"protobuf", "framing":"raw", "message_name":"demo.Event", "buf": valid});
    let parsed: Binding = serde_json::from_value(cfg.clone()).unwrap();
    crate::decoding::validate(&[parsed]).unwrap();
    for (key, value) in [
        ("format", serde_json::json!("avro")),
        ("framing", serde_json::json!("confluent")),
        ("message_name", serde_json::Value::Null),
        ("schema_file", serde_json::json!("/tmp/event.pb")),
        (
            "catalog",
            serde_json::json!({"directory":"/tmp", "schema":"event"}),
        ),
        (
            "registry",
            serde_json::json!({"url":"https://registry.test"}),
        ),
    ] {
        let mut invalid = cfg.clone();
        invalid[key] = value;
        let parsed: Binding = serde_json::from_value(invalid).unwrap();
        assert!(crate::decoding::validate(&[parsed]).is_err(), "{key}");
    }
}

#[test]
fn moving_labels_pin_before_fetch_and_reopen_without_reinterpreting_rows() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = resolutions.clone();
    let first = descriptors();
    let mut second = prost_types::FileDescriptorSet::decode(first.as_slice()).unwrap();
    let field = &mut second
        .file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some("event.proto"))
        .unwrap()
        .message_type[0]
        .field[0];
    field.name = Some("renamed".into());
    field.json_name = None;
    const NEXT: &str = "1123456789abcdef0123456789abcdef";
    let server = Server::start_bytes(true, move |request| {
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer label-token")
        );
        let path = request.split_whitespace().nth(1).unwrap();
        if path == "/buf.registry.module.v1.CommitService/GetCommits" {
            assert!(request.starts_with("POST "));
            let body: serde_json::Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(
                body,
                serde_json::json!({"resourceRefs":[{"name":{"owner":"demo","module":"events","labelName":"release/v1"}}]})
            );
            let revision = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                COMMIT
            } else {
                NEXT
            };
            (
                200,
                serde_json::json!({"commits":[{"id":revision}]})
                    .to_string()
                    .into_bytes(),
            )
        } else if path == format!("/demo/events/descriptor/{COMMIT}") {
            // The label already points elsewhere; the descriptor request must still use COMMIT.
            (200, first.clone())
        } else {
            assert_eq!(path, format!("/demo/events/descriptor/{NEXT}"));
            (200, second.encode_to_vec())
        }
    });
    let mut cfg = binding(&server);
    let buf = cfg.buf.as_mut().unwrap();
    buf.revision = None;
    buf.label = Some("release/v1".into());
    buf.token_env = Some("BUF_TOKEN".into());
    buf.resolve(&|_| Some("label-token".into())).unwrap();
    let mut bindings = Bindings::new(vec![cfg.clone()]);
    let mut original = page();
    bindings
        .project("events", &mut original, || Ok(()))
        .unwrap();
    assert_eq!(
        original.rows[0].cells[1],
        Some(Value::Json(r#"{"child":{"id":7}}"#.into()))
    );
    let identity = original.rows[0].cells[2].clone();
    assert!(identity.as_ref().unwrap().text().unwrap().contains(COMMIT));
    bindings.check(|| Ok(())).unwrap();
    let mut refreshed = page();
    bindings
        .project("events", &mut refreshed, || Ok(()))
        .unwrap();
    assert_eq!(refreshed.rows[0].cells, original.rows[0].cells);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    let mut reopened = Bindings::new(vec![cfg]);
    let mut changed = page();
    reopened.project("events", &mut changed, || Ok(())).unwrap();
    assert_eq!(
        changed.rows[0].cells[1],
        Some(Value::Json(r#"{"renamed":{"id":7}}"#.into()))
    );
    assert!(
        changed.rows[0].cells[2]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains(NEXT)
    );
    assert_eq!(original.rows[0].cells[2], identity);
    assert_eq!(resolutions.load(Ordering::SeqCst), 2);
    assert_eq!(server.requests.lock().unwrap().len(), 4);
}

#[test]
fn label_failures_are_bounded_cached_and_never_fetch_an_unresolved_reference() {
    for body in [
        r#"{"commits":[]}"#.to_owned(),
        r#"{"commits":[{"id":"main"}]}"#.into(),
        serde_json::json!({"commits":[{"id":COMMIT},{"id":COMMIT}]}).to_string(),
        "x".repeat(262145),
    ] {
        let server = Server::start(false, move |request| {
            assert!(request.starts_with("POST /buf.registry.module.v1.CommitService/GetCommits "));
            (200, body.clone())
        });
        let mut cfg = binding(&server);
        let buf = cfg.buf.as_mut().unwrap();
        buf.revision = None;
        buf.label = Some("main".into());
        let mut bindings = Bindings::new(vec![cfg]);
        let mut displayed = page();
        bindings
            .project("events", &mut displayed, || Ok(()))
            .unwrap();
        assert!(displayed.rows[0].cells[1].is_none() && displayed.rows[0].cells[3].is_some());
        assert!(
            displayed.rows[0].cells[2]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains("#label=main:")
        );
        assert_eq!(displayed.rows[0].cells[0], page().rows[0].cells[0]);
        assert!(bindings.check(|| Ok(())).is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    let server = Server::start(false, move |_| {
        (
            200,
            serde_json::json!({"commits":[{"id":COMMIT}]}).to_string(),
        )
    });
    let mut cfg = binding(&server).buf.unwrap();
    cfg.revision = None;
    cfg.label = Some("main".into());
    let result = cfg.load(&|| {
        ensure!(
            server.requests.lock().unwrap().is_empty(),
            "cancelled after label resolution"
        );
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(server.requests.lock().unwrap().len(), 1);

    let server = Server::start(false, |request| {
        if request.starts_with("POST ") {
            (
                200,
                serde_json::json!({"commits":[{"id":COMMIT}]}).to_string(),
            )
        } else {
            (404, "descriptor unavailable".into())
        }
    });
    let mut cfg = binding(&server);
    cfg.buf.as_mut().unwrap().revision = None;
    cfg.buf.as_mut().unwrap().label = Some("main".into());
    let mut displayed = page();
    Bindings::new(vec![cfg])
        .project("events", &mut displayed, || Ok(()))
        .unwrap();
    assert!(
        displayed.rows[0].cells[2]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains(COMMIT)
    );
    assert!(
        displayed.rows[0].cells[3]
            .as_ref()
            .unwrap()
            .text()
            .unwrap()
            .contains("descriptor unavailable")
    );
    assert_eq!(server.requests.lock().unwrap().len(), 2);
}

#[test]
fn labels_are_explicit_mutually_exclusive_and_offline() {
    let valid = serde_json::json!({"url":"https://buf.build", "module":"demo/events", "label":"release/v1.0"});
    serde_json::from_value::<Config>(valid.clone())
        .unwrap()
        .validate()
        .unwrap();
    let mut both = valid.clone();
    both["revision"] = COMMIT.into();
    assert!(
        serde_json::from_value::<Config>(both)
            .unwrap()
            .validate()
            .is_err()
    );
    for label in [
        "".to_owned(),
        "x".repeat(251),
        "has space".into(),
        "bad\nlabel".into(),
        "label?query".into(),
        "label#fragment".into(),
    ] {
        let mut invalid = valid.clone();
        invalid["label"] = label.into();
        assert!(
            serde_json::from_value::<Config>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut neither = valid;
    neither.as_object_mut().unwrap().remove("label");
    assert!(
        serde_json::from_value::<Config>(neither)
            .unwrap()
            .validate()
            .is_err()
    );
}

#[test]
fn label_and_descriptor_requests_share_one_deadline() {
    let bytes = descriptors();
    let server = Server::start_bytes(false, move |request| {
        std::thread::sleep(Duration::from_millis(1200));
        if request.starts_with("POST ") {
            (
                200,
                serde_json::json!({"commits":[{"id":COMMIT}]})
                    .to_string()
                    .into_bytes(),
            )
        } else {
            (200, bytes.clone())
        }
    });
    let mut config = binding(&server).buf.unwrap();
    config.revision = None;
    config.label = Some("main".into());
    let started = Instant::now();
    assert!(config.load(&|| Ok(())).is_err());
    assert!(started.elapsed() < Duration::from_millis(2300));
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    assert!(config.identity().contains(COMMIT));
}

#[test]
#[ignore = "read-only public buf.build check; requires Internet, run explicitly"]
fn hosted_buf_label_and_pinned_commit_decode_the_same_message() {
    let mut config: Config = serde_json::from_value(
        serde_json::json!({"url":"https://buf.build", "module":"connectrpc/eliza", "label":"main"}),
    )
    .unwrap();
    config
        .resolve(&|_| panic!("public module needs no credentials"))
        .unwrap();
    let bytes = config.load(&|| Ok(())).unwrap();
    let identity = config.identity();
    let commit = identity.split_once("#commit=").unwrap().1.to_owned();
    assert!(commit.len() == 32 && commit.bytes().all(|b| b.is_ascii_hexdigit()));
    let decoder = onetui_protobuf::Decoder::new(&bytes, "connectrpc.eliza.v1.SayRequest").unwrap();
    let json = decoder.decode(b"\x0a\x05hello").unwrap().json().unwrap();
    assert_eq!(json, r#"{"sentence":"hello"}"#);
    config.label = None;
    config.revision = Some(commit);
    let pinned = config.load(&|| Ok(())).unwrap();
    assert_eq!(bytes, pinned);
    assert_eq!(identity, config.identity());
    eprintln!("{identity} ({} descriptor bytes): {json}", bytes.len());
}
