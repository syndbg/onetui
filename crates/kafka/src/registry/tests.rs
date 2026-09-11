use super::*;
#[path = "../../tests/support/registry.rs"]
mod server;
use server::Server;

fn config(server: &Server) -> Config {
    Config {
        url: server.url.clone(),
        ca_file: server.ca_file.clone(),
        username_env: None,
        password_env: None,
        token_env: None,
        authorization: None,
        secrets: vec![],
    }
}

fn envelope(id: u32, payload: &[u8]) -> Vec<u8> {
    [vec![0], id.to_be_bytes().to_vec(), payload.to_vec()].concat()
}

#[test]
fn exact_ids_references_cache_and_errors() {
    let server = Server::start(false, |request| {
        let path = request.split_whitespace().nth(1).unwrap();
        let body = match path {
            "/schemas/types" => serde_json::json!(["AVRO"]),
            "/schemas/ids/1" => serde_json::json!({"schema": "\"long\""}),
            "/schemas/ids/2" => serde_json::json!({"schema": "\"string\""}),
            "/schemas/ids/3" => {
                serde_json::json!({"schema": r#"{"type":"record","name":"Root","fields":[{"name":"child","type":"Child"}]}"#, "references":[{"name":"Child","subject":"child/name","version":7}]})
            }
            "/subjects/child%2Fname/versions/7" => {
                serde_json::json!({"schema": r#"{"type":"record","name":"Child","fields":[{"name":"id","type":"long"}]}"#})
            }
            _ => {
                return (
                    404,
                    "{\"error_code\":40403,\"message\":\"Schema not found\"}".into(),
                );
            }
        };
        (200, body.to_string())
    });
    let mut registry = Registry::new(config(&server), Format::Avro).unwrap();
    registry.check(&|| Ok(())).unwrap();
    assert_eq!(
        registry.preview(&envelope(1, &[14]), &|| Ok(())).1.unwrap(),
        "7"
    );
    assert_eq!(
        registry
            .preview(&envelope(2, &[2, b'a']), &|| Ok(()))
            .1
            .unwrap(),
        "\"a\""
    );
    assert_eq!(
        registry.preview(&envelope(3, &[14]), &|| Ok(())).1.unwrap(),
        r#"{"child":{"id":7}}"#
    );
    let before = server.requests.lock().unwrap().len();
    assert_eq!(
        registry.preview(&envelope(1, &[16]), &|| Ok(())).1.unwrap(),
        "8"
    );
    assert_eq!(server.requests.lock().unwrap().len(), before);
    assert!(
        registry
            .preview(&envelope(1, &[255]), &|| Ok(()))
            .1
            .is_err()
    );
    assert!(registry.preview(&[0, 1], &|| Ok(())).1.is_err());
    assert!(registry.preview(&[1, 0, 0, 0, 1], &|| Ok(())).1.is_err());
    let missing = registry.preview(&envelope(4, &[14]), &|| Ok(()));
    assert!(missing.0.unwrap().ends_with("#id=4"));
    assert!(missing.1.unwrap_err().to_string().contains("40403"));
    let before = server.requests.lock().unwrap().len();
    assert!(registry.preview(&envelope(4, &[14]), &|| Ok(())).1.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), before);
    for id in 5..16 {
        let _ = registry.preview(&envelope(id, &[14]), &|| Ok(()));
    }
    assert_eq!(registry.cache.len(), CACHE_ENTRIES);
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.contains("latest"))
    );
}

#[test]
fn tls_auth_isolation_redaction_redirects_and_bounds() {
    let server = Server::start(true, |request| {
        assert!(
            request.to_ascii_lowercase().contains(
                "authorization: basic dXNlcjpmaXh0dXJl"
                    .to_ascii_lowercase()
                    .as_str()
            )
        );
        (401, "{\"message\":\"fixture dXNlcjpmaXh0dXJl\"}".into())
    });
    let mut cfg = config(&server);
    cfg.username_env = Some("REGISTRY_USER".into());
    cfg.password_env = Some("REGISTRY_PASSWORD".into());
    cfg.validate().unwrap();
    cfg.resolve(&|name| {
        Some(
            if name == "REGISTRY_USER" {
                "user"
            } else {
                "fixture"
            }
            .into(),
        )
    })
    .unwrap();
    let mut registry = Registry::new(cfg.clone(), Format::Avro).unwrap();
    let error = registry
        .preview(&envelope(1, &[14]), &|| Ok(()))
        .1
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("401") && !error.contains("fixture") && !error.contains("dXNlcjpmaXh0dXJl"),
        "{error}"
    );
    cfg.ca_file = None;
    assert!(
        Registry::new(cfg, Format::Avro)
            .unwrap()
            .check(&|| Ok(()))
            .is_err()
    );
    for (status, body) in [
        (302, "redirect".into()),
        (200, "x".repeat(RESPONSE_BYTES + 1)),
    ] {
        let server = Server::start(false, move |_| (status, body.clone()));
        let mut registry = Registry::new(config(&server), Format::Avro).unwrap();
        assert!(registry.preview(&envelope(1, &[14]), &|| Ok(())).1.is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
    let server = Server::start(false, |_| {
        (200, serde_json::json!({"schema":"\"long\""}).to_string())
    });
    let mut registry = Registry::new(config(&server), Format::Avro).unwrap();
    assert!(
        registry
            .preview(&envelope(1, &[14]), &|| anyhow::bail!("cancelled"))
            .1
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(
        registry.preview(&envelope(1, &[14]), &|| Ok(())).1.unwrap(),
        "7"
    );
    let other = Server::start(false, |_| {
        (200, serde_json::json!({"schema":"\"string\""}).to_string())
    });
    let mut isolated = Registry::new(config(&other), Format::Avro).unwrap();
    assert_eq!(
        isolated
            .preview(&envelope(1, &[2, b'a']), &|| Ok(()))
            .1
            .unwrap(),
        "\"a\""
    );
    assert_eq!(other.requests.lock().unwrap().len(), 1);
    let before = server.requests.lock().unwrap().len();
    let mut reopened = Registry::new(config(&server), Format::Avro).unwrap();
    assert_eq!(
        reopened.preview(&envelope(1, &[14]), &|| Ok(())).1.unwrap(),
        "7"
    );
    assert_eq!(server.requests.lock().unwrap().len(), before + 1);
    let mut cfg = config(&server);
    cfg.url = "http://remote.invalid".into();
    assert!(cfg.validate().is_err());
    cfg.url = "https://user:secret@example.test".into();
    assert!(cfg.validate().is_err());
}

#[test]
fn reference_depth_timeout_and_config_validation() {
    let server = Server::start(false, |request| {
        let path = request.split_whitespace().nth(1).unwrap();
        let n: u32 = path.rsplit('/').next().unwrap().parse().unwrap();
        (200, serde_json::json!({"schema":"\"long\"", "references":[{"name":format!("R{n}"),"subject":"ref","version":n+1}]}).to_string())
    });
    let mut registry = Registry::new(config(&server), Format::Avro).unwrap();
    assert!(
        registry
            .preview(&envelope(1, &[14]), &|| Ok(()))
            .1
            .unwrap_err()
            .to_string()
            .contains("depth exceeds 8")
    );
    assert_eq!(server.requests.lock().unwrap().len(), 9);
    let stalled = Server::start(false, |_| {
        std::thread::sleep(Duration::from_millis(2400));
        (200, "{}".into())
    });
    let started = Instant::now();
    assert!(
        Registry::new(config(&stalled), Format::Avro)
            .unwrap()
            .check(&|| Ok(()))
            .is_err()
    );
    assert!(started.elapsed() < Duration::from_millis(2300));
    let prefix = "bootstrap_servers=['127.0.0.1:9092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic='events'\nfield='value'\nframing='confluent'\n";
    for invalid in [
        "format='protobuf'\nmessage_name='Event'\nregistry={url='https://registry.test'}",
        "format='avro'\nschema_file='/tmp/schema'\nregistry={url='https://registry.test'}",
        "format='avro'\nregistry={url='http://localhost:8081'}",
        "format='avro'\nregistry={url='https://registry.test', username_env='USER'}",
        "format='avro'\nregistry={url='https://registry.test', token_env='TOKEN', password_env='P', username_env='U'}",
        "format='avro'\nregistry={url='https://registry.test', authorization='secret'}",
    ] {
        let options = toml::from_str(&format!("{prefix}{invalid}")).unwrap();
        assert!(crate::config::Config::parse(&options).is_err());
    }
}

#[test]
fn protobuf_indexes_imports_cache_and_raw_failure_recovery() {
    let server = Server::start(false, |request| {
        let path = request.split_whitespace().nth(1).unwrap();
        let body = match path {
            "/schemas/types" => serde_json::json!(["PROTOBUF"]),
            "/schemas/ids/1" => {
                serde_json::json!({"schemaType":"PROTOBUF", "schema": r#"syntax="proto3"; package demo; import "child.proto"; message First {int32 id=1;} message Outer {message Inner {Child child=1;}}"#, "references":[{"name":"child.proto","subject":"child/name","version":7}]})
            }
            "/subjects/child%2Fname/versions/7" => {
                serde_json::json!({"schemaType":"PROTOBUF", "schema":r#"syntax="proto3"; package demo; message Child {int32 id=1;}"#})
            }
            "/schemas/ids/2" => serde_json::json!({"schemaType":"PROTOBUF", "schema":"invalid"}),
            "/schemas/ids/3" => serde_json::json!({"schema":"\"long\""}),
            _ => return (404, "missing writer".into()),
        };
        (200, body.to_string())
    });
    let mut registry = Registry::new(config(&server), Format::Protobuf).unwrap();
    registry.check(&|| Ok(())).unwrap();
    let valid = envelope(1, &[4, 2, 0, 10, 2, 8, 7]);
    let (identity, result) = registry.preview(&valid, &|| Ok(()));
    assert!(
        identity
            .unwrap()
            .ends_with("#id=1&message=demo.Outer.Inner")
    );
    assert_eq!(result.unwrap(), r#"{"child":{"id":7}}"#);
    let requests = server.requests.lock().unwrap().len();
    for payload in [&[0, 8, 7][..], &[2, 0, 8, 7]] {
        assert_eq!(
            registry
                .preview(&envelope(1, payload), &|| Ok(()))
                .1
                .unwrap(),
            r#"{"id":7}"#
        );
    }
    assert_eq!(server.requests.lock().unwrap().len(), requests);
    for payload in [
        &[][..],
        &[128],
        &[1],
        &[66],
        &[4, 2],
        &[2, 255, 255, 255, 255, 16],
        &[2, 6],
        &[4, 2, 2],
        &[0, 255],
    ] {
        assert!(
            registry
                .preview(&envelope(1, payload), &|| Ok(()))
                .1
                .is_err(),
            "{payload:?}"
        );
    }
    for id in [2, 3, 4] {
        assert!(registry.preview(&envelope(id, &[0]), &|| Ok(())).1.is_err());
    }
    assert_eq!(
        registry.preview(&valid, &|| Ok(())).1.unwrap(),
        r#"{"child":{"id":7}}"#
    );
    assert!(
        Registry::new(config(&server), Format::Avro)
            .unwrap()
            .check(&|| Ok(()))
            .is_err()
    );
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| !r.contains("latest"))
    );
    let options = toml::from_str("bootstrap_servers=['127.0.0.1:9092']\nsecurity_protocol='PLAINTEXT'\n[[decoders]]\ntopic='events'\nfield='value'\nframing='confluent'\nformat='protobuf'\nregistry={url='https://registry.test'}").unwrap();
    assert!(crate::config::Config::parse(&options).is_ok());
}
