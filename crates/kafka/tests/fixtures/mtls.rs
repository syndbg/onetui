use super::*;
use std::io::Write;

pub(super) fn fixture_file(name: &str) -> tempfile::NamedTempFile {
    let output = std::process::Command::new("docker")
        .args([
            "compose",
            "--project-name",
            "onetui-fixtures",
            "--env-file",
            "/dev/null",
            "-f",
            "hack/compose.yaml",
            "exec",
            "-T",
            "kafka",
            "cat",
            &format!("/tmp/onetui-kafka-tls/{name}"),
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap();
    assert!(output.status.success(), "fixture file unavailable: {name}");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&output.stdout).unwrap();
    file
}

#[tokio::test]
#[ignore = "requires disposable Kafka mTLS listener and seeded records; read-only"]
async fn client_certificate_authentication_trust_secrets_and_reopen() {
    let ca = fixture_file("ca.crt");
    // Reopen for every case to catch credentials leaking across selected aliases.
    for (certificate, key, password, expected) in [
        (Some("reader.crt"), "reader.key", None, None),
        (
            Some("reader.crt"),
            "reader-encrypted.key",
            Some("fixture-client-key-only"),
            None,
        ),
        (None, "reader.key", None, Some("certificate")),
        (Some("untrusted.crt"), "untrusted.key", None, Some("SSL")),
        (Some("reader.crt"), "denied.key", None, Some("key")),
        (
            Some("reader.crt"),
            "reader-encrypted.key",
            Some("wrong-client-key-secret"),
            Some("key"),
        ),
        (
            Some("denied.crt"),
            "denied.key",
            None,
            Some("TOPIC_AUTHORIZATION_FAILED"),
        ),
        (Some("reader.crt"), "reader.key", None, None),
    ] {
        let mut options: toml::Table =
            toml::from_str("bootstrap_servers=['localhost:19095']").unwrap();
        options.insert("ca_file".into(), ca.path().to_str().unwrap().into());
        let files = certificate.map(|name| (fixture_file(name), fixture_file(key)));
        if let Some((cert, key)) = &files {
            options.insert(
                "client_cert_file".into(),
                cert.path().to_str().unwrap().into(),
            );
            options.insert(
                "client_key_file".into(),
                key.path().to_str().unwrap().into(),
            );
        }
        if password.is_some() {
            options.insert(
                "client_key_password_env".into(),
                "FIXTURE_KEY_PASSWORD".into(),
            );
        }
        KafkaProvider.validate_config(&options).unwrap();
        let mut executor = KafkaProvider
            .configure(&options, &|name| {
                assert_eq!(name, "FIXTURE_KEY_PASSWORD");
                password.map(str::to_owned)
            })
            .unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let result = executor
            .fetch_page(
                PageRequest {
                    resource: Resource::new(
                        "kafka.records",
                        vec!["demo_events".into(), "0".into()],
                    ),
                    continuation: None,
                },
                context,
            )
            .await;
        if expected.is_none()
            && let Ok(first) = &result
        {
            let second = fetch(
                &executor,
                Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
                first.continuation.clone(),
            )
            .await;
            assert_eq!(second.rows.len(), 100);
            let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
            executor.check(context).await.unwrap();
        }
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
        match expected {
            None => assert_eq!(result.unwrap().rows.len(), 100),
            Some(expected) => {
                let error = format!("{:#}", result.unwrap_err());
                assert!(error.contains(expected), "{certificate:?} {key}: {error}");
                assert!(!error.contains("wrong-client-key-secret"));
                assert!(!error.contains("fixture-client-key-only"));
                assert!(!error.contains("PRIVATE KEY"));
            }
        }
    }
}
