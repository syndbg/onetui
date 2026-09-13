use super::*;
use std::io::Write;

const CREDS: &str = include_str!("../../../../hack/fixtures/nats-test.creds");
const SEED: &str = "SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM";

fn tls_file(name: &str) -> tempfile::NamedTempFile {
    let output = docker(&["exec", "-T", "nats-secure", "cat", &format!("/tls/{name}")]);
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(&output.stdout).unwrap();
    file
}

#[tokio::test]
#[ignore = "requires disposable NATS mTLS/NKEY/domain listener"]
async fn nkey_mtls_domain_reads_trust_errors_and_reopen() {
    let ca = tls_file("ca.crt");
    let cert = tls_file("reader.crt");
    let key = tls_file("reader.key");
    let wrong_key = tls_file("server.key");
    let tls = format!(
        "servers=['tls://127.0.0.1:14224']\nca_file={:?}\ncert_file={:?}\nkey_file={:?}\ndomain='FIXTURE'\nnkey_env='SEED'",
        ca.path(),
        cert.path(),
        key.path()
    );
    let admin = async_nats::ConnectOptions::new()
        .user_and_password("fixture-admin".into(), "fixture-admin-only".into())
        .add_root_certificates(ca.path().into())
        .add_client_certificate(cert.path().into(), key.path().into())
        .connect("tls://127.0.0.1:14224")
        .await
        .unwrap();
    let js = async_nats::jetstream::with_domain(admin.clone(), "FIXTURE");
    let stream = format!("AUTH_{}", std::process::id());
    let subject = format!("demo.auth_{}", std::process::id());
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await
    .unwrap();
    js.publish(subject, "secure data".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let name = stream.clone();
    let tested = tokio::spawn(async move {
        for tls_first in [false, true] {
            let mut executor = NatsProvider
                .configure(
                    &toml::from_str(&format!("{tls}\ntls_first={tls_first}")).unwrap(),
                    &|_| Some(SEED.into()),
                )
                .unwrap();
            let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
            executor.check(context).await.unwrap();
            let page = read(&executor, "nats.messages", &[&name], None, false)
                .await
                .unwrap();
            assert_eq!(
                page.rows[0].cells[3],
                Some(Value::Bytes(b"secure data".to_vec()))
            );
            close(&mut executor).await;
        }
        for extra in [
            "wrong-domain",
            "missing-cert",
            "untrusted",
            "bad-seed",
            "mismatched-key",
        ] {
            let mut options: toml::Table = toml::from_str(&tls).unwrap();
            match extra {
                "wrong-domain" => {
                    options.insert("domain".into(), "OTHER".into());
                }
                "missing-cert" => {
                    options.remove("cert_file");
                    options.remove("key_file");
                }
                "untrusted" => {
                    options.remove("ca_file");
                }
                "mismatched-key" => {
                    options.insert("key_file".into(), wrong_key.path().to_str().unwrap().into());
                }
                _ => {}
            }
            let seed = if extra == "bad-seed" {
                "invalid-fixture-secret"
            } else {
                SEED
            };
            let mut executor = NatsProvider
                .configure(&options, &|_| Some(seed.into()))
                .unwrap();
            let (_cancel, context) = RequestContext::new(Duration::from_secs(3));
            let error = executor
                .check(context)
                .await
                .err()
                .expect(extra)
                .to_string();
            assert!(
                !error.contains(SEED) && !error.contains("invalid-fixture-secret"),
                "{error}"
            );
            assert!(!error.is_empty());
            close(&mut executor).await;
        }
    })
    .await;
    js.delete_stream(&stream).await.unwrap();
    tested.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable operator-JWT NATS listener"]
async fn jwt_credentials_native_nonce_signing_subscriptions_and_rejection() {
    let config: toml::Table = toml::from_str("servers=['nats://127.0.0.1:14225']\ntls=false\njetstream=false\ncredentials_env='CREDS'\nsubjects=['demo.jwt']").unwrap();
    let admin = async_nats::ConnectOptions::new()
        .credentials(CREDS)
        .unwrap()
        .connect("nats://127.0.0.1:14225")
        .await
        .unwrap();
    let mut executor = NatsProvider
        .configure(&config, &|_| Some(CREDS.into()))
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    let initial = read(&executor, "nats.core_messages", &["demo.jwt"], None, true)
        .await
        .unwrap();
    admin.publish("demo.jwt", "JWT data".into()).await.unwrap();
    admin.flush().await.unwrap();
    let mut continuation = initial.continuation;
    let until = tokio::time::Instant::now() + Duration::from_secs(3);
    let page = loop {
        let page = read(
            &executor,
            "nats.core_messages",
            &["demo.jwt"],
            continuation,
            true,
        )
        .await
        .unwrap();
        if !page.rows.is_empty() {
            break page;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "JWT subscription received no message"
        );
        continuation = page.continuation;
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(
        page.rows[0].cells[2],
        Some(Value::Bytes(b"JWT data".to_vec()))
    );
    close(&mut executor).await;
    let jwt = CREDS.lines().find(|l| l.starts_with("eyJ")).unwrap();
    let mut parts: Vec<_> = jwt.split('.').collect();
    parts[2] =
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let invalid = CREDS.replace(jwt, &parts.join("."));
    let mut executor = NatsProvider
        .configure(&config, &|_| Some(invalid.clone()))
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(3));
    let error = executor.check(context).await.err().unwrap().to_string();
    assert!(!error.contains(SEED) && !error.contains(jwt));
    assert!(error.to_lowercase().contains("authorization"), "{error}");
    close(&mut executor).await;
}
