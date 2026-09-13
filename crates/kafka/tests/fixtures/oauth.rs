use super::*;
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde_json::json;
use std::{
    io::Write,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

fn jwt(key: &std::path::Path, principal: &str, audience: &str, issuer: &str, ttl: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","kid":"onetui-fixture"}"#);
    let claims = URL_SAFE_NO_PAD.encode(
        json!({"sub":principal,"iss":issuer,"aud":audience,"iat":now,"exp":now+ttl}).to_string(),
    );
    let payload = format!("{header}.{claims}");
    let mut signer = Command::new("openssl")
        .args(["dgst", "-sha256", "-sign"])
        .arg(key)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    signer
        .stdin
        .take()
        .unwrap()
        .write_all(payload.as_bytes())
        .unwrap();
    let output = signer.wait_with_output().unwrap();
    assert!(output.status.success());
    format!("{payload}.{}", URL_SAFE_NO_PAD.encode(output.stdout))
}

fn configured(server: &server::Server, ca: &std::path::Path, secret: &str) -> KafkaExecutor {
    let options = toml::from_str(&format!("bootstrap_servers=['localhost:19096']\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='OAUTHBEARER'\nca_file={:?}\n[oauth]\ntoken_url={:?}\nclient_id_env='OAUTH_ID'\nclient_secret_env='OAUTH_SECRET'\n{}", ca.to_str().unwrap(), format!("{}/token", server.url), server.ca_file.as_ref().map_or(String::new(), |ca| format!("ca_file={ca:?}")))).unwrap();
    KafkaProvider.validate_config(&options).unwrap();
    KafkaProvider
        .configure(&options, &|name| {
            Some(
                match name {
                    "OAUTH_ID" => "fixture-client",
                    "OAUTH_SECRET" => secret,
                    _ => panic!("unrelated secret lookup"),
                }
                .into(),
            )
        })
        .unwrap()
}

async fn read(executor: &KafkaExecutor) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .fetch_page(
            PageRequest {
                resource: Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
                continuation: None,
            },
            context,
        )
        .await
}

#[tokio::test]
#[ignore = "requires disposable Kafka signed-JWT OAuth listener; read-only"]
async fn signed_tokens_refresh_paging_follow_and_reopen() {
    let ca = mtls::fixture_file("ca.crt");
    let key = mtls::fixture_file("server.key");
    let issued = Arc::new(AtomicUsize::new(0));
    let count = issued.clone();
    let server = server::Server::start(true, move |request| {
        assert!(request.starts_with("POST /token "));
        assert!(request.contains(&format!(
            "Basic {}",
            STANDARD.encode("fixture-client:fixture-oauth-secret")
        )));
        assert!(request.ends_with("grant_type=client_credentials"));
        count.fetch_add(1, Ordering::SeqCst);
        (200, json!({"access_token":jwt(key.path(), "fixture-reader", "onetui-kafka", "onetui-fixtures", 8),"token_type":"Bearer","expires_in":8}).to_string())
    });
    let mut executor = configured(&server, ca.path(), "fixture-oauth-secret");
    let first = read(&executor).await.unwrap();
    assert_eq!(first.rows.len(), 100);
    let second = fetch(
        &executor,
        Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
        first.continuation,
    )
    .await;
    assert_eq!(second.rows.len(), 100);
    let before = issued.load(Ordering::SeqCst);
    let resource = Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]);
    let mut live = follow(&executor, resource.clone(), None).await;
    tokio::time::timeout(Duration::from_secs(12), async {
        while issued.load(Ordering::SeqCst) == before {
            tokio::time::sleep(Duration::from_secs(1)).await;
            live = follow(&executor, resource.clone(), live.continuation.take()).await;
        }
    })
    .await
    .unwrap();
    // No idle HTTP: librdkafka refresh callbacks are served on the next active poll.
    let before = issued.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(issued.load(Ordering::SeqCst), before);
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.check(context).await.unwrap();
    assert!(issued.load(Ordering::SeqCst) > before);
    let third = fetch(&executor, resource.clone(), second.continuation).await;
    assert_eq!(third.rows.len(), 100);
    let page = follow(
        &executor,
        Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]),
        None,
    )
    .await;
    assert!(page.continuation.is_some());
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
    let mut reopened = configured(&server, ca.path(), "fixture-oauth-secret");
    assert_eq!(read(&reopened).await.unwrap().rows.len(), 100);
    reopened
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable Kafka signed-JWT OAuth listener; read-only"]
async fn rejected_credentials_signatures_claims_and_acl_preserve_native_errors() {
    let ca = mtls::fixture_file("ca.crt");
    for (key_name, principal, audience, issuer, status, expected) in [
        (
            "server.key",
            "fixture-reader",
            "onetui-kafka",
            "onetui-fixtures",
            401,
            "invalid_client",
        ),
        (
            "untrusted.key",
            "fixture-reader",
            "onetui-kafka",
            "onetui-fixtures",
            200,
            "Authentication",
        ),
        (
            "server.key",
            "fixture-reader",
            "wrong-audience",
            "onetui-fixtures",
            200,
            "Authentication",
        ),
        (
            "server.key",
            "fixture-reader",
            "onetui-kafka",
            "wrong-issuer",
            200,
            "Authentication",
        ),
        (
            "server.key",
            "fixture-denied",
            "onetui-kafka",
            "onetui-fixtures",
            200,
            "TOPIC_AUTHORIZATION_FAILED",
        ),
    ] {
        let key = mtls::fixture_file(key_name);
        let token = jwt(key.path(), principal, audience, issuer, 300);
        let body = if status == 401 {
            json!({"error":"invalid_client","error_description":"wrong-oauth-secret"})
        } else {
            json!({"access_token":token,"token_type":"Bearer"})
        }
        .to_string();
        let server = server::Server::start(false, move |_| (status, body.clone()));
        let mut executor = configured(&server, ca.path(), "wrong-oauth-secret");
        let error = format!("{:#}", read(&executor).await.unwrap_err());
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
        assert!(
            error.contains(expected),
            "{key_name} {principal} {audience} {issuer}: {error}"
        );
        assert!(!error.contains("wrong-oauth-secret") && !error.contains(&token));
    }
}

#[tokio::test]
#[ignore = "requires disposable Kafka OAuth listener and local token endpoint; read-only"]
async fn slow_token_endpoint_cancellation_releases_native_owner() {
    let ca = mtls::fixture_file("ca.crt");
    let requested = Arc::new(AtomicBool::new(false));
    let seen = requested.clone();
    let server = server::Server::start(false, move |_| {
        seen.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1500));
        (503, "temporarily_unavailable".into())
    });
    let executor = configured(&server, ca.path(), "fixture-oauth-secret");
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    let task = tokio::spawn(async move {
        let result = executor.check(context).await;
        (executor, result)
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !requested.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    cancel.send(()).unwrap();
    let (mut executor, result) = tokio::time::timeout(Duration::from_millis(300), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(3)))
        .await
        .unwrap();
    let mut plaintext = super::executor();
    assert_eq!(read(&plaintext).await.unwrap().rows.len(), 100);
    plaintext
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
}
