use super::*;
use std::{io::Write, process::Command};

#[test]
#[ignore = "requires disposable Kafka/KDC fixtures; uses private child-process ticket caches"]
fn ticket_cache_authentication_and_failures() {
    // Kerberos libraries cache process-wide state. Never change the test runner's credentials.
    if let Ok(case) = std::env::var("ONETUI_KERBEROS_TEST_CASE") {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(check_case(&case));
        return;
    }
    let ca = mtls::fixture_file("ca.crt");
    for case in [
        "reader",
        "denied",
        "expired",
        "missing",
        "wrong_service",
        "offline",
        "cancel",
        "deadline",
        "reader",
    ] {
        let mut profile = tempfile::NamedTempFile::new().unwrap();
        let stalled = matches!(case, "cancel" | "deadline").then(|| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            listener
        });
        let port = stalled
            .as_ref()
            .map_or(if case == "offline" { 1 } else { 18888 }, |listener| {
                listener.local_addr().unwrap().port()
            });
        let transport = if cfg!(target_os = "macos") {
            "tcp/"
        } else {
            ""
        };
        // MIT and macOS Heimdal use different names for the TCP threshold.
        write!(profile, "[libdefaults]\n default_realm = ONETUI.TEST\n dns_lookup_kdc = false\n dns_lookup_realm = false\n rdns = false\n dns_canonicalize_hostname = false\n qualify_shortname = \"\"\n udp_preference_limit = 1\n[realms]\n ONETUI.TEST = {{\n  kdc = {transport}127.0.0.1:{port}\n }}\n").unwrap();
        let cache = match case {
            "missing" => tempfile::NamedTempFile::new().unwrap(),
            "denied" => mtls::fixture_file("krb5/denied.ccache"),
            "expired" => mtls::fixture_file("krb5/expired.ccache"),
            _ => mtls::fixture_file("krb5/reader.ccache"),
        };
        let absent_keytab = tempfile::tempdir().unwrap();
        let mut contact = tempfile::NamedTempFile::new().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "kerberos::ticket_cache_authentication_and_failures",
                "--nocapture",
            ])
            .env("ONETUI_KERBEROS_TEST_CASE", case)
            .env("ONETUI_KERBEROS_TEST_CA", ca.path())
            .env("ONETUI_KERBEROS_TEST_CONTACT", contact.path())
            .env("KRB5_CONFIG", profile.path())
            .env("KRB5CCNAME", format!("FILE:{}", cache.path().display()))
            .env("KRB5_KTNAME", absent_keytab.path().join("absent.keytab"))
            .env(
                "KRB5_CLIENT_KTNAME",
                absent_keytab.path().join("absent.keytab"),
            )
            .spawn()
            .unwrap();
        let started = std::time::Instant::now();
        let mut stalled = stalled;
        let mut accepted = None;
        let mut contacted = false;
        let status = loop {
            if let Some(listener) = &stalled {
                if accepted.is_none()
                    && let Ok((socket, _)) = listener.accept()
                {
                    contacted = true;
                    contact.write_all(b"1").unwrap();
                    accepted = Some((socket, std::time::Instant::now()));
                }
                // Release native GSSAPI after the foreground cancellation/deadline check.
                if accepted
                    .as_ref()
                    .is_some_and(|(_, at)| at.elapsed() > Duration::from_secs(2))
                {
                    accepted.take();
                    stalled.take();
                }
            }
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if started.elapsed() > Duration::from_secs(35) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("Kerberos child timed out: {case}");
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(status.success(), "Kerberos case failed: {case}");
        if matches!(case, "cancel" | "deadline") {
            assert!(contacted, "{case} must reach the stalled KDC");
        }
    }
}

async fn check_case(case: &str) {
    let principal = if case == "denied" {
        "fixture-denied"
    } else {
        "fixture-reader"
    };
    let service = if case == "wrong_service" {
        "missing"
    } else {
        "kafka"
    };
    let options = toml::from_str(&format!(
        "bootstrap_servers=['localhost:19097']\nsecurity_protocol='SASL_SSL'\nsasl_mechanism='GSSAPI'\nkerberos_principal='{principal}@ONETUI.TEST'\nkerberos_service_name='{service}'\nca_file={:?}",
        std::env::var("ONETUI_KERBEROS_TEST_CA").unwrap()
    )).unwrap();
    let mut executor = KafkaProvider
        .configure(&options, &|_| panic!("no password lookup"))
        .unwrap();
    let resource = Resource::new("kafka.records", vec!["demo_events".into(), "0".into()]);
    if matches!(case, "cancel" | "deadline") {
        let started = tokio::time::Instant::now();
        let (cancel, context) = RequestContext::new(if case == "deadline" {
            Duration::from_millis(250)
        } else {
            Duration::from_secs(5)
        });
        let mut cancel = Some(cancel);
        let mut cancelled_at = None;
        let (result, ()) = tokio::join!(executor.check(context), async {
            if case == "cancel" {
                let contact = std::env::var("ONETUI_KERBEROS_TEST_CONTACT").unwrap();
                tokio::time::timeout(Duration::from_secs(3), async {
                    while std::fs::metadata(&contact).unwrap().len() == 0 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("native authentication reached KDC");
                cancelled_at = Some(tokio::time::Instant::now());
                cancel.take().unwrap().send(()).unwrap();
            }
        });
        let error = format!("{:#}", result.err().expect("interrupted authentication"));
        assert!(
            error.contains(if case == "cancel" {
                "cancelled"
            } else {
                "timed out"
            }),
            "{case}: {error}"
        );
        assert!(cancelled_at.unwrap_or(started).elapsed() < Duration::from_millis(750));
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(6)))
            .await
            .unwrap();
        return;
    }
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let result = executor
        .fetch_page(
            PageRequest {
                resource: resource.clone(),
                continuation: None,
            },
            context,
        )
        .await;
    if case == "reader" {
        let first = result.unwrap();
        assert_eq!(first.rows.len(), 100);
        let second = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(second.rows.len(), 100);
        let again = fetch(&executor, resource.clone(), first.continuation).await;
        assert_eq!(second.rows.len(), again.rows.len());
        for (expected, actual) in second.rows.iter().zip(&again.rows) {
            assert_eq!(expected.cells, actual.cells);
        }
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
        let live = follow(&executor, resource.clone(), None).await;
        assert!(live.continuation.is_some());
        assert!(
            follow(&executor, resource.clone(), live.continuation)
                .await
                .rows
                .is_empty()
        );
    } else {
        let error = format!("{:#}", result.unwrap_err());
        let expected = if case == "denied" {
            "TOPIC_AUTHORIZATION_FAILED"
        } else {
            "SASL"
        };
        assert!(error.contains(expected), "{case}: {error}");
    }
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(2)))
        .await
        .unwrap();
    if case == "expired" {
        use std::os::unix::process::CommandExt;

        let renewed = mtls::fixture_file("krb5/reader.ccache");
        let cache = std::env::var("KRB5CCNAME").unwrap();
        std::fs::copy(renewed.path(), cache.strip_prefix("FILE:").unwrap()).unwrap();
        drop(renewed);
        // macOS can retain negative GSSAPI lookups across client recreation.
        // Re-exec keeps the same private cache and the parent's timeout/ownership.
        let error = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "kerberos::ticket_cache_authentication_and_failures",
                "--nocapture",
            ])
            .env("ONETUI_KERBEROS_TEST_CASE", "reader")
            .exec();
        panic!("restart after ticket replacement: {error}");
    }
    if case == "reader" {
        let mut reopened = KafkaProvider.configure(&options, &|_| None).unwrap();
        assert_eq!(fetch(&reopened, resource, None).await.rows.len(), 100);
        reopened
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
    }
}
