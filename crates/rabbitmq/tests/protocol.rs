use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, RequestContext, ShutdownContext,
};
use onetui_core::{Page, Resource, Value};
use onetui_rabbitmq::{RabbitMqExecutor, RabbitMqProvider};
use serde_json::json;

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Server {
    fn start(responses: Vec<(String, Duration)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        listener.set_nonblocking(true).unwrap();
        let worker = std::thread::spawn(move || {
            for (response, delay) in responses {
                let until = std::time::Instant::now() + Duration::from_secs(5);
                let stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(std::time::Instant::now() < until, "expected HTTP request");
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("{error}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(stream);
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    assert!(reader.read_line(&mut line).unwrap() > 0);
                    request.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                }
                seen.lock().unwrap().push(request);
                std::thread::sleep(delay);
                let _ = reader.get_mut().write_all(response.as_bytes());
            }
        });
        Self {
            url,
            requests,
            worker: Some(worker),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !std::thread::panicking() {
                result.unwrap();
            }
        }
    }
}
fn response(status: &str, body: &str) -> (String, Duration) {
    (
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
        Duration::ZERO,
    )
}
fn executor(url: &str) -> RabbitMqExecutor {
    RabbitMqProvider
        .configure(
            &toml::from_str(&format!(
                "url='{url}'\nusername_env='USER'\npassword_env='PASS'"
            ))
            .unwrap(),
            &|name| {
                Some(
                    if name == "USER" {
                        "reader"
                    } else {
                        "secret-only"
                    }
                    .into(),
                )
            },
        )
        .unwrap()
}
async fn fetch(
    executor: &RabbitMqExecutor,
    id: &'static str,
    path: Vec<String>,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    executor
        .fetch_page(
            PageRequest {
                resource: Resource::new(id, path),
                continuation,
            },
            context,
        )
        .await
}

#[tokio::test]
async fn catalog_config_and_offline_menus_do_not_connect() {
    onetui_core::provider::validate_catalog(&[RabbitMqProvider]).unwrap();
    let mut executor = executor("http://127.0.0.1:1");
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
    let menu = fetch(&executor, "rabbitmq.resources", vec![], None)
        .await
        .unwrap();
    assert_eq!(menu.rows.len(), 10);
    assert!(menu.rows.iter().all(|r| r.target.is_some()));
    assert_eq!(
        fetch(&executor, "rabbitmq.vhost", vec!["/".into()], None)
            .await
            .unwrap()
            .rows
            .len(),
        7
    );
    for id in [
        "rabbitmq.unknown",
        "rabbitmq.resources",
        "rabbitmq.overview",
    ] {
        assert!(
            fetch(&executor, id, vec!["unexpected".into()], None)
                .await
                .is_err()
        );
    }
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    cancel.send(()).unwrap();
    assert!(
        executor
            .check(context)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("cancelled")
    );
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    assert!(
        fetch(&executor, "rabbitmq.resources", vec![], None)
            .await
            .is_err()
    );
    for url in [
        "http://remote.invalid",
        "https://user:pass@example.com",
        "https://example.com/api",
        "https://example.com/?x=1",
        "ftp://localhost",
        "https://example.com/#fragment",
    ] {
        let options =
            toml::from_str(&format!("url='{url}'\nusername_env='U'\npassword_env='P'")).unwrap();
        assert!(RabbitMqProvider.validate_config(&options).is_err(), "{url}");
    }
    let options = toml::from_str(
        "url='https://example.invalid'\nusername_env='U'\npassword_env='P'\nca_file='/missing'",
    )
    .unwrap();
    RabbitMqProvider.validate_config(&options).unwrap();
    assert!(RabbitMqProvider.configure(&options, &|_| None).is_err());
    assert!(
        RabbitMqProvider
            .configure(&options, &|_| Some("bad:username".into()))
            .is_err()
    );
}

#[tokio::test]
async fn native_pages_preserve_json_nulls_bytes_and_bound_bookmarks() {
    let first = json!({"page":1,"page_count":2,"items":[{"name":"q / София","vhost":"/","messages_ready":9007199254740993_u64,"future":{"flag":true},"state":null}]}).to_string();
    let second = json!({"page":2,"page_count":2,"items":[{"name":"last","vhost":"/"}]}).to_string();
    let server = Server::start(vec![
        response("200 OK", &first),
        response("200 OK", &second),
        response("200 OK", &first),
    ]);
    let executor = executor(&server.url);
    let page = fetch(&executor, "rabbitmq.queues", vec!["/".into()], None)
        .await
        .unwrap();
    assert_eq!(page.rows[0].cells[3], None);
    assert_eq!(
        page.rows[0].cells[4],
        Some(Value::Json("9007199254740993".into()))
    );
    let Some(Value::Json(details)) = &page.rows[0].cells[7] else {
        panic!("missing JSON")
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(details).unwrap()["future"]["flag"],
        true
    );
    let token = page.continuation.clone().unwrap();
    let last = fetch(
        &executor,
        "rabbitmq.queues",
        vec!["/".into()],
        Some(token.clone()),
    )
    .await
    .unwrap();
    assert_eq!(last.rows[0].cells[0], Some("last".into()));
    assert!(!last.next);
    assert!(
        fetch(&executor, "rabbitmq.nodes", vec![], Some(token.clone()))
            .await
            .is_err()
    );
    assert!(
        fetch(
            &executor,
            "rabbitmq.queues",
            vec!["other".into()],
            Some(token.clone())
        )
        .await
        .is_err()
    );
    let other = self::executor(&server.url);
    assert!(
        fetch(&other, "rabbitmq.queues", vec!["/".into()], Some(token))
            .await
            .is_err()
    );
    assert_eq!(
        fetch(&executor, "rabbitmq.queues", vec!["/".into()], None)
            .await
            .unwrap()
            .rows[0]
            .cells[0],
        page.rows[0].cells[0]
    );
    let requests = server.requests.lock().unwrap();
    assert!(
        requests[0]
            .starts_with("GET /api/queues/%2F?pagination=true&page=1&page_size=100 HTTP/1.1")
    );
    assert!(requests[1].contains("page=2"));
    assert!(requests[0].contains("authorization: Basic cmVhZGVyOnNlY3JldC1vbmx5"));
}

#[tokio::test]
async fn array_pages_empty_lists_and_scoped_paths_remain_bounded() {
    let values = (0..205)
        .map(|n| json!({"name":format!("node-{n:03}"),"running":true}))
        .collect::<Vec<_>>();
    let body = serde_json::to_string(&values).unwrap();
    let server = Server::start(vec![
        response("200 OK", &body),
        response("200 OK", &body),
        response("200 OK", &body),
        response("200 OK", "[]"),
    ]);
    let executor = executor(&server.url);
    let first = fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    assert_eq!(first.rows.len(), 100);
    let second = fetch(&executor, "rabbitmq.nodes", vec![], first.continuation)
        .await
        .unwrap();
    assert_eq!(second.rows[0].cells[0], Some("node-100".into()));
    let last = fetch(&executor, "rabbitmq.nodes", vec![], second.continuation)
        .await
        .unwrap();
    assert_eq!(last.rows.len(), 5);
    assert!(!last.next);
    assert!(
        fetch(
            &executor,
            "rabbitmq.connections",
            vec!["demo / София".into()],
            None
        )
        .await
        .unwrap()
        .rows
        .is_empty()
    );
    assert!(
        server.requests.lock().unwrap()[3]
            .contains("/api/vhosts/demo%20%2F%20%D0%A1%D0%BE%D1%84%D0%B8%D1%8F/connections?")
    );
}

#[tokio::test]
async fn native_errors_redirects_invalid_json_and_size_limits_fail_without_credentials() {
    for reply in [
        response("403 Forbidden", "{\"error\":\"not_authorised\",\"reason\":\"secret-only denied\\u001b\"}"),
        response("200 OK", "not JSON"),
        response("200 OK", "{\"items\":[null],\"page\":1,\"page_count\":1}"),
        response("200 OK", "{\"items\":[],\"page\":99,\"page_count\":100}"),
        ("HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/stolen\r\nContent-Length: 0\r\n\r\n".into(), Duration::ZERO),
        ("HTTP/1.1 200 OK\r\nContent-Length: 1048577\r\n\r\n".into(), Duration::ZERO),
        response("200 OK", &json!([{"name":"x".repeat(onetui_core::PAGE_BYTES)}]).to_string()),
    ] {
        let server = Server::start(vec![reply]);
        let executor = executor(&server.url);
        let error = fetch(&executor, "rabbitmq.nodes", vec![], None).await.unwrap_err().to_string();
        assert!(!error.contains("secret-only"));
        assert!(!error.contains('\u{1b}'));
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn cancellation_and_deadline_retire_requests_and_allow_reopen() {
    let mut slow = response("200 OK", "[]");
    slow.1 = Duration::from_millis(150);
    let server = Server::start(vec![
        slow.clone(),
        response("200 OK", "[]"),
        slow,
        response("200 OK", "[]"),
    ]);
    let executor = executor(&server.url);
    let (_cancel, context) = RequestContext::new(Duration::from_millis(50));
    assert!(
        executor
            .fetch_page(
                PageRequest {
                    resource: Resource::new("rabbitmq.nodes", vec![]),
                    continuation: None
                },
                context
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out")
    );
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.send(()).unwrap();
    });
    assert!(
        executor
            .fetch_page(
                PageRequest {
                    resource: Resource::new("rabbitmq.nodes", vec![]),
                    continuation: None
                },
                context
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
}

#[tokio::test]
async fn successful_reads_reuse_one_http_connection_and_shutdown_closes_it() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let worker = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline);
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("{error}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut reader = BufReader::new(stream);
        for _ in 0..2 {
            let mut first = String::new();
            assert!(reader.read_line(&mut first).unwrap() > 0);
            assert!(first.starts_with("GET /api/nodes HTTP/1.1"));
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            reader
                .get_mut()
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n[]")
                .unwrap();
        }
        assert_eq!(reader.read_line(&mut String::new()).unwrap(), 0);
        assert!(
            listener.accept().is_err(),
            "unexpected second HTTP connection"
        );
    });
    let mut executor = executor(&url);
    fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    fetch(&executor, "rabbitmq.nodes", vec![], None)
        .await
        .unwrap();
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || worker.join().unwrap())
        .await
        .unwrap();
}
