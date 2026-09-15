use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, Provider, RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, Page, Resource};
use onetui_qdrant::{QdrantExecutor, QdrantProvider};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct Reply {
    status: u16,
    body: Vec<u8>,
    delay: Duration,
    chunked: bool,
}

struct Server {
    url: String,
    reply: Arc<Mutex<Reply>>,
    requests: Arc<Mutex<Vec<String>>>,
    accepted: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

struct ConnectionClosed(Arc<AtomicUsize>);
impl Drop for ConnectionClosed {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn new(value: Value) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let reply = Arc::new(Mutex::new(Reply {
            status: 200,
            body: Vec::new(),
            delay: Duration::ZERO,
            chunked: false,
        }));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let accepted = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let c = closed.clone();
        let (r, q, a) = (reply.clone(), requests.clone(), accepted.clone());
        let task = tokio::spawn(async move {
            let mut clients = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = clients.join_next(), if !clients.is_empty() => {},
                    socket = listener.accept() => {
                        let (mut socket, _) = socket.unwrap();
                        a.fetch_add(1, Ordering::SeqCst);
                        let (r, q) = (r.clone(), q.clone());
                        let closed = ConnectionClosed(c.clone());
                        clients.spawn(async move {
                            let _closed = closed;
                            loop {
                                let mut request = Vec::new();
                                while !request.ends_with(b"\r\n\r\n") {
                                    let Ok(byte) = socket.read_u8().await else { return };
                                    request.push(byte);
                                    assert!(request.len() < 16 * 1024);
                                }
                                q.lock().unwrap().push(String::from_utf8(request).unwrap());
                                let reply = r.lock().unwrap().clone();
                                let framing = if reply.chunked { "Transfer-Encoding: chunked".into() } else { format!("Content-Length: {}", reply.body.len()) };
                                let headers = format!("HTTP/1.1 {} Test\r\n{framing}\r\nLocation: /elsewhere\r\n\r\n", reply.status);
                                if socket.write_all(headers.as_bytes()).await.is_err() { return; }
                                tokio::time::sleep(reply.delay).await;
                                let body = if reply.chunked {
                                    let mut body = format!("{:x}\r\n", reply.body.len()).into_bytes();
                                    body.extend(&reply.body);
                                    body.extend(b"\r\n0\r\n\r\n");
                                    body
                                } else { reply.body };
                                if socket.write_all(&body).await.is_err() { return; }
                            }
                        });
                    }
                }
            }
        });
        let server = Self {
            url,
            reply,
            requests,
            accepted,
            closed,
            task,
        };
        server.result(value);
        server
    }

    fn result(&self, value: Value) {
        self.reply.lock().unwrap().body =
            serde_json::to_vec(&json!({"status": "ok", "result": value})).unwrap();
    }

    fn executor(&self) -> QdrantExecutor {
        // A dead gRPC endpoint proves topology does not dial gRPC first.
        QdrantProvider
            .configure(
                &toml::from_str(&format!(
                    "url='http://127.0.0.1:1'\nrest_url='{}'\napi_key_env='KEY'",
                    self.url
                ))
                .unwrap(),
                &|_| Some("fixture-secret".into()),
            )
            .unwrap()
    }
}

async fn fetch(
    executor: &QdrantExecutor,
    id: &'static str,
    path: &[&str],
    token: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    executor
        .fetch_page(
            PageRequest {
                resource: Resource::new(id, path.iter().map(|s| (*s).into()).collect()),
                continuation: token,
            },
            context,
        )
        .await
}

fn text(page: &Page, row: usize, column: usize) -> Option<&str> {
    page.rows[row].cells[column]
        .as_ref()
        .and_then(onetui_core::Value::text)
}

fn cluster() -> Value {
    json!({"status":"enabled", "peer_id":18446744073709551615u64, "peers":{"18446744073709551615":{"uri":"http://do-not-contact.invalid:6335"},"2":{"uri":"http://other.invalid:6335"}}, "raft_info":{"leader":2,"term":42,"commit":9007199254740993u64,"role":"Follower","pending_operations":0}, "consensus_thread_status":{"consensus_thread_status":"working"}, "message_send_failures":{"2":{"count":3}}})
}

#[tokio::test]
async fn menus_are_offline_and_topology_needs_explicit_rest_url() {
    onetui_core::provider::validate_catalog(&[QdrantProvider]).unwrap();
    let options = toml::from_str("url='http://127.0.0.1:1'").unwrap();
    let executor = QdrantProvider
        .configure(&options, &|_| panic!("no secrets"))
        .unwrap();
    let menu = fetch(&executor, "qdrant.resources", &[], None)
        .await
        .unwrap();
    assert_eq!(menu.rows.len(), 3);
    assert_eq!(
        menu.rows[0].target.as_ref().unwrap().id,
        "qdrant.collections"
    );
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
    let menu = fetch(&executor, "qdrant.collection", &["demo"], None)
        .await
        .unwrap();
    assert!(
        menu.rows
            .iter()
            .any(|row| row.target.as_ref().unwrap().id == "qdrant.shards")
    );
    assert!(
        fetch(&executor, "qdrant.cluster", &[], None)
            .await
            .unwrap_err()
            .to_string()
            .contains("rest_url")
    );
    for url in [
        "http://remote.invalid:6333",
        "https://user:secret@example.com",
        "https://example.com/path",
        "https://example.com?q=secret",
    ] {
        let options =
            toml::from_str(&format!("url='https://example.com:6334'\nrest_url='{url}'")).unwrap();
        let error = QdrantProvider
            .validate_config(&options)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret"));
    }
}

#[tokio::test]
async fn cluster_peers_reuse_http_and_preserve_ids_and_full_details() {
    let server = Server::new(cluster()).await;
    let mut executor = server.executor();
    let page = fetch(&executor, "qdrant.cluster", &[], None).await.unwrap();
    assert_eq!(text(&page, 0, 1), Some("18446744073709551615"));
    assert_eq!(text(&page, 0, 5), Some("9007199254740993"));
    assert!(text(&page, 0, 7).unwrap().contains("message_send_failures"));
    let page = fetch(&executor, "qdrant.peers", &[], None).await.unwrap();
    assert_eq!(text(&page, 0, 0), Some("2"));
    assert_eq!(text(&page, 0, 3), Some("true"));
    assert_eq!(text(&page, 1, 2), Some("true"));
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    for request in server.requests.lock().unwrap().iter() {
        assert!(request.starts_with("GET /cluster HTTP/1.1"));
        assert!(request.contains("api-key: fixture-secret\r\n"));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server.requests.lock().unwrap().len(),
        2,
        "idle metadata request"
    );
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    tokio::time::timeout(Duration::from_secs(1), async {
        while server.closed.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("REST pool kept its socket after shutdown");
    assert!(fetch(&executor, "qdrant.peers", &[], None).await.is_err());
}

#[tokio::test]
async fn topology_paging_binds_bookmarks_to_session_resource_and_path() {
    let mut value = cluster();
    value["peers"] = Value::Object(
        (0..205)
            .map(|id| {
                (
                    id.to_string(),
                    json!({"uri":format!("http://peer-{id}.invalid:6335")}),
                )
            })
            .collect(),
    );
    let server = Server::new(value).await;
    let executor = server.executor();
    let first = fetch(&executor, "qdrant.peers", &[], None).await.unwrap();
    assert_eq!(first.rows.len(), 100);
    let token = first.continuation.unwrap();
    let second = fetch(&executor, "qdrant.peers", &[], Some(token.clone()))
        .await
        .unwrap();
    assert_eq!(text(&second, 0, 0), Some("100"));
    let third = fetch(&executor, "qdrant.peers", &[], second.continuation)
        .await
        .unwrap();
    assert_eq!(third.rows.len(), 5);
    assert!(!third.next);
    let replay = fetch(&executor, "qdrant.peers", &[], Some(token.clone()))
        .await
        .unwrap();
    assert_eq!(second.rows[0].cells, replay.rows[0].cells);
    let count = server.requests.lock().unwrap().len();
    assert!(
        fetch(&server.executor(), "qdrant.peers", &[], Some(token.clone()))
            .await
            .is_err()
    );
    assert!(
        fetch(&executor, "qdrant.shards", &["demo"], Some(token.clone()))
            .await
            .is_err()
    );
    assert!(
        fetch(&executor, "qdrant.cluster", &[], Some(token))
            .await
            .is_err()
    );
    assert_eq!(server.requests.lock().unwrap().len(), count);
}

#[tokio::test]
async fn shards_transfers_and_resharding_keep_native_fields_and_escape_paths() {
    let server = Server::new(json!({"peer_id":7,"shard_count":2,"local_shards":[{"shard_id":1,"state":"Active","points_count":9007199254740993u64,"shard_key":"София"}],"remote_shards":[{"shard_id":1,"peer_id":9,"state":"Resharding"}],"shard_transfers":[{"shard_id":1,"from":7,"to":9,"sync":true,"method":"wal_delta","to_shard_id":2,"comment":"in progress","future_field":true}],"resharding_operations":[{"direction":"up","shard_id":2,"peer_id":9}]})).await;
    let executor = server.executor();
    let page = fetch(&executor, "qdrant.shards", &["a/b?#"], None)
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert_eq!(text(&page, 0, 1), Some("7"));
    assert_eq!(text(&page, 0, 4), Some("9007199254740993"));
    assert_eq!(text(&page, 1, 2), Some("remote"));
    assert_eq!(text(&page, 1, 4), None);
    let page = fetch(&executor, "qdrant.transfers", &["a/b?#"], None)
        .await
        .unwrap();
    assert_eq!(text(&page, 0, 4), Some("wal_delta"));
    assert!(text(&page, 0, 7).unwrap().contains("future_field"));
    let page = fetch(&executor, "qdrant.collection_cluster", &["a/b?#"], None)
        .await
        .unwrap();
    assert!(text(&page, 0, 0).unwrap().contains("resharding_operations"));
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.starts_with("GET /collections/a%2Fb%3F%23/cluster HTTP/1.1"))
    );
    assert!(
        fetch(&executor, "qdrant.shards", &[".."], None)
            .await
            .is_err()
    );
    server.result(json!({"status":"disabled"}));
    let page = fetch(&executor, "qdrant.peers", &[], None).await.unwrap();
    assert!(page.rows.is_empty());
    assert!(page.notice.contains("disabled"));
    let page = fetch(&executor, "qdrant.cluster", &[], None).await.unwrap();
    assert_eq!(text(&page, 0, 0), Some("disabled"));
}

#[tokio::test]
async fn rest_errors_preserve_server_wording_redact_keys_and_refuse_redirects() {
    let server = Server::new(cluster()).await;
    let executor = server.executor();
    for status in [401, 403, 500, 302] {
        *server.reply.lock().unwrap() = Reply {
            status,
            body: b"permission denied: fixture-secret\x1b[31m".to_vec(),
            delay: Duration::ZERO,
            chunked: false,
        };
        let before = server.requests.lock().unwrap().len();
        let error = fetch(&executor, "qdrant.cluster", &[], None)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(&status.to_string()));
        assert!(error.contains("permission denied: [REDACTED]"));
        assert!(!error.contains("fixture-secret"));
        assert!(!error.contains('\x1b'));
        assert_eq!(server.requests.lock().unwrap().len(), before + 1);
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    }
    server.reply.lock().unwrap().status = 200;
    for body in ["{broken", "{}", "{\"status\":\"ok\",\"result\":[]}"] {
        server.reply.lock().unwrap().body = body.as_bytes().to_vec();
        assert!(fetch(&executor, "qdrant.cluster", &[], None).await.is_err());
    }
    server.result(json!({}));
    assert!(
        fetch(&executor, "qdrant.shards", &["demo"], None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rest_limits_cover_declared_and_chunked_bodies() {
    let server = Server::new(cluster()).await;
    let executor = server.executor();
    for chunked in [false, true] {
        *server.reply.lock().unwrap() = Reply {
            status: 200,
            body: vec![b' '; PAGE_BYTES + 1],
            delay: Duration::ZERO,
            chunked,
        };
        assert!(
            fetch(&executor, "qdrant.cluster", &[], None)
                .await
                .unwrap_err()
                .to_string()
                .contains("1 MiB")
        );
    }
    server.result(cluster());
    assert!(fetch(&executor, "qdrant.cluster", &[], None).await.is_ok());
}

#[tokio::test]
async fn cancellation_and_deadlines_discard_rest_client_and_allow_reopen() {
    let server = Server::new(cluster()).await;
    server.reply.lock().unwrap().delay = Duration::from_secs(2);
    let executor = server.executor();
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    let request = || PageRequest {
        resource: Resource::new("qdrant.cluster", vec![]),
        continuation: None,
    };
    let (result, ()) = tokio::join!(executor.fetch_page(request(), context), async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.requests.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.send(()).unwrap();
    });
    assert!(result.unwrap_err().to_string().contains("cancel"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    let (_cancel, context) = RequestContext::new(Duration::from_millis(30));
    assert!(executor.fetch_page(request(), context).await.is_err());
    server.reply.lock().unwrap().delay = Duration::ZERO;
    assert!(fetch(&executor, "qdrant.cluster", &[], None).await.is_ok());
    assert!(server.accepted.load(Ordering::SeqCst) >= 3);
}
