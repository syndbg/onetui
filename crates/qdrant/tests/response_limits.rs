//! Exercises the Qdrant connector against a local gRPC peer, without a database fixture.
use std::convert::Infallible;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use tokio_stream::StreamExt;

use onetui_core::provider::{Executor, Provider, RequestContext, ShutdownContext};
use qdrant_client::qdrant::{
    CollectionDescription, ListCollectionsRequest, ListCollectionsResponse,
    PointsOperationResponse, UpdateResult, UpdateStatus, UpsertPoints,
};
use std::time::Duration;
use tonic::codegen::{BoxFuture, Service, http};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ListReply(
    Result<ListCollectionsResponse, Status>,
    Arc<AtomicUsize>,
    Duration,
);
impl ListReply {
    fn new(result: Result<ListCollectionsResponse, Status>) -> Self {
        Self(result, Arc::new(AtomicUsize::new(0)), Duration::ZERO)
    }
}

impl tonic::server::NamedService for ListReply {
    const NAME: &'static str = "qdrant.Collections";
}

impl tonic::server::UnaryService<ListCollectionsRequest> for ListReply {
    type Response = ListCollectionsResponse;
    type Future = std::future::Ready<Result<Response<Self::Response>, Status>>;

    fn call(&mut self, _: Request<ListCollectionsRequest>) -> Self::Future {
        std::future::ready(self.0.clone().map(Response::new))
    }
}

impl Service<http::Request<tonic::body::Body>> for ListReply {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        self.1.fetch_add(1, Ordering::SeqCst);
        let reply = self.clone();
        Box::pin(async move {
            if !reply.2.is_zero() {
                tokio::time::sleep(reply.2).await;
            }
            let codec =
                tonic_prost::ProstCodec::<ListCollectionsResponse, ListCollectionsRequest>::default(
                );
            Ok(tonic::server::Grpc::new(codec).unary(reply, request).await)
        })
    }
}

type CapturedUpserts = Arc<std::sync::Mutex<Vec<(UpsertPoints, Option<String>)>>>;

#[derive(Clone)]
struct UpsertReply {
    result: std::result::Result<PointsOperationResponse, Status>,
    requests: Arc<AtomicUsize>,
    captured: CapturedUpserts,
    delay: Duration,
}

impl UpsertReply {
    fn new(result: std::result::Result<PointsOperationResponse, Status>) -> Self {
        Self {
            result,
            requests: Arc::new(AtomicUsize::new(0)),
            captured: Arc::new(std::sync::Mutex::new(Vec::new())),
            delay: Duration::ZERO,
        }
    }
}

impl tonic::server::NamedService for UpsertReply {
    const NAME: &'static str = "qdrant.Points";
}

impl tonic::server::UnaryService<UpsertPoints> for UpsertReply {
    type Response = PointsOperationResponse;
    type Future = BoxFuture<Response<Self::Response>, Status>;

    fn call(&mut self, request: Request<UpsertPoints>) -> Self::Future {
        let reply = self.clone();
        let api_key = request
            .metadata()
            .get("api-key")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let point_request = request.into_inner();
        Box::pin(async move {
            reply.requests.fetch_add(1, Ordering::SeqCst);
            reply
                .captured
                .lock()
                .unwrap()
                .push((point_request, api_key));
            if !reply.delay.is_zero() {
                tokio::time::sleep(reply.delay).await;
            }
            reply.result.clone().map(Response::new)
        })
    }
}

impl Service<http::Request<tonic::body::Body>> for UpsertReply {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let reply = self.clone();
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::<PointsOperationResponse, UpsertPoints>::default();
            Ok(tonic::server::Grpc::new(codec).unary(reply, request).await)
        })
    }
}

async fn upsert_fixture(reply: UpsertReply) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(reply)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (address, server)
}

fn upsert_request() -> onetui_core::provider::QueryRequest {
    onetui_core::provider::QueryRequest {
        page: onetui_core::provider::PageRequest {
            resource: onetui_core::Resource::new("qdrant.query", vec!["demo".into()]),
            continuation: None,
        },
        text: r#"{"operation":"upsert","points":[{"id":42,"vector":[0.1,0.2],"payload":{"label":"demo"}}]}"#.into(),
    }
}

fn write_result(
    result: onetui_core::provider::QueryExecution,
) -> onetui_core::provider::WriteResult {
    let onetui_core::provider::QueryExecution::Write(result) = result else {
        panic!("expected write result");
    };
    result
}

#[tokio::test]
async fn point_upsert_uses_selected_collection_and_one_completed_rpc() {
    let reply = UpsertReply::new(Ok(PointsOperationResponse {
        result: Some(UpdateResult {
            operation_id: Some(7),
            status: UpdateStatus::Completed as i32,
        }),
        ..Default::default()
    }));
    let requests = reply.requests.clone();
    let captured = reply.captured.clone();
    let (address, server) = upsert_fixture(reply).await;
    let options = toml::from_str(&format!("url='http://{address}'\napi_key_env='KEY'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| Some("fake-secret".into()))
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));

    let result = write_result(
        executor
            .execute_query(upsert_request(), context)
            .await
            .unwrap(),
    );
    assert_eq!(result.outcome, onetui_core::provider::WriteOutcome::Applied);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    {
        let captured = captured.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].0.collection_name, "demo");
        assert_eq!(captured[0].0.wait, Some(true));
        assert_eq!(captured[0].0.points.len(), 1);
        assert_eq!(captured[0].1.as_deref(), Some("fake-secret"));
    }

    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn qdrant_wait_timeout_response_is_unknown_and_not_retried() {
    let reply = UpsertReply::new(Ok(PointsOperationResponse {
        result: Some(UpdateResult {
            operation_id: Some(7),
            status: UpdateStatus::WaitTimeout as i32,
        }),
        ..Default::default()
    }));
    let requests = reply.requests.clone();
    let (address, server) = upsert_fixture(reply).await;
    let options = toml::from_str(&format!("url='http://{address}'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| None)
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));

    let result = write_result(
        executor
            .execute_query(upsert_request(), context)
            .await
            .unwrap(),
    );
    assert_eq!(result.outcome, onetui_core::provider::WriteOutcome::Unknown);
    assert!(result.summary.contains("WaitTimeout"));
    assert_eq!(requests.load(Ordering::SeqCst), 1, "write was retried");

    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn permission_denied_is_redacted_and_not_retried() {
    let mut status = Status::permission_denied("denied for fake-secret");
    status
        .metadata_mut()
        .insert("api-key", "metadata-secret".parse().unwrap());
    let reply = UpsertReply::new(Err(status));
    let requests = reply.requests.clone();
    let (address, server) = upsert_fixture(reply).await;
    let options = toml::from_str(&format!("url='http://{address}'\napi_key_env='KEY'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| Some("fake-secret".into()))
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));

    let result = write_result(
        executor
            .execute_query(upsert_request(), context)
            .await
            .unwrap(),
    );
    assert_eq!(
        result.outcome,
        onetui_core::provider::WriteOutcome::Rejected
    );
    assert!(result.summary.contains("PermissionDenied"));
    assert!(!result.summary.contains("fake-secret"));
    assert!(!result.summary.contains("metadata-secret"));
    assert_eq!(requests.load(Ordering::SeqCst), 1, "write was retried");

    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn cancel_before_dispatch_sends_nothing_and_cancel_after_acceptance_is_unknown() {
    let mut reply = UpsertReply::new(Ok(PointsOperationResponse::default()));
    reply.delay = Duration::from_secs(10);
    let requests = reply.requests.clone();
    let (address, server) = upsert_fixture(reply).await;
    let options = toml::from_str(&format!("url='http://{address}'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| None)
        .unwrap();

    let (cancel, context) = RequestContext::new(Duration::from_secs(10));
    cancel.send(()).unwrap();
    assert!(
        executor
            .execute_query(upsert_request(), context)
            .await
            .is_err()
    );
    assert_eq!(requests.load(Ordering::SeqCst), 0);

    let (cancel, context) = RequestContext::new(Duration::from_secs(10));
    let mut write = Box::pin(executor.execute_query(upsert_request(), context));
    let accepted = async {
        while requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    };
    tokio::select! {
        _ = &mut write => panic!("write completed before server acceptance"),
        result = tokio::time::timeout(Duration::from_secs(5), accepted) => {
            result.expect("server accepted the write");
        }
    }
    cancel.send(()).unwrap();
    let result = write_result(write.await.unwrap());
    assert_eq!(result.outcome, onetui_core::provider::WriteOutcome::Unknown);
    assert_eq!(requests.load(Ordering::SeqCst), 1, "write was retried");

    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

async fn run_reply(reply: ListReply) -> anyhow::Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reply)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let options = toml::from_str(&format!("url='http://{address}'\napi_key_env='KEY'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| Some("fake-secret-do-not-print".into()))
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    let output = executor.check(context).await.map(|result| result.summary);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
    output
}

#[tokio::test]
async fn provider_reuses_channel_without_idle_queries_and_shutdown_closes_it() {
    let reply = ListReply::new(Ok(ListCollectionsResponse::default()));
    let requests = reply.1.clone();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).map(move |socket| {
        if socket.is_ok() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        socket
    });
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reply)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let options = toml::from_str(&format!("url='http://{address}'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| panic!("no secrets"))
        .unwrap();
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        0,
        "configure opened transport"
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    executor.check(context).await.unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    executor.check(context).await.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        requests.load(Ordering::SeqCst),
        2,
        "idle session issued a background check"
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    let result = executor
        .fetch_page(
            onetui_core::provider::PageRequest {
                resource: onetui_core::Resource::new("qdrant.points", vec![]),
                continuation: None,
            },
            context,
        )
        .await;
    assert!(result.unwrap_err().to_string().contains("resource or path"));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
    assert!(executor.check(context).await.is_err());
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("channel kept server connection alive after shutdown")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn provider_cancel_during_rpc_releases_transport_and_can_check_again() {
    let mut reply = ListReply::new(Ok(ListCollectionsResponse::default()));
    reply.2 = Duration::from_millis(300);
    let requests = reply.1.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reply)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let options = toml::from_str(&format!("url='http://{address}'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| None)
        .unwrap();
    let (cancel, context) = RequestContext::new(Duration::from_secs(2));
    let (result, ()) = tokio::join!(executor.check(context), async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while requests.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        cancel.send(()).unwrap();
    });
    assert!(result.err().unwrap().to_string().contains("cancelled"));
    assert_eq!(
        *executor.status().borrow(),
        onetui_core::provider::ConnectionStatus::Disconnected
    );
    let (_cancel, context) = RequestContext::new(Duration::from_secs(2));
    executor.check(context).await.unwrap();
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn oversized_metadata_is_rejected_with_a_useful_limit_error() {
    let output = run_reply(ListReply::new(Ok(ListCollectionsResponse {
        collections: vec![CollectionDescription {
            name: "x".repeat(2 * 1024 * 1024),
        }],
        ..Default::default()
    })))
    .await;
    let error = output.unwrap_err().to_string();
    assert!(error.contains("OutOfRange (11)"), "{error}");
    assert!(error.contains("1048576"), "{error}");
}

#[tokio::test]
async fn server_errors_preserve_codes_and_messages_but_redact_keys_and_controls() {
    let output = run_reply(ListReply::new(Ok(ListCollectionsResponse::default()))).await;
    assert!(output.is_ok(), "{output:?}");
    for code in [
        tonic::Code::PermissionDenied,
        tonic::Code::Unauthenticated,
        tonic::Code::OutOfRange,
        tonic::Code::DeadlineExceeded,
        tonic::Code::NotFound,
        tonic::Code::Internal,
    ] {
        let mut status = Status::new(
            code,
            "Exact backend message: fake-secret-do-not-print\u{1b}[31m",
        );
        status
            .metadata_mut()
            .insert("api-key", "metadata-must-not-print".parse().unwrap());
        let output = run_reply(ListReply::new(Err(status))).await;
        let error = output.unwrap_err().to_string();
        assert_eq!(
            error,
            format!(
                "Qdrant [gRPC {code:?} ({})] Exact backend message: [REDACTED]\\u{{1b}}[31m",
                code as i32
            )
        );
        assert!(!error.contains("fake-secret-do-not-print"));
        assert!(!error.contains("metadata-must-not-print"));
        assert!(!error.contains('\u{1b}'));
    }
}
