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

async fn run_reply(reply: ListReply) -> anyhow::Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reply)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let options = toml::from_str(&format!("url='http://{address}'")).unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(&options, &|_| panic!("no secrets"))
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
    assert!(result.unwrap_err().to_string().contains("not implemented"));
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
    assert!(error.contains("1 MiB"), "{error}");
}

#[tokio::test]
async fn small_metadata_succeeds_and_server_errors_do_not_leak_details() {
    let output = run_reply(ListReply::new(Ok(ListCollectionsResponse::default()))).await;
    assert!(output.is_ok(), "{output:?}");
    for (code, expected) in [
        (tonic::Code::PermissionDenied, "denied"),
        (tonic::Code::Unauthenticated, "authentication"),
        (tonic::Code::OutOfRange, "1 MiB"),
        (tonic::Code::DeadlineExceeded, "timed out"),
    ] {
        let mut status = Status::new(code, "fake-secret-do-not-print\u{1b}[31m");
        status
            .metadata_mut()
            .insert("api-key", "fake-secret-do-not-print".parse().unwrap());
        let output = run_reply(ListReply::new(Err(status))).await;
        let error = output.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("fake-secret-do-not-print"));
        assert!(!error.contains('\u{1b}'));
    }
}
