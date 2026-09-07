//! Exercises the Qdrant connector against a local gRPC peer, without a database fixture.
use std::convert::Infallible;
use std::task::{Context, Poll};

use qdrant_client::qdrant::{
    CollectionDescription, ListCollectionsRequest, ListCollectionsResponse,
};
use tonic::codegen::{BoxFuture, Service, http};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct ListReply(Result<ListCollectionsResponse, Status>);

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
        let reply = self.clone();
        Box::pin(async move {
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
    let output = onetui_qdrant::check(
        &format!("http://{address}"),
        None,
        std::time::Duration::from_secs(2),
    )
    .await;
    server.abort();
    let _ = server.await;
    output
}

#[tokio::test]
async fn oversized_metadata_is_rejected_with_a_useful_limit_error() {
    let output = run_reply(ListReply(Ok(ListCollectionsResponse {
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
    let output = run_reply(ListReply(Ok(ListCollectionsResponse::default()))).await;
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
        let output = run_reply(ListReply(Err(status))).await;
        let error = output.unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("fake-secret-do-not-print"));
        assert!(!error.contains('\u{1b}'));
    }
}
