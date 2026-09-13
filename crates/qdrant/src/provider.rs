use anyhow::{Context, Result, anyhow, ensure};
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryDescriptor, QueryRequest, RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page};
use qdrant_client::qdrant::{
    GetCollectionInfoRequest, GetPointsBuilder, ListCollectionsRequest, ScrollPointsBuilder,
    collections_client::CollectionsClient, points_client::PointsClient,
};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio::time::Instant;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub struct QdrantProvider;
static NEXT_EXECUTOR: AtomicU64 = AtomicU64::new(1);

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    follow_resources: &[],
    query: Some(QueryDescriptor {
        resource: "qdrant.query",
        language: "Scroll JSON",
        example: "{\n  \"filter\": {\"must\": []},\n  \"limit\": 100\n}",
        path_depth: 1,
        scope_resources: &[],
    }),
    kind: "qdrant",
    entry_resource: Some("qdrant.collections"),
    browsing: "collections / points / payload / vectors",
    resources: crate::browse::RESOURCES,
    documentation: crate::capabilities,
};

impl Provider for QdrantProvider {
    type Executor = QdrantExecutor;
    fn descriptor(&self) -> &'static ProviderDescriptor {
        &DESCRIPTOR
    }
    fn validate_config(&self, options: &toml::Table) -> Result<()> {
        crate::config::Config::parse(options).map(|_| ())
    }
    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<QdrantExecutor> {
        let config = crate::config::Config::parse(options)?;
        let key = config
            .api_key_env
            .as_deref()
            .map(|name| onetui_core::config::secret(name, env))
            .transpose()?;
        let api_key = key
            .map(|key| {
                key.parse()
                    .map_err(|_| anyhow!("Qdrant API key is not valid ASCII metadata"))
            })
            .transpose()?;
        Ok(QdrantExecutor {
            url: config.url,
            api_key,
            client: Mutex::new(None),
            identity: NEXT_EXECUTOR.fetch_add(1, Ordering::Relaxed),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

pub struct QdrantExecutor {
    url: String,
    api_key: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    client: Mutex<Option<Channel>>,
    identity: u64,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Lease<'a> {
    channel: MutexGuard<'a, Option<Channel>>,
    status: &'a watch::Sender<ConnectionStatus>,
    clean: bool,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if !self.clean {
            self.channel.take();
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
    }
}

impl QdrantExecutor {
    fn request<T>(&self, message: T, deadline: Instant) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(deadline.saturating_duration_since(Instant::now()));
        if let Some(key) = &self.api_key {
            request.metadata_mut().insert("api-key", key.clone());
        }
        request
    }

    async fn execute<T, F, Fut>(&self, mut context: RequestContext, operation: F) -> Result<T>
    where
        F: FnOnce(Channel, Instant) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        ensure!(!self.closed, "Qdrant session is closed");
        let mut lease = Lease {
            channel: context.run(self.client.lock()).await?,
            status: &self.status,
            clean: false,
        };
        let deadline = context.deadline;
        let result = context
            .run(async {
                if lease.channel.is_none() {
                    self.status.send_replace(ConnectionStatus::Connecting);
                    let url = crate::qdrant_url(&self.url)?;
                    let mut endpoint = Endpoint::from_shared(url.to_string())
                        .context("Qdrant endpoint")?
                        .connect_timeout(
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .keep_alive_while_idle(false);
                    if url.scheme() == "https" {
                        endpoint = endpoint
                            .tls_config(ClientTlsConfig::new().with_native_roots())
                            .context("Qdrant TLS")?;
                    }
                    let channel = endpoint.connect().await.context("Qdrant connection")?;
                    *lease.channel = Some(channel);
                }
                operation(
                    lease.channel.as_ref().expect("connected channel").clone(),
                    deadline,
                )
                .await
            })
            .await?;
        // Formatting runs on the worker too; check cancellation/deadline again after it.
        let result = context.run(std::future::ready(result)).await?;
        if result.is_ok() {
            lease.clean = true;
            self.status.send_replace(ConnectionStatus::Connected);
        }
        result.map_err(|error| {
            onetui_core::diagnostic(
                error,
                &[self
                    .api_key
                    .as_ref()
                    .and_then(|key| key.to_str().ok())
                    .unwrap_or("")],
            )
        })
    }
}

impl Executor for QdrantExecutor {
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        let scroll = crate::query::prepare(&request, self.identity)?;
        self.execute(context, |channel, deadline| async move {
            let result = PointsClient::new(channel)
                .max_decoding_message_size(PAGE_BYTES)
                .scroll(self.request(scroll, deadline))
                .await
                .map_err(crate::rpc_error)?
                .into_inner();
            let mut page = crate::browse::points(
                &request.page.resource,
                result.result,
                result.next_page_offset,
                self.identity,
            )?;
            page.continuation = page
                .continuation
                .map(|inner| crate::query::continuation(request.text, inner))
                .transpose()?;
            crate::browse::bounded(page)
        })
        .await
    }
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }

    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        self.execute(context, |channel, deadline| async move {
            // High-level SDK helpers remove this decoding limit.
            let response = CollectionsClient::new(channel)
                .max_decoding_message_size(PAGE_BYTES)
                .list(self.request(ListCollectionsRequest {}, deadline))
                .await
                .map_err(crate::rpc_error)?
                .into_inner();
            Ok(CheckResult {
                summary: format!(
                    "collection metadata readable ({} collections)",
                    response.collections.len()
                ),
            })
        })
        .await
    }

    async fn fetch_page(&self, request: PageRequest, mut context: RequestContext) -> Result<Page> {
        ensure!(!self.closed, "Qdrant session is closed");
        ensure!(
            request.resource.id != "qdrant.query",
            "Use the provider query operation"
        );
        let offset = crate::browse::validate(&request, self.identity)?;
        if let Some(page) = crate::browse::menu(&request.resource) {
            return context
                .run(std::future::ready(crate::browse::bounded(page)))
                .await?;
        }
        self.execute(context, |channel, deadline| async move {
            let resource = &request.resource;
            match resource.id {
                "qdrant.collections" => {
                    let mut client =
                        CollectionsClient::new(channel).max_decoding_message_size(PAGE_BYTES);
                    let result = client
                        .list(self.request(ListCollectionsRequest {}, deadline))
                        .await
                        .map_err(crate::rpc_error)?
                        .into_inner();
                    let offset = match offset {
                        Some(crate::browse::Offset::Collections(n)) => n,
                        _ => 0,
                    };
                    crate::browse::collections(
                        resource,
                        result.collections.into_iter().map(|c| c.name).collect(),
                        offset,
                        self.identity,
                    )
                }
                "qdrant.metadata" => {
                    let mut client =
                        CollectionsClient::new(channel).max_decoding_message_size(PAGE_BYTES);
                    let result = client
                        .get(self.request(
                            GetCollectionInfoRequest {
                                collection_name: resource.path[0].clone(),
                            },
                            deadline,
                        ))
                        .await
                        .map_err(crate::rpc_error)?
                        .into_inner();
                    crate::browse::metadata(
                        resource,
                        result.result.ok_or_else(|| {
                            anyhow!("Qdrant collection metadata missing; refresh its parent")
                        })?,
                    )
                }
                "qdrant.points" => {
                    let mut client =
                        PointsClient::new(channel).max_decoding_message_size(PAGE_BYTES);
                    let mut scroll = ScrollPointsBuilder::new(&resource.path[0])
                        .limit(PAGE_SIZE as u32)
                        .with_payload(false)
                        .with_vectors(false);
                    if let Some(crate::browse::Offset::Point(id)) = offset {
                        scroll = scroll.offset(id.native());
                    }
                    let result = client
                        .scroll(self.request(scroll.build(), deadline))
                        .await
                        .map_err(crate::rpc_error)?
                        .into_inner();
                    crate::browse::points(
                        resource,
                        result.result,
                        result.next_page_offset,
                        self.identity,
                    )
                }
                "qdrant.payload" | "qdrant.vectors" => {
                    let mut client =
                        PointsClient::new(channel).max_decoding_message_size(PAGE_BYTES);
                    let get = GetPointsBuilder::new(
                        &resource.path[0],
                        vec![crate::browse::point_id(resource)?],
                    )
                    .with_payload(resource.id == "qdrant.payload")
                    .with_vectors(resource.id == "qdrant.vectors")
                    .build();
                    let result = client
                        .get(self.request(get, deadline))
                        .await
                        .map_err(crate::rpc_error)?
                        .into_inner();
                    crate::browse::detail(resource, result.result)
                }
                _ => unreachable!("validated resource"),
            }
        })
        .await
    }

    async fn shutdown(&mut self, _context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        self.client.get_mut().take();
        self.status.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_validation_is_strict_and_offline() {
        let options =
            toml::from_str("url='https://example.invalid:6334'\napi_key_env='KEY'").unwrap();
        QdrantProvider.validate_config(&options).unwrap();
        let executor = QdrantProvider
            .configure(&options, &|name| {
                assert_eq!(name, "KEY");
                Some("fixture-only-key".into())
            })
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        for text in [
            "url='http://remote.invalid'",
            "url='http://localhost'\napi_key='secret'",
            "url='http://localhost'\napi_key_env='bad name'",
        ] {
            assert!(
                QdrantProvider
                    .validate_config(&toml::from_str(text).unwrap())
                    .is_err()
            );
        }
        assert!(
            QdrantProvider
                .configure(&options, &|_| Some("bad\nkey".into()))
                .is_err()
        );
    }
}
