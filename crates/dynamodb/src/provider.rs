use anyhow::{Result, anyhow, ensure};
use aws_sdk_dynamodb::{Client, config::Credentials};
use onetui_core::{
    Page,
    provider::{
        CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
        QueryDescriptor, QueryRequest, RequestContext, ShutdownContext,
    },
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, MutexGuard, watch};

pub struct DynamoDbProvider;
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[
        onetui_core::provider::ConnectionField::text("region"),
        onetui_core::provider::ConnectionField::text("profile"),
        onetui_core::provider::ConnectionField::text("endpoint_url"),
        onetui_core::provider::ConnectionField::text("streams_endpoint_url"),
        onetui_core::provider::ConnectionField::text("access_key_id_env"),
        onetui_core::provider::ConnectionField::text("secret_access_key_env"),
        onetui_core::provider::ConnectionField::text("session_token_env"),
    ],
    kind: "dynamodb",
    entry_resource: Some("dynamodb.resources"),
    browsing: "tables / items / metadata / replicas / streams / shards / records",
    follow_resources: &["dynamodb.records"],
    query: Some(QueryDescriptor {
        resource: "dynamodb.query",
        language: "DynamoDB read JSON",
        watermark: "{\n  \"operation\": \"Scan\",\n  \"limit\": 100\n}",
        contextual_watermark: Some(crate::query::watermark),
        path_depth: 1,
        scope_resources: &[
            "dynamodb.table",
            "dynamodb.items",
            "dynamodb.query",
            "dynamodb.table_info",
            "dynamodb.indexes",
            "dynamodb.stream",
            "dynamodb.stream_info",
            "dynamodb.shards",
            "dynamodb.shard_details",
            "dynamodb.records",
        ],
    }),
    resources: crate::browse::RESOURCES,
    documentation: crate::capabilities,
};

impl Provider for DynamoDbProvider {
    type Executor = DynamoDbExecutor;
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
    ) -> Result<DynamoDbExecutor> {
        let config = crate::config::Config::parse(options)?;
        let (credentials, secrets) = config.credentials(env)?;
        Ok(DynamoDbExecutor {
            config,
            credentials,
            secrets,
            client: Mutex::new(None),
            session: NEXT_SESSION.fetch_add(1, Ordering::Relaxed),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

pub struct DynamoDbExecutor {
    config: crate::config::Config,
    credentials: Option<Credentials>,
    secrets: Vec<String>,
    client: Mutex<Option<Session>>,
    session: u64,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Session {
    database: Client,
    streams: aws_sdk_dynamodbstreams::Client,
}

struct Lease<'a> {
    client: MutexGuard<'a, Option<Session>>,
    status: &'a watch::Sender<ConnectionStatus>,
    clean: bool,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        if !self.clean {
            self.client.take();
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
    }
}

impl DynamoDbExecutor {
    async fn client(&self) -> Result<Session> {
        let shared = self.config.load(self.credentials.clone()).await?;
        let mut config = aws_sdk_dynamodb::config::Builder::from(&shared);
        // Endpoint selection belongs to this alias, not AWS_ENDPOINT_URL or profile services.
        config.set_endpoint_url(self.config.endpoint_url.clone());
        let mut streams = aws_sdk_dynamodbstreams::config::Builder::from(&shared);
        streams.set_endpoint_url(self.config.streams_endpoint_url.clone());
        Ok(Session {
            database: Client::from_conf(config.build()),
            streams: aws_sdk_dynamodbstreams::Client::from_conf(streams.build()),
        })
    }

    async fn read(
        &self,
        request: PageRequest,
        text: &str,
        follow: bool,
        mut context: RequestContext,
    ) -> Result<Page> {
        ensure!(!self.closed, "DynamoDB session is closed");
        ensure!(
            !follow || request.resource.id == "dynamodb.records",
            "Only DynamoDB shard records support following"
        );
        let bookmark_scope = if follow { "follow" } else { text };
        let position = crate::browse::position(&request, self.session, bookmark_scope)?;
        if matches!(
            request.resource.id,
            "dynamodb.resources" | "dynamodb.table" | "dynamodb.stream"
        ) {
            ensure!(position.is_none(), "DynamoDB menus have no continuation");
            return context
                .run(async { crate::browse::menu(request.resource.id, &request.resource.path) })
                .await;
        }
        let query = if matches!(request.resource.id, "dynamodb.items" | "dynamodb.query") {
            Some(crate::query::Read::parse(text)?)
        } else {
            None
        };
        if let Some(query) = &query {
            query.validate_scope(&request.resource.path[0])?;
        }
        let stream_read = crate::streams::is_resource(request.resource.id)
            || matches!(query, Some(crate::query::Read::GetRecords { .. }));
        ensure!(
            !stream_read
                || self.config.endpoint_url.is_none()
                || self.config.streams_endpoint_url.is_some(),
            "Custom DynamoDB endpoints require streams_endpoint_url for Streams reads"
        );
        if matches!(query, Some(crate::query::Read::GetRecords { .. })) {
            ensure!(
                request.resource.path[0].starts_with("arn:")
                    && request.resource.path[0].contains(":table/")
                    && request.resource.path[0].contains("/stream/"),
                "GetRecords requires a selected stream ARN; open Streams first"
            );
        }
        let mut lease = Lease {
            client: context.run(self.client.lock()).await?,
            status: &self.status,
            clean: false,
        };
        let result = context
            .run(async {
                if lease.client.is_none() {
                    self.status.send_replace(ConnectionStatus::Connecting);
                    *lease.client = Some(self.client().await?);
                }
                let session = lease.client.as_ref().unwrap();
                let client = &session.database;
                let name = request
                    .resource
                    .path
                    .first()
                    .map(String::as_str)
                    .unwrap_or("");
                let (mut page, token) = if let Some(crate::query::Read::GetRecords {
                    shard_id,
                    sequence_number,
                    after,
                    limit,
                }) = &query
                {
                    crate::streams::replay(
                        &session.streams,
                        name,
                        shard_id,
                        sequence_number,
                        *after,
                        *limit,
                        position.as_ref(),
                    )
                    .await?
                } else if stream_read {
                    crate::streams::read(
                        &session.streams,
                        &request.resource,
                        position.as_ref(),
                        follow,
                    )
                    .await?
                } else if let Some(query) = query {
                    let batch = matches!(query, crate::query::Read::BatchGetItem { .. });
                    let transaction = matches!(query, crate::query::Read::TransactGetItems { .. } | crate::query::Read::ExecuteTransaction { .. });
                    let statement_batch = matches!(query, crate::query::Read::BatchExecuteStatement { .. });
                    let statement_limit = match &query {
                        crate::query::Read::ExecuteStatement { limit, .. } => Some(*limit as usize),
                        _ => None,
                    };
                    let vector_limit = match &query {
                        crate::query::Read::SearchVectors { top_k, .. } => Some(*top_k as usize),
                        _ => None,
                    };
                    let body = crate::api::query(client, name, query, position.as_ref()).await?;
                    if let Some(limit) = statement_limit {
                        crate::partiql::page(body, limit)?
                    } else if statement_batch {
                        let (mut page, _) = crate::browse::metadata("dynamodb.query", body)?;
                        page.notice = "Native PartiQL batch response; inspect each Responses entry for item or error; not atomic, no automatic retries".into();
                        (page, None)
                    } else if let Some(limit) = vector_limit {
                        if let Some(results) = body.get("SearchResults") {
                            ensure!(results.as_array().is_some_and(|results| results.len() <= limit), "DynamoDB SearchResults must be an array within top_k");
                        }
                        let (mut page, _) = crate::browse::metadata("dynamodb.query", body)?;
                        page.notice = "Native vector response; SearchResults retain service ranking, scores, typed items and capacity; no continuation".into();
                        (page, None)
                    } else if batch || transaction {
                        crate::browse::multi_items(body, name, batch)?
                    } else {
                        crate::browse::items(body)?
                    }
                } else {
                    crate::browse::metadata(
                        request.resource.id,
                        crate::api::metadata(client, request.resource.id, &request.resource.path, position.as_ref())
                            .await?,
                    )?
                };
                let closed = stream_read && token.as_ref().is_some_and(crate::streams::closed);
                crate::browse::continuation(
                    &mut page,
                    &request,
                    self.session,
                    bookmark_scope,
                    token,
                )?;
                if closed {
                    page.next = false;
                }
                if follow {
                    ensure!(
                        page.continuation.as_ref().is_some_and(|c| c.len() <= 4096),
                        "DynamoDB live bookmark exceeds 4 KiB"
                    );
                }
                Ok::<_, anyhow::Error>(page)
            })
            .await
            .and_then(|r| r);
        if result.is_ok() {
            lease.clean = true;
            self.status.send_replace(ConnectionStatus::Connected);
        }
        result.map_err(|error| {
            onetui_core::diagnostic(
                error,
                &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
    }
}

impl Executor for DynamoDbExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }
    async fn check(&self, mut context: RequestContext) -> Result<CheckResult> {
        ensure!(!self.closed, "DynamoDB session is closed");
        let mut lease = Lease {
            client: context.run(self.client.lock()).await?,
            status: &self.status,
            clean: false,
        };
        let result = context
            .run(async {
                if lease.client.is_none() {
                    self.status.send_replace(ConnectionStatus::Connecting);
                    *lease.client = Some(self.client().await?);
                }
                let capture = crate::response::Capture::default();
                let result = lease
                    .client
                    .as_ref()
                    .unwrap()
                    .database
                    .list_tables()
                    .limit(1)
                    .customize()
                    .config_override(
                        aws_sdk_dynamodb::config::Builder::new().retry_classifier(capture.clone()),
                    )
                    .interceptor(capture.clone())
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("{}", aws_sdk_dynamodb::error::DisplayErrorContext(e)));
                capture.finish(result)?;
                Ok::<_, anyhow::Error>(CheckResult {
                    summary: "DynamoDB table inventory readable".into(),
                })
            })
            .await
            .and_then(|r| r);
        if result.is_ok() {
            lease.clean = true;
            self.status.send_replace(ConnectionStatus::Connected);
        }
        result.map_err(|e| {
            onetui_core::diagnostic(
                e,
                &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
    }
    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        ensure!(
            request.resource.id != "dynamodb.query",
            "Use query_page for DynamoDB queries"
        );
        let text = if request.resource.id == "dynamodb.items" {
            r#"{"operation":"Scan"}"#
        } else {
            ""
        };
        self.read(request, text, false, context).await
    }
    async fn follow_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        self.read(request, "", true, context).await
    }
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "dynamodb.query",
            "Invalid DynamoDB query resource"
        );
        self.read(request.page, &request.text, false, context).await
    }
    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        self.status.send_replace(ConnectionStatus::Closing);
        let result = tokio::time::timeout_at(context.deadline, self.client.lock()).await;
        self.closed = true;
        match result {
            Ok(mut client) => {
                client.take();
                self.status.send_replace(ConnectionStatus::Closed);
                Ok(())
            }
            Err(error) => {
                self.status.send_replace(ConnectionStatus::Disconnected);
                Err(error.into())
            }
        }
    }
}
