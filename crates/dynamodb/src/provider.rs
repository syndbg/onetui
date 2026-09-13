use anyhow::{Result, anyhow, ensure};
use aws_sdk_dynamodb::{
    Client,
    config::{Credentials, retry::RetryConfig, timeout::TimeoutConfig},
};
use onetui_core::{
    Page,
    provider::{
        CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
        QueryDescriptor, QueryRequest, RequestContext, ShutdownContext,
    },
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::sync::{Mutex, MutexGuard, watch};

pub struct DynamoDbProvider;
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "dynamodb",
    entry_resource: Some("dynamodb.resources"),
    browsing: "tables / items / metadata / replicas",
    follow_resources: &[],
    query: Some(QueryDescriptor {
        resource: "dynamodb.query",
        language: "DynamoDB read JSON",
        example: "{\n  \"operation\": \"Scan\",\n  \"limit\": 100\n}",
        path_depth: 1,
        scope_resources: &[
            "dynamodb.table",
            "dynamodb.items",
            "dynamodb.query",
            "dynamodb.table_info",
            "dynamodb.indexes",
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
    client: Mutex<Option<Client>>,
    session: u64,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Lease<'a> {
    client: MutexGuard<'a, Option<Client>>,
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
    async fn client(&self) -> Client {
        let shared = self.config.load(self.credentials.clone()).await;
        let mut config = aws_sdk_dynamodb::config::Builder::from(&shared)
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(2))
                    .read_timeout(Duration::from_secs(5))
                    .operation_timeout(Duration::from_secs(10))
                    .build(),
            );
        // Endpoint selection belongs to this alias, not AWS_ENDPOINT_URL or profile services.
        config.set_endpoint_url(self.config.endpoint_url.clone());
        Client::from_conf(config.build())
    }

    async fn read(
        &self,
        request: PageRequest,
        text: &str,
        mut context: RequestContext,
    ) -> Result<Page> {
        ensure!(!self.closed, "DynamoDB session is closed");
        let position = crate::browse::position(&request, self.session, text)?;
        if matches!(request.resource.id, "dynamodb.resources" | "dynamodb.table") {
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
        let mut lease = Lease {
            client: context.run(self.client.lock()).await?,
            status: &self.status,
            clean: false,
        };
        let result = context
            .run(async {
                if lease.client.is_none() {
                    self.status.send_replace(ConnectionStatus::Connecting);
                    *lease.client = Some(self.client().await);
                }
                let client = lease.client.as_ref().unwrap();
                let name = request
                    .resource
                    .path
                    .first()
                    .map(String::as_str)
                    .unwrap_or("");
                let (mut page, token) = if let Some(query) = query {
                    crate::browse::items(
                        crate::api::query(client, name, query, position.as_ref()).await?,
                    )?
                } else {
                    crate::browse::metadata(
                        request.resource.id,
                        crate::api::metadata(client, request.resource.id, name, position.as_ref())
                            .await?,
                    )?
                };
                crate::browse::continuation(&mut page, &request, self.session, text, token)?;
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
                    *lease.client = Some(self.client().await);
                }
                let capture = crate::response::Capture::default();
                let result = lease
                    .client
                    .as_ref()
                    .unwrap()
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
        self.read(request, text, context).await
    }
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "dynamodb.query",
            "Invalid DynamoDB query resource"
        );
        self.read(request.page, &request.text, context).await
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
