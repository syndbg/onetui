use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

use anyhow::{Result, anyhow, ensure};
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryDescriptor, QueryExecution, QueryRequest, RequestContext, ShutdownContext, WriteOutcome,
    WriteResult,
};
use onetui_core::{PAGE_BYTES, Page, Resource};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct PostgresProvider;

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[
        onetui_core::provider::ConnectionField::text("url_env"),
        onetui_core::provider::ConnectionField::text("ca_file"),
    ],
    follow_resources: &[],
    query: Some(QueryDescriptor {
        resource: "postgres.query",
        language: "SQL",
        watermark: "SELECT 1 AS value",
        contextual_watermark: None,
        path_depth: 0,
        scope_resources: &[],
    }),
    kind: "postgres",
    entry_resource: Some("postgres.resources"),
    browsing: "rows, metadata and connected replication processes",
    resources: &[
        &crate::replication::ROOT,
        &crate::SCHEMAS,
        &crate::RELATIONS,
        &crate::COLUMNS,
        &crate::ROWS,
        &crate::query::RESOURCE,
        &crate::replication::REPLICAS,
        &crate::replication::RECEIVER,
    ],
    documentation: crate::capabilities,
};

impl Provider for PostgresProvider {
    type Executor = PostgresExecutor;

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
    ) -> Result<PostgresExecutor> {
        let config = crate::config::Config::parse(options)?;
        let url = onetui_core::config::secret(&config.url_env, env)?;
        // Parse without opening transport or loading trust stores.
        crate::check::postgres_config(&url, Duration::from_secs(5))?;
        Ok(PostgresExecutor::new(url, config.ca_file))
    }
}

// Neither configuration nor client state may expose credentials through Debug.
pub struct PostgresExecutor {
    identity: u64,
    url: String,
    ca_file: Option<PathBuf>,
    session: Mutex<Option<Session>>,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Driver(tokio::task::JoinHandle<()>);

impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Session {
    client: Client,
    driver: Driver,
    tls: MakeRustlsConnect,
}

struct Lease<'a> {
    session: MutexGuard<'a, Option<Session>>,
    clean: bool,
}

impl Drop for Lease<'_> {
    fn drop(&mut self) {
        // A dropped operation future must not leave work running or reusable protocol state.
        if !self.clean {
            self.session.take();
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    session: u64,
    resource: String,
    path: Vec<String>,
    offset: i64,
    inner: Option<String>,
    #[serde(default)]
    query: Option<String>,
}

enum ReadResult {
    Check(CheckResult),
    Page(Page),
}

impl PostgresExecutor {
    pub(crate) fn new(url: String, ca_file: Option<PathBuf>) -> Self {
        Self {
            identity: NEXT_SESSION.fetch_add(1, Ordering::Relaxed),
            url,
            ca_file,
            session: Mutex::new(None),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        }
    }

    async fn connect(
        &self,
        session: &mut Option<Session>,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        if session.is_some() {
            return Ok(());
        }
        self.status.send_replace(ConnectionStatus::Connecting);
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let config = crate::check::postgres_config(&self.url, remaining)?;
        let tls = crate::check::postgres_tls(&config, self.ca_file.as_deref()).await?;
        let (client, connection) = config
            .connect(tls.clone())
            .await
            .map_err(crate::browse::pg_error)?;
        let status = self.status.clone();
        let driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
            status.send_if_modified(|state| {
                if matches!(*state, ConnectionStatus::Closing | ConnectionStatus::Closed) {
                    return false;
                }
                *state = ConnectionStatus::Disconnected;
                true
            });
        }));
        *session = Some(Session {
            client,
            driver,
            tls,
        });
        self.status.send_replace(ConnectionStatus::Connected);
        Ok(())
    }

    fn diagnostic(&self, error: anyhow::Error) -> anyhow::Error {
        let config = self.url.parse::<tokio_postgres::Config>().ok();
        let password = config
            .as_ref()
            .and_then(|config| config.get_password())
            .map(String::from_utf8_lossy);
        onetui_core::diagnostic(error, &[&self.url, password.as_deref().unwrap_or("")])
    }

    async fn read(
        &self,
        request: Option<PageRequest>,
        query: Option<String>,
        mut context: RequestContext,
    ) -> Result<ReadResult> {
        ensure!(!self.closed, "PostgreSQL session is closed");
        let guard = context.run(self.session.lock()).await?;
        let mut lease = Lease {
            session: guard,
            clean: false,
        };
        if lease.session.as_ref().is_some_and(|s| s.client.is_closed()) {
            close(
                lease.session.take(),
                ShutdownContext::new(Duration::from_secs(1)),
                false,
            )
            .await?;
        }
        let deadline = context.deadline;
        let attempt = context.run(async {
            self.connect(&mut lease.session, deadline).await?;
            let client = &lease.session.as_ref().expect("connected session").client;
            let milliseconds = deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis().max(1).to_string();
            client.query_one("SELECT pg_catalog.set_config('statement_timeout', $1, false)", &[&milliseconds]).await.map_err(crate::browse::pg_error)?;
            match request {
                None => {
                    let row = client.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE pg_catalog.has_schema_privilege(oid, 'USAGE'))", &[]).await.map_err(crate::browse::pg_error)?;
                    Ok(ReadResult::Check(CheckResult { summary: if row.get::<_, bool>(0) { "schema metadata readable" } else { "connected; no accessible schemas" }.into() }))
                }
                Some(request) => {
                    let position = decode(self.identity, &request.resource, request.continuation.as_deref())?;
                    ensure!(position.offset == 0 || position.query == query, "Continuation belongs to another query; run from the beginning");
                    let mut page = if let Some(text) = &query {
                        crate::query::fetch(client, text, position.offset).await?
                    } else if request.resource.id == "postgres.rows" {
                        crate::rows::fetch(client, &request.resource, position.offset, position.inner.as_deref()).await?
                    } else if matches!(request.resource.id, "postgres.replication" | "postgres.wal_receiver") {
                        crate::replication::fetch(client, &request.resource, position.offset).await?
                    } else {
                        crate::browse::metadata(client, &request.resource, position.offset).await?
                    };
                    page.continuation = if page.next {
                        Some(serde_json::to_string(&Position {
                            session: self.identity,
                            resource: request.resource.id.into(), path: request.resource.path,
                            offset: position.offset.checked_add(onetui_core::PAGE_SIZE).ok_or_else(|| anyhow!("Page offset exhausted; refresh"))?,
                            inner: page.continuation.take(),
                            query,
                        })?)
                    } else { None };
                    ensure!(page.bytes() <= PAGE_BYTES, "Page exceeds the 1 MiB display limit; current page retained");
                    Ok(ReadResult::Page(page))
                }
            }
        }).await;
        let interrupted = attempt.is_err();
        let result = attempt.and_then(|result| result);
        if result.is_ok() {
            lease.clean = true;
        } else if !interrupted && let Some(session) = lease.session.as_ref() {
            // A failed paged read may leave its read-only transaction open.
            lease.clean = context
                .run(async {
                    session.client.batch_execute("ROLLBACK").await?;
                    session.client.batch_execute("DISCARD ALL").await
                })
                .await
                .is_ok_and(|result| result.is_ok());
        }
        if !lease.clean {
            let _ = close(
                lease.session.take(),
                ShutdownContext::new(Duration::from_secs(1)),
                true,
            )
            .await;
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result.map_err(|error| self.diagnostic(error))
    }
}

fn decode(session: u64, resource: &Resource, token: Option<&str>) -> Result<Position> {
    ensure!(
        DESCRIPTOR.resource(resource.id).is_some_and(|r| r.paging),
        "Unsupported PostgreSQL resource"
    );
    match token {
        None => Ok(Position {
            session,
            resource: resource.id.into(),
            path: resource.path.clone(),
            offset: 0,
            inner: None,
            query: None,
        }),
        Some(token) => {
            ensure!(token.len() <= PAGE_BYTES, "Invalid continuation; refresh");
            let position: Position = serde_json::from_str(token)
                .map_err(|_| anyhow!("Invalid continuation; refresh"))?;
            ensure!(
                position.session == session
                    && position.resource == resource.id
                    && position.path == resource.path
                    && position.offset > 0,
                "Mismatched continuation; refresh"
            );
            Ok(position)
        }
    }
}

async fn close(session: Option<Session>, context: ShutdownContext, cancel: bool) -> Result<()> {
    let Some(Session {
        client,
        mut driver,
        tls,
    }) = session
    else {
        return Ok(());
    };
    if cancel {
        let _ = tokio::time::timeout_at(context.deadline, client.cancel_token().cancel_query(tls))
            .await;
    }
    drop(client);
    if tokio::time::timeout_at(context.deadline, &mut driver.0)
        .await
        .is_err()
    {
        driver.0.abort();
        let _ = (&mut driver.0).await;
        anyhow::bail!("PostgreSQL shutdown timed out; connection discarded");
    }
    Ok(())
}

impl Executor for PostgresExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }

    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        match self.read(None, None, context).await? {
            ReadResult::Check(result) => Ok(result),
            ReadResult::Page(_) => unreachable!(),
        }
    }

    async fn fetch_page(&self, request: PageRequest, mut context: RequestContext) -> Result<Page> {
        ensure!(!self.closed, "PostgreSQL session is closed");
        if request.resource.id == "postgres.resources" {
            return context
                .run(std::future::ready(crate::replication::root(
                    &request.resource,
                    request.continuation.as_deref(),
                )))
                .await?;
        }
        ensure!(
            request.resource.id != "postgres.query",
            "Use the provider query operation"
        );
        match self.read(Some(request), None, context).await? {
            ReadResult::Page(page) => Ok(page),
            ReadResult::Check(_) => unreachable!(),
        }
    }

    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        let result = close(self.session.get_mut().take(), context, false).await;
        self.status.send_replace(ConnectionStatus::Closed);
        result
    }

    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "postgres.query" && request.page.resource.path.is_empty(),
            "Invalid SQL query resource"
        );
        match self
            .read(Some(request.page), Some(request.text), context)
            .await?
        {
            ReadResult::Page(page) => Ok(page),
            ReadResult::Check(_) => unreachable!(),
        }
    }

    async fn execute_query(
        &self,
        request: QueryRequest,
        mut context: RequestContext,
    ) -> Result<QueryExecution> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "postgres.query"
                && request.page.resource.path.is_empty()
                && request.page.continuation.is_none(),
            "Invalid SQL query resource"
        );
        ensure!(!self.closed, "PostgreSQL session is closed");
        let guard = context.run(self.session.lock()).await?;
        let mut lease = Lease {
            session: guard,
            clean: false,
        };
        if lease.session.as_ref().is_some_and(|s| s.client.is_closed()) {
            close(
                lease.session.take(),
                ShutdownContext::new(Duration::from_secs(1)),
                false,
            )
            .await?;
        }
        let deadline = context.deadline;
        let mut dispatched = false;
        let attempt = context
            .run(async {
                self.connect(&mut lease.session, deadline).await?;
                let client = &lease.session.as_ref().expect("connected session").client;
                let milliseconds = deadline
                    .saturating_duration_since(tokio::time::Instant::now())
                    .as_millis()
                    .max(1)
                    .to_string();
                client
                    .query_one(
                        "SELECT pg_catalog.set_config('statement_timeout', $1, false)",
                        &[&milliseconds],
                    )
                    .await
                    .map_err(crate::browse::pg_error)?;
                client.prepare(&request.text).await?;
                dispatched = true;
                crate::query::execute_once(client, &request.text).await
            })
            .await;
        let reusable = matches!(&attempt, Ok(Ok(_)))
            || matches!(&attempt, Ok(Err(error)) if error.downcast_ref::<tokio_postgres::Error>().is_some_and(|error| error.as_db_error().is_some()));
        let result = match attempt {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) if dispatched => {
                let rejected = error
                    .downcast_ref::<tokio_postgres::Error>()
                    .is_some_and(|error| error.as_db_error().is_some());
                let outcome = if rejected {
                    WriteOutcome::Rejected
                } else {
                    WriteOutcome::Unknown
                };
                let error = match error.downcast::<tokio_postgres::Error>() {
                    Ok(error) => self.diagnostic(crate::browse::pg_error(error)),
                    Err(error) => self.diagnostic(error),
                };
                Ok(QueryExecution::Write(WriteResult {
                    outcome,
                    summary: if rejected {
                        format!("PostgreSQL rejected the statement: {error}")
                    } else {
                        format!(
                            "Statement outcome unknown: {error}. Inspect the target before retrying"
                        )
                    },
                }))
            }
            Err(error) if dispatched => Ok(QueryExecution::Write(WriteResult {
                outcome: WriteOutcome::Unknown,
                summary: format!(
                    "Statement outcome unknown: {}. Inspect the target before retrying",
                    self.diagnostic(error)
                ),
            })),
            Ok(Err(error)) | Err(error) => {
                let error = match error.downcast::<tokio_postgres::Error>() {
                    Ok(error) => crate::browse::pg_error(error),
                    Err(error) => error,
                };
                Err(self.diagnostic(error))
            }
        };
        let clean = if reusable {
            if let Some(session) = lease.session.as_ref() {
                // One-shot queries must not leave transaction or session state for browsing.
                tokio::time::timeout(Duration::from_secs(1), async {
                    session.client.batch_execute("ROLLBACK").await?;
                    session.client.batch_execute("DISCARD ALL").await
                })
                .await
                .is_ok_and(|result| result.is_ok())
            } else {
                false
            }
        } else {
            false
        };
        if clean {
            lease.clean = true;
        } else {
            let _ = close(
                lease.session.take(),
                ShutdownContext::new(Duration::from_secs(1)),
                !reusable && dispatched,
            )
            .await;
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_strict_and_only_configure_resolves_secrets() {
        let options = toml::from_str("url_env='DSN'").unwrap();
        PostgresProvider.validate_config(&options).unwrap();
        let executor = PostgresProvider
            .configure(&options, &|name| {
                assert_eq!(name, "DSN");
                Some("host=localhost sslmode=disable".into())
            })
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        for text in [
            "url_env='DSN'\nurl='secret'",
            "url_env='bad name'",
            "url_env='DSN'\nca_file='relative.pem'",
        ] {
            assert!(
                PostgresProvider
                    .validate_config(&toml::from_str(text).unwrap())
                    .is_err()
            );
        }
    }

    #[test]
    fn continuation_is_bound_to_executor_and_resource() {
        let resource = Resource::new("postgres.relations", vec!["public".into()]);
        let token = serde_json::to_string(&Position {
            query: None,
            session: 7,
            resource: resource.id.into(),
            path: resource.path.clone(),
            offset: 100,
            inner: None,
        })
        .unwrap();
        assert!(decode(7, &resource, Some(&token)).is_ok());
        assert!(decode(8, &resource, Some(&token)).is_err());
        assert!(
            decode(
                7,
                &Resource::new("postgres.relations", vec!["other".into()]),
                Some(&token)
            )
            .is_err()
        );
        assert!(decode(7, &Resource::new("postgres.columns", vec![]), Some(&token)).is_err());
    }
}
