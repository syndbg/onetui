use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);

use anyhow::{Result, anyhow, ensure};
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, Page, Resource};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, watch};
use tokio_postgres::Client;
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct PostgresProvider;

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "postgres",
    entry_resource: Some("postgres.schemas"),
    browsing: "rows + metadata",
    resources: &[
        &crate::SCHEMAS,
        &crate::RELATIONS,
        &crate::COLUMNS,
        &crate::ROWS,
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

    async fn read(
        &self,
        request: Option<PageRequest>,
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
        let result = context.run(async {
            if lease.session.is_none() {
                self.status.send_replace(ConnectionStatus::Connecting);
                let mut config = crate::check::postgres_config(&self.url, deadline.saturating_duration_since(tokio::time::Instant::now()))?;
                config.application_name(if request.is_some() { "onetui-browse" } else { "onetui-check" });
                let tls = crate::check::postgres_tls(&config, self.ca_file.as_deref()).await?;
                let (client, connection) = config.connect(tls.clone()).await.map_err(crate::browse::pg_error)?;
                let status = self.status.clone();
                let driver = Driver(tokio::spawn(async move {
                    let _ = connection.await;
                    status.send_if_modified(|state| {
                        if matches!(*state, ConnectionStatus::Closing | ConnectionStatus::Closed) { return false; }
                        *state = ConnectionStatus::Disconnected;
                        true
                    });
                }));
                *lease.session = Some(Session { client, driver, tls });
                self.status.send_replace(ConnectionStatus::Connected);
            }
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
                    let mut page = if request.resource.id == "postgres.rows" {
                        crate::rows::fetch(client, &request.resource, position.offset, position.inner.as_deref()).await?
                    } else {
                        crate::browse::metadata(client, &request.resource, position.offset).await?
                    };
                    page.continuation = if page.next {
                        Some(serde_json::to_string(&Position {
                            session: self.identity,
                            resource: request.resource.id.into(), path: request.resource.path,
                            offset: position.offset.checked_add(onetui_core::PAGE_SIZE).ok_or_else(|| anyhow!("Page offset exhausted; refresh"))?,
                            inner: page.continuation.take(),
                        })?)
                    } else { None };
                    ensure!(page.bytes() <= PAGE_BYTES, "Page exceeds the 1 MiB display limit; current page retained");
                    Ok(ReadResult::Page(page))
                }
            }
        }).await.and_then(|r| r);
        if result.is_ok() {
            lease.clean = true;
        } else {
            let _ = close(
                lease.session.take(),
                ShutdownContext::new(Duration::from_secs(1)),
                true,
            )
            .await;
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result.map_err(|error| {
            let config = self.url.parse::<tokio_postgres::Config>().ok();
            let password = config
                .as_ref()
                .and_then(|config| config.get_password())
                .map(String::from_utf8_lossy);
            onetui_core::diagnostic(error, &[&self.url, password.as_deref().unwrap_or("")])
        })
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
        match self.read(None, context).await? {
            ReadResult::Check(result) => Ok(result),
            ReadResult::Page(_) => unreachable!(),
        }
    }

    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        match self.read(Some(request), context).await? {
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
