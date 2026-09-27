use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::Engine;
use onetui_core::provider::{
    CheckResult, ConnectionField, ConnectionStatus, Executor, PageRequest, Provider,
    ProviderDescriptor, QueryDescriptor, QueryExecution, QueryRequest, RequestContext,
    ShutdownContext, WriteOutcome, WriteResult,
};
use onetui_core::{Column, PAGE_BYTES, PAGE_SIZE, Page, Resource, Row};
use rustls::pki_types::pem::PemObject;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::{
    ConnectionError, ConnectionPoolError, ConnectionSetupRequestErrorKind, DbError, ExecutionError,
    MetadataError, NewSessionError, PrepareError, RequestAttemptError,
};
use scylla::response::{PagingState, PagingStateResponse};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

use crate::config::{Config, quote};

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub struct CqlProvider;

static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "cql",
    entry_resource: Some("cql.keyspaces"),
    browsing: "keyspaces / tables / columns / rows",
    resources: crate::RESOURCES,
    query: Some(QueryDescriptor {
        resource: "cql.query",
        syntax: onetui_core::provider::Syntax::Sql {
            keywords: crate::CQL_KEYWORDS,
        },
        language: "CQL",
        watermark: "SELECT keyspace_name FROM system_schema.keyspaces",
        is_statement: false,
        contextual_watermark: Some(watermark),
        path_depth: 0,
        scope_resources: &[],
    }),
    follow_resources: &[],
    connection_fields: &[
        ConnectionField::list("nodes"),
        ConnectionField::text("username_env"),
        ConnectionField::text("password_env"),
        ConnectionField::boolean("tls"),
        ConnectionField::text("ca_file"),
        ConnectionField::text("keyspace"),
    ],
    documentation: crate::capabilities,
};

impl Provider for CqlProvider {
    type Executor = CqlExecutor;
    fn descriptor(&self) -> &'static ProviderDescriptor {
        &DESCRIPTOR
    }
    fn validate_config(&self, options: &toml::Table) -> Result<()> {
        Config::parse(options).map(|_| ())
    }
    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<CqlExecutor> {
        let config = Config::parse(options)?;
        let credentials = if let Some(user) =
            config.username.as_ref().or(config.username_env.as_ref())
        {
            let username = match &config.username {
                Some(value) => value.clone(),
                None => onetui_core::config::secret(user, env)?,
            };
            let password = match &config.password {
                Some(value) => value.clone(),
                None => onetui_core::config::secret(config.password_env.as_deref().unwrap(), env)?,
            };
            Some((username, password))
        } else {
            None
        };
        Ok(CqlExecutor {
            identity: NEXT_SESSION.fetch_add(1, Ordering::Relaxed),
            config,
            credentials,
            session: Mutex::new(None),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

// Credentials stay out of Debug, like every other executor.
pub struct CqlExecutor {
    identity: u64,
    config: Config,
    credentials: Option<(String, String)>,
    session: Mutex<Option<Arc<Session>>>,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

/// A continuation carries the driver's opaque paging state, bound to the session,
/// resource and query that produced it so it cannot resume a different read.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Position {
    session: u64,
    resource: String,
    path: Vec<String>,
    query: Option<String>,
    state: String,
}

/// One statement's first or next page, and whether it produced rows at all.
enum Outcome {
    Rows(Page),
    Applied,
}

impl CqlExecutor {
    /// The driver owns per-node pools and reconnects them itself, so one session lives
    /// until shutdown. A failed build leaves the slot empty and the next request retries.
    async fn session(&self) -> Result<Arc<Session>> {
        let mut slot = self.session.lock().await;
        if let Some(session) = slot.as_ref() {
            return Ok(session.clone());
        }
        self.status.send_replace(ConnectionStatus::Connecting);
        let mut builder = SessionBuilder::new()
            .known_nodes(&self.config.nodes)
            .connection_timeout(CONNECT_TIMEOUT);
        if let Some((user, password)) = &self.credentials {
            builder = builder.user(user, password);
        }
        if self.config.tls {
            builder = builder.tls_context(Some(tls(self.config.ca_file.clone()).await?));
        }
        if let Some(keyspace) = &self.config.keyspace {
            // Case-sensitive, so the name is used exactly as configured.
            builder = builder.use_keyspace(keyspace, true);
        }
        let session = Arc::new(builder.build().await?);
        *slot = Some(session.clone());
        Ok(session)
    }

    /// Run work with cancellation and the deadline, then settle status. Errors keep their
    /// driver type so callers can classify them; callers redact before returning.
    async fn run<T>(
        &self,
        context: &mut RequestContext,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        ensure!(!self.closed, "CQL session is closed");
        let result = context.run(work).await.and_then(|result| result);
        match &result {
            Ok(_) => {
                self.status.send_replace(ConnectionStatus::Connected);
            }
            // A server answer proves the connection works.
            Err(error) if answered(error) => {
                self.status.send_replace(ConnectionStatus::Connected);
            }
            Err(_) => {
                self.status.send_replace(ConnectionStatus::Disconnected);
            }
        }
        result
    }

    fn diagnostic(&self, error: anyhow::Error) -> anyhow::Error {
        let password = self
            .credentials
            .as_ref()
            .map(|(_, password)| password.as_str())
            .unwrap_or("");
        // A server answer carries its own code and message; the driver's wrapping
        // ("Preparation failed on every connection ...") only buries it.
        let error = match db_error(&error).or_else(|| setup_db_error(&error)) {
            Some((db, message)) => anyhow!("{db}: {message}"),
            None => error,
        };
        onetui_core::diagnostic(error, &[password])
    }

    /// Execute one page of a prepared statement. Preparing first means a statement the
    /// server cannot parse is refused before anything runs.
    async fn page(
        &self,
        text: &str,
        values: &[&str],
        state: Option<Vec<u8>>,
        resource: &Resource,
        query: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Result<Outcome> {
        let session = self.session().await?;
        let mut prepared = session.prepare(text).await?;
        prepared.set_page_size(PAGE_SIZE as i32);
        prepared.set_request_timeout(Some(remaining(deadline)));
        let state = state.map_or_else(PagingState::start, PagingState::new_from_raw_bytes);
        let (result, next) = session
            .execute_single_page(&prepared, values, state)
            .await?;
        // The server has answered; a failure from here on is local, not an unknown write.
        self.render(result, next, resource, query).context(Received)
    }

    fn render(
        &self,
        result: scylla::response::query_result::QueryResult,
        next: PagingStateResponse,
        resource: &Resource,
        query: Option<&str>,
    ) -> Result<Outcome> {
        let Ok(rows) = result.into_rows_result() else {
            return Ok(Outcome::Applied);
        };
        let columns = rows
            .column_specs()
            .iter()
            .map(|spec| Column {
                name: spec.name().into(),
                datatype: crate::value::type_name(spec.typ()),
            })
            .collect::<Vec<_>>();
        let mut page = Page {
            columns,
            notice: "Native CQL pages; each page is read at the time it is requested.".into(),
            ..Page::default()
        };
        for row in rows.rows::<scylla::value::Row>()? {
            let cells = row?
                .columns
                .iter()
                .map(|cell| crate::value::cell(cell.as_ref()))
                .collect::<Vec<_>>();
            let target = target(resource, &cells);
            page.rows.push(Row { cells, target });
            ensure!(
                page.bytes() <= PAGE_BYTES,
                "CQL page exceeds the 1 MiB display limit; current page retained"
            );
        }
        if let PagingStateResponse::HasMorePages { state } = next
            && let Some(bytes) = state.as_bytes_slice()
        {
            page.next = true;
            page.continuation = Some(serde_json::to_string(&Position {
                session: self.identity,
                resource: resource.id.into(),
                path: resource.path.clone(),
                query: query.map(Into::into),
                state: base64::engine::general_purpose::STANDARD.encode(bytes),
            })?);
        }
        Ok(Outcome::Rows(page))
    }

    fn resume(
        &self,
        resource: &Resource,
        query: Option<&str>,
        token: Option<&str>,
    ) -> Result<Option<Vec<u8>>> {
        let Some(token) = token else {
            return Ok(None);
        };
        ensure!(
            token.len() <= PAGE_BYTES,
            "Invalid CQL continuation; refresh"
        );
        let position: Position = serde_json::from_str(token)
            .map_err(|_| anyhow!("Invalid CQL continuation; refresh"))?;
        ensure!(
            position.session == self.identity
                && position.resource == resource.id
                && position.path == resource.path,
            "CQL continuation belongs to another session or resource; refresh"
        );
        ensure!(
            position.query.as_deref() == query,
            "Continuation belongs to another query; run from the beginning"
        );
        Ok(Some(
            base64::engine::general_purpose::STANDARD
                .decode(position.state)
                .map_err(|_| anyhow!("Invalid CQL continuation; refresh"))?,
        ))
    }
}

impl Executor for CqlExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }

    async fn check(&self, mut context: RequestContext) -> Result<CheckResult> {
        let deadline = context.deadline;
        self.run(&mut context, async {
            let resource = Resource::new("cql.query", vec![]);
            let Outcome::Rows(page) = self
                .page(
                    "SELECT release_version FROM system.local",
                    &[],
                    None,
                    &resource,
                    None,
                    deadline,
                )
                .await?
            else {
                bail!("CQL node returned no release version");
            };
            let version = page
                .rows
                .first()
                .and_then(|row| row.cells.first())
                .and_then(Option::as_ref)
                .and_then(onetui_core::Value::text)
                .unwrap_or("unknown");
            Ok(CheckResult {
                summary: format!("CQL node readable, release {version}"),
            })
        })
        .await
        .map_err(|error| self.diagnostic(error))
    }

    async fn fetch_page(&self, request: PageRequest, mut context: RequestContext) -> Result<Page> {
        ensure!(
            request.resource.id != "cql.query",
            "Use the provider query operation"
        );
        let (text, values) = statement(&request.resource)?;
        let state = self.resume(&request.resource, None, request.continuation.as_deref())?;
        let deadline = context.deadline;
        self.run(&mut context, async {
            let values = values.iter().map(String::as_str).collect::<Vec<_>>();
            match self
                .page(&text, &values, state, &request.resource, None, deadline)
                .await?
            {
                Outcome::Rows(page) => Ok(page),
                Outcome::Applied => bail!("CQL metadata read returned no rows"),
            }
        })
        .await
        .map_err(|error| self.diagnostic(error))
    }

    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        match self.execute_query(request, context).await? {
            QueryExecution::Page(page) => Ok(page),
            QueryExecution::Write(result) => Err(anyhow!(result.summary)),
        }
    }

    /// The statement is sent as written. Rows come back as a pageable result; anything
    /// else reports an outcome. A server error is a rejection; a request lost after it
    /// may have reached a node has an unknown outcome.
    async fn execute_query(
        &self,
        request: QueryRequest,
        mut context: RequestContext,
    ) -> Result<QueryExecution> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "cql.query" && request.page.resource.path.is_empty(),
            "Invalid CQL query resource"
        );
        let text = request.text.trim();
        let state = self.resume(
            &request.page.resource,
            Some(text),
            request.page.continuation.as_deref(),
        )?;
        let deadline = context.deadline;
        let mut dispatched = false;
        let result = self
            .run(&mut context, async {
                // Connecting first keeps a connection failure out of the unknown-outcome path.
                self.session().await?;
                dispatched = true;
                self.page(
                    text,
                    &[],
                    state,
                    &request.page.resource,
                    Some(text),
                    deadline,
                )
                .await
            })
            .await;
        match result {
            Ok(Outcome::Rows(page)) => Ok(QueryExecution::Page(page)),
            Ok(Outcome::Applied) => Ok(QueryExecution::Write(WriteResult {
                outcome: WriteOutcome::Applied,
                summary: "CQL statement applied".into(),
            })),
            // Classify on the driver's error type first; redaction rebuilds it as text.
            // A response that failed to render ran fine on the server: report the error.
            Err(error) if error.downcast_ref::<Received>().is_some() => Err(self.diagnostic(error)),
            Err(error) if refused(&error) => Ok(QueryExecution::Write(WriteResult {
                outcome: WriteOutcome::Rejected,
                summary: format!("CQL rejected the statement: {}", self.diagnostic(error)),
            })),
            Err(error) if dispatched => Ok(QueryExecution::Write(WriteResult {
                outcome: WriteOutcome::Unknown,
                summary: format!(
                    "CQL statement outcome unknown: {}. Inspect the target before retrying",
                    self.diagnostic(error)
                ),
            })),
            Err(error) => Err(self.diagnostic(error)),
        }
    }

    async fn shutdown(&mut self, _context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        self.session.get_mut().take();
        self.status.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}

/// Marks a failure after the server responded, such as a page over the display limit.
#[derive(Debug)]
struct Received;

impl std::fmt::Display for Received {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CQL response")
    }
}

/// The server's own error, from preparing or executing.
fn db_error(error: &anyhow::Error) -> Option<(&DbError, &str)> {
    if let Some(ExecutionError::LastAttemptError(RequestAttemptError::DbError(db, message))) =
        error.downcast_ref::<ExecutionError>()
    {
        return Some((db, message));
    }
    match error.downcast_ref::<PrepareError>() {
        Some(PrepareError::AllAttemptsFailed {
            first_attempt: RequestAttemptError::DbError(db, message),
        }) => Some((db, message)),
        _ => None,
    }
}

/// A server refusal while opening the session, such as bad credentials. The driver
/// wraps it in four layers of pool and metadata errors. Kept apart from `db_error` so a
/// failed login does not count as a working connection.
fn setup_db_error(error: &anyhow::Error) -> Option<(&DbError, &str)> {
    let Some(NewSessionError::MetadataError(MetadataError::ConnectionPoolError(
        ConnectionPoolError::Broken {
            last_connection_error: ConnectionError::ConnectionSetupRequestError(setup),
        },
    ))) = error.downcast_ref::<NewSessionError>()
    else {
        return None;
    };
    match &setup.error {
        ConnectionSetupRequestErrorKind::DbError(db, message) => Some((db, message)),
        _ => None,
    }
}

/// The server responded, so the connection works even though the request failed.
fn answered(error: &anyhow::Error) -> bool {
    db_error(error).is_some() || error.downcast_ref::<Received>().is_some()
}

/// The server refused before applying anything. Coordinator timeouts and replica
/// failures are excluded: some replicas may already hold the write, so its outcome is
/// unknown, and retrying a counter or list append would apply it twice.
fn refused(error: &anyhow::Error) -> bool {
    db_error(error).is_some_and(|(db, _)| {
        !matches!(
            db,
            DbError::ReadTimeout { .. }
                | DbError::WriteTimeout { .. }
                | DbError::ReadFailure { .. }
                | DbError::WriteFailure { .. }
                | DbError::ServerError
                | DbError::TruncateError
                | DbError::Other(_)
        )
    })
}

fn remaining(deadline: tokio::time::Instant) -> Duration {
    deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .max(Duration::from_millis(1))
}

/// The statement and bind values a browse resource reads. Metadata comes from
/// `system_schema`, present on Cassandra 3+ and ScyllaDB.
fn statement(resource: &Resource) -> Result<(String, Vec<String>)> {
    ensure!(
        resource
            .path
            .iter()
            .all(|part| !part.is_empty() && part.len() <= 4096),
        "Invalid CQL resource path"
    );
    Ok(match (resource.id, resource.path.as_slice()) {
        ("cql.keyspaces", []) => (
            "SELECT keyspace_name, replication, durable_writes FROM system_schema.keyspaces"
                .into(),
            vec![],
        ),
        ("cql.tables", [keyspace]) => (
            "SELECT table_name, comment FROM system_schema.tables WHERE keyspace_name = ?".into(),
            vec![keyspace.clone()],
        ),
        ("cql.columns", [keyspace, table]) => (
            "SELECT column_name, kind, position, type FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?"
                .into(),
            vec![keyspace.clone(), table.clone()],
        ),
        ("cql.rows", [keyspace, table]) => (
            format!("SELECT * FROM {}.{}", quote(keyspace), quote(table)),
            vec![],
        ),
        _ => bail!("Unsupported CQL resource or path"),
    })
}

/// Where Enter on a row leads.
fn target(resource: &Resource, cells: &[Option<onetui_core::Value>]) -> Option<Resource> {
    let name = cells.first()?.as_ref()?.text()?.to_owned();
    match (resource.id, resource.path.as_slice()) {
        ("cql.keyspaces", []) => Some(Resource::new("cql.tables", vec![name])),
        ("cql.tables", [keyspace]) => Some(Resource::new("cql.rows", vec![keyspace.clone(), name])),
        _ => None,
    }
}

/// Prefill the editor with a read of whatever table the view has open.
fn watermark(resource: &Resource, row: Option<&Row>) -> String {
    let selected = row
        .and_then(|row| row.cells.first())
        .and_then(Option::as_ref)
        .and_then(onetui_core::Value::text);
    let path = resource.path.iter().map(String::as_str).collect::<Vec<_>>();
    match (resource.id, path.as_slice(), selected) {
        ("cql.rows" | "cql.columns", &[keyspace, table, ..], _)
        | ("cql.tables", &[keyspace], Some(table)) => {
            format!("SELECT * FROM {}.{}", quote(keyspace), quote(table))
        }
        ("cql.keyspaces", &[], Some(keyspace)) | ("cql.tables", &[keyspace], None) => format!(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{}'",
            keyspace.replace('\'', "''")
        ),
        _ => "SELECT keyspace_name FROM system_schema.keyspaces".into(),
    }
}

/// Trust roots load off the async workers: native store discovery can block.
async fn tls(ca_file: Option<std::path::PathBuf>) -> Result<Arc<rustls::ClientConfig>> {
    tokio::task::spawn_blocking(move || {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = ca_file {
            for certificate in rustls::pki_types::CertificateDer::pem_file_iter(path)? {
                roots.add(certificate?)?;
            }
        } else {
            for certificate in rustls_native_certs::load_native_certs().certs {
                roots.add(certificate)?;
            }
        }
        ensure!(!roots.is_empty(), "No CQL TLS trust roots available");
        Ok(Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
        ))
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn executor() -> CqlExecutor {
        CqlProvider
            .configure(&"nodes = ['127.0.0.1:9042']".parse().unwrap(), &|_| None)
            .unwrap()
    }

    #[test]
    fn browse_statements_quote_names_and_bind_values() {
        let (text, values) = statement(&Resource::new(
            "cql.rows",
            vec!["Ks".into(), r#"a"b"#.into()],
        ))
        .unwrap();
        // Names reach the statement quoted, so case and quotes survive as one token.
        assert_eq!(text, r#"SELECT * FROM "Ks"."a""b""#);
        assert!(values.is_empty());
        // Metadata filters are bound, never interpolated.
        let (text, values) =
            statement(&Resource::new("cql.tables", vec!["x' OR 1=1".into()])).unwrap();
        assert!(text.ends_with("keyspace_name = ?"));
        assert_eq!(values, vec!["x' OR 1=1".to_owned()]);
        for resource in [
            Resource::new("cql.keyspaces", vec!["extra".into()]),
            Resource::new("cql.tables", vec![]),
            Resource::new("cql.rows", vec!["ks".into(), String::new()]),
            Resource::new("cql.unknown", vec![]),
        ] {
            assert!(statement(&resource).is_err(), "{}", resource.id);
        }
    }

    #[test]
    fn only_errors_that_applied_nothing_are_rejections() {
        use scylla::frame::types::Consistency;
        let executed = |db: DbError| {
            anyhow::Error::new(ExecutionError::LastAttemptError(
                RequestAttemptError::DbError(db, "server message".into()),
            ))
        };
        // Refused outright: nothing ran, so retrying is safe.
        for error in [
            executed(DbError::Unauthorized),
            executed(DbError::SyntaxError),
            executed(DbError::Unavailable {
                consistency: Consistency::One,
                required: 1,
                alive: 0,
            }),
            anyhow::Error::new(PrepareError::AllAttemptsFailed {
                first_attempt: RequestAttemptError::DbError(DbError::Invalid, "bad".into()),
            }),
        ] {
            assert!(refused(&error) && answered(&error), "{error}");
        }
        // The shown error is the server's code and message, not the driver's wrapping.
        let prepared = executor().diagnostic(anyhow::Error::new(PrepareError::AllAttemptsFailed {
            first_attempt: RequestAttemptError::DbError(
                DbError::SyntaxError,
                "line 1:7 no viable alternative at input 'FROM'".into(),
            ),
        }));
        assert_eq!(
            prepared.to_string(),
            format!(
                "{}: line 1:7 no viable alternative at input 'FROM'",
                DbError::SyntaxError
            )
        );
        // The coordinator answered, but replicas may hold the write: outcome unknown.
        let timeout = executed(DbError::WriteTimeout {
            consistency: Consistency::One,
            received: 0,
            required: 1,
            write_type: scylla::errors::WriteType::Simple,
        });
        assert!(answered(&timeout) && !refused(&timeout));
        // A local failure after the response is answered, never a rejection.
        let local = anyhow!("page too large").context(Received);
        assert!(answered(&local) && !refused(&local));
        // A transport failure is neither.
        let transport = anyhow!("connection reset");
        assert!(!answered(&transport) && !refused(&transport));
    }

    #[test]
    fn continuations_are_bound_to_session_resource_and_query() {
        let e = executor();
        let resource = Resource::new("cql.rows", vec!["ks".into(), "t".into()]);
        let token = |session, query: Option<&str>| {
            serde_json::to_string(&Position {
                session,
                resource: resource.id.into(),
                path: resource.path.clone(),
                query: query.map(Into::into),
                state: base64::engine::general_purpose::STANDARD.encode(b"state"),
            })
            .unwrap()
        };
        assert_eq!(
            e.resume(&resource, None, Some(&token(e.identity, None)))
                .unwrap(),
            Some(b"state".to_vec())
        );
        assert!(e.resume(&resource, None, None).unwrap().is_none());
        // Another executor, resource or query cannot resume this read.
        assert!(
            e.resume(&resource, None, Some(&token(e.identity + 1, None)))
                .is_err()
        );
        assert!(
            e.resume(
                &Resource::new("cql.rows", vec!["ks".into(), "other".into()]),
                None,
                Some(&token(e.identity, None))
            )
            .is_err()
        );
        assert!(
            e.resume(
                &resource,
                Some("SELECT 2"),
                Some(&token(e.identity, Some("SELECT 1")))
            )
            .is_err()
        );
        assert!(e.resume(&resource, None, Some("not json")).is_err());
    }

    #[test]
    fn watermark_reads_the_open_table() {
        let row = Row {
            cells: vec![Some("events".into())],
            target: None,
        };
        assert_eq!(
            watermark(&Resource::new("cql.tables", vec!["ks".into()]), Some(&row)),
            r#"SELECT * FROM "ks"."events""#
        );
        assert_eq!(
            watermark(
                &Resource::new("cql.rows", vec!["ks".into(), "t".into()]),
                None
            ),
            r#"SELECT * FROM "ks"."t""#
        );
        // A keyspace literal doubles its quotes.
        assert_eq!(
            watermark(
                &Resource::new("cql.keyspaces", vec![]),
                Some(&Row {
                    cells: vec![Some("o'k".into())],
                    target: None,
                })
            ),
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'o''k'"
        );
        assert_eq!(
            watermark(&Resource::new("cql.keyspaces", vec![]), None),
            "SELECT keyspace_name FROM system_schema.keyspaces"
        );
    }

    #[test]
    fn configure_resolves_only_referenced_secrets() {
        let options = "nodes = ['127.0.0.1:9042']\nusername_env = 'U'\npassword_env = 'P'"
            .parse()
            .unwrap();
        let e = CqlProvider
            .configure(&options, &|name| Some(format!("{name}-secret")))
            .unwrap();
        assert_eq!(e.credentials, Some(("U-secret".into(), "P-secret".into())));
        assert_eq!(*e.status().borrow(), ConnectionStatus::Configured);
        assert!(CqlProvider.configure(&options, &|_| None).is_err());
        let mixed = "nodes=['127.0.0.1:9042']\nusername='user'\npassword_env='P'"
            .parse()
            .unwrap();
        let direct = CqlProvider
            .configure(&mixed, &|_| Some("secret".into()))
            .unwrap();
        assert_eq!(direct.credentials, Some(("user".into(), "secret".into())));
        // The password never survives into an error message.
        assert_eq!(
            e.diagnostic(anyhow!("auth failed for P-secret"))
                .to_string(),
            "auth failed for [REDACTED]"
        );
    }
}
