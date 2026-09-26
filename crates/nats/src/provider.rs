use anyhow::{Result, anyhow, ensure};
use onetui_core::Page;
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryDescriptor, QueryExecution, QueryRequest, RequestContext, ShutdownContext, WriteOutcome,
    WriteResult,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, watch};

pub struct NatsProvider;
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[
        onetui_core::provider::ConnectionField::list("servers"),
        onetui_core::provider::ConnectionField::boolean("tls"),
        onetui_core::provider::ConnectionField::boolean("jetstream"),
        onetui_core::provider::ConnectionField::boolean("system_discovery"),
        onetui_core::provider::ConnectionField::list("subjects"),
        onetui_core::provider::ConnectionField::text("token_env"),
        onetui_core::provider::ConnectionField::text("username_env"),
        onetui_core::provider::ConnectionField::text("password_env"),
        onetui_core::provider::ConnectionField::text("nkey_env"),
        onetui_core::provider::ConnectionField::text("credentials_env"),
        onetui_core::provider::ConnectionField::text("ca_file"),
        onetui_core::provider::ConnectionField::text("cert_file"),
        onetui_core::provider::ConnectionField::text("key_file"),
        onetui_core::provider::ConnectionField::boolean("tls_first"),
        onetui_core::provider::ConnectionField::text("domain"),
    ],
    kind: "nats",
    entry_resource: Some("nats.resources"),
    follow_resources: &["nats.messages", "nats.core_messages", "nats.kv_history"],
    query: Some(QueryDescriptor {
        resource: "nats.query",
        language: "NATS CONSUME/PRODUCE",
        contextual_watermark: Some(crate::statement::watermark),
        watermark: "PRODUCE stream subject\n\nhello",
        path_depth: 1,
        scope_resources: &["nats.messages", "nats.stream_info", "nats.query"],
    }),
    browsing: "JetStream streams / stored messages / Core subscriptions / live following",
    resources: crate::browse::RESOURCES,
    documentation: crate::capabilities,
};
impl Provider for NatsProvider {
    type Executor = NatsExecutor;
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
    ) -> Result<NatsExecutor> {
        let mut config = crate::config::Config::parse(options)?;
        let (options, mut secrets) = config.options(env)?;
        for binding in &mut config.decoders {
            if let Some(registry) = &mut binding.registry {
                secrets.extend(registry.resolve(env)?);
            }
            if let Some(buf) = &mut binding.buf {
                secrets.extend(buf.resolve(env)?);
            }
        }
        Ok(NatsExecutor {
            decoders: crate::decoding::Cache::new(config.decoders.clone()),
            config,
            options: Box::new(options),
            secrets,
            session: NEXT_SESSION.fetch_add(1, Ordering::Relaxed),
            client: Mutex::new(None),
            status: watch::channel(ConnectionStatus::Configured).0,
            errors: watch::channel(None).0,
            generation: Arc::new(AtomicU64::new(0)),
            closed: false,
        })
    }
}

pub struct NatsExecutor {
    decoders: Arc<crate::decoding::Cache>,
    config: crate::config::Config,
    // Keep the SDK's large configuration out of the statically dispatched executor enum.
    options: Box<async_nats::ConnectOptions>,
    secrets: Vec<String>,
    session: u64,
    // The SDK subscription/pending message must not enlarge every built-in executor variant.
    client: Mutex<Option<Box<Session>>>,
    status: watch::Sender<ConnectionStatus>,
    errors: watch::Sender<Option<String>>,
    generation: Arc<AtomicU64>,
    closed: bool,
}

struct Session {
    client: async_nats::Client,
    subscription: Option<crate::core_subscription::Subscription>,
}

/// Classify a publish that produced no acknowledgment. Once the message is dispatched,
/// nothing proves it was not stored, and a Core subscriber may have received it even
/// when JetStream refused to store it. A failure before dispatch never reached the wire,
/// so it stays an ordinary error rather than sending the user to inspect an unchanged
/// stream.
fn publish_failure(error: anyhow::Error, dispatched: bool) -> Result<QueryExecution> {
    if dispatched {
        Ok(QueryExecution::Write(WriteResult {
            outcome: WriteOutcome::Unknown,
            summary: format!(
                "Publish outcome unknown: {error}. Core subscribers may have received the message; inspect before retrying"
            ),
        }))
    } else {
        Err(error)
    }
}

impl NatsExecutor {
    async fn connect(&self) -> Result<async_nats::Client> {
        self.errors.send_replace(None);
        self.status.send_replace(ConnectionStatus::Connecting);
        let status = self.status.clone();
        let errors = self.errors.clone();
        let generation = self.generation.clone();
        let current = generation.fetch_add(1, Ordering::Relaxed) + 1;
        let mut options = self.options.as_ref().clone().event_callback(move |event| {
            let status = status.clone();
            let errors = errors.clone();
            let generation = generation.clone();
            async move {
                if generation.load(Ordering::Relaxed) != current {
                    return;
                }
                if matches!(event, async_nats::Event::Closed)
                    && *status.borrow() == ConnectionStatus::Closing
                {
                    status.send_replace(ConnectionStatus::Closed);
                    return;
                }
                if matches!(
                    *status.borrow(),
                    ConnectionStatus::Closing | ConnectionStatus::Closed
                ) {
                    return;
                }
                match event {
                    async_nats::Event::Connected => {
                        status.send_replace(ConnectionStatus::Connected);
                    }
                    async_nats::Event::Disconnected | async_nats::Event::Closed => {
                        status.send_replace(ConnectionStatus::Disconnected);
                        errors.send_replace(Some(
                            "NATS disconnected; live messages may be lost; restart following"
                                .into(),
                        ));
                    }
                    async_nats::Event::ServerError(error) => {
                        errors.send_replace(Some(error.to_string()));
                    }
                    async_nats::Event::SlowConsumer(_) => {
                        errors.send_replace(Some(
                            "NATS subscription queue overflow; messages lost; restart following"
                                .into(),
                        ));
                    }
                    _ => {}
                }
            }
        });
        for path in [
            &self.config.ca_file,
            &self.config.cert_file,
            &self.config.key_file,
        ]
        .into_iter()
        .flatten()
        {
            let meta = tokio::fs::metadata(path).await?;
            ensure!(
                meta.is_file() && meta.len() <= 1048576,
                "NATS certificate/key must be a regular PEM file no larger than 1 MiB"
            );
        }
        if let Some(path) = &self.config.ca_file {
            options = options.add_root_certificates(path.into());
        }
        if let (Some(cert), Some(key)) = (&self.config.cert_file, &self.config.key_file) {
            options = options.add_client_certificate(cert.into(), key.into());
        }
        let result = options.connect(self.config.servers.clone()).await;
        self.status.send_replace(if result.is_ok() {
            ConnectionStatus::Connected
        } else {
            ConnectionStatus::Disconnected
        });
        Ok(result?)
    }
    async fn ensure_session(
        &self,
        slot: &mut Option<Box<Session>>,
        context: &mut RequestContext,
    ) -> Result<()> {
        if slot.as_ref().is_some_and(|session| {
            session.client.connection_state() == async_nats::connection::State::Connected
        }) {
            return Ok(());
        }
        *slot = None;
        match context.run(self.connect()).await.and_then(|result| result) {
            Ok(client) => {
                *slot = Some(Box::new(Session {
                    client,
                    subscription: None,
                }))
            }
            Err(error) => {
                self.generation.fetch_add(1, Ordering::Relaxed);
                self.status.send_replace(ConnectionStatus::Disconnected);
                return Err(error);
            }
        }
        Ok(())
    }
    async fn execute(
        &self,
        page: Option<(PageRequest, bool)>,
        replay: Option<crate::replay::Replay>,
        mut context: RequestContext,
    ) -> Result<Page> {
        let result = async {
            ensure!(!self.closed, "NATS session is closed");
            if let Some((request, live)) = &page {
                crate::browse::validate(request, *live)?;
                if !matches!(request.resource.id, "nats.resources" | "nats.subjects" | "nats.core_messages" | "nats.servers") {
                    ensure!(self.config.jetstream, "JetStream is disabled for this connection");
                }
                if request.resource.id == "nats.servers" {
                    ensure!(self.config.system_discovery, "NATS server discovery requires system_discovery=true");
                    ensure!(request.continuation.is_none(), "NATS server discovery has one bounded observation page; refresh to rediscover");
                }
                if request.resource.id == "nats.core_messages" {
                    ensure!(self.config.subjects.contains(&request.resource.path[0]), "NATS subject is not configured");
                }
            }
            let mut slot = context.run(self.client.lock()).await?;
            self.ensure_session(&mut slot, &mut context).await?;
            let session = slot.as_mut().unwrap();
            let deadline = context.deadline;
            let client = &session.client;
            let prefix = self.config.domain.as_ref().map_or_else(|| "$JS.API".into(), |domain| format!("$JS.{domain}.API"));
            let api = crate::browse::Api { client, prefix: &prefix };
            let mut errors = self.errors.subscribe();
            let attempt = context.run(async {
                if let Some(error) = errors.borrow().clone() { return Err(anyhow!(error)); }
                tokio::select! {
                    biased;
                    _ = errors.changed() => Err(anyhow!(errors.borrow().clone().unwrap_or_else(|| "NATS connection failed".into()))),
                    result = async {
                        let check = page.is_none();
                        let page = match page {
                            Some((request, true)) if request.resource.id == "nats.core_messages" => {
                                if request.continuation.is_none() {
                                    if let Some(previous) = session.subscription.take() { previous.stop(client).await?; }
                                    session.subscription = Some(crate::core_subscription::Subscription::start(client, request.resource.path[0].clone(), NEXT_SESSION.fetch_add(1, Ordering::Relaxed)).await?);
                                }
                                session.subscription.as_mut().ok_or_else(|| anyhow!("NATS subscription is no longer active; restart following"))?.page(client, &request).await
                            }
                            Some((request, live)) => {
                                if let Some(previous) = session.subscription.take() { previous.stop(client).await?; }
                                if let Some(page) = crate::browse::local_page(&self.config, &request)? { return Ok(page); }
                                if request.resource.id == "nats.servers" { return crate::discovery::page(client).await; }
                                crate::browse::page(&api, self.session, request, live, replay.as_ref()).await
                            },
                            None => {
                                client.flush().await?;
                                if self.config.jetstream { api.call("INFO", serde_json::json!({})).await?; }
                                Ok(Page::default())
                            }
                        }?;
                        if self.config.decoders.is_empty() { Ok(page) }
                        else { self.decoders.apply(page, check, deadline).await }
                    } => result,
                }
            }).await;
            let completed = attempt.is_ok();
            let result = attempt.and_then(|result| result);
            if result.is_err() {
                let rejected = completed && result.as_ref().err().is_some_and(|error| {
                    error.downcast_ref::<crate::browse::ApiError>().is_some()
                });
                if rejected && session.client.connection_state() == async_nats::connection::State::Connected {
                    self.status.send_replace(ConnectionStatus::Connected);
                } else {
                    // Dropping the owner also discards unanswered request-multiplexer entries.
                    self.generation.fetch_add(1, Ordering::Relaxed);
                    slot.take();
                    self.status.send_replace(ConnectionStatus::Disconnected);
                }
            }
            result
        }.await;
        result.map_err(|error| self.diagnostic(error))
    }

    async fn publish(
        &self,
        request: PageRequest,
        subject: &str,
        headers: &[(&str, &str)],
        payload: &[u8],
        mut context: RequestContext,
    ) -> Result<QueryExecution> {
        ensure!(!self.closed, "NATS session is closed");
        ensure!(
            self.config.jetstream,
            "JetStream is disabled for this connection"
        );
        ensure!(
            request.continuation.is_none(),
            "NATS publish does not support continuation"
        );
        let stream = request.resource.path[0].clone();
        let mut slot = context.run(self.client.lock()).await?;
        self.ensure_session(&mut slot, &mut context).await?;
        let client = slot.as_ref().expect("connected session").client.clone();
        let jetstream = match self.config.domain.as_deref() {
            Some(domain) => async_nats::jetstream::with_domain(client, domain),
            None => async_nats::jetstream::new(client),
        };
        let mut message = async_nats::jetstream::message::PublishMessage::build()
            .payload(payload.to_vec().into())
            .expected_stream(&stream);
        for (name, value) in headers {
            message = message.header(*name, *value);
        }
        // The message is only in flight once send_publish resolves. An error from the
        // call itself never reached the wire, so it stays an ordinary rejection; a
        // cancellation or failure while awaiting the acknowledgment does not.
        let mut dispatched = false;
        let mut acknowledged = None;
        let attempt = context
            .run(async {
                let pending = jetstream.send_publish(subject.to_owned(), message).await?;
                dispatched = true;
                acknowledged = Some(pending.await?);
                anyhow::Ok(())
            })
            .await;
        let result = match (acknowledged, attempt) {
            (Some(ack), _) => Ok(QueryExecution::Write(WriteResult {
                outcome: WriteOutcome::Applied,
                summary: format!("Published to {} at sequence {}", ack.stream, ack.sequence),
            })),
            (None, Ok(Err(error)) | Err(error)) => {
                publish_failure(self.diagnostic(error), dispatched)
            }
            (None, Ok(Ok(()))) => unreachable!("successful publish has an acknowledgment"),
        };
        if slot.as_ref().is_some_and(|session| {
            session.client.connection_state() == async_nats::connection::State::Connected
        }) {
            self.status.send_replace(ConnectionStatus::Connected);
        } else {
            slot.take();
            self.generation.fetch_add(1, Ordering::Relaxed);
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result
    }

    fn diagnostic(&self, error: anyhow::Error) -> anyhow::Error {
        onetui_core::diagnostic(
            error,
            &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    }

    async fn consume(
        &self,
        mut page: PageRequest,
        stream: &str,
        replay: crate::replay::Replay,
        context: RequestContext,
    ) -> Result<Page> {
        page.resource.path = vec![stream.to_owned()];
        self.execute(Some((page, false)), Some(replay), context)
            .await
    }
}

impl Executor for NatsExecutor {
    async fn stop_follow(&self, context: ShutdownContext) -> Result<()> {
        let result = tokio::time::timeout_at(context.deadline, async {
            let mut slot = self.client.lock().await;
            if let Some(session) = slot.as_mut()
                && let Some(subscription) = session.subscription.take()
            {
                subscription.stop(&session.client).await?;
            }
            anyhow::Ok(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|r| r);
        if result.is_err() {
            if let Ok(mut slot) = self.client.try_lock() {
                slot.take();
            }
            self.generation.fetch_add(1, Ordering::Relaxed);
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result.map_err(|error| self.diagnostic(error))
    }
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }
    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        self.execute(None, None, context).await?;
        Ok(CheckResult {
            summary: if self.config.jetstream {
                "NATS JetStream account metadata readable"
            } else {
                "NATS Core connection established; subject permissions checked when subscribing"
            }
            .into(),
        })
    }
    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        self.execute(Some((request, false)), None, context).await
    }
    async fn follow_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        self.execute(Some((request, true)), None, context).await
    }
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "nats.query",
            "Invalid NATS query resource"
        );
        match crate::statement::parse(&request.text)? {
            crate::statement::Statement::Consume { stream, replay } => {
                self.consume(request.page, stream, replay, context).await
            }
            crate::statement::Statement::Produce { .. } => Err(anyhow!(
                "NATS PRODUCE returns an acknowledgment; it does not page"
            )),
        }
    }
    async fn execute_query(
        &self,
        request: QueryRequest,
        context: RequestContext,
    ) -> Result<QueryExecution> {
        request.validate()?;
        ensure!(
            request.page.resource.id == "nats.query",
            "Invalid NATS query resource"
        );
        match crate::statement::parse(&request.text)? {
            crate::statement::Statement::Consume { stream, replay } => self
                .consume(request.page, stream, replay, context)
                .await
                .map(QueryExecution::Page),
            crate::statement::Statement::Produce {
                stream,
                subject,
                headers,
                payload,
            } => {
                let mut page = request.page;
                page.resource.path = vec![stream.to_owned()];
                crate::browse::validate(&page, false)?;
                self.publish(page, subject, &headers, &payload, context)
                    .await
            }
        }
    }
    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        let result = if let Some(session) = self.client.get_mut().take() {
            let client = session.client;
            drop(session.subscription);
            tokio::time::timeout_at(context.deadline, async {
                client.drain().await?;
                let mut status = self.status.subscribe();
                while *status.borrow_and_update() != ConnectionStatus::Closed
                    && client.connection_state() != async_nats::connection::State::Disconnected
                {
                    status.changed().await?;
                }
                anyhow::Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(|r| r)
        } else {
            Ok(())
        };
        self.status.send_replace(ConnectionStatus::Closed);
        result.map_err(|error| self.diagnostic(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_dispatched_publish_has_an_unknown_outcome() {
        // Cancellation, a lost acknowledgment or a JetStream refusal after the message
        // is on the wire: the stream may or may not hold it.
        let dispatched = publish_failure(anyhow!("Request cancelled"), true).unwrap();
        let QueryExecution::Write(write) = dispatched else {
            panic!("a dispatched failure reports a write result")
        };
        assert_eq!(write.outcome, WriteOutcome::Unknown);
        assert!(write.summary.contains("Request cancelled"));
        assert!(write.summary.contains("inspect before retrying"));

        // Refused by the client before sending, so the stream cannot have changed.
        let Err(error) = publish_failure(anyhow!("payload too large"), false) else {
            panic!("a publish that never reached the wire is an ordinary error")
        };
        assert_eq!(error.to_string(), "payload too large");
    }
}
