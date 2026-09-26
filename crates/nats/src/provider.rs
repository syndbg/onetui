use anyhow::{Result, anyhow, ensure};
use onetui_core::Page;
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryDescriptor, QueryRequest, RequestContext, ShutdownContext,
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
        language: "Replay JSON",
        contextual_watermark: None,
        watermark: "{\n  \"subject\": \">\",\n  \"start_sequence\": 1\n}",
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
            if slot.as_ref().is_none_or(|c| c.client.connection_state() != async_nats::connection::State::Connected) {
                *slot = None;
                match context.run(self.connect()).await.and_then(|result| result) {
                    Ok(client) => *slot = Some(Box::new(Session { client, subscription: None })),
                    Err(error) => {
                        self.generation.fetch_add(1, Ordering::Relaxed);
                        self.status.send_replace(ConnectionStatus::Disconnected);
                        return Err(error);
                    }
                }
            }
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
        result.map_err(|error| {
            onetui_core::diagnostic(
                error,
                &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
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
        result.map_err(|error| {
            onetui_core::diagnostic(
                error,
                &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
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
        let replay = crate::replay::Replay::parse(&request.text)?;
        self.execute(Some((request.page, false)), Some(replay), context)
            .await
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
        result.map_err(|error| {
            onetui_core::diagnostic(
                error,
                &self.secrets.iter().map(String::as_str).collect::<Vec<_>>(),
            )
        })
    }
}
