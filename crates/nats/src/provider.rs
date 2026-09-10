use anyhow::{Result, anyhow, ensure};
use onetui_core::Page;
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    RequestContext, ShutdownContext,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, watch};

pub struct NatsProvider;
static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "nats",
    entry_resource: Some("nats.streams"),
    follow_resource: Some("nats.messages"),
    query: None,
    browsing: "JetStream streams / stored messages / live following",
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
        let config = crate::config::Config::parse(options)?;
        let (options, secrets) = config.options(env)?;
        Ok(NatsExecutor {
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
    config: crate::config::Config,
    // Keep the SDK's large configuration out of the statically dispatched executor enum.
    options: Box<async_nats::ConnectOptions>,
    secrets: Vec<String>,
    session: u64,
    client: Mutex<Option<async_nats::Client>>,
    status: watch::Sender<ConnectionStatus>,
    errors: watch::Sender<Option<String>>,
    generation: Arc<AtomicU64>,
    closed: bool,
}

impl NatsExecutor {
    async fn connect(&self) -> Result<async_nats::Client> {
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
                    }
                    async_nats::Event::ServerError(error) => {
                        errors.send_replace(Some(error.to_string()));
                    }
                    async_nats::Event::SlowConsumer(_) => {
                        errors.send_replace(Some("NATS client reply queue overflow".into()));
                    }
                    _ => {}
                }
            }
        });
        if let Some(path) = &self.config.ca_file {
            let meta = tokio::fs::metadata(path).await?;
            ensure!(
                meta.is_file() && meta.len() <= 1048576,
                "NATS ca_file must be a regular PEM file no larger than 1 MiB"
            );
            options = options.add_root_certificates(path.into());
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
        mut context: RequestContext,
    ) -> Result<Page> {
        let result = async {
            ensure!(!self.closed, "NATS session is closed");
            if let Some((request, live)) = &page { crate::browse::validate(request, *live)?; }
            let mut slot = context.run(self.client.lock()).await?;
            if slot.as_ref().is_none_or(|c| c.connection_state() != async_nats::connection::State::Connected) {
                *slot = None;
                match context.run(self.connect()).await.and_then(|result| result) {
                    Ok(client) => *slot = Some(client),
                    Err(error) => {
                        self.generation.fetch_add(1, Ordering::Relaxed);
                        self.status.send_replace(ConnectionStatus::Disconnected);
                        return Err(error);
                    }
                }
            }
            let client = slot.as_ref().unwrap();
            self.errors.send_replace(None);
            let mut errors = self.errors.subscribe();
            let result = context.run(async {
                tokio::select! {
                    result = async {
                        match page {
                            Some((request, live)) => crate::browse::page(client, self.session, request, live).await,
                            None => { crate::browse::api(client, "INFO", serde_json::json!({})).await?; Ok(Page::default()) }
                        }
                    } => result,
                    _ = errors.changed() => Err(anyhow!(errors.borrow().clone().unwrap_or_else(|| "NATS connection failed".into()))),
                }
            }).await.and_then(|result| result);
            if result.is_err() {
                // Dropping the owner also discards unanswered request-multiplexer entries.
                self.generation.fetch_add(1, Ordering::Relaxed);
                slot.take();
                self.status.send_replace(ConnectionStatus::Disconnected);
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
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }
    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        self.execute(None, context).await?;
        Ok(CheckResult {
            summary: "NATS JetStream account metadata readable".into(),
        })
    }
    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        self.execute(Some((request, false)), context).await
    }
    async fn follow_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        self.execute(Some((request, true)), context).await
    }
    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        let result = if let Some(client) = self.client.get_mut().take() {
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
