use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, ensure};
use base64::Engine;
use onetui_core::provider::{
    CheckResult, ConnectionField, ConnectionStatus, Executor, PageRequest, Provider,
    ProviderDescriptor, RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, Page, Resource};
use rustls::pki_types::pem::PemObject;
use tokio::sync::{Mutex, MutexGuard, watch};

static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
static TLS_SETUP: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

pub struct RabbitMqProvider;

static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "rabbitmq",
    entry_resource: Some("rabbitmq.resources"),
    browsing: "management metadata, metrics and cluster nodes",
    resources: crate::browse::RESOURCES,
    query: None,
    follow_resources: &[],
    connection_fields: &[
        ConnectionField::text("url"),
        ConnectionField::text("username_env"),
        ConnectionField::text("password_env"),
        ConnectionField::text("ca_file"),
    ],
    documentation: crate::capabilities,
};

impl Provider for RabbitMqProvider {
    type Executor = RabbitMqExecutor;
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
    ) -> Result<Self::Executor> {
        let config = crate::config::Config::parse(options)?;
        let username = onetui_core::config::secret(&config.username_env, env)?;
        let password = onetui_core::config::secret(&config.password_env, env)?;
        ensure!(
            !username.contains(':'),
            "RabbitMQ Basic authentication username cannot contain a colon"
        );
        let encoded_credentials =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        Ok(RabbitMqExecutor {
            config,
            username,
            password,
            encoded_credentials,
            identity: NEXT_SESSION.fetch_add(1, Ordering::Relaxed),
            client: Mutex::new(None),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

pub struct RabbitMqExecutor {
    config: crate::config::Config,
    username: String,
    password: String,
    encoded_credentials: String,
    identity: u64,
    client: Mutex<Option<reqwest::Client>>,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Lease<'a> {
    client: MutexGuard<'a, Option<reqwest::Client>>,
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

impl RabbitMqExecutor {
    async fn client(&self) -> Result<reqwest::Client> {
        let https = crate::config::endpoint(&self.config.url)?.scheme() == "https";
        let ca_file = self.config.ca_file.clone();
        let permit = TLS_SETUP.acquire().await?;
        // Native trust loading can block. Cancellation keeps the setup slot until it finishes.
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut roots = rustls::RootCertStore::empty();
            if https {
                if let Some(path) = ca_file {
                    for certificate in rustls::pki_types::CertificateDer::pem_file_iter(path)? {
                        roots.add(certificate?)?;
                    }
                } else {
                    for certificate in rustls_native_certs::load_native_certs().certs {
                        roots.add(certificate)?;
                    }
                }
                ensure!(!roots.is_empty(), "No RabbitMQ TLS trust roots available");
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Ok(reqwest::Client::builder()
                .tls_backend_preconfigured(tls)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .http1_only()
                .pool_max_idle_per_host(1)
                .build()?)
        })
        .await?
    }

    async fn read(
        &self,
        resource: &Resource,
        number: u32,
        mut context: RequestContext,
    ) -> Result<Page> {
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
                let url = crate::browse::request_url(&self.config.url, resource, number)?;
                let mut response = lease
                    .client
                    .as_ref()
                    .expect("HTTP client")
                    .get(url)
                    .basic_auth(&self.username, Some(&self.password))
                    .send()
                    .await?;
                let status = response.status();
                ensure!(
                    response
                        .content_length()
                        .is_none_or(|length| length <= PAGE_BYTES as u64),
                    "RabbitMQ HTTP {status}: response exceeds 1 MiB"
                );
                let mut body = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    ensure!(
                        chunk.len() <= PAGE_BYTES - body.len(),
                        "RabbitMQ HTTP {status}: response exceeds 1 MiB"
                    );
                    body.extend_from_slice(&chunk);
                }
                ensure!(
                    status.is_success(),
                    "RabbitMQ HTTP {status}: {}",
                    String::from_utf8_lossy(&body)
                );
                crate::browse::page(
                    resource,
                    serde_json::from_slice(&body)?,
                    number,
                    self.identity,
                )
            })
            .await?;
        let result = context.run(std::future::ready(result)).await?;
        if result.is_ok() {
            lease.clean = true;
            self.status.send_replace(ConnectionStatus::Connected);
        }
        result.map_err(|error| {
            onetui_core::diagnostic(error, &[&self.password, &self.encoded_credentials])
        })
    }
}

impl Executor for RabbitMqExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }
    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        ensure!(!self.closed, "RabbitMQ session is closed");
        self.read(&Resource::new("rabbitmq.overview", vec![]), 1, context)
            .await?;
        Ok(CheckResult {
            summary: "RabbitMQ management overview readable".into(),
        })
    }
    async fn fetch_page(&self, request: PageRequest, mut context: RequestContext) -> Result<Page> {
        ensure!(!self.closed, "RabbitMQ session is closed");
        let number = crate::browse::position(
            &request.resource,
            request.continuation.as_deref(),
            self.identity,
        )?;
        if let Some(page) = crate::browse::menu(&request.resource) {
            return context.run(std::future::ready(Ok(page))).await?;
        }
        self.read(&request.resource, number, context).await
    }
    async fn shutdown(&mut self, _context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        self.client.get_mut().take();
        self.status.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}
