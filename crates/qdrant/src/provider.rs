use anyhow::{Result, anyhow, bail, ensure};
use onetui_core::Page;
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    RequestContext, ShutdownContext,
};
use qdrant_client::qdrant::{ListCollectionsRequest, collections_client::CollectionsClient};
use tokio::sync::{Mutex, watch};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub struct QdrantProvider;

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    kind: "qdrant",
    entry_resource: None,
    browsing: "not implemented; --check only",
    resources: &[],
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
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

pub struct QdrantExecutor {
    url: String,
    api_key: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    client: Mutex<Option<CollectionsClient<Channel>>>,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

impl Executor for QdrantExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }

    async fn check(&self, mut context: RequestContext) -> Result<CheckResult> {
        ensure!(!self.closed, "Qdrant session is closed");
        let mut client = context.run(self.client.lock()).await?;
        let deadline = context.deadline;
        let result = context.run(async {
            if client.is_none() {
                self.status.send_replace(ConnectionStatus::Connecting);
                let url = crate::qdrant_url(&self.url)?;
                let mut endpoint = Endpoint::from_shared(url.to_string()).map_err(|_| anyhow!("invalid Qdrant gRPC endpoint"))?
                    .connect_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .keep_alive_while_idle(false);
                if url.scheme() == "https" {
                    endpoint = endpoint.tls_config(ClientTlsConfig::new().with_native_roots()).map_err(|_| anyhow!("cannot configure Qdrant TLS using native trust roots"))?;
                }
                let channel = endpoint.connect().await.map_err(|_| anyhow!("Qdrant connection failed; verify gRPC endpoint, reachability and TLS certificate trust"))?;
                // The high-level SDK helper removes this decoding limit.
                *client = Some(CollectionsClient::new(channel).max_decoding_message_size(1024 * 1024));
                self.status.send_replace(ConnectionStatus::Connected);
            }
            let mut request = tonic::Request::new(ListCollectionsRequest {});
            request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
            if let Some(key) = &self.api_key { request.metadata_mut().insert("api-key", key.clone()); }
            let response = client.as_mut().expect("connected client").list(request).await.map_err(crate::rpc_error)?;
            self.status.send_replace(ConnectionStatus::Connected);
            Ok(CheckResult { summary: format!("collection metadata readable ({} collections)", response.into_inner().collections.len()) })
        }).await.and_then(|r| r);
        if result.is_err() {
            client.take();
            self.status.send_replace(ConnectionStatus::Disconnected);
        }
        result
    }

    async fn fetch_page(&self, _request: PageRequest, _context: RequestContext) -> Result<Page> {
        bail!("Qdrant browsing is not implemented; use --check for now")
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
