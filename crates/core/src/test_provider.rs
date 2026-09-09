use crate::Page;
use crate::provider::*;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use tokio::sync::watch;

pub struct FakeProvider;
pub const CATALOG: &[FakeProvider] = &[FakeProvider];
static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    query: None,
    kind: "fake",
    entry_resource: None,
    browsing: "check only",
    resources: &[],
    documentation: || serde_json::json!({"operations": ["check"]}),
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    secret_env: Option<String>,
}

impl Provider for FakeProvider {
    type Executor = FakeExecutor;
    fn descriptor(&self) -> &'static ProviderDescriptor {
        &DESCRIPTOR
    }
    fn validate_config(&self, options: &toml::Table) -> Result<()> {
        let _: Options = options
            .clone()
            .try_into()
            .map_err(|_| anyhow!("invalid fake config"))?;
        Ok(())
    }
    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<FakeExecutor> {
        self.validate_config(options)?;
        let options: Options = options.clone().try_into()?;
        if let Some(name) = options.secret_env {
            crate::config::secret(&name, env)?;
        }
        Ok(FakeExecutor(watch::channel(ConnectionStatus::Configured).0))
    }
}

pub struct FakeExecutor(watch::Sender<ConnectionStatus>);
impl Executor for FakeExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.0.subscribe()
    }
    async fn check(&self, _: RequestContext) -> Result<CheckResult> {
        Ok(CheckResult {
            summary: "fake".into(),
        })
    }
    async fn fetch_page(&self, _: PageRequest, _: RequestContext) -> Result<Page> {
        anyhow::bail!("unsupported")
    }
    async fn shutdown(&mut self, _: ShutdownContext) -> Result<()> {
        self.0.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}
