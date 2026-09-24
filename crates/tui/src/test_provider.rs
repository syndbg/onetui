use anyhow::{Result, anyhow};
use onetui_core::Page;
use onetui_core::catalog::{Action, ActionSource, ResourceAction, ResourceDescriptor};
use onetui_core::provider::*;
use tokio::sync::watch;

pub struct FakeProvider(bool);
pub const CATALOG: &[FakeProvider] = &[FakeProvider(true), FakeProvider(false)];

const SCHEMAS: ResourceDescriptor = ResourceDescriptor {
    id: "fake.schemas",
    description: "schemas",
    columns: &["name"],
    paging: true,
    actions: &[],
};
const RELATIONS: ResourceDescriptor = ResourceDescriptor {
    id: "fake.relations",
    description: "relations",
    columns: &["name", "kind", "permission"],
    paging: true,
    actions: &[ResourceAction {
        id: Action::Columns,
        target: "fake.columns",
        source: ActionSource::SelectedTarget,
    }],
};
const ROWS: ResourceDescriptor = ResourceDescriptor {
    id: "fake.rows",
    description: "rows",
    columns: &[],
    paging: true,
    actions: &[ResourceAction {
        id: Action::Columns,
        target: "fake.columns",
        source: ActionSource::Current,
    }],
};
const COLUMNS: ResourceDescriptor = ResourceDescriptor {
    id: "fake.columns",
    description: "columns",
    columns: &["name", "type", "nullable"],
    paging: true,
    actions: &[],
};
static BROWSE: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[onetui_core::provider::ConnectionField::text("token_env")],
    follow_resources: &["fake.rows"],
    query: Some(QueryDescriptor {
        resource: "fake.rows",
        language: "Test query",
        contextual_watermark: None,
        watermark: "select 1",
        path_depth: 0,
        scope_resources: &[],
    }),
    kind: "fake",
    entry_resource: Some("fake.schemas"),
    browsing: "fake rows",
    resources: &[&SCHEMAS, &RELATIONS, &ROWS, &COLUMNS],
    documentation: || serde_json::json!({}),
};
static CHECK: ProviderDescriptor = ProviderDescriptor {
    connection_fields: &[],
    follow_resources: &[],
    query: None,
    kind: "checkonly",
    entry_resource: None,
    browsing: "check only",
    resources: &[],
    documentation: || serde_json::json!({}),
};

impl Provider for FakeProvider {
    type Executor = FakeExecutor;
    fn descriptor(&self) -> &'static ProviderDescriptor {
        if self.0 { &BROWSE } else { &CHECK }
    }
    fn validate_config(&self, _: &toml::Table) -> Result<()> {
        Ok(())
    }
    fn configure(
        &self,
        _: &toml::Table,
        _: &dyn Fn(&str) -> Option<String>,
    ) -> Result<FakeExecutor> {
        Ok(FakeExecutor(watch::channel(ConnectionStatus::Configured).0))
    }
}

pub struct FakeExecutor(watch::Sender<ConnectionStatus>);
impl Executor for FakeExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.0.subscribe()
    }
    async fn check(&self, _: RequestContext) -> Result<CheckResult> {
        Err(anyhow!("fake connection failure"))
    }
    async fn fetch_page(&self, _: PageRequest, _: RequestContext) -> Result<Page> {
        Err(anyhow!("fake connection failure"))
    }
    async fn shutdown(&mut self, _: ShutdownContext) -> Result<()> {
        self.0.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}
