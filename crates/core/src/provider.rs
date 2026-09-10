use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use anyhow::{Result, anyhow, ensure};
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;

use crate::catalog::ResourceDescriptor;
use crate::{Page, Resource};

pub struct ProviderDescriptor {
    pub follow_resource: Option<&'static str>,
    pub query: Option<QueryDescriptor>,
    pub kind: &'static str,
    pub entry_resource: Option<&'static str>,
    pub browsing: &'static str,
    pub resources: &'static [&'static ResourceDescriptor],
    pub documentation: fn() -> serde_json::Value,
}

impl ProviderDescriptor {
    pub fn resource(&self, id: &str) -> Option<&'static ResourceDescriptor> {
        self.resources.iter().copied().find(|r| r.id == id)
    }

    pub fn capabilities(&self) -> serde_json::Value {
        let mut value = (self.documentation)();
        value["query"] = serde_json::to_value(self.query).expect("query descriptor");
        value["follow_resource"] = serde_json::json!(self.follow_resource);
        if self.query.is_some() {
            value["query_max_bytes"] = QUERY_BYTES.into();
        }
        value["id"] = self.kind.into();
        value["entry_resource"] = serde_json::json!(self.entry_resource);
        value["resources"] = serde_json::to_value(self.resources).expect("static descriptors");
        value
    }
}

pub trait Provider: Send + Sync {
    type Executor: Executor;

    fn descriptor(&self) -> &'static ProviderDescriptor;
    fn validate_config(&self, options: &toml::Table) -> Result<()>;
    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self::Executor>;
}

/// Native failures retain backend codes/messages and underlying transport causes.
/// Executors redact known connection secrets and escape controls before returning diagnostics.
pub trait Executor: Send + Sync {
    /// One bounded live batch. No cursor starts at the current end; even an empty
    /// batch returns a cursor. Cancellation/error must not advance the caller's cursor.
    fn follow_page(
        &self,
        _request: PageRequest,
        _context: RequestContext,
    ) -> impl Future<Output = Result<Page>> + Send {
        async { anyhow::bail!("Live following is unavailable for this provider") }
    }
    fn query_page(
        &self,
        _request: QueryRequest,
        _context: RequestContext,
    ) -> impl Future<Output = Result<Page>> + Send {
        async { anyhow::bail!("Queries are unavailable for this provider") }
    }
    fn status(&self) -> watch::Receiver<ConnectionStatus>;
    fn check(&self, context: RequestContext) -> impl Future<Output = Result<CheckResult>> + Send;
    fn fetch_page(
        &self,
        request: PageRequest,
        context: RequestContext,
    ) -> impl Future<Output = Result<Page>> + Send;
    fn shutdown(&mut self, context: ShutdownContext) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionStatus {
    Configured,
    Connecting,
    Connected,
    Disconnected,
    Closing,
    Closed,
}

pub struct CheckResult {
    pub summary: String,
}

pub struct PageRequest {
    pub resource: Resource,
    pub continuation: Option<String>,
}

pub const QUERY_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, serde::Serialize)]
pub struct QueryDescriptor {
    pub resource: &'static str,
    pub language: &'static str,
    pub example: &'static str,
    /// Number of current resource path components needed to scope a query.
    pub path_depth: usize,
}

pub struct QueryRequest {
    pub page: PageRequest,
    pub text: String,
}

impl QueryRequest {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.text.trim().is_empty(), "Query is empty");
        ensure!(self.text.len() <= QUERY_BYTES, "Query exceeds 16 KiB");
        Ok(())
    }
}

pub struct RequestContext {
    pub deadline: Instant,
    pub cancel: oneshot::Receiver<()>,
}

impl RequestContext {
    pub fn new(timeout: Duration) -> (oneshot::Sender<()>, Self) {
        let (cancel, receiver) = oneshot::channel();
        (
            cancel,
            Self {
                deadline: Instant::now() + timeout,
                cancel: receiver,
            },
        )
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub async fn run<T>(&mut self, future: impl Future<Output = T>) -> Result<T> {
        tokio::select! {
            biased;
            _ = &mut self.cancel => Err(anyhow!("Request cancelled")),
            _ = tokio::time::sleep_until(self.deadline) => Err(anyhow!("Request timed out")),
            value = future => Ok(value),
        }
    }
}

pub struct ShutdownContext {
    pub deadline: Instant,
}

impl ShutdownContext {
    pub fn new(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
        }
    }
}

pub fn find_provider<'a, P: Provider>(catalog: &'a [P], kind: &str) -> Result<&'a P> {
    catalog
        .iter()
        .find(|p| p.descriptor().kind == kind)
        .ok_or_else(|| anyhow!("unknown datasource; use schema for supported kinds"))
}

pub fn validate_catalog<P: Provider>(catalog: &[P]) -> Result<()> {
    let mut kinds = BTreeSet::new();
    let mut resources = BTreeSet::new();
    for provider in catalog {
        let descriptor = provider.descriptor();
        ensure!(
            crate::config::safe_name(descriptor.kind) && kinds.insert(descriptor.kind),
            "invalid or duplicate provider kind"
        );
        for resource in descriptor.resources {
            ensure!(
                resource.id.starts_with(&format!("{}.", descriptor.kind))
                    && resource.id.split('.').all(crate::config::safe_name)
                    && resources.insert(resource.id),
                "invalid or duplicate resource ID"
            );
            let mut actions = BTreeSet::new();
            for action in resource.actions {
                ensure!(
                    actions.insert(action.id)
                        && descriptor.resource(action.target).is_some_and(|r| r.paging),
                    "invalid or duplicate resource action"
                );
            }
        }
        ensure!(
            descriptor
                .entry_resource
                .is_none_or(|id| descriptor.resource(id).is_some_and(|r| r.paging)),
            "invalid provider entry resource"
        );
        ensure!(
            descriptor
                .follow_resource
                .is_none_or(|id| descriptor.resource(id).is_some_and(|r| r.paging)),
            "invalid provider follow resource"
        );
        ensure!(
            descriptor.query.is_none_or(|q| descriptor
                .resource(q.resource)
                .is_some_and(|r| r.paging)
                && !q.example.is_empty()
                && q.example.len() <= QUERY_BYTES),
            "invalid provider query descriptor"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_provider::{CATALOG, FakeExecutor, FakeProvider};

    struct Declared(&'static ProviderDescriptor);
    impl Provider for Declared {
        type Executor = FakeExecutor;
        fn descriptor(&self) -> &'static ProviderDescriptor {
            self.0
        }
        fn validate_config(&self, _: &toml::Table) -> Result<()> {
            panic!("catalog must not parse config")
        }
        fn configure(
            &self,
            _: &toml::Table,
            _: &dyn Fn(&str) -> Option<String>,
        ) -> Result<FakeExecutor> {
            panic!("catalog must not configure")
        }
    }

    #[test]
    fn catalog_checks_kinds_resources_and_entry_without_executors() {
        validate_catalog(CATALOG).unwrap();
        assert!(find_provider(CATALOG, "unknown").is_err());
        assert!(validate_catalog(&[FakeProvider, FakeProvider]).is_err());
        static RESOURCE: ResourceDescriptor = ResourceDescriptor {
            id: "fake.rows",
            description: "rows",
            columns: &[],
            paging: true,
            actions: &[],
        };
        static DUPLICATE: ProviderDescriptor = ProviderDescriptor {
            follow_resource: None,
            query: None,
            kind: "fake",
            entry_resource: None,
            browsing: "",
            resources: &[&RESOURCE, &RESOURCE],
            documentation: || serde_json::json!({}),
        };
        static MISSING: ProviderDescriptor = ProviderDescriptor {
            follow_resource: None,
            query: None,
            kind: "fake",
            entry_resource: Some("fake.missing"),
            browsing: "",
            resources: &[&RESOURCE],
            documentation: || serde_json::json!({}),
        };
        assert!(validate_catalog(&[Declared(&DUPLICATE)]).is_err());
        assert!(validate_catalog(&[Declared(&MISSING)]).is_err());
        assert_eq!(FakeProvider.descriptor().capabilities()["id"], "fake");
    }

    #[tokio::test]
    async fn cancellation_and_deadlines_do_not_restart_when_work_is_queued() {
        let (cancel, mut context) = RequestContext::new(Duration::from_secs(5));
        cancel.send(()).unwrap();
        assert!(
            context
                .run(async { 42 })
                .await
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
        let (_cancel, mut context) = RequestContext::new(Duration::ZERO);
        assert!(
            context
                .run(std::future::pending::<()>())
                .await
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
        let mut executor = FakeProvider
            .configure(&toml::Table::new(), &|_| None)
            .unwrap();
        tokio::spawn(async move {
            let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
            executor.check(context).await.unwrap();
            executor
                .shutdown(ShutdownContext::new(Duration::from_secs(1)))
                .await
                .unwrap();
            assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
        })
        .await
        .unwrap();
    }
}
