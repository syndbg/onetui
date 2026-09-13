use anyhow::Result;
use onetui_core::Page;
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryRequest, RequestContext, ShutdownContext,
};
use onetui_dynamodb::{DynamoDbExecutor, DynamoDbProvider};
use onetui_kafka::{KafkaExecutor, KafkaProvider};
use onetui_nats::{NatsExecutor, NatsProvider};
use onetui_postgres::{PostgresExecutor, PostgresProvider};
use onetui_qdrant::{QdrantExecutor, QdrantProvider};
use tokio::sync::watch;

pub enum BuiltinProvider {
    Postgres(PostgresProvider),
    Qdrant(QdrantProvider),
    Kafka(KafkaProvider),
    Nats(NatsProvider),
    DynamoDb(DynamoDbProvider),
}

pub const BUILTINS: &[BuiltinProvider] = &[
    BuiltinProvider::Postgres(PostgresProvider),
    BuiltinProvider::Qdrant(QdrantProvider),
    BuiltinProvider::Kafka(KafkaProvider),
    BuiltinProvider::Nats(NatsProvider),
    BuiltinProvider::DynamoDb(DynamoDbProvider),
];

pub enum BuiltinExecutor {
    Postgres(PostgresExecutor),
    Qdrant(QdrantExecutor),
    Kafka(KafkaExecutor),
    Nats(NatsExecutor),
    DynamoDb(DynamoDbExecutor),
}

impl Provider for BuiltinProvider {
    type Executor = BuiltinExecutor;

    fn descriptor(&self) -> &'static ProviderDescriptor {
        match self {
            Self::Postgres(p) => p.descriptor(),
            Self::Qdrant(p) => p.descriptor(),
            Self::Kafka(p) => p.descriptor(),
            Self::Nats(p) => p.descriptor(),
            Self::DynamoDb(p) => p.descriptor(),
        }
    }

    fn validate_config(&self, options: &toml::Table) -> Result<()> {
        match self {
            Self::Postgres(p) => p.validate_config(options),
            Self::Qdrant(p) => p.validate_config(options),
            Self::Kafka(p) => p.validate_config(options),
            Self::Nats(p) => p.validate_config(options),
            Self::DynamoDb(p) => p.validate_config(options),
        }
    }

    fn configure(
        &self,
        options: &toml::Table,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<BuiltinExecutor> {
        match self {
            Self::Postgres(p) => p.configure(options, env).map(BuiltinExecutor::Postgres),
            Self::Qdrant(p) => p.configure(options, env).map(BuiltinExecutor::Qdrant),
            Self::Kafka(p) => p.configure(options, env).map(BuiltinExecutor::Kafka),
            Self::Nats(p) => p.configure(options, env).map(BuiltinExecutor::Nats),
            Self::DynamoDb(p) => p.configure(options, env).map(BuiltinExecutor::DynamoDb),
        }
    }
}

impl Executor for BuiltinExecutor {
    async fn stop_follow(&self, context: ShutdownContext) -> Result<()> {
        match self {
            Self::Postgres(e) => e.stop_follow(context).await,
            Self::Qdrant(e) => e.stop_follow(context).await,
            Self::Kafka(e) => e.stop_follow(context).await,
            Self::Nats(e) => e.stop_follow(context).await,
            Self::DynamoDb(e) => e.stop_follow(context).await,
        }
    }
    async fn follow_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        match self {
            Self::Postgres(e) => e.follow_page(request, context).await,
            Self::Qdrant(e) => e.follow_page(request, context).await,
            Self::Kafka(e) => e.follow_page(request, context).await,
            Self::Nats(e) => e.follow_page(request, context).await,
            Self::DynamoDb(e) => e.follow_page(request, context).await,
        }
    }
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        match self {
            Self::Postgres(e) => e.query_page(request, context).await,
            Self::Qdrant(e) => e.query_page(request, context).await,
            Self::Kafka(e) => e.query_page(request, context).await,
            Self::Nats(e) => e.query_page(request, context).await,
            Self::DynamoDb(e) => e.query_page(request, context).await,
        }
    }
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        match self {
            Self::Postgres(e) => e.status(),
            Self::Qdrant(e) => e.status(),
            Self::Kafka(e) => e.status(),
            Self::Nats(e) => e.status(),
            Self::DynamoDb(e) => e.status(),
        }
    }

    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        match self {
            Self::Postgres(e) => e.check(context).await,
            Self::Qdrant(e) => e.check(context).await,
            Self::Kafka(e) => e.check(context).await,
            Self::Nats(e) => e.check(context).await,
            Self::DynamoDb(e) => e.check(context).await,
        }
    }

    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        match self {
            Self::Postgres(e) => e.fetch_page(request, context).await,
            Self::Qdrant(e) => e.fetch_page(request, context).await,
            Self::Kafka(e) => e.fetch_page(request, context).await,
            Self::Nats(e) => e.fetch_page(request, context).await,
            Self::DynamoDb(e) => e.fetch_page(request, context).await,
        }
    }

    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        match self {
            Self::Postgres(e) => e.shutdown(context).await,
            Self::Qdrant(e) => e.shutdown(context).await,
            Self::Kafka(e) => e.shutdown(context).await,
            Self::Nats(e) => e.shutdown(context).await,
            Self::DynamoDb(e) => e.shutdown(context).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::provider::{find_provider, validate_catalog};
    use std::time::Duration;

    #[tokio::test]
    async fn dynamodb_variant_registers_without_loading_ambient_credentials() {
        let provider = find_provider(BUILTINS, "dynamodb").unwrap();
        let options = toml::from_str("region='us-east-1'").unwrap();
        provider.validate_config(&options).unwrap();
        let mut executor = provider
            .configure(&options, &|_| panic!("no configured secret"))
            .unwrap();
        assert!(matches!(executor, BuiltinExecutor::DynamoDb(_)));
        assert_eq!(
            provider.descriptor().entry_resource,
            Some("dynamodb.resources")
        );
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .check(context)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("cancelled")
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    }

    #[tokio::test]
    async fn nats_variant_delegates_lazy_configuration_cancellation_and_shutdown() {
        let provider = find_provider(BUILTINS, "nats").unwrap();
        let options = toml::from_str("servers=['nats://localhost:4222']").unwrap();
        let mut executor = provider
            .configure(&options, &|_| panic!("no configured secret"))
            .unwrap();
        assert!(matches!(executor, BuiltinExecutor::Nats(_)));
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .check(context)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("cancelled")
        );
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .follow_page(
                    PageRequest {
                        resource: onetui_core::Resource::new("nats.messages", vec!["DEMO".into()]),
                        continuation: None,
                    },
                    context
                )
                .await
                .is_err()
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    }

    #[tokio::test]
    async fn postgres_variant_delegates_configuration_status_cancel_and_shutdown() {
        validate_catalog(BUILTINS).unwrap();
        assert_eq!(BUILTINS.len(), 5);
        let provider = find_provider(BUILTINS, "postgres").unwrap();
        let options = toml::from_str("url_env='DSN'").unwrap();
        provider.validate_config(&options).unwrap();
        let mut executor = provider
            .configure(&options, &|_| Some("host=localhost sslmode=disable".into()))
            .unwrap();
        assert!(matches!(executor, BuiltinExecutor::Postgres(_)));
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .check(context)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("cancelled")
        );
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .fetch_page(
                    PageRequest {
                        resource: onetui_core::Resource::new("postgres.schemas", vec![]),
                        continuation: None
                    },
                    context
                )
                .await
                .is_err()
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    }

    #[tokio::test]
    async fn qdrant_variant_delegates_configuration_capabilities_and_shutdown() {
        let provider = find_provider(BUILTINS, "qdrant").unwrap();
        let options = toml::from_str("url='http://localhost:6334'").unwrap();
        provider.validate_config(&options).unwrap();
        assert_eq!(
            provider.descriptor().entry_resource,
            Some("qdrant.collections")
        );
        let mut executor = provider
            .configure(&options, &|_| panic!("no configured secret"))
            .unwrap();
        assert!(matches!(executor, BuiltinExecutor::Qdrant(_)));
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(executor.check(context).await.is_err());
        let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
        assert!(
            executor
                .fetch_page(
                    PageRequest {
                        resource: onetui_core::Resource::new("qdrant.points", vec![]),
                        continuation: None
                    },
                    context
                )
                .await
                .is_err()
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    }

    #[tokio::test]
    async fn kafka_variant_delegates_replay_without_connecting() {
        let provider = find_provider(BUILTINS, "kafka").unwrap();
        let options =
            toml::from_str("bootstrap_servers=['localhost:19092']\nsecurity_protocol='PLAINTEXT'")
                .unwrap();
        provider.validate_config(&options).unwrap();
        assert_eq!(
            provider.descriptor().entry_resource,
            Some("kafka.resources")
        );
        assert_eq!(provider.descriptor().query.unwrap().resource, "kafka.query");
        assert_eq!(provider.descriptor().follow_resources, &["kafka.records"]);
        let mut executor = provider
            .configure(&options, &|_| panic!("no secret configured"))
            .unwrap();
        assert!(matches!(executor, BuiltinExecutor::Kafka(_)));
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(executor.check(context).await.is_err());
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .query_page(
                    QueryRequest {
                        page: PageRequest {
                            resource: onetui_core::Resource::new(
                                "kafka.query",
                                vec!["demo_events".into(), "0".into()]
                            ),
                            continuation: None,
                        },
                        text: "{}".into(),
                    },
                    context
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
        let (cancel, context) = RequestContext::new(Duration::from_secs(1));
        cancel.send(()).unwrap();
        assert!(
            executor
                .follow_page(
                    PageRequest {
                        resource: onetui_core::Resource::new(
                            "kafka.records",
                            vec!["demo_live".into(), "0".into()]
                        ),
                        continuation: None,
                    },
                    context
                )
                .await
                .is_err()
        );
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    }
}
