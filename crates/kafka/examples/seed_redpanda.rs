//! Seed only the disposable Redpanda broker and registry, never a user endpoint.
#[path = "support/redpanda.rs"]
mod fixture;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fixture::seed("demo_avro", 1000).await
}
