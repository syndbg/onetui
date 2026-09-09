use anyhow::{Result, anyhow, ensure};
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page};
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore, oneshot, watch};
use tokio::time::Instant;

pub struct KafkaProvider;
static NEXT_EXECUTOR: AtomicU64 = AtomicU64::new(1);
// Native destruction and certificate/DNS reads cannot be aborted by dropping an async future.
// Keep the slot until the native owner exits, including after a timed-out shutdown.
static NATIVE_OWNER: Semaphore = Semaphore::const_new(1);

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    query: None,
    kind: "kafka",
    entry_resource: Some("kafka.topics"),
    browsing: "topics / partitions / read-committed records",
    resources: crate::browse::RESOURCES,
    documentation: crate::capabilities,
};

impl Provider for KafkaProvider {
    type Executor = KafkaExecutor;
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
    ) -> Result<KafkaExecutor> {
        let identity = NEXT_EXECUTOR.fetch_add(1, Ordering::Relaxed);
        let (config, secrets) = crate::config::Config::parse(options)?.native(identity, env)?;
        Ok(KafkaExecutor {
            config,
            secrets,
            identity,
            owner: Mutex::new(None),
            status: watch::channel(ConnectionStatus::Configured).0,
            closed: false,
        })
    }
}

pub struct KafkaExecutor {
    config: ClientConfig,
    secrets: Vec<String>,
    identity: u64,
    owner: Mutex<Option<Owner>>,
    status: watch::Sender<ConnectionStatus>,
    closed: bool,
}

struct Owner {
    send: mpsc::SyncSender<Job>,
    done: oneshot::Receiver<()>,
}

enum Operation {
    Check,
    Page(PageRequest),
}

enum Response {
    Check(CheckResult),
    Page(Page),
}

struct Job {
    operation: Operation,
    deadline: Instant,
    reply: oneshot::Sender<Result<Response>>,
}

impl Job {
    fn remaining(&self) -> Result<Duration> {
        ensure!(!self.reply.is_closed(), "Request cancelled");
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "Request timed out");
        Ok(remaining)
    }
}

struct NativeContext {
    status: watch::Sender<ConnectionStatus>,
    last_error: Arc<StdMutex<Option<String>>>,
    secrets: Vec<String>,
}

impl ClientContext for NativeContext {
    // Native logs bypass the terminal renderer and can include authentication data.
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, error: KafkaError, reason: &str) {
        let secrets: Vec<_> = self.secrets.iter().map(String::as_str).collect();
        let text = onetui_core::diagnostic(anyhow!("{error}: {reason}"), &secrets).to_string();
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(text);
        self.status.send_replace(ConnectionStatus::Disconnected);
    }
}
impl ConsumerContext for NativeContext {}

impl KafkaExecutor {
    async fn execute(&self, operation: Operation, mut context: RequestContext) -> Result<Response> {
        ensure!(!self.closed, "Kafka session is closed");
        let mut owner = context.run(self.owner.lock()).await?;
        if owner.is_none() {
            let permit = context.run(NATIVE_OWNER.acquire()).await??;
            let (send, receive) = mpsc::sync_channel::<Job>(1);
            let (done, finished) = oneshot::channel();
            let config = self.config.clone();
            let status = self.status.clone();
            let secrets = self.secrets.clone();
            let identity = self.identity;
            std::thread::Builder::new()
                .name("onetui-kafka".into())
                .spawn(move || {
                    let _permit = permit;
                    let mut client = None;
                    let last_error = Arc::new(StdMutex::new(None));
                    while let Ok(job) = receive.recv() {
                        let result = (|| {
                            job.remaining()?;
                            if client.is_none() {
                                status.send_replace(ConnectionStatus::Connecting);
                                client = Some(config.create_with_context::<_, BaseConsumer<_>>(
                                    NativeContext {
                                        status: status.clone(),
                                        last_error: last_error.clone(),
                                        secrets: secrets.clone(),
                                    },
                                )?);
                            }
                            *last_error.lock().unwrap_or_else(|e| e.into_inner()) = None;
                            let result = run(client.as_ref().unwrap(), &job, identity);
                            job.remaining()?;
                            result
                        })();
                        let result = result.map_err(|error| {
                            let error =
                                match last_error.lock().unwrap_or_else(|e| e.into_inner()).take() {
                                    Some(native) => error.context(native),
                                    None => error,
                                };
                            onetui_core::diagnostic(
                                error,
                                &secrets.iter().map(String::as_str).collect::<Vec<_>>(),
                            )
                        });
                        if result.is_ok() {
                            status.send_replace(ConnectionStatus::Connected);
                        } else {
                            status.send_replace(ConnectionStatus::Disconnected);
                        }
                        let _ = job.reply.send(result);
                    }
                    drop(client);
                    status.send_replace(ConnectionStatus::Closed);
                    let _ = done.send(());
                })?;
            *owner = Some(Owner {
                send,
                done: finished,
            });
        }
        let (reply, receive) = oneshot::channel();
        owner
            .as_ref()
            .unwrap()
            .send
            .try_send(Job {
                operation,
                deadline: context.deadline,
                reply,
            })
            .map_err(|_| anyhow!("Kafka native worker is busy or stopped"))?;
        context
            .run(receive)
            .await?
            .map_err(|_| anyhow!("Kafka native worker stopped"))?
    }
}

impl Executor for KafkaExecutor {
    fn status(&self) -> watch::Receiver<ConnectionStatus> {
        self.status.subscribe()
    }
    async fn check(&self, context: RequestContext) -> Result<CheckResult> {
        match self.execute(Operation::Check, context).await? {
            Response::Check(result) => Ok(result),
            _ => unreachable!(),
        }
    }
    async fn fetch_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        crate::browse::validate(&request, self.identity)?;
        match self.execute(Operation::Page(request), context).await? {
            Response::Page(page) => Ok(page),
            _ => unreachable!(),
        }
    }
    async fn shutdown(&mut self, context: ShutdownContext) -> Result<()> {
        self.closed = true;
        self.status.send_replace(ConnectionStatus::Closing);
        if let Some(Owner { send, done }) = self.owner.get_mut().take() {
            drop(send);
            tokio::time::timeout_at(context.deadline, done)
                .await
                .map_err(|_| {
                    anyhow!("Kafka native cleanup still running; its slot remains reserved")
                })?
                .map_err(|_| anyhow!("Kafka native worker stopped during shutdown"))?;
        }
        self.status.send_replace(ConnectionStatus::Closed);
        Ok(())
    }
}

fn run(client: &BaseConsumer<NativeContext>, job: &Job, identity: u64) -> Result<Response> {
    let request = match &job.operation {
        Operation::Check => {
            let metadata = client.fetch_metadata(None, job.remaining()?)?;
            return Ok(Response::Check(CheckResult {
                summary: format!(
                    "Kafka metadata readable ({} brokers, {} topics)",
                    metadata.brokers().len(),
                    metadata.topics().len()
                ),
            }));
        }
        Operation::Page(request) => request,
    };
    let position = crate::browse::validate(request, identity)?;
    if request.resource.id != "kafka.records" {
        let metadata = client.fetch_metadata(
            request.resource.path.first().map(String::as_str),
            job.remaining()?,
        )?;
        return crate::browse::metadata(
            &request.resource,
            &metadata,
            position.map_or(0, |p| p.offset),
            identity,
        )
        .map(Response::Page);
    }
    let topic = &request.resource.path[0];
    let partition: i32 = request.resource.path[1].parse()?;
    let (low, high) = client.fetch_watermarks(topic, partition, job.remaining()?)?;
    let (start, end) = position.map_or((low, high), |p| (p.offset, p.end.unwrap()));
    ensure!(
        start >= low && start <= end && end <= high,
        "Kafka offsets unavailable: requested [{start}, {end}), available [{low}, {high}); refresh"
    );
    let mut page = crate::browse::page(
        &request.resource,
        &format!(
            "Read-committed offsets [{start}, {end}); no snapshot or offset commits. Headers retain ordered names and nullable byte arrays."
        ),
    );
    if start == end {
        return crate::browse::finish(page, &request.resource, identity, None).map(Response::Page);
    }
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, partition, Offset::Offset(start))?;
    client.assign(&assignment)?;
    let result = (|| {
        let mut next = start;
        loop {
            let wait = job.remaining()?.min(Duration::from_millis(50));
            match client.poll(wait) {
                Some(Ok(message)) => {
                    ensure!(
                        message.topic() == topic
                            && message.partition() == partition
                            && message.offset() >= next,
                        "Kafka returned a record outside the requested position"
                    );
                    if message.offset() >= end {
                        next = end;
                        break;
                    }
                    next = message
                        .offset()
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("Kafka offset overflow"))?;
                    page.rows.push(crate::browse::record(&message)?);
                    ensure!(
                        page.bytes() <= PAGE_BYTES,
                        "Kafka page exceeds 1 MiB; current page and bookmark retained"
                    );
                    if next >= end || page.rows.len() >= PAGE_SIZE as usize {
                        break;
                    }
                }
                Some(Err(KafkaError::PartitionEOF(_))) => {
                    let position = client.position()?;
                    let reached = position
                        .find_partition(topic, partition)
                        .map(|p| p.offset());
                    if matches!(reached, Some(Offset::Offset(offset)) if offset >= end) {
                        next = end;
                        break;
                    }
                    // A transaction can hold back read-committed visibility below the captured high watermark.
                    // Return a visible failure instead of declaring EOF or silently moving past that transaction.
                    anyhow::bail!(
                        "Kafka read-committed end is below the captured offset {end}; transaction or log state changed; refresh"
                    );
                }
                Some(Err(error)) => return Err(error.into()),
                None => {}
            }
        }
        crate::browse::finish(
            page,
            &request.resource,
            identity,
            (next < end).then_some((next, Some(end))),
        )
    })();
    let unassigned = client.unassign();
    let page = result?;
    unassigned?;
    Ok(Response::Page(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::{Resource, Value};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    async fn fetch(
        executor: &KafkaExecutor,
        resource: Resource,
        continuation: Option<String>,
    ) -> Page {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor
            .fetch_page(
                PageRequest {
                    resource,
                    continuation,
                },
                context,
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn native_metadata_paging_bookmarks_cancel_and_shutdown() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("records", 2, 1).unwrap();
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", cluster.bootstrap_servers())
            .create()
            .unwrap();
        for n in 0..250u32 {
            producer
                .send(
                    BaseRecord::to("records")
                        .partition(0)
                        .key(&[255u8, 0][..])
                        .payload(&n.to_be_bytes()[..]),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();
        let options = toml::from_str(&format!(
            "bootstrap_servers={:?}\nsecurity_protocol='PLAINTEXT'",
            vec![cluster.bootstrap_servers()]
        ))
        .unwrap();
        let mut executor = KafkaProvider
            .configure(&options, &|_| panic!("no credentials"))
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
        let (cancel, context) = RequestContext::new(Duration::from_secs(5));
        cancel.send(()).unwrap();
        assert!(executor.check(context).await.is_err());
        assert!(
            executor.owner.lock().await.is_none(),
            "cancelled request must not start native work"
        );
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        let check = executor.check(context).await.unwrap();
        assert!(check.summary.contains("metadata readable"));
        let topics = fetch(&executor, Resource::new("kafka.topics", vec![]), None).await;
        assert_eq!(topics.rows.len(), 1);
        let partitions = fetch(&executor, topics.rows[0].target.clone().unwrap(), None).await;
        assert_eq!(partitions.rows.len(), 2);
        let resource = partitions.rows[0].target.clone().unwrap();
        let first = fetch(&executor, resource.clone(), None).await;
        assert_eq!(first.rows.len(), 100);
        assert_eq!(first.rows[0].cells[0], Some("0".into()));
        assert_eq!(first.rows[0].cells[2], Some(Value::Bytes(vec![255, 0])));
        assert_eq!(
            first.rows[0].cells[3],
            Some(Value::Bytes(0u32.to_be_bytes().to_vec()))
        );
        let second = fetch(&executor, resource.clone(), first.continuation.clone()).await;
        assert_eq!(second.rows[0].cells[0], Some("100".into()));
        let last = fetch(&executor, resource.clone(), second.continuation).await;
        assert_eq!(last.rows.len(), 50);
        assert!(!last.next);
        let previous = fetch(&executor, resource.clone(), first.continuation).await;
        assert_eq!(previous.rows[0].cells[0], Some("100".into()));
        let beginning = fetch(&executor, resource, None).await;
        assert_eq!(beginning.rows[0].cells[0], Some("0".into()));
        let empty = fetch(&executor, partitions.rows[1].target.clone().unwrap(), None).await;
        assert!(empty.rows.is_empty() && !empty.next);
        // A separate observer checks the private group without committing or subscribing itself.
        let observer: BaseConsumer = executor.config.create().unwrap();
        let mut offsets = TopicPartitionList::new();
        offsets.add_partition("records", 0);
        let committed = observer
            .committed_offsets(offsets, Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            committed.find_partition("records", 0).unwrap().offset(),
            Offset::Invalid
        );
        drop(observer);
        executor
            .shutdown(ShutdownContext::new(Duration::from_secs(2)))
            .await
            .unwrap();
        assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
        let (_cancel, context) = RequestContext::new(Duration::from_secs(1));
        assert!(executor.check(context).await.is_err());
        let _permit = tokio::time::timeout(Duration::from_secs(1), NATIVE_OWNER.acquire())
            .await
            .unwrap()
            .unwrap();
    }
}
