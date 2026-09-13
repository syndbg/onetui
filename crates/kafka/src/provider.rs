use anyhow::{Result, anyhow, ensure};
use onetui_core::provider::{
    CheckResult, ConnectionStatus, Executor, PageRequest, Provider, ProviderDescriptor,
    QueryDescriptor, QueryRequest, RequestContext, ShutdownContext,
};
use onetui_core::{PAGE_BYTES, PAGE_SIZE, Page};
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
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
pub(crate) const NATIVE_ERROR_COUNT: usize = 4;
pub(crate) const NATIVE_ERROR_BYTES: usize = 2048;
static NEXT_EXECUTOR: AtomicU64 = AtomicU64::new(1);
// Native destruction and certificate/DNS reads cannot be aborted by dropping an async future.
// Keep the slot until the native owner exits, including after a timed-out shutdown.
static NATIVE_OWNER: Semaphore = Semaphore::const_new(1);

pub static DESCRIPTOR: ProviderDescriptor = ProviderDescriptor {
    follow_resource: Some("kafka.records"),
    query: Some(QueryDescriptor {
        resource: "kafka.query",
        language: "Kafka replay JSON",
        example: "{}",
        path_depth: 2,
    }),
    kind: "kafka",
    entry_resource: Some("kafka.resources"),
    browsing: "topics / partitions / records and configuration; brokers / configuration; groups / members and offsets",
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
        let mut options = crate::config::Config::parse(options)?;
        let (config, mut secrets) = options.native(identity, env)?;
        for binding in &mut options.decoders {
            if let Some(registry) = &mut binding.registry {
                secrets.extend(registry.resolve(env)?);
            }
            if let Some(buf) = &mut binding.buf {
                secrets.extend(buf.resolve(env)?);
            }
        }
        Ok(KafkaExecutor {
            config,
            decoders: options.decoders,
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
    decoders: Vec<crate::decoding::Binding>,
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
    Follow(PageRequest),
    Query(QueryRequest),
}

enum Response {
    Check(CheckResult),
    Page(Page),
}

struct Job {
    operation: Operation,
    deadline: Instant,
    reply: oneshot::Sender<Result<Response>>,
    errors: Arc<StdMutex<Vec<String>>>,
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
    errors: StdMutex<Arc<StdMutex<Vec<String>>>>,
    secrets: Vec<String>,
}

impl ClientContext for NativeContext {
    // Native logs bypass the terminal renderer and can include authentication data.
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, error: KafkaError, reason: &str) {
        let secrets: Vec<_> = self.secrets.iter().map(String::as_str).collect();
        let mut text = onetui_core::diagnostic(anyhow!("{error}: {reason}"), &secrets).to_string();
        if text.len() > NATIVE_ERROR_BYTES {
            text.truncate(text.floor_char_boundary(NATIVE_ERROR_BYTES - 3));
            text.push_str("...");
        }
        let errors = self.errors.lock().unwrap_or_else(|e| e.into_inner());
        let mut errors = errors.lock().unwrap_or_else(|e| e.into_inner());
        if !errors.contains(&text) {
            if errors.len() == NATIVE_ERROR_COUNT {
                errors.remove(0);
            }
            errors.push(text);
        }
        self.status.send_replace(ConnectionStatus::Disconnected);
    }
}
impl ConsumerContext for NativeContext {}

fn request_error(error: anyhow::Error, errors: &StdMutex<Vec<String>>) -> anyhow::Error {
    let errors = errors.lock().unwrap_or_else(|e| e.into_inner());
    if errors.is_empty() {
        error
    } else {
        error.context(errors.join("; "))
    }
}

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
            let bindings = self.decoders.clone();
            std::thread::Builder::new()
                .name("onetui-kafka".into())
                .spawn(move || {
                    let _permit = permit;
                    let mut client = None;
                    let mut decoders = crate::decoding::Bindings::new(bindings);
                    while let Ok(job) = receive.recv() {
                        let result = (|| {
                            job.remaining()?;
                            if matches!(job.operation, Operation::Check) {
                                decoders.check(|| job.remaining().map(|_| ()))?;
                            }
                            if client.is_none() {
                                status.send_replace(ConnectionStatus::Connecting);
                                client = Some(config.create_with_context::<_, BaseConsumer<_>>(
                                    NativeContext {
                                        status: status.clone(),
                                        errors: StdMutex::new(job.errors.clone()),
                                        secrets: secrets.clone(),
                                    },
                                )?);
                            }
                            *client
                                .as_ref()
                                .unwrap()
                                .context()
                                .errors
                                .lock()
                                .unwrap_or_else(|e| e.into_inner()) = job.errors.clone();
                            let topic = match &job.operation {
                                Operation::Page(request) | Operation::Follow(request) => {
                                    request.resource.path.first()
                                }
                                Operation::Query(request) => request.page.resource.path.first(),
                                Operation::Check => None,
                            };
                            let raw_limit =
                                topic.map_or(PAGE_BYTES, |topic| decoders.raw_page_limit(topic));
                            let result = run(client.as_ref().unwrap(), &job, identity, raw_limit)
                                .and_then(|mut response| {
                                    let request = match &job.operation {
                                        Operation::Page(request) | Operation::Follow(request) => {
                                            Some(request)
                                        }
                                        Operation::Query(request) => Some(&request.page),
                                        Operation::Check => None,
                                    };
                                    if let (Some(request), Response::Page(page)) =
                                        (request, &mut response)
                                        && matches!(
                                            request.resource.id,
                                            "kafka.records" | "kafka.query"
                                        )
                                        && let Some(topic) = request.resource.path.first()
                                    {
                                        decoders
                                            .project(topic, page, || job.remaining().map(|_| ()))?;
                                    }
                                    Ok(response)
                                });
                            job.remaining()?;
                            result
                        })();
                        let result = result.map_err(|error| {
                            let error = request_error(error, &job.errors);
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
                    drop(decoders);
                    // A completed shutdown must make its owner slot immediately reusable.
                    drop(_permit);
                    status.send_replace(ConnectionStatus::Closed);
                    let _ = done.send(());
                })?;
            *owner = Some(Owner {
                send,
                done: finished,
            });
        }
        let (reply, receive) = oneshot::channel();
        // Foreground deadlines can win the race with native cleanup. Keep this request's
        // diagnostics separately so neither timeout nor the next request replaces them.
        let errors = Arc::new(StdMutex::new(Vec::new()));
        owner
            .as_ref()
            .unwrap()
            .send
            .try_send(Job {
                operation,
                deadline: context.deadline,
                reply,
                errors: errors.clone(),
            })
            .map_err(|_| anyhow!("Kafka native worker is busy or stopped"))?;
        context
            .run(receive)
            .await
            .map_err(|error| request_error(error, &errors))?
            .map_err(|_| anyhow!("Kafka native worker stopped"))?
    }
}

impl Executor for KafkaExecutor {
    async fn query_page(&self, request: QueryRequest, context: RequestContext) -> Result<Page> {
        crate::query::prepare(&request, self.identity)?;
        match self.execute(Operation::Query(request), context).await? {
            Response::Page(page) => Ok(page),
            _ => unreachable!(),
        }
    }
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
        let position = crate::browse::validate(&request, self.identity, false)?;
        if matches!(
            request.resource.id,
            "kafka.resources" | "kafka.topic" | "kafka.group"
        ) {
            ensure!(!self.closed, "Kafka session is closed");
            let mut context = context;
            return context
                .run(async {
                    crate::browse::resources(
                        &request.resource,
                        position.map_or(0, |p| p.offset),
                        self.identity,
                    )
                })
                .await?;
        }
        match self.execute(Operation::Page(request), context).await? {
            Response::Page(page) => Ok(page),
            _ => unreachable!(),
        }
    }
    async fn follow_page(&self, request: PageRequest, context: RequestContext) -> Result<Page> {
        crate::browse::validate(&request, self.identity, true)?;
        match self.execute(Operation::Follow(request), context).await? {
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

fn native_request<T>(
    client: &BaseConsumer<NativeContext>,
    job: &Job,
    mut request: impl FnMut(Duration) -> Result<T, KafkaError>,
) -> Result<T> {
    loop {
        // Metadata/watermark waits do not service the consumer's error queue. Poll between
        // bounded attempts so certificate/authentication failures reach the UI before timeout.
        let result = request(job.remaining()?.min(Duration::from_millis(250)));
        match result {
            Ok(value) => return Ok(value),
            Err(error) => {
                if let Some(Err(native)) =
                    client.poll(job.remaining()?.min(Duration::from_millis(50)))
                    && !transient(&native)
                {
                    return Err(native.into());
                }
                if !transient(&error) {
                    return Err(error.into());
                }
            }
        }
    }
}

fn transient(error: &KafkaError) -> bool {
    !matches!(error, KafkaError::MessageConsumptionFatal(_))
        && matches!(
            error.rdkafka_error_code(),
            Some(
                RDKafkaErrorCode::BrokerTransportFailure
                    | RDKafkaErrorCode::AllBrokersDown
                    | RDKafkaErrorCode::OperationTimedOut
            )
        )
}

fn run(
    client: &BaseConsumer<NativeContext>,
    job: &Job,
    identity: u64,
    raw_limit: usize,
) -> Result<Response> {
    let prepared;
    let request = match &job.operation {
        Operation::Check => {
            let metadata = native_request(client, job, |wait| client.fetch_metadata(None, wait))?;
            return Ok(Response::Check(CheckResult {
                summary: format!(
                    "Kafka metadata readable ({} brokers, {} topics)",
                    metadata.brokers().len(),
                    metadata.topics().len()
                ),
            }));
        }
        Operation::Page(request) | Operation::Follow(request) => request,
        Operation::Query(request) => {
            prepared = crate::query::prepare(request, identity)?;
            &prepared.1
        }
    };
    let following = matches!(job.operation, Operation::Follow(_));
    let position = crate::browse::validate(request, identity, following)?;
    if request.resource.id == "kafka.records" && request.resource.path.len() == 1 {
        let topic = &request.resource.path[0];
        let metadata =
            native_request(client, job, |wait| client.fetch_metadata(Some(topic), wait))?;
        let metadata = metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .ok_or_else(|| anyhow!("Kafka topic missing from metadata; refresh its parent"))?;
        ensure!(
            metadata.error().is_none(),
            "Kafka topic metadata: {:?}",
            metadata.error()
        );
        ensure!(
            metadata.partitions().len() <= crate::topic::PARTITIONS,
            "Kafka topic-wide view supports at most 32 partitions; open an individual partition"
        );
        let mut windows = Vec::new();
        for partition in metadata.partitions() {
            ensure!(
                partition.error().is_none(),
                "Kafka partition metadata: {:?}",
                partition.error()
            );
            let (low, end) = native_request(client, job, |wait| {
                client.fetch_watermarks(topic, partition.id(), wait)
            })?;
            windows.push((partition.id(), low, end));
        }
        return crate::topic::page(
            request,
            identity,
            following,
            raw_limit,
            windows,
            |partition, window, limit| {
                read_window(
                    client,
                    job,
                    &onetui_core::Resource::new(
                        "kafka.records",
                        vec![topic.clone(), partition.to_string()],
                    ),
                    window,
                    limit,
                    true,
                    raw_limit,
                )
            },
        )
        .map(Response::Page);
    }
    if matches!(
        request.resource.id,
        "kafka.topic_config" | "kafka.broker_config" | "kafka.offsets"
    ) {
        return crate::inspect::page(
            client,
            &request.resource,
            position.map_or(0, |p| p.offset),
            identity,
            || job.remaining(),
            |topic, partition| {
                native_request(client, job, |wait| {
                    client.fetch_watermarks(topic, partition, wait)
                })
            },
        )
        .map(Response::Page);
    }
    if matches!(request.resource.id, "kafka.groups" | "kafka.members") {
        let groups = native_request(client, job, |wait| {
            crate::groups::Groups::fetch(
                client.client(),
                request.resource.path.first().map(String::as_str),
                wait,
            )
        })?;
        return groups
            .page(
                &request.resource,
                position.map_or(0, |p| p.offset),
                identity,
            )
            .map(Response::Page);
    }
    if request.resource.id != "kafka.records" {
        let metadata = native_request(client, job, |wait| {
            client.fetch_metadata(request.resource.path.first().map(String::as_str), wait)
        })?;
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
    // Watermark lookup may report UnknownPartition while authentication/metadata is still
    // pending. Resolve the selected topic first so its actual authorization error survives.
    let metadata = native_request(client, job, |wait| client.fetch_metadata(Some(topic), wait))?;
    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| anyhow!("Kafka topic missing from metadata; refresh its parent"))?;
    ensure!(
        topic_metadata.error().is_none(),
        "Kafka topic metadata: {:?}",
        topic_metadata.error()
    );
    let partition_metadata = topic_metadata
        .partitions()
        .iter()
        .find(|p| p.id() == partition)
        .ok_or_else(|| anyhow!("Kafka partition missing from metadata; refresh its parent"))?;
    ensure!(
        partition_metadata.error().is_none(),
        "Kafka partition metadata: {:?}",
        partition_metadata.error()
    );
    let (low, stable_end) = native_request(client, job, |wait| {
        client.fetch_watermarks(topic, partition, wait)
    })?;
    let (start, end) = if following {
        ensure!(
            position
                .as_ref()
                .is_none_or(|p| p.end.is_some_and(|end| end <= stable_end)),
            "Kafka live window moved backwards; following stopped, start again explicitly"
        );
        (position.map_or(stable_end, |p| p.offset), stable_end)
    } else {
        if let Some(position) = position {
            (position.offset, position.end.unwrap())
        } else if let Operation::Query(query) = &job.operation {
            let (replay, _) = crate::query::prepare(query, identity)?;
            let end = replay.end_offset.unwrap_or(stable_end);
            ensure!(
                end >= low && end <= stable_end,
                "Kafka replay end_offset {end} outside available [{low}, {stable_end}]"
            );
            let start = if let Some(timestamp) = replay.timestamp_ms {
                let offsets = native_request(client, job, |wait| {
                    let mut timestamps = TopicPartitionList::new();
                    timestamps.add_partition_offset(topic, partition, Offset::Offset(timestamp))?;
                    client.offsets_for_times(timestamps, wait)
                })?;
                let result = offsets
                    .find_partition(topic, partition)
                    .ok_or_else(|| anyhow!("Kafka timestamp lookup returned no partition"))?;
                result.error()?;
                // A timestamp after the last record has no match. A match beyond the
                // requested read-committed window also yields an empty replay.
                match result.offset() {
                    Offset::Offset(offset) if offset >= 0 => offset.min(end),
                    Offset::End => end,
                    offset => {
                        anyhow::bail!("Kafka timestamp lookup returned invalid offset {offset:?}")
                    }
                }
            } else {
                replay.offset.unwrap_or(low)
            };
            (start, end)
        } else {
            (low, stable_end)
        }
    };
    ensure!(
        start >= low && start <= end && end <= stable_end,
        "Kafka offsets unavailable: requested [{start}, {end}), available [{low}, {stable_end}); refresh"
    );
    let (page, next) = read_window(
        client,
        job,
        &request.resource,
        start..end,
        PAGE_SIZE as usize,
        following,
        raw_limit,
    )?;
    let page = crate::browse::finish(
        page,
        &request.resource,
        identity,
        (following || next < end).then_some((next, Some(end))),
        following,
    )?;
    response(page, job)
}

fn read_window(
    client: &BaseConsumer<NativeContext>,
    job: &Job,
    resource: &onetui_core::Resource,
    window: std::ops::Range<i64>,
    limit: usize,
    split_bytes: bool,
    raw_limit: usize,
) -> Result<(Page, i64)> {
    let topic = &resource.path[0];
    let partition = resource.path[1].parse::<i32>()?;
    let (start, end) = (window.start, window.end);
    let mut page = crate::browse::page(
        resource,
        &format!(
            "Read-committed offsets [{start}, {end}); no snapshot or offset commits. Headers retain ordered names and nullable byte arrays."
        ),
    );
    if start == end {
        return Ok((page, end));
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
                    let row = crate::browse::record(&message)?;
                    page.rows.push(row);
                    if (split_bytes || raw_limit < PAGE_BYTES)
                        && page.bytes() > raw_limit
                        && page.rows.len() > 1
                    {
                        page.rows.pop();
                        next = message.offset();
                        break;
                    }
                    next = message
                        .offset()
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("Kafka offset overflow"))?;
                    ensure!(
                        page.bytes() <= raw_limit,
                        "Kafka page exceeds {raw_limit} bytes; current page and bookmark retained"
                    );
                    if next >= end || page.rows.len() >= limit {
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
                    // Log/leader changes can invalidate the captured stable end between lookup and fetch.
                    // An earlier EOF must not silently move the bookmark past missing data.
                    anyhow::bail!(
                        "Kafka read-committed end is below the captured offset {end}; transaction or log state changed; refresh"
                    );
                }
                // A failed bootstrap address can remain queued after another address connects.
                // Let librdkafka reconnect within the same request deadline.
                Some(Err(error)) if transient(&error) => {}
                Some(Err(error)) => return Err(error.into()),
                None => {}
            }
        }
        Ok((page, next))
    })();
    let unassigned = client.unassign();
    let page = result?;
    unassigned?;
    Ok(page)
}

fn response(page: Page, job: &Job) -> Result<Response> {
    Ok(Response::Page(match &job.operation {
        Operation::Query(query) => crate::query::finish(page, query.text.clone())?,
        _ => page,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use onetui_core::{Resource, Value};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    #[tokio::test]
    async fn foreground_timeout_keeps_bounded_native_errors_for_its_own_request() {
        let first = Arc::new(StdMutex::new(Vec::new()));
        let native = NativeContext {
            status: watch::channel(ConnectionStatus::Connected).0,
            errors: StdMutex::new(first.clone()),
            secrets: vec!["fixture-secret".into()],
        };
        let error = KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure);
        native.error(error.clone(), "TLS alert fixture-secret\x1b[31m");
        native.error(error.clone(), "IPv6 connection refused");
        let (_cancel, mut context) = RequestContext::new(Duration::from_millis(1));
        let timeout = context.run(std::future::pending::<()>()).await.unwrap_err();
        let diagnostic = format!("{:#}", request_error(timeout, &first));
        assert!(diagnostic.contains("TLS alert"));
        assert!(diagnostic.contains("IPv6 connection refused"));
        assert!(diagnostic.contains("timed out"));
        assert!(!diagnostic.contains("fixture-secret"));
        assert!(!diagnostic.contains('\x1b'));
        let second = Arc::new(StdMutex::new(Vec::new()));
        *native.errors.lock().unwrap() = second.clone();
        assert_eq!(
            request_error(anyhow!("Request timed out"), &second).to_string(),
            "Request timed out"
        );
        for n in 0..10 {
            native.error(error.clone(), &format!("{n}: {}", "界".repeat(1000)));
        }
        let errors = second.lock().unwrap();
        assert_eq!(errors.len(), 4);
        assert!(
            errors
                .iter()
                .all(|text| text.len() <= 2048 && text.ends_with("..."))
        );
        assert_eq!(first.lock().unwrap().len(), 2);
    }

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

    async fn follow(
        executor: &KafkaExecutor,
        resource: Resource,
        continuation: Option<String>,
    ) -> Page {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor
            .follow_page(
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
        let menu = fetch(&executor, Resource::new("kafka.resources", vec![]), None).await;
        assert_eq!(menu.rows.len(), 3);
        assert_eq!(
            menu.rows[0].target,
            Some(Resource::new("kafka.topics", vec![]))
        );
        assert!(
            executor.owner.lock().await.is_none(),
            "local resource menu must not start a native client"
        );
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
        let brokers = fetch(&executor, Resource::new("kafka.brokers", vec![]), None).await;
        assert_eq!(brokers.rows.len(), 1);
        assert_eq!(brokers.rows[0].cells[0], Some("1".into()));
        assert_eq!(topics.rows.len(), 1);
        let topic_menu = fetch(&executor, topics.rows[0].target.clone().unwrap(), None).await;
        assert_eq!(topic_menu.rows.len(), 3);
        assert_eq!(
            topic_menu.rows[1].target.as_ref().unwrap().id,
            "kafka.topic_config"
        );
        let partitions = fetch(&executor, topic_menu.rows[0].target.clone().unwrap(), None).await;
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
        let beginning = fetch(&executor, resource.clone(), None).await;
        assert_eq!(beginning.rows[0].cells[0], Some("0".into()));
        let replay_text = r#"{"offset":125,"end_offset":240}"#;
        let mut continuation = None;
        for (offset, count) in [(125, 100), (225, 15)] {
            let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
            let page = executor
                .query_page(
                    QueryRequest {
                        page: PageRequest {
                            resource: Resource::new("kafka.query", resource.path.clone()),
                            continuation,
                        },
                        text: replay_text.into(),
                    },
                    context,
                )
                .await
                .unwrap();
            assert_eq!(page.rows.len(), count);
            assert_eq!(page.rows[0].cells[0], Some(offset.to_string().into()));
            continuation = page.continuation;
        }
        assert!(continuation.is_none());
        let empty = fetch(&executor, partitions.rows[1].target.clone().unwrap(), None).await;
        assert!(empty.rows.is_empty() && !empty.next);
        let tail = follow(&executor, resource.clone(), None).await;
        assert!(tail.rows.is_empty() && tail.continuation.is_some());
        let quiet = follow(&executor, resource.clone(), tail.continuation.clone()).await;
        assert!(quiet.rows.is_empty());
        assert_eq!(tail.continuation, quiet.continuation);
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        assert!(
            executor
                .fetch_page(
                    PageRequest {
                        resource: resource.clone(),
                        continuation: tail.continuation.clone()
                    },
                    context
                )
                .await
                .is_err()
        );
        for n in 250..500u32 {
            producer
                .send(
                    BaseRecord::to("records")
                        .partition(0)
                        .key("key")
                        .payload(&n.to_be_bytes()[..]),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(5)).unwrap();
        let mut token = tail.continuation.clone();
        let mut seen = Vec::new();
        for count in [100, 100, 50] {
            let batch = follow(&executor, resource.clone(), token).await;
            assert_eq!(batch.rows.len(), count);
            seen.extend(batch.rows.iter().map(|row| {
                row.cells[0]
                    .as_ref()
                    .unwrap()
                    .text()
                    .unwrap()
                    .parse::<u32>()
                    .unwrap()
            }));
            token = batch.continuation;
        }
        assert_eq!(seen, (250..500).collect::<Vec<_>>());
        let replay = follow(&executor, resource.clone(), tail.continuation).await;
        assert_eq!(replay.rows[0].cells[0], Some("250".into()));
        let quiet = follow(&executor, resource.clone(), token).await;
        assert!(quiet.rows.is_empty());
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
        cluster.broker_down(1).unwrap();
        for _ in 0..5 {
            let started = Instant::now();
            let (cancel, context) = RequestContext::new(Duration::from_secs(5));
            let (result, ()) = tokio::join!(executor.check(context), async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                let _ = cancel.send(());
            });
            assert!(
                result
                    .err()
                    .expect("cancelled request")
                    .to_string()
                    .contains("cancelled")
            );
            assert!(started.elapsed() < Duration::from_millis(500));
            executor
                .shutdown(ShutdownContext::new(Duration::from_secs(2)))
                .await
                .unwrap();
            assert_eq!(NATIVE_OWNER.available_permits(), 1);
            executor = KafkaProvider.configure(&options, &|_| None).unwrap();
        }
        cluster.broker_up(1).unwrap();
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        executor.check(context).await.unwrap();
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
