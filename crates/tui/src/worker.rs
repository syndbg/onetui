use crate::app::Request;
use anyhow::{Result, anyhow};
use onetui_core::Page;
use onetui_core::provider::{
    ConnectionStatus, Executor, PageRequest, RequestContext, ShutdownContext,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) enum WorkerEvent {
    Page(Result<Page>),
    Status(ConnectionStatus),
    Finished(Result<()>),
}

pub(crate) struct Worker {
    pub alias: String,
    pub session: u64,
    pub request: Option<Request>,
    pub task: tokio::task::JoinHandle<Result<()>>,
    pub results: mpsc::Receiver<Result<Page>>,
    pub status: watch::Receiver<ConnectionStatus>,
    pub status_open: bool,
    pub results_open: bool,
    pub closing: bool,
    closing_deadline: Option<tokio::time::Instant>,
    commands: mpsc::Sender<(Request, RequestContext)>,
    cancel: Option<oneshot::Sender<()>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Worker {
    pub async fn event(&mut self) -> WorkerEvent {
        loop {
            tokio::select! {
                biased;
                result = &mut self.task => return WorkerEvent::Finished(result.map_err(|_| anyhow!("browsing worker failed; terminal restored")).and_then(|r| r)),
                _ = async { tokio::time::sleep_until(self.closing_deadline.expect("closing deadline")).await }, if self.closing_deadline.is_some() => {
                    self.task.abort();
                    let _ = (&mut self.task).await;
                    return WorkerEvent::Finished(Err(anyhow!("browsing worker shutdown timed out; connections discarded")));
                },
                result = self.results.recv(), if self.results_open => match result {
                    Some(page) => {
                        self.cancel.take();
                        return WorkerEvent::Page(page);
                    }
                    None => self.results_open = false,
                },
                result = self.status.changed(), if self.status_open => {
                    self.status_open = result.is_ok();
                    if self.status_open { return WorkerEvent::Status(*self.status.borrow_and_update()); }
                },
            }
        }
    }

    pub fn new<E: Executor + 'static>(alias: String, session: u64, mut executor: E) -> Self {
        let (commands, mut input) = mpsc::channel::<(Request, RequestContext)>(1);
        let (output, results) = mpsc::channel(1);
        let (stop, mut stopped) = oneshot::channel();
        let status = executor.status();
        let task = tokio::spawn(async move {
            loop {
                let (request, context) = tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    next = input.recv() => match next { Some(next) => next, None => break },
                };
                let result = executor
                    .fetch_page(
                        PageRequest {
                            resource: request.resource.clone(),
                            continuation: request.continuation.clone(),
                        },
                        context,
                    )
                    .await;
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    result = output.send(result) => if result.is_err() { break; },
                }
            }
            let context = ShutdownContext::new(Duration::from_secs(1));
            tokio::time::timeout_at(context.deadline, executor.shutdown(context))
                .await
                .map_err(|_| anyhow!("session shutdown timed out"))?
        });
        Self {
            alias,
            session,
            request: None,
            task,
            results,
            status,
            commands,
            cancel: None,
            stop: Some(stop),
            closing: false,
            closing_deadline: None,
            status_open: true,
            results_open: true,
        }
    }

    pub fn submit(&mut self, request: Request, timeout: Duration) -> Result<()> {
        let (cancel, receiver) = oneshot::channel();
        let context = RequestContext {
            deadline: request.queued_at + timeout,
            cancel: receiver,
        };
        self.commands
            .try_send((request.clone(), context))
            .map_err(|_| anyhow!("browsing worker is unavailable"))?;
        self.cancel = Some(cancel);
        self.request = Some(request);
        Ok(())
    }

    pub fn cancel(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }

    pub fn stop(&mut self) {
        if !self.closing {
            self.closing_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
        }
        self.closing = true;
        self.cancel();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stop();
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::App;
    use onetui_core::catalog::Action;
    use onetui_core::config::Config;
    use onetui_core::provider::CheckResult;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct Probe {
        calls: Arc<AtomicUsize>,
        stopped: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
        status: watch::Sender<ConnectionStatus>,
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl Executor for Probe {
        fn status(&self) -> watch::Receiver<ConnectionStatus> {
            self.status.subscribe()
        }
        async fn check(&self, _: RequestContext) -> Result<CheckResult> {
            unreachable!()
        }
        async fn fetch_page(&self, _: PageRequest, mut context: RequestContext) -> Result<Page> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.status.send_replace(ConnectionStatus::Connected);
            if call == 0 {
                context.run(std::future::pending::<()>()).await?;
            }
            Ok(Page::default())
        }
        async fn shutdown(&mut self, _: ShutdownContext) -> Result<()> {
            self.stopped.store(true, Ordering::SeqCst);
            self.status.send_replace(ConnectionStatus::Closed);
            Ok(())
        }
    }

    fn app() -> App {
        App::new(
            Config::parse(
                "[connections.a]\nkind='fake'",
                crate::test_provider::CATALOG,
            )
            .unwrap(),
            Some("a"),
        )
    }

    async fn page(worker: &mut Worker) -> Result<Page> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match worker.event().await {
                    WorkerEvent::Page(result) => return result,
                    WorkerEvent::Status(_) => {}
                    WorkerEvent::Finished(result) => panic!("worker stopped: {result:?}"),
                }
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn cancel_then_new_request_reuses_executor_and_shutdown_drops_it() {
        let mut app = app();
        let calls = Arc::new(AtomicUsize::new(0));
        let stopped = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let executor = Probe {
            calls: calls.clone(),
            stopped: stopped.clone(),
            dropped: dropped.clone(),
            status: watch::channel(ConnectionStatus::Configured).0,
        };
        let mut worker = Worker::new("a".into(), app.session, executor);
        worker
            .submit(app.request.take().unwrap(), Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            worker.event().await,
            WorkerEvent::Status(ConnectionStatus::Connected)
        ));
        let started = std::time::Instant::now();
        app.act(Action::Cancel);
        worker.cancel();
        let result = page(&mut worker).await;
        assert!(result.is_err());
        eprintln!("pending-worker cancellation: {:?}", started.elapsed());
        let stale = worker.request.take().unwrap();
        app.act(Action::Refresh);
        app.complete(&stale, result);
        assert!(app.loading, "stale error replaced new request state");
        worker
            .submit(app.request.take().unwrap(), Duration::from_secs(5))
            .unwrap();
        let result = page(&mut worker).await;
        app.complete(&worker.request.take().unwrap(), result);
        assert!(!app.loading);
        assert!(app.error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!stopped.load(Ordering::SeqCst));
        worker.stop();
        (&mut worker.task).await.unwrap().unwrap();
        assert!(stopped.load(Ordering::SeqCst));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn abandoned_worker_aborts_owned_executor_and_old_status_is_ignored() {
        let mut app = app();
        let old_session = app.session;
        let dropped = Arc::new(AtomicBool::new(false));
        let mut worker = Worker::new(
            "a".into(),
            app.session,
            Probe {
                calls: Arc::new(AtomicUsize::new(0)),
                stopped: Arc::new(AtomicBool::new(false)),
                dropped: dropped.clone(),
                status: watch::channel(ConnectionStatus::Configured).0,
            },
        );
        worker
            .submit(app.request.take().unwrap(), Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            worker.event().await,
            WorkerEvent::Status(ConnectionStatus::Connected)
        ));
        app.act(Action::Connections);
        app.act(Action::Open);
        app.update_connection_status(old_session, "a", ConnectionStatus::Disconnected);
        assert_eq!(app.connection_status, Some(ConnectionStatus::Configured));
        drop(worker);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
