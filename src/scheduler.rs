//! Shared FIFO task queue and the scheduler that assigns local or remote work.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::compile::bundle::TaskJob;
use crate::compile::{TaskError, TaskResult};
use crate::compile_protocol::{ErrorCode, LeaseId, TaskId};
use crate::remote::{Coordinator, LocalAdmission};

/// Where a queued task came from.
pub enum TaskOrigin {
    Local,
    Remote { task_id: TaskId, lease_id: LeaseId },
}

/// How the scheduler returns a result.
pub enum TaskReply {
    Local(oneshot::Sender<Result<TaskResult, TaskError>>),
    Remote,
}

/// One task waiting in the shared FIFO queue.
pub struct QueuedTask {
    pub job: TaskJob,
    pub origin: TaskOrigin,
    pub reply: TaskReply,
    pub rustc: Option<String>,
}

/// One ordered queue shared by local and remote task ingress.
pub struct TaskQueue {
    tasks: Mutex<VecDeque<QueuedTask>>,
    notify: Notify,
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self {
            tasks: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }
}

impl TaskQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_back(&self, task: QueuedTask) {
        self.lock().push_back(task);
        self.notify.notify_one();
    }

    pub fn pop_front(&self) -> Option<QueuedTask> {
        self.lock().pop_front()
    }

    pub fn notify_one(&self) {
        self.notify.notify_one();
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    pub fn contains(&self, task_id: TaskId, lease_id: LeaseId) -> bool {
        self.lock().iter().any(|task| {
            matches!(
                &task.origin,
                TaskOrigin::Remote {
                    task_id: queued_task,
                    lease_id: queued_lease,
                } if *queued_task == task_id && *queued_lease == lease_id
            )
        })
    }

    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, VecDeque<QueuedTask>> {
        self.tasks.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Single scheduler loop for the shared task queue.
pub struct TaskScheduler {
    queue: Arc<TaskQueue>,
    coordinator: Arc<Coordinator>,
}

impl TaskScheduler {
    pub fn new(queue: Arc<TaskQueue>, coordinator: Arc<Coordinator>) -> Self {
        Self { queue, coordinator }
    }

    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut jobs = JoinSet::new();

        loop {
            if let Some(task) = self.queue.pop_front() {
                if !self.clone().dispatch(task, &mut jobs) {
                    // 本地和远端都没有空闲：任务已放回队尾，等 100ms 再进循环。
                    sleep(Duration::from_millis(100)).await;
                }
                continue;
            }

            tokio::select! {
                _ = self.queue.notified() => {}
                Some(_) = jobs.join_next(), if !jobs.is_empty() => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }

        while let Some(task) = self.queue.pop_front() {
            self.fail_queued_on_shutdown(task);
        }
        jobs.shutdown().await;
    }

    /// 返回 `true` 表示已派出或已给出最终答复；`false` 表示已放回队列。
    fn dispatch(self: Arc<Self>, task: QueuedTask, jobs: &mut JoinSet<()>) -> bool {
        if matches!(&task.origin, TaskOrigin::Local) {
            self.dispatch_local(task, jobs)
        } else {
            self.dispatch_remote(task, jobs);
            true
        }
    }

    fn dispatch_local(self: Arc<Self>, task: QueuedTask, jobs: &mut JoinSet<()>) -> bool {
        let QueuedTask { job, reply, .. } = task;
        let TaskReply::Local(reply) = reply else {
            return true;
        };

        let Some(rustc) = self.coordinator.local_rustc(&job) else {
            // 本机没有这个工具链：直接交给 wrapper 走真 rustc。
            let _ = reply.send(Err(TaskError::new(
                "toolchain_missing",
                "requested toolchain is not available locally",
            )));
            return true;
        };

        if let LocalAdmission::Reserved(lease) = self.coordinator.admit_local(&job) {
            let coordinator = self.coordinator.clone();
            jobs.spawn(async move {
                let result = coordinator.run_local_with_lease(job, rustc, lease).await;
                let _ = reply.send(result);
            });
            return true;
        }

        let providers = self.coordinator.remote_candidates(&job);
        if providers.is_empty() {
            // 本地池满、远端也没有：放回队尾，等 100ms。
            self.queue.push_back(QueuedTask {
                job,
                origin: TaskOrigin::Local,
                reply: TaskReply::Local(reply),
                rustc: None,
            });
            return false;
        }

        let coordinator = self.coordinator.clone();
        let queue = self.queue.clone();
        jobs.spawn(async move {
            match coordinator.try_remote(&job, providers).await {
                Some(Ok(result)) => {
                    let _ = reply.send(Ok(result));
                }
                Some(Err(_)) | None => {
                    // 发送后被拒：放回队尾并等待。
                    sleep(Duration::from_millis(100)).await;
                    queue.push_back(QueuedTask {
                        job,
                        origin: TaskOrigin::Local,
                        reply: TaskReply::Local(reply),
                        rustc: None,
                    });
                }
            }
        });
        true
    }

    fn dispatch_remote(self: Arc<Self>, task: QueuedTask, jobs: &mut JoinSet<()>) {
        let QueuedTask {
            job,
            origin: TaskOrigin::Remote { task_id, lease_id },
            rustc,
            ..
        } = task
        else {
            return;
        };

        if !self.coordinator.task_is_queued(task_id, lease_id) {
            self.coordinator.fail_remote_queued(
                task_id,
                ErrorCode::InvalidState,
                "remote task is not queued",
            );
            return;
        }

        let Some(rustc) = rustc.or_else(|| self.coordinator.local_rustc(&job)) else {
            self.coordinator.fail_remote_queued(
                task_id,
                ErrorCode::Rejected,
                "toolchain unavailable",
            );
            return;
        };

        match self
            .coordinator
            .start_remote_local(job, task_id, lease_id, rustc)
        {
            Ok(handle) => {
                jobs.spawn(async move {
                    let _ = handle.await;
                });
            }
            Err(_) => {
                self.coordinator.fail_remote_queued(
                    task_id,
                    ErrorCode::Busy,
                    "local capacity unavailable",
                );
            }
        }
    }

    fn fail_queued_on_shutdown(&self, task: QueuedTask) {
        match task.origin {
            TaskOrigin::Local => {
                if let TaskReply::Local(reply) = task.reply {
                    let _ = reply.send(Err(TaskError::new("shutdown", "task scheduler stopped")));
                }
            }
            TaskOrigin::Remote { task_id, .. } => {
                self.coordinator.fail_remote_queued(
                    task_id,
                    ErrorCode::Internal,
                    "scheduler shutdown",
                );
            }
        }
    }
}
