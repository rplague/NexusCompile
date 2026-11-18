//! Provider role: capacity, subscriptions, tasks, and remote task execution.

use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::capacity::CapacitySnapshot;
use crate::compile::bundle::TaskJob;
use crate::compile::cache::ResultCache;
use crate::compile::hash::hex_hash;
use crate::compile_protocol::{
    AppRequest, AppResponse, ErrorCode, Hash32, LeaseId, OutputRef, PeerId, TaskId, TaskState,
    ToolchainCapability,
};
use crate::log::{LogLevel, LogStruct};
use crate::util::time::now_ms;
use crate::workspace::Workspace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub consumer: PeerId,
    pub seq: u64,
    pub last_sent_ms: u64,
    pub last_sent_free: u32,
    pub last_sent_queue: u32,
    pub last_sent_accepting: bool,
}

/// Lease lifetime for cache-only and shared results.
pub const LEASE_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub task_id: TaskId,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEntry {
    pub consumer: PeerId,
    pub lease_id: LeaseId,
    pub input_manifest: Hash32,
    pub toolchain_id: String,
    pub target: String,
    pub state: TaskState,
}

#[derive(Debug)]
struct Inner {
    provider_id: PeerId,
    max_slots: u32,
    free_slots: u32,
    capabilities: Vec<ToolchainCapability>,
    subscriptions: HashMap<PeerId, Subscription>,
    tasks: HashMap<TaskId, TaskEntry>,
    leases: HashMap<LeaseId, Lease>,
}

/// Provider role. Internal state is protected by its own lock; callers never
/// hold a global state mutex across cache or filesystem I/O.
#[derive(Debug)]
pub struct Provider {
    inner: RwLock<Inner>,
}

impl Provider {
    pub fn new(provider_id: PeerId, max_slots: u32) -> Self {
        Self {
            inner: RwLock::new(Inner {
                provider_id,
                max_slots,
                free_slots: max_slots,
                capabilities: Vec::new(),
                subscriptions: HashMap::new(),
                tasks: HashMap::new(),
                leases: HashMap::new(),
            }),
        }
    }

    pub fn placeholder() -> Self {
        Self::new(PeerId::new("local"), 1)
    }

    pub fn set_provider_id(&self, provider_id: PeerId) {
        self.write().provider_id = provider_id;
    }

    pub fn provider_id(&self) -> PeerId {
        self.read().provider_id.clone()
    }

    pub fn max_slots(&self) -> u32 {
        self.read().max_slots
    }

    pub fn active_slots(&self) -> u32 {
        let inner = self.read();
        active_slot_count(&inner)
    }

    pub fn available_slots(&self) -> u32 {
        let inner = self.read();
        inner.free_slots.saturating_sub(active_slot_count(&inner))
    }

    pub fn queue_len(&self) -> u32 {
        self.read()
            .tasks
            .values()
            .filter(|entry| matches!(entry.state, TaskState::Queued))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    /// Free slots come from the latest capacity sample. They are deliberately
    /// not reduced by active tasks; `/proc` usage already includes them.
    pub fn free_slots(&self) -> u32 {
        self.read().free_slots
    }

    pub fn set_capacity(&self, max_slots: u32, free_slots: u32) {
        let mut inner = self.write();
        inner.max_slots = max_slots;
        inner.free_slots = free_slots.min(max_slots);
    }

    pub fn set_capabilities(&self, capabilities: Vec<ToolchainCapability>) {
        self.write().capabilities = capabilities;
    }

    pub fn capabilities(&self) -> Vec<ToolchainCapability> {
        self.read().capabilities.clone()
    }

    pub fn capacity_snapshot(&self) -> CapacitySnapshot {
        let inner = self.read();
        CapacitySnapshot {
            max_slots: inner.max_slots,
            free_slots: inner.free_slots,
            queue_len: inner
                .tasks
                .values()
                .filter(|entry| matches!(entry.state, TaskState::Queued))
                .count()
                .try_into()
                .unwrap_or(u32::MAX),
            accepting: inner.free_slots.saturating_sub(active_slot_count(&inner)) > 0,
        }
    }

    pub fn subscribe(&self, consumer: PeerId) -> AppResponse {
        let mut inner = self.write();
        inner.subscriptions.insert(
            consumer.clone(),
            Subscription {
                consumer,
                seq: 0,
                last_sent_ms: 0,
                last_sent_free: 0,
                last_sent_queue: 0,
                last_sent_accepting: false,
            },
        );
        let (toolchain, target) = inner
            .capabilities
            .first()
            .map(|capability| (capability.id.clone(), capability.target.clone()))
            .unwrap_or_default();
        AppResponse::Subscribed {
            provider: inner.provider_id.clone(),
            toolchain,
            target,
            max_slots: inner.max_slots,
            capabilities: inner.capabilities.clone(),
        }
    }

    pub fn unsubscribe(&self, consumer: &PeerId) -> AppResponse {
        self.write().subscriptions.remove(consumer);
        AppResponse::Ack
    }

    pub fn subscription_count(&self) -> usize {
        self.read().subscriptions.len()
    }

    pub fn has_subscription(&self, consumer: &PeerId) -> bool {
        self.read().subscriptions.contains_key(consumer)
    }

    pub fn due_capacity_updates(
        &self,
        now: u64,
        snapshot: CapacitySnapshot,
        interval_ms: u64,
    ) -> Vec<(PeerId, AppRequest)> {
        let mut inner = self.write();
        let provider = inner.provider_id.clone();
        let mut updates = Vec::new();

        for subscription in inner.subscriptions.values_mut() {
            let changed = subscription.last_sent_ms == 0
                || subscription.last_sent_free != snapshot.free_slots
                || subscription.last_sent_queue != snapshot.queue_len
                || subscription.last_sent_accepting != snapshot.accepting;
            let keepalive = now.saturating_sub(subscription.last_sent_ms) >= interval_ms;

            if !changed && !keepalive {
                continue;
            }

            subscription.seq = subscription.seq.saturating_add(1);
            subscription.last_sent_ms = now;
            subscription.last_sent_free = snapshot.free_slots;
            subscription.last_sent_queue = snapshot.queue_len;
            subscription.last_sent_accepting = snapshot.accepting;

            updates.push((
                subscription.consumer.clone(),
                AppRequest::CapacityUpdate {
                    provider: provider.clone(),
                    seq: subscription.seq,
                    free_slots: snapshot.free_slots,
                    queue_len: snapshot.queue_len,
                    accepting: snapshot.accepting,
                },
            ));
        }

        updates
    }

    pub fn reserve(
        &self,
        consumer: PeerId,
        task_id: TaskId,
        slots: u32,
        input_manifest: Hash32,
        toolchain_id: String,
        target: String,
    ) -> AppResponse {
        if slots != 1 {
            return AppResponse::Rejected {
                code: ErrorCode::BadRequest,
                message: "exactly one slot must be requested".into(),
            };
        }

        let mut inner = self.write();
        if let Some(existing) = inner.tasks.get(&task_id).cloned() {
            match &existing.state {
                TaskState::Queued | TaskState::Running => {
                    return AppResponse::Granted {
                        task_id,
                        lease_id: existing.lease_id,
                    };
                }
                _ => {
                    inner.tasks.remove(&task_id);
                    inner.leases.retain(|_, lease| lease.task_id != task_id);
                }
            }
        }

        if inner.free_slots.saturating_sub(active_slot_count(&inner)) == 0 {
            return AppResponse::Busy {
                retry_after_ms: 1_000,
            };
        }

        let lease_id = new_lease(&mut inner, task_id);
        inner.tasks.insert(
            task_id,
            TaskEntry {
                consumer,
                lease_id,
                input_manifest,
                toolchain_id,
                target,
                state: TaskState::Queued,
            },
        );

        AppResponse::Granted { task_id, lease_id }
    }

    pub fn task_ready(&self, task_id: TaskId, lease_id: LeaseId) -> AppResponse {
        let mut inner = self.write();
        let Some(entry) = inner.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };
        if entry.lease_id != lease_id {
            return AppResponse::error(ErrorCode::NotFound, "lease does not match task");
        }

        match entry.state {
            TaskState::Queued => {
                entry.state = TaskState::Running;
                AppResponse::Ack
            }
            TaskState::Running => AppResponse::Ack,
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not queued"),
        }
    }

    pub fn task_entry(&self, task_id: &TaskId) -> Option<TaskEntry> {
        self.read().tasks.get(task_id).cloned()
    }

    pub fn discard_task(&self, task_id: TaskId) {
        let mut inner = self.write();
        inner.tasks.remove(&task_id);
        inner.leases.retain(|_, lease| lease.task_id != task_id);
    }

    /// Create a lease for a result served from cache, independent of a task.
    pub fn create_lease(&self, task_id: TaskId) -> LeaseId {
        let mut inner = self.write();
        new_lease(&mut inner, task_id)
    }

    /// Drop expired leases. A finished task is removed once no lease remains.
    pub fn expire_leases(&self, now: u64) {
        let mut inner = self.write();
        let expired: Vec<LeaseId> = inner
            .leases
            .iter()
            .filter(|(_, lease)| lease.expires_at_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        for lease_id in expired {
            if let Some(lease) = inner.leases.remove(&lease_id)
                && !inner.leases.values().any(|l| l.task_id == lease.task_id)
                && inner.tasks.get(&lease.task_id).is_some_and(|entry| {
                    matches!(entry.state, TaskState::Done { .. } | TaskState::Queued)
                })
            {
                inner.tasks.remove(&lease.task_id);
            }
        }
    }

    pub fn lease_count_for(&self, task_id: TaskId) -> u32 {
        self.read()
            .leases
            .values()
            .filter(|lease| lease.task_id == task_id)
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    pub fn status(&self, task_id: TaskId) -> AppResponse {
        match self.read().tasks.get(&task_id) {
            Some(entry) => AppResponse::TaskStatus {
                state: entry.state.clone(),
            },
            None => AppResponse::error(ErrorCode::NotFound, "unknown task"),
        }
    }

    pub fn cancel(&self, task_id: TaskId) -> AppResponse {
        let mut inner = self.write();
        let Some(entry) = inner.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        if matches!(entry.state, TaskState::Queued | TaskState::Running) {
            entry.state = TaskState::Cancelled;
        }
        inner.leases.retain(|_, lease| lease.task_id != task_id);
        AppResponse::Ack
    }

    pub fn result_ack(&self, lease_id: LeaseId) -> Option<TaskId> {
        let mut inner = self.write();
        let lease = inner.leases.remove(&lease_id)?;
        let task_id = lease.task_id;
        let remove = !inner.leases.values().any(|l| l.task_id == task_id)
            && inner
                .tasks
                .get(&task_id)
                .is_some_and(|entry| matches!(entry.state, TaskState::Done { .. }));
        if remove {
            inner.tasks.remove(&task_id);
        }
        Some(task_id)
    }

    pub fn complete_task(&self, task_id: TaskId, outputs: Vec<OutputRef>) -> AppResponse {
        let mut inner = self.write();
        let Some(entry) = inner.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        match entry.state {
            TaskState::Queued | TaskState::Running => {
                entry.state = TaskState::Done { outputs };
                AppResponse::Ack
            }
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not running"),
        }
    }

    pub fn fail_task(
        &self,
        task_id: TaskId,
        code: ErrorCode,
        message: impl Into<String>,
    ) -> AppResponse {
        self.fail_task_with_logs(task_id, code, message, Vec::new(), Vec::new())
    }

    pub fn fail_task_with_logs(
        &self,
        task_id: TaskId,
        code: ErrorCode,
        message: impl Into<String>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    ) -> AppResponse {
        let mut inner = self.write();
        let Some(entry) = inner.tasks.get_mut(&task_id) else {
            return AppResponse::error(ErrorCode::NotFound, "unknown task");
        };

        match entry.state {
            TaskState::Queued | TaskState::Running => {
                entry.state = TaskState::Failed {
                    code,
                    message: message.into(),
                    stdout,
                    stderr,
                };
                inner.leases.retain(|_, lease| lease.task_id != task_id);
                AppResponse::Ack
            }
            _ => AppResponse::error(ErrorCode::InvalidState, "task is not running"),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Inner> {
        self.inner.read().expect("Provider RwLock 被污染")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Inner> {
        self.inner.write().expect("Provider RwLock 被污染")
    }
}

fn active_slot_count(inner: &Inner) -> u32 {
    inner
        .tasks
        .values()
        .filter(|entry| matches!(entry.state, TaskState::Queued | TaskState::Running))
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn new_lease(inner: &mut Inner, task_id: TaskId) -> LeaseId {
    let lease_id = LeaseId::new();
    inner.leases.insert(
        lease_id,
        Lease {
            task_id,
            expires_at_ms: now_ms().saturating_add(LEASE_TTL_MS),
        },
    );
    lease_id
}

pub fn spawn_task(
    provider: Arc<Provider>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    job: TaskJob,
    rustc: String,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        run_task_blocking(provider, cache, workspace, job, rustc);
    })
}

fn run_task_blocking(
    provider: Arc<Provider>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    job: TaskJob,
    rustc: String,
) {
    let task_id = TaskId(job.task_id);
    let task_dir = workspace.task_dir(&task_id.0);
    let digest_prefix = &hex_hash(&task_id.0)[..8];
    LogStruct::new(
        LogLevel::Debug,
        "远端任务开始",
        format!("action={digest_prefix}"),
    )
    .emit();
    let fail = |provider: &Arc<Provider>, code, message: String, stdout, stderr| {
        LogStruct::new(
            LogLevel::Warning,
            "远端任务失败",
            format!("action={digest_prefix}: {message}"),
        )
        .emit();
        provider.fail_task_with_logs(task_id, code, message, stdout, stderr);
        let _ = fs::remove_dir_all(&task_dir);
    };

    let result = match crate::compile::executor::execute_job(
        &job,
        &rustc,
        &cache,
        &workspace,
        crate::compile::TaskSource::Provider,
    ) {
        Ok(result) => result,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                error.message,
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };

    if result.exit_code != 0 {
        fail(
            &provider,
            ErrorCode::Compile,
            format!("rustc exited with {}", result.exit_code),
            result.stdout,
            result.stderr,
        );
        return;
    }

    LogStruct::new(
        LogLevel::Debug,
        "远端任务完成",
        format!("action={digest_prefix}"),
    )
    .emit();
    provider.complete_task(task_id, result.outputs);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::CapacitySnapshot;

    fn peer(value: &str) -> PeerId {
        PeerId::new(value)
    }

    fn task(value: u8) -> TaskId {
        TaskId(Hash32([value; 32]))
    }

    fn manifest(value: u8) -> Hash32 {
        Hash32([value; 32])
    }

    fn snapshot(available: u32, queue_len: u32, accepting: bool) -> CapacitySnapshot {
        CapacitySnapshot {
            max_slots: 4,
            free_slots: available,
            queue_len,
            accepting,
        }
    }

    fn update_seq(request: &AppRequest) -> u64 {
        match request {
            AppRequest::CapacityUpdate { seq, .. } => *seq,
            other => panic!("expected capacity update, got {other:?}"),
        }
    }

    #[test]
    fn subscribe_and_due_updates() {
        let provider = Provider::new(peer("provider"), 4);
        let first = provider.subscribe(peer("consumer"));
        let second = provider.subscribe(peer("consumer"));

        for response in [first, second] {
            assert!(matches!(
                response,
                AppResponse::Subscribed {
                    ref provider,
                    max_slots: 4,
                    ..
                } if provider == &peer("provider")
            ));
        }
        assert_eq!(provider.read().subscriptions.len(), 1);
        assert_eq!(provider.read().subscriptions[&peer("consumer")].seq, 0);

        provider.set_capacity(4, 2);
        let initial = provider.due_capacity_updates(1_000, snapshot(2, 0, true), 30_000);
        assert_eq!(initial.len(), 1);
        assert_eq!(update_seq(&initial[0].1), 1);

        let no_change = provider.due_capacity_updates(2_000, snapshot(2, 0, true), 30_000);
        assert!(no_change.is_empty());

        let changed = provider.due_capacity_updates(3_000, snapshot(1, 1, true), 30_000);
        assert_eq!(changed.len(), 1);
        assert_eq!(update_seq(&changed[0].1), 2);

        let keepalive = provider.due_capacity_updates(33_000, snapshot(1, 1, true), 30_000);
        assert_eq!(keepalive.len(), 1);
        assert_eq!(update_seq(&keepalive[0].1), 3);
    }

    #[test]
    fn reserve_slots_and_idempotency() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);

        assert!(matches!(
            provider.reserve(peer("consumer"), id, 1, manifest(2), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
        assert!(matches!(
            provider.reserve(peer("consumer"), id, 1, manifest(2), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
        assert!(matches!(
            provider.reserve(peer("consumer"), task(2), 2, manifest(3), String::new(), String::new()),
            AppResponse::Rejected {
                code: ErrorCode::BadRequest,
                ..
            }
        ));
        assert_eq!(provider.available_slots(), 0);

        provider.fail_task(id, ErrorCode::Internal, "reset");
        provider.discard_task(id);

        provider.set_capacity(1, 0);
        assert_eq!(
            provider.reserve(peer("consumer"), task(3), 1, manifest(4), String::new(), String::new()),
            AppResponse::Busy {
                retry_after_ms: 1_000
            }
        );
        assert_eq!(provider.free_slots(), 0);

        provider.set_capacity(1, 1);
        assert_eq!(provider.free_slots(), 1);
        assert!(matches!(
            provider.reserve(peer("consumer"), task(2), 1, manifest(3), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
    }

    #[test]
    fn task_lifecycle_cancel_fail_and_unknown() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);

        let lease = match provider.reserve(peer("consumer"), id, 1, manifest(2), String::new(), String::new()) {
            AppResponse::Granted { lease_id, .. } => lease_id,
            other => panic!("expected granted, got {other:?}"),
        };
        assert_eq!(provider.task_ready(id, lease), AppResponse::Ack);
        assert_eq!(
            provider.status(id),
            AppResponse::TaskStatus {
                state: TaskState::Running
            }
        );

        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: manifest(9),
            size: 10,
            mode: 0o644,
        };
        assert_eq!(
            provider.complete_task(id, vec![output.clone()]),
            AppResponse::Ack
        );
        assert_eq!(provider.free_slots(), 1);
        assert_eq!(
            provider.status(id),
            AppResponse::TaskStatus {
                state: TaskState::Done {
                    outputs: vec![output]
                }
            }
        );
        assert!(matches!(
            provider.reserve(peer("consumer"), id, 1, manifest(2), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
        provider.discard_task(id);

        let cancelled = task(3);
        assert!(matches!(
            provider.reserve(peer("consumer"), cancelled, 1, manifest(4), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
        assert_eq!(provider.cancel(cancelled), AppResponse::Ack);
        assert_eq!(
            provider.status(cancelled),
            AppResponse::TaskStatus {
                state: TaskState::Cancelled
            }
        );

        let failed = task(5);
        assert!(matches!(
            provider.reserve(peer("consumer"), failed, 1, manifest(6), String::new(), String::new()),
            AppResponse::Granted { .. }
        ));
        assert_eq!(
            provider.fail_task(failed, ErrorCode::Internal, "boom"),
            AppResponse::Ack
        );

        assert!(matches!(
            provider.status(task(7)),
            AppResponse::Error {
                code: ErrorCode::NotFound,
                ..
            }
        ));
    }

    #[test]
    fn done_task_is_dropped_on_new_reservation() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);
        let lease_one = match provider.reserve(peer("a"), id, 1, manifest(2), String::new(), String::new()) {
            AppResponse::Granted { lease_id, .. } => lease_id,
            other => panic!("expected granted, got {other:?}"),
        };
        assert_eq!(provider.task_ready(id, lease_one), AppResponse::Ack);
        let output = OutputRef {
            path: "libdemo.rlib".into(),
            hash: manifest(9),
            size: 10,
            mode: 0o644,
        };
        provider.complete_task(id, vec![output]);

        // Done 条目不再参与命中：下次申请按新任务处理，并清掉旧租约。
        let lease_two = match provider.reserve(peer("b"), id, 1, manifest(2), String::new(), String::new()) {
            AppResponse::Granted { lease_id, .. } => lease_id,
            other => panic!("expected granted, got {other:?}"),
        };
        assert_ne!(lease_one, lease_two);
        assert_eq!(provider.lease_count_for(id), 1);
        assert_eq!(provider.result_ack(lease_one), None);
        assert!(matches!(
            provider.task_entry(&id).unwrap().state,
            TaskState::Queued
        ));
    }

    #[test]
    fn expired_queued_reservation_releases_capacity() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);
        let _lease = provider.reserve(peer("consumer"), id, 1, manifest(2), String::new(), String::new());
        assert_eq!(provider.available_slots(), 0);

        provider.expire_leases(now_ms().saturating_add(LEASE_TTL_MS + 1));

        assert!(provider.task_entry(&id).is_none());
        assert_eq!(provider.available_slots(), 1);
    }

    #[test]
    fn expired_lease_is_dropped() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);
        let lease = provider.create_lease(id);
        provider.expire_leases(now_ms().saturating_add(LEASE_TTL_MS + 1));
        assert_eq!(provider.lease_count_for(id), 0);
        assert_eq!(provider.result_ack(lease), None);
    }
}
