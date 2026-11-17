//! Provider role: capacity, subscriptions, tasks, and remote task execution.

use std::collections::HashMap;
use std::fs;
use std::process::{Command, Stdio};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::capacity::CapacitySnapshot;
use crate::compile::bundle::{decode_bundle, materialize_bundle};
use crate::compile::cache::ResultCache;
use crate::compile::hash::{hash_file, hex_hash};
use crate::compile_protocol::{
    AppRequest, AppResponse, ErrorCode, Hash32, LeaseId, OutputRef, PeerId, TaskId, TaskState,
};
use crate::config::ServiceConfig;
use crate::local_protocol::truncate_diagnostic;
use crate::log::{LogLevel, LogStruct, log_compile_failure};
use crate::toolchain::resolve_rustc;
use crate::util::process;
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
    pub state: TaskState,
}

#[derive(Debug)]
struct Inner {
    provider_id: PeerId,
    max_slots: u32,
    free_slots: u32,
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
        self.read()
            .tasks
            .values()
            .filter(|entry| matches!(entry.state, TaskState::Queued | TaskState::Running))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
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
            accepting: inner.free_slots > 0,
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
        AppResponse::Subscribed {
            provider: inner.provider_id.clone(),
            toolchain: String::new(),
            target: String::new(),
            max_slots: inner.max_slots,
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
                TaskState::Done { outputs } => {
                    let lease_id = new_lease(&mut inner, task_id);
                    return AppResponse::Cached {
                        task_id,
                        outputs: outputs.clone(),
                        lease_id,
                    };
                }
                TaskState::Queued | TaskState::Running => {
                    return AppResponse::Granted {
                        task_id,
                        lease_id: existing.lease_id,
                    };
                }
                TaskState::Failed { .. } | TaskState::Cancelled => {
                    inner.tasks.remove(&task_id);
                }
            }
        }

        if inner.free_slots == 0 {
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

    /// Create a lease for a result served from cache, independent of a task.
    pub fn create_lease(&self, task_id: TaskId) -> LeaseId {
        let mut inner = self.write();
        new_lease(&mut inner, task_id)
    }

    /// Drop expired leases and return the affected tasks so the caller can
    /// release cache pins. A finished task is removed once no lease remains.
    pub fn expire_leases(&self, now: u64) -> Vec<(TaskId, u32)> {
        let mut inner = self.write();
        let expired: Vec<LeaseId> = inner
            .leases
            .iter()
            .filter(|(_, lease)| lease.expires_at_ms <= now)
            .map(|(id, _)| *id)
            .collect();
        let mut affected: Vec<(TaskId, u32)> = Vec::new();
        for lease_id in expired {
            if let Some(lease) = inner.leases.remove(&lease_id) {
                if !inner.leases.values().any(|l| l.task_id == lease.task_id)
                    && inner
                        .tasks
                        .get(&lease.task_id)
                        .is_some_and(|entry| matches!(entry.state, TaskState::Done { .. }))
                {
                    inner.tasks.remove(&lease.task_id);
                }
                match affected.iter_mut().find(|(id, _)| *id == lease.task_id) {
                    Some((_, count)) => *count += 1,
                    None => affected.push((lease.task_id, 1)),
                }
            }
        }
        affected
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
    config: ServiceConfig,
    task_id: TaskId,
    bundle_hash: Hash32,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        run_task_blocking(provider, cache, workspace, config, task_id, bundle_hash);
    })
}

fn run_task_blocking(
    provider: Arc<Provider>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    config: ServiceConfig,
    task_id: TaskId,
    bundle_hash: Hash32,
) {
    let task_dir = workspace.task_dir(&task_id.0);
    let bundle_path = task_dir.join("bundle");
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

    let actual = match hash_file(&bundle_path) {
        Ok((hash, _)) => hash,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("read task bundle: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };
    if actual != bundle_hash {
        fail(
            &provider,
            ErrorCode::Internal,
            "task bundle hash mismatch".into(),
            Vec::new(),
            Vec::new(),
        );
        return;
    }

    let bytes = match fs::read(&bundle_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("read task bundle: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };
    let bundle = match decode_bundle(&bytes) {
        Ok(bundle) => bundle,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                error,
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };
    let digest = match bundle.manifest.digest() {
        Ok(digest) => digest,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                error.to_string(),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };
    if digest != task_id.0 {
        fail(
            &provider,
            ErrorCode::Internal,
            "manifest digest does not match task id".into(),
            Vec::new(),
            Vec::new(),
        );
        return;
    }

    let rustc = match resolve_rustc(
        &config,
        &bundle.manifest.toolchain_id,
        &bundle.manifest.target,
    ) {
        Ok(rustc) => rustc,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("toolchain check failed: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };

    let work = task_dir.join("work");
    let work_out = work.join("out");
    let work_extern = work.join("extern");
    if let Err(error) = fs::create_dir_all(&work)
        .and_then(|_| materialize_bundle(&bundle, &work))
        .and_then(|_| fs::create_dir_all(&work_out))
        .and_then(|_| fs::create_dir_all(&work_extern))
        .and_then(|_| fs::create_dir_all(work.join("target")))
    {
        fail(
            &provider,
            ErrorCode::Internal,
            format!("materialize task: {error}"),
            Vec::new(),
            Vec::new(),
        );
        return;
    }

    let prepared = match crate::compile::bundle::prepare_task(&bundle, &work) {
        Ok(prepared) => prepared,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("prepare task: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };
    let output = Command::new(&rustc)
        .args(&prepared.args)
        .current_dir(&prepared.cwd)
        .envs(&prepared.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(output) => output,
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("spawn rustc: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };

    if !output.status.success() {
        let exit_code = process::exit_code(output.status);
        let bundle_size = fs::metadata(&bundle_path)
            .map(|metadata| metadata.len())
            .ok();
        log_compile_failure(
            "provider",
            &hex_hash(&task_id.0),
            &bundle.manifest.crate_name,
            &rustc,
            &prepared.cwd.to_string_lossy(),
            exit_code,
            0,
            &prepared.args,
            &prepared.env,
            &output.stdout,
            &output.stderr,
            Some(&bundle.manifest),
            Some(&bundle_hash),
            bundle_size,
            Some(bundle.files.len()),
        );
        fail(
            &provider,
            ErrorCode::Compile,
            format!("rustc exited with {exit_code}"),
            truncate_diagnostic(output.stdout),
            truncate_diagnostic(output.stderr),
        );
        return;
    }

    let outputs = match cache.collect_outputs(&work_out) {
        Ok(outputs) if !outputs.is_empty() => outputs,
        Ok(_) => {
            fail(
                &provider,
                ErrorCode::Internal,
                "rustc produced no outputs".into(),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
        Err(error) => {
            fail(
                &provider,
                ErrorCode::Internal,
                format!("collect outputs: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };

    let stored = cache.store(task_id.0, outputs.clone()).unwrap_or(false);
    if stored {
        cache.pin(&task_id.0);
    }
    LogStruct::new(
        LogLevel::Debug,
        "远端任务完成",
        format!("action={digest_prefix}"),
    )
    .emit();
    provider.complete_task(task_id, outputs);
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
                    provider: ref provider,
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
            provider.reserve(peer("consumer"), id, 1, manifest(2)),
            AppResponse::Granted { .. }
        ));
        assert!(matches!(
            provider.reserve(peer("consumer"), id, 1, manifest(2)),
            AppResponse::Granted { .. }
        ));
        assert!(matches!(
            provider.reserve(peer("consumer"), task(2), 2, manifest(3)),
            AppResponse::Rejected {
                code: ErrorCode::BadRequest,
                ..
            }
        ));

        provider.set_capacity(1, 0);
        assert_eq!(
            provider.reserve(peer("consumer"), task(3), 1, manifest(4)),
            AppResponse::Busy {
                retry_after_ms: 1_000
            }
        );
        assert_eq!(provider.free_slots(), 0);

        provider.set_capacity(1, 1);
        assert_eq!(provider.free_slots(), 1);
        assert!(matches!(
            provider.reserve(peer("consumer"), task(2), 1, manifest(3)),
            AppResponse::Granted { .. }
        ));
    }

    #[test]
    fn task_lifecycle_cancel_fail_and_unknown() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);

        let lease = match provider.reserve(peer("consumer"), id, 1, manifest(2)) {
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
            provider.reserve(peer("consumer"), id, 1, manifest(2)),
            AppResponse::Cached { .. }
        ));

        let cancelled = task(3);
        assert!(matches!(
            provider.reserve(peer("consumer"), cancelled, 1, manifest(4)),
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
            provider.reserve(peer("consumer"), failed, 1, manifest(6)),
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
    fn shared_done_task_needs_all_lease_acks() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);
        let lease_one = match provider.reserve(peer("a"), id, 1, manifest(2)) {
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

        let lease_two = match provider.reserve(peer("b"), id, 1, manifest(2)) {
            AppResponse::Cached { lease_id, .. } => lease_id,
            other => panic!("expected cached, got {other:?}"),
        };
        assert_ne!(lease_one, lease_two);

        assert_eq!(provider.result_ack(lease_one), Some(id));
        assert!(provider.task_entry(&id).is_some());

        assert_eq!(provider.result_ack(lease_two), Some(id));
        assert!(provider.task_entry(&id).is_none());
    }

    #[test]
    fn expired_leases_are_returned_for_pin_release() {
        let provider = Provider::new(peer("local"), 1);
        let id = task(1);
        let _lease = provider.create_lease(id);
        let affected = provider.expire_leases(now_ms().saturating_add(LEASE_TTL_MS + 1));
        assert_eq!(affected, vec![(id, 1)]);
    }
}
