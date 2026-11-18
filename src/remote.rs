//! Consumer-side remote task coordinator.

use crate::compile::bundle::TaskJob;
use crate::compile::cache::ResultCache;
use crate::compile::executor;
use crate::compile::hash::{hash_bytes, hex_hash};
use crate::compile::{TaskError, TaskResult, TaskSource};
use crate::compile_protocol::{
    AppRequest, AppResponse, ErrorCode, Hash32, LeaseId, OutputRef, PeerId, TaskId, TaskState,
};
use crate::config::ServiceConfig;
use crate::control::{self, ControlHandle};
use crate::log::{LogLevel, LogStruct};
use crate::pool::{Consumer, Provider};
use crate::toolchain::resolve_toolchain;
use crate::workspace::Workspace;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};

const CACHE_FETCH_TIMEOUT: Duration = Duration::from_secs(2);

/// 本地容量准入的结果。`Busy` 表示「现在没有 slot」，调用方应回队等待而不是失败。
pub(crate) enum LocalAdmission {
    Reserved(LeaseId),
    Busy,
}

pub struct Coordinator {
    provider: Arc<Provider>,
    consumer: Arc<Consumer>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    control: ControlHandle,
    config: ServiceConfig,
}

impl Coordinator {
    pub fn new(
        provider: Arc<Provider>,
        consumer: Arc<Consumer>,
        cache: Arc<ResultCache>,
        workspace: Arc<Workspace>,
        control: ControlHandle,
        config: ServiceConfig,
    ) -> Self {
        Self {
            provider,
            consumer,
            cache,
            workspace,
            control,
            config,
        }
    }

    /// 远端缓存查询：按记录表逐个询问，命中即导入本地缓存。
    pub(crate) async fn import_cached(&self, digest: Hash32) -> Option<Vec<OutputRef>> {
        let providers = self.consumer.remote_cache_providers(&digest);
        if providers.is_empty() {
            return None;
        }

        let transfer = RemoteTransfer::new(
            self.control.clone(),
            self.cache.clone(),
            self.config.service_name.clone(),
        );

        for provider in providers {
            match transfer.fetch_cached(&provider, digest).await {
                Ok(outputs) => return Some(outputs),
                Err(error) => {
                    self.consumer.remove_remote_cache(&digest, &provider);
                    LogStruct::new(
                        LogLevel::Debug,
                        "缓存",
                        format!("remote cache miss provider={provider}: {error}"),
                    )
                    .emit();
                }
            }
        }

        None
    }

    /// 订阅视图里当前可用的远端 provider。
    pub(crate) fn remote_candidates(&self, job: &TaskJob) -> Vec<PeerId> {
        self.consumer
            .providers()
            .into_iter()
            .filter(|view| {
                view.accepting
                    && view.free_slots > 0
                    && (view.capabilities.is_empty()
                        || view.capabilities.iter().any(|capability| {
                            capability.id == job.requirements.toolchain.id
                                && capability.target == job.requirements.toolchain.target
                        }))
            })
            .map(|view| view.provider)
            .collect()
    }

    /// 在调度线程内同步完成「判断 + 扣减」。
    pub(crate) fn admit_local(&self, job: &TaskJob) -> LocalAdmission {
        let task_id = TaskId(job.task_id);
        let consumer = self.provider.provider_id();
        match self.provider.reserve(
            consumer,
            task_id,
            job.requirements.slots,
            job.bundle_hash,
            job.requirements.toolchain.id.clone(),
            job.requirements.toolchain.target.clone(),
        ) {
            AppResponse::Granted { lease_id, .. } => LocalAdmission::Reserved(lease_id),
            AppResponse::Cached { .. } => {
                // 本地缓存已在 process_task 查过且未命中，这个 Done 条目不可用；
                // 丢掉它，下一轮重新编译，避免一直拿到同一个缓存答复。
                self.provider.discard_task(task_id);
                LocalAdmission::Busy
            }
            _ => LocalAdmission::Busy,
        }
    }

    pub(crate) async fn run_local_with_lease(
        &self,
        job: TaskJob,
        rustc: String,
        lease: LeaseId,
    ) -> Result<TaskResult, TaskError> {
        let task_id = TaskId(job.task_id);
        if !matches!(
            self.provider.task_ready(task_id, lease),
            crate::compile_protocol::AppResponse::Ack
        ) {
            let _ = self.provider.fail_task(
                task_id,
                crate::compile_protocol::ErrorCode::Internal,
                "local task was not ready",
            );
            self.provider.discard_task(task_id);
            return Err(TaskError::new("reserve_error", "local task was not ready"));
        }

        let cache = self.cache.clone();
        let workspace = self.workspace.clone();
        let result = tokio::task::spawn_blocking(move || {
            executor::execute_job(&job, &rustc, &cache, &workspace, TaskSource::Local)
        })
        .await
        .map_err(|error| TaskError::new("join_error", error.to_string()))?;

        match &result {
            Ok(task_result) if task_result.exit_code == 0 => {
                self.provider
                    .complete_task(task_id, task_result.outputs.clone());
                self.provider.result_ack(lease);
            }
            Ok(task_result) => {
                self.provider.fail_task(
                    task_id,
                    crate::compile_protocol::ErrorCode::Compile,
                    format!("rustc exited with {}", task_result.exit_code),
                );
                self.provider.discard_task(task_id);
            }
            Err(error) => {
                self.provider.fail_task(
                    task_id,
                    crate::compile_protocol::ErrorCode::Internal,
                    error.message.clone(),
                );
                self.provider.discard_task(task_id);
            }
        }

        result
    }

    pub(crate) fn local_rustc(&self, job: &TaskJob) -> Option<String> {
        resolve_toolchain(&self.config, &job.requirements.toolchain)
            .ok()
            .map(|resolved| resolved.rustc)
    }

    pub(crate) fn task_is_queued(&self, task_id: TaskId, lease_id: LeaseId) -> bool {
        self.provider.task_entry(&task_id).is_some_and(|entry| {
            entry.lease_id == lease_id && matches!(entry.state, TaskState::Queued)
        })
    }

    pub(crate) fn start_remote_local(
        &self,
        job: TaskJob,
        task_id: TaskId,
        lease_id: LeaseId,
        rustc: String,
    ) -> Result<tokio::task::JoinHandle<()>, TaskError> {
        if !matches!(
            self.provider.task_ready(task_id, lease_id),
            crate::compile_protocol::AppResponse::Ack
        ) {
            return Err(TaskError::new("reserve_error", "remote task was not ready"));
        }
        Ok(crate::pool::spawn_task(
            self.provider.clone(),
            self.cache.clone(),
            self.workspace.clone(),
            job,
            rustc,
        ))
    }

    pub(crate) fn fail_remote_queued(
        &self,
        task_id: TaskId,
        code: crate::compile_protocol::ErrorCode,
        message: impl Into<String>,
    ) {
        let message = message.into();
        self.provider.fail_task(task_id, code, message);
        let _ = std::fs::remove_dir_all(self.workspace.task_dir(&task_id.0));
    }

    fn retry_provider(
        &self,
        provider: &crate::compile_protocol::PeerId,
        task: &str,
        reason: &str,
    ) {
        LogStruct::new(
            LogLevel::Debug,
            "远端重试",
            format!("provider={provider} action={task} reason={reason}"),
        )
        .emit();
    }

    pub(crate) async fn try_remote(
        &self,
        job: &TaskJob,
        providers: Vec<PeerId>,
    ) -> Option<Result<TaskResult, TaskError>> {
        let task = hex_hash(&job.task_id);
        let transfer = RemoteTransfer::new(
            self.control.clone(),
            self.cache.clone(),
            self.config.service_name.clone(),
        );
        let consumer = self.provider.provider_id();

        for provider in providers {
            LogStruct::new(
                LogLevel::Debug,
                "远端编译",
                format!(
                    "尝试 provider={provider}, action={}",
                    &hex_hash(&job.task_id)[..8]
                ),
            )
            .emit();
            let task_id = TaskId(job.task_id);
            let lease = match control::send(
                &self.control,
                &self.config.service_name,
                &provider,
                AppRequest::Reserve {
                    consumer: consumer.clone(),
                    task_id,
                    slots: 1,
                    input_manifest: job.bundle_hash,
                    toolchain_id: job.requirements.toolchain.id.clone(),
                    target: job.requirements.toolchain.target.clone(),
                },
            )
            .await
            {
                Ok(AppResponse::Granted { lease_id, .. }) => {
                    self.consumer.clear_cooldown(&provider);
                    lease_id
                }
                Ok(AppResponse::Busy { .. }) => {
                    self.retry_provider(&provider, &task, "reserve_busy");
                    continue;
                }
                Ok(AppResponse::Rejected { code, message }) => {
                    self.retry_provider(
                        &provider,
                        &task,
                        &format!("reserve_rejected code={code:?} message={message}"),
                    );
                    continue;
                }
                Ok(other) => {
                    self.retry_provider(
                        &provider,
                        &task,
                        &format!("reserve_unexpected response={other:?}"),
                    );
                    continue;
                }
                Err(error) => {
                    self.retry_provider(
                        &provider,
                        &task,
                        &format!("reserve_error message={error}"),
                    );
                    continue;
                }
            };

            if let Err(error) = transfer
                .upload_bundle(&provider, task_id, job.bundle_hash, &job.bundle_bytes)
                .await
            {
                let _ = control::send(
                    &self.control,
                    &self.config.service_name,
                    &provider,
                    AppRequest::Cancel { task_id },
                )
                .await;
                self.retry_provider(&provider, &task, &format!("upload_error message={error}"));
                return Some(Err(TaskError::new("upload_error", error)));
            }
            match control::send(
                &self.control,
                &self.config.service_name,
                &provider,
                AppRequest::TaskReady {
                    task_id,
                    lease_id: lease,
                },
            )
            .await
            {
                Ok(AppResponse::Ack) => {}
                Ok(other) => {
                    self.retry_provider(
                        &provider,
                        &task,
                        &format!("task_ready_rejected response={other:?}"),
                    );
                    continue;
                }
                Err(error) => {
                    self.retry_provider(
                        &provider,
                        &task,
                        &format!("task_ready_error message={}", error.message),
                    );
                    continue;
                }
            }

            match transfer
                .wait_for_task(&provider, task_id, lease, job.task_id)
                .await
            {
                RemoteOutcome::Success(outputs) => {
                    self.consumer.record_remote_cache(job.task_id, &provider);
                    self.consumer.clear_cooldown(&provider);
                    LogStruct::new(
                        LogLevel::Debug,
                        "远端编译完成",
                        format!("provider={provider}"),
                    )
                    .emit();
                    return Some(Ok(TaskResult {
                        action_digest: job.task_id,
                        exit_code: 0,
                        cache_hit: false,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        duration_ms: 0,
                        outputs,
                    }));
                }
                RemoteOutcome::CompileError(stdout, stderr) => {
                    LogStruct::new(
                        LogLevel::Error,
                        "远端编译失败",
                        format!(
                            "source=remote provider={provider} action={} stdout_bytes={} stderr_bytes={}",
                            hex_hash(&job.task_id),
                            stdout.len(),
                            stderr.len()
                        ),
                    )
                    .emit();
                    return Some(Ok(TaskResult {
                        action_digest: job.task_id,
                        exit_code: 1,
                        cache_hit: false,
                        stdout,
                        stderr,
                        duration_ms: 0,
                        outputs: Vec::new(),
                    }));
                }
                RemoteOutcome::Retry => {
                    let _ = control::send(
                        &self.control,
                        &self.config.service_name,
                        &provider,
                        AppRequest::Cancel { task_id },
                    )
                    .await;
                    self.retry_provider(&provider, &task, "remote_retry");
                    continue;
                }
            }
        }
        LogStruct::new(LogLevel::Debug, "远端编译", "全部 provider 失败").emit();
        None
    }
}

pub(super) enum RemoteOutcome {
    Success(Vec<OutputRef>),
    CompileError(Vec<u8>, Vec<u8>),
    Retry,
}

const CHUNK_SIZE: usize = 1024 * 1024;
const TASK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

struct RemoteTransfer {
    control: ControlHandle,
    cache: Arc<ResultCache>,
    service_name: String,
}

impl RemoteTransfer {
    fn new(control: ControlHandle, cache: Arc<ResultCache>, service_name: String) -> Self {
        Self {
            control,
            cache,
            service_name,
        }
    }

    async fn send(&self, provider: &PeerId, request: AppRequest) -> Result<AppResponse, String> {
        control::send(&self.control, &self.service_name, provider, request)
            .await
            .map_err(|e| e.message)
    }

    async fn fetch_cached(
        &self,
        provider: &PeerId,
        digest: Hash32,
    ) -> Result<Vec<OutputRef>, String> {
        let task_id = TaskId(digest);
        let response = timeout(
            CACHE_FETCH_TIMEOUT,
            self.send(provider, AppRequest::FetchCached { task_id }),
        )
        .await
        .map_err(|_| "cache fetch timeout".to_string())??;

        match response {
            AppResponse::Cached {
                task_id: cached_task,
                outputs,
                lease_id,
            } if cached_task == task_id => {
                let result = self.import_outputs(provider, digest, outputs).await;
                let _ = self
                    .send(provider, AppRequest::ResultAck { lease_id })
                    .await;
                result
            }
            other => Err(format!("cache fetch miss: {other:?}")),
        }
    }

    async fn upload_bundle(
        &self,
        provider: &PeerId,
        task_id: TaskId,
        hash: Hash32,
        bundle: &[u8],
    ) -> Result<(), String> {
        let mut offset = 0;
        while offset < bundle.len() {
            let end = (offset + CHUNK_SIZE).min(bundle.len());
            let response = self
                .send(
                    provider,
                    AppRequest::BlobPut {
                        task_id,
                        hash,
                        offset: offset as u64,
                        data: bundle[offset..end].to_vec(),
                    },
                )
                .await?;
            if response != AppResponse::Ack {
                return Err("provider rejected task data".into());
            }
            offset = end;
        }
        Ok(())
    }

    async fn wait_for_task(
        &self,
        provider: &PeerId,
        task_id: TaskId,
        lease: LeaseId,
        digest: Hash32,
    ) -> RemoteOutcome {
        let poll = async {
            loop {
                let response = self.send(provider, AppRequest::Status { task_id }).await;
                match response {
                    Ok(AppResponse::TaskStatus { state }) => match state {
                        TaskState::Done { outputs, .. } => {
                            let outputs =
                                match self.import_outputs(provider, digest, outputs).await {
                                    Ok(outputs) => outputs,
                                    Err(_) => return RemoteOutcome::Retry,
                                };
                            let _ = self
                                .send(provider, AppRequest::ResultAck { lease_id: lease })
                                .await;
                            return RemoteOutcome::Success(outputs);
                        }
                        TaskState::Failed {
                            code,
                            message,
                            stdout,
                            stderr,
                        } => {
                            if code == ErrorCode::Compile {
                                return RemoteOutcome::CompileError(
                                    stdout,
                                    if stderr.is_empty() {
                                        message.into_bytes()
                                    } else {
                                        stderr
                                    },
                                );
                            }
                            return RemoteOutcome::Retry;
                        }
                        TaskState::Cancelled => return RemoteOutcome::Retry,
                        TaskState::Queued | TaskState::Running => {}
                    },
                    Ok(_) | Err(_) => return RemoteOutcome::Retry,
                }
                sleep(Duration::from_secs(1)).await;
            }
        };

        match timeout(TASK_TIMEOUT, poll).await {
            Ok(outcome) => outcome,
            Err(_) => RemoteOutcome::Retry,
        }
    }

    async fn import_outputs(
        &self,
        provider: &PeerId,
        digest: Hash32,
        outputs: Vec<OutputRef>,
    ) -> Result<Vec<OutputRef>, String> {
        for output in &outputs {
            let bytes = self
                .download_blob(provider, &output.hash, output.size)
                .await?;
            if bytes.len() as u64 != output.size {
                return Err("result size mismatch".into());
            }
            let blob = self
                .cache
                .cas()
                .put_bytes(&bytes)
                .map_err(|e| e.to_string())?;
            if blob.hash != output.hash {
                return Err("result hash mismatch".into());
            }
        }
        self.cache
            .store(digest, outputs.clone())
            .map_err(|e| e.to_string())?;
        Ok(outputs)
    }

    async fn download_blob(
        &self,
        provider: &PeerId,
        hash: &Hash32,
        size: u64,
    ) -> Result<Vec<u8>, String> {
        let mut data = Vec::with_capacity(size.min(usize::MAX as u64) as usize);
        let mut offset = 0u64;
        while offset < size {
            let len = ((size - offset) as usize).min(CHUNK_SIZE);
            let response = self
                .send(
                    provider,
                    AppRequest::BlobGet {
                        hash: *hash,
                        offset,
                        len: len as u32,
                    },
                )
                .await?;
            match response {
                AppResponse::Chunk { data: chunk, .. } if !chunk.is_empty() => {
                    data.extend_from_slice(&chunk);
                    offset += chunk.len() as u64;
                }
                _ => return Err("provider returned no blob data".into()),
            }
        }
        if hash_bytes(&data) != *hash {
            return Err("result hash mismatch".into());
        }
        Ok(data)
    }
}
