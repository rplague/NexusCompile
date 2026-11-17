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
use crate::log::{LogLevel, LogStruct, log_task_error};
use crate::pool::{Consumer, Provider};
use crate::toolchain::resolve_rustc;
use crate::workspace::Workspace;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::{sleep, timeout};

const DEFAULT_PROVIDER_COOLDOWN_MS: u64 = 30_000;
const MIN_BUSY_COOLDOWN_MS: u64 = 1_000;

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

    pub async fn execute(&self, job: TaskJob) -> Result<TaskResult, TaskError> {
        let digest = job.task_id;
        if let Some(cached) = self
            .cache
            .lookup_and_materialize(&digest, &job.prepared.out_dir)
        {
            return Ok(TaskResult {
                action_digest: digest,
                exit_code: 0,
                cache_hit: true,
                stdout: Vec::new(),
                stderr: Vec::new(),
                duration_ms: 0,
                outputs: cached.outputs,
            });
        }

        if self.provider.free_slots() == 0
            && let Some(Ok(result)) = self.try_remote(&job).await
        {
            return Ok(result);
        }

        self.run_local(job).await
    }

    async fn run_local(&self, job: TaskJob) -> Result<TaskResult, TaskError> {
        let rustc = resolve_rustc(
            &self.config,
            &job.manifest.toolchain_id,
            &job.manifest.target,
        )
        .map_err(|error| TaskError::new("toolchain_error", error))?;
        let cache = self.cache.clone();
        let workspace = self.workspace.clone();
        match tokio::task::spawn_blocking(move || {
            executor::execute_job(&job, &rustc, &cache, &workspace, TaskSource::Local)
        })
        .await
        {
            Ok(result) => result,
            Err(error) => Err(TaskError::new("join_error", error.to_string())),
        }
    }

    fn cool_down_provider(
        &self,
        provider: &crate::compile_protocol::PeerId,
        task: &str,
        cooldown_ms: u64,
        reason: &str,
    ) {
        self.consumer.cool_down_provider(provider, cooldown_ms);
        log_task_error(
            "remote",
            task,
            "provider_cooldown",
            &format!("provider={provider} cooldown_ms={cooldown_ms} reason={reason}"),
        );
    }

    async fn try_remote(&self, job: &TaskJob) -> Option<Result<TaskResult, TaskError>> {
        let task = hex_hash(&job.task_id);
        let transfer = RemoteTransfer::new(
            self.control.clone(),
            self.cache.clone(),
            self.config.service_name.clone(),
        );

        let (consumer, providers) = {
            let consumer = self.provider.provider_id();
            let providers: Vec<_> = self
                .consumer
                .providers()
                .into_iter()
                .filter(|view| view.accepting && view.free_slots > 0)
                .map(|view| view.provider)
                .collect();
            (consumer, providers)
        };

        if providers.is_empty() {
            LogStruct::new(LogLevel::Debug, "远端编译", "没有可用的 provider").emit();
            return None;
        }

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
                    toolchain_id: job.manifest.toolchain_id.clone(),
                    target: job.manifest.target.clone(),
                },
            )
            .await
            {
                Ok(AppResponse::Granted { lease_id, .. }) => {
                    self.consumer.clear_cooldown(&provider);
                    lease_id
                }
                Ok(AppResponse::Cached {
                    lease_id, outputs, ..
                }) => {
                    let outputs = match transfer
                        .import_outputs(&provider, job.task_id, outputs, &job.prepared.out_dir)
                        .await
                    {
                        Ok(outputs) => outputs,
                        Err(error) => {
                            return Some(Err(TaskError::new("result_error", error)));
                        }
                    };
                    self.consumer.clear_cooldown(&provider);
                    let _ = control::send(
                        &self.control,
                        &self.config.service_name,
                        &provider,
                        AppRequest::ResultAck { lease_id },
                    )
                    .await;
                    return Some(Ok(TaskResult {
                        action_digest: job.task_id,
                        exit_code: 0,
                        cache_hit: true,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        duration_ms: 0,
                        outputs,
                    }));
                }
                Ok(AppResponse::Busy { retry_after_ms }) => {
                    let cooldown_ms = retry_after_ms.max(MIN_BUSY_COOLDOWN_MS);
                    self.cool_down_provider(&provider, &task, cooldown_ms, "reserve_busy");
                    continue;
                }
                Ok(AppResponse::Rejected { code, message }) => {
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        &format!("reserve_rejected code={code:?} message={message}"),
                    );
                    continue;
                }
                Ok(other) => {
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        &format!("reserve_unexpected response={other:?}"),
                    );
                    continue;
                }
                Err(error) => {
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        &format!("reserve_error message={error}"),
                    );
                    continue;
                }
            };

            if let Err(error) = transfer
                .upload_bundle(&provider, task_id, job.bundle_hash, &job.bundle_bytes)
                .await
            {
                self.cool_down_provider(
                    &provider,
                    &task,
                    DEFAULT_PROVIDER_COOLDOWN_MS,
                    &format!("upload_error message={error}"),
                );
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
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        &format!("task_ready_rejected response={other:?}"),
                    );
                    continue;
                }
                Err(error) => {
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        &format!("task_ready_error message={}", error.message),
                    );
                    continue;
                }
            }

            match transfer
                .wait_for_task(
                    &provider,
                    task_id,
                    lease,
                    job.task_id,
                    &job.prepared.out_dir,
                )
                .await
            {
                RemoteOutcome::Success(outputs) => {
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
                    self.cool_down_provider(
                        &provider,
                        &task,
                        DEFAULT_PROVIDER_COOLDOWN_MS,
                        "remote_retry",
                    );
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
        out_dir: &Path,
    ) -> RemoteOutcome {
        let poll = async {
            loop {
                let response = self.send(provider, AppRequest::Status { task_id }).await;
                match response {
                    Ok(AppResponse::TaskStatus { state }) => match state {
                        TaskState::Done { outputs, .. } => {
                            let outputs = match self
                                .import_outputs(provider, digest, outputs, out_dir)
                                .await
                            {
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
        out_dir: &Path,
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
        self.cache
            .materialize_outputs(&outputs, out_dir)
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
