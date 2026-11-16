//! Consumer-side remote task coordinator.

mod transfer;

use crate::compile::bundle::build_bundle;
use crate::compile::cache::ResultCache;
use crate::compile::executor;
use crate::compile::hash::{hash_bytes, hex_hash};
use crate::compile::manifest::collect_manifest;
use crate::compile_protocol::{AppRequest, AppResponse, Hash32, TaskId};
use crate::config::ServiceConfig;
use crate::control::{self, ControlHandle};
use crate::local_protocol::{LocalBody, LocalRequest, LocalResponse};
use crate::log::{LogLevel, LogStruct, log_task_error};
use crate::pool::{Consumer, Provider};
use crate::util::args;
use crate::util::path::resolve_path;
use crate::workspace::Workspace;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use transfer::RemoteTransfer;

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

    pub async fn execute(&self, request: &LocalRequest) -> LocalResponse {
        let env: HashMap<String, String> = request.env.iter().cloned().collect();
        if let Err(reason) = executor::is_supported(&request.args, &env) {
            return LocalResponse::error("unsupported", reason);
        }

        let cwd = Path::new(&request.cwd);
        let Some(out_dir) =
            args::value(&request.args, "--out-dir").map(|value| resolve_path(cwd, &value))
        else {
            return LocalResponse::error("unsupported", "missing --out-dir");
        };
        let manifest = match collect_manifest(cwd, Path::new(&request.rustc), &request.args, &env) {
            Ok(manifest) => manifest,
            Err(error) => return LocalResponse::error("manifest_error", error.to_string()),
        };
        let digest = match manifest.digest() {
            Ok(digest) => digest,
            Err(error) => return LocalResponse::error("manifest_error", error.to_string()),
        };

        if self
            .cache
            .lookup_and_materialize(&digest, &out_dir)
            .is_some()
        {
            return LocalResponse::new(LocalBody::Compiled {
                action_digest: digest,
                exit_code: 0,
                cache_hit: true,
                stdout: Vec::new(),
                stderr: Vec::new(),
                duration_ms: 0,
            });
        }

        let local_free = self.provider.free_slots() > 0;
        if local_free {
            return self.run_local(request).await;
        }

        if let Some(response) = self
            .try_remote(request, &manifest, digest, &out_dir, &env)
            .await
            && !matches!(response.body, LocalBody::Error { .. })
        {
            return response;
        }

        self.run_local(request).await
    }

    async fn run_local(&self, request: &LocalRequest) -> LocalResponse {
        let request = request.clone();
        let cache = self.cache.clone();
        let workspace = self.workspace.clone();
        match tokio::task::spawn_blocking(move || {
            executor::execute_local(&request, &cache, &workspace)
        })
        .await
        {
            Ok(response) => response,
            Err(error) => LocalResponse::error("join_error", error.to_string()),
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

    async fn try_remote(
        &self,
        request: &LocalRequest,
        manifest: &crate::compile::manifest::ActionManifest,
        digest: Hash32,
        out_dir: &Path,
        env: &HashMap<String, String>,
    ) -> Option<LocalResponse> {
        let task = hex_hash(&digest);
        let bundle = match build_bundle(Path::new(&request.cwd), &request.args, env, manifest) {
            Ok(bundle) => bundle,
            Err(error) => {
                return Some(LocalResponse::error("bundle_error", error));
            }
        };
        let bundle_hash = hash_bytes(&bundle);
        let task_id = TaskId(digest);
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
                    &crate::compile::hash::hex_hash(&digest)[..8]
                ),
            )
            .emit();
            let lease = match control::send(
                &self.control,
                &self.config.service_name,
                &provider,
                AppRequest::Reserve {
                    consumer: consumer.clone(),
                    task_id,
                    slots: 1,
                    input_manifest: bundle_hash,
                    toolchain_id: manifest.toolchain_id.clone(),
                    target: manifest.target.clone(),
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
                    if let Err(error) = transfer
                        .import_outputs(&provider, digest, outputs, out_dir)
                        .await
                    {
                        return Some(LocalResponse::error("result_error", error));
                    }
                    self.consumer.clear_cooldown(&provider);
                    let _ = control::send(
                        &self.control,
                        &self.config.service_name,
                        &provider,
                        AppRequest::ResultAck { lease_id },
                    )
                    .await;
                    return Some(success_response(digest));
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
                .upload_bundle(&provider, task_id, bundle_hash, &bundle)
                .await
            {
                self.cool_down_provider(
                    &provider,
                    &task,
                    DEFAULT_PROVIDER_COOLDOWN_MS,
                    &format!("upload_error message={error}"),
                );
                return Some(LocalResponse::error("upload_error", error));
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
                .wait_for_task(&provider, task_id, lease, digest, out_dir)
                .await
            {
                RemoteOutcome::Success => {
                    self.consumer.clear_cooldown(&provider);
                    LogStruct::new(
                        LogLevel::Debug,
                        "远端编译完成",
                        format!("provider={provider}"),
                    )
                    .emit();
                    return Some(success_response(digest));
                }
                RemoteOutcome::CompileError(stdout, stderr) => {
                    LogStruct::new(
                        LogLevel::Error,
                        "远端编译失败",
                        format!(
                            "source=remote provider={provider} action={} stdout_bytes={} stderr_bytes={}",
                            hex_hash(&digest),
                            stdout.len(),
                            stderr.len()
                        ),
                    )
                    .emit();
                    return Some(LocalResponse::new(LocalBody::Compiled {
                        action_digest: digest,
                        exit_code: 1,
                        cache_hit: false,
                        stdout,
                        stderr,
                        duration_ms: 0,
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
    Success,
    CompileError(Vec<u8>, Vec<u8>),
    Retry,
}

fn success_response(digest: Hash32) -> LocalResponse {
    LocalResponse::new(LocalBody::Compiled {
        action_digest: digest,
        exit_code: 0,
        cache_hit: false,
        stdout: Vec::new(),
        stderr: Vec::new(),
        duration_ms: 0,
    })
}
