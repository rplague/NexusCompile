//! Consumer-side remote task coordinator.

mod transfer;

use crate::compile::bundle::build_bundle;
use crate::compile::cache::ResultCache;
use crate::compile::executor;
use crate::compile::hash::hash_bytes;
use crate::compile::manifest::collect_manifest;
use crate::compile_protocol::{AppRequest, AppResponse, Hash32, TaskId};
use crate::config::ServiceConfig;
use crate::local_protocol::{LocalBody, LocalRequest, LocalResponse};
use crate::pool::PoolOps;
use crate::state::NodeState;
use crate::util::args;
use crate::util::path::resolve_path;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use transfer::RemoteTransfer;

pub struct Coordinator {
    state: Arc<Mutex<NodeState>>,
    cache: Arc<ResultCache>,
    ops: Arc<dyn PoolOps>,
    config: ServiceConfig,
}

impl Coordinator {
    pub fn new(
        state: Arc<Mutex<NodeState>>,
        cache: Arc<ResultCache>,
        ops: Arc<dyn PoolOps>,
        config: ServiceConfig,
    ) -> Self {
        Self {
            state,
            cache,
            ops,
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

        let local_free = self
            .state
            .lock()
            .map(|state| state.free_slots > 0)
            .unwrap_or(false);
        if local_free {
            return self.run_local(request).await;
        }

        if let Some(response) = self
            .try_remote(request, &manifest, digest, &out_dir, &env)
            .await
        {
            if !matches!(response.body, LocalBody::Error { .. }) {
                return response;
            }
        }

        self.run_local(request).await
    }

    async fn run_local(&self, request: &LocalRequest) -> LocalResponse {
        let request = request.clone();
        let cache = self.cache.clone();
        match tokio::task::spawn_blocking(move || executor::execute_local(&request, &cache)).await {
            Ok(response) => response,
            Err(error) => LocalResponse::error("join_error", error.to_string()),
        }
    }

    async fn try_remote(
        &self,
        request: &LocalRequest,
        manifest: &crate::compile::manifest::ActionManifest,
        digest: Hash32,
        out_dir: &Path,
        env: &HashMap<String, String>,
    ) -> Option<LocalResponse> {
        let bundle = match build_bundle(Path::new(&request.cwd), &request.args, env, manifest) {
            Ok(bundle) => bundle,
            Err(error) => {
                return Some(LocalResponse::error("bundle_error", error));
            }
        };
        let bundle_hash = hash_bytes(&bundle);
        let task_id = TaskId(digest);
        let transfer = RemoteTransfer::new(self.ops.as_ref(), self.cache.as_ref());

        let (consumer, providers) = {
            let state = self.state.lock().ok()?;
            let providers: Vec<_> = state
                .providers
                .values()
                .filter(|view| {
                    view.accepting
                        && view.free_slots > 0
                        && (self.config.node.toolchain.is_empty()
                            || view.toolchain == self.config.node.toolchain)
                        && (self.config.node.target.is_empty()
                            || view.target == self.config.node.target)
                })
                .map(|view| view.provider.clone())
                .collect();
            (state.provider_id.clone(), providers)
        };

        for provider in providers {
            let lease = match self
                .ops
                .send(
                    &provider,
                    AppRequest::Reserve {
                        consumer: consumer.clone(),
                        task_id,
                        slots: 1,
                        input_manifest: bundle_hash,
                    },
                )
                .await
            {
                Ok(AppResponse::Granted { lease_id, .. }) => lease_id,
                Ok(AppResponse::Cached {
                    lease_id, outputs, ..
                }) => {
                    if let Err(error) = transfer
                        .import_outputs(&provider, digest, outputs, out_dir)
                        .await
                    {
                        return Some(LocalResponse::error("result_error", error));
                    }
                    let _ = self
                        .ops
                        .send(&provider, AppRequest::ResultAck { lease_id })
                        .await;
                    return Some(success_response(digest));
                }
                Ok(AppResponse::Busy { .. }) | Ok(AppResponse::Rejected { .. }) => continue,
                Ok(_) | Err(_) => continue,
            };

            if let Err(error) = transfer
                .upload_bundle(&provider, task_id, bundle_hash, &bundle)
                .await
            {
                return Some(LocalResponse::error("upload_error", error));
            }
            if self
                .ops
                .send(
                    &provider,
                    AppRequest::TaskReady {
                        task_id,
                        lease_id: lease,
                    },
                )
                .await
                .is_err()
            {
                continue;
            }

            match transfer
                .wait_for_task(&provider, task_id, lease, digest, out_dir)
                .await
            {
                RemoteOutcome::Success => return Some(success_response(digest)),
                RemoteOutcome::CompileError(stdout, stderr) => {
                    return Some(LocalResponse::new(LocalBody::Compiled {
                        action_digest: digest,
                        exit_code: 1,
                        cache_hit: false,
                        stdout,
                        stderr,
                        duration_ms: 0,
                    }));
                }
                RemoteOutcome::Retry => continue,
            }
        }
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
