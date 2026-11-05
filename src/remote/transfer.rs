use std::path::Path;
use std::time::Duration;

use tokio::time::{sleep, timeout};

use crate::compile::cache::ResultCache;
use crate::compile::hash::hash_bytes;
use crate::compile_protocol::{
    AppRequest, AppResponse, ErrorCode, Hash32, LeaseId, OutputRef, PeerId, TaskId, TaskState,
};
use crate::pool::PoolOps;

use super::RemoteOutcome;

const CHUNK_SIZE: usize = 1024 * 1024;
const TASK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub(super) struct RemoteTransfer<'a> {
    ops: &'a dyn PoolOps,
    cache: &'a ResultCache,
}

impl<'a> RemoteTransfer<'a> {
    pub(super) fn new(ops: &'a dyn PoolOps, cache: &'a ResultCache) -> Self {
        Self { ops, cache }
    }

    pub(super) async fn upload_bundle(
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
                .ops
                .send(
                    provider,
                    AppRequest::BlobPut {
                        task_id,
                        hash,
                        offset: offset as u64,
                        data: bundle[offset..end].to_vec(),
                    },
                )
                .await
                .map_err(|e| e.message)?;
            if response != AppResponse::Ack {
                return Err("provider rejected task data".into());
            }
            offset = end;
        }
        Ok(())
    }

    pub(super) async fn wait_for_task(
        &self,
        provider: &PeerId,
        task_id: TaskId,
        lease: LeaseId,
        digest: Hash32,
        out_dir: &Path,
    ) -> RemoteOutcome {
        let poll = async {
            loop {
                let response = self
                    .ops
                    .send(provider, AppRequest::Status { task_id })
                    .await;
                match response {
                    Ok(AppResponse::TaskStatus { state }) => match state {
                        TaskState::Done { outputs, .. } => {
                            if let Err(error) = self
                                .import_outputs(provider, digest, outputs, out_dir)
                                .await
                            {
                                return RemoteOutcome::CompileError(Vec::new(), error.into_bytes());
                            }
                            let _ = self
                                .ops
                                .send(provider, AppRequest::ResultAck { lease_id: lease })
                                .await;
                            return RemoteOutcome::Success;
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

    pub(super) async fn import_outputs(
        &self,
        provider: &PeerId,
        digest: Hash32,
        outputs: Vec<OutputRef>,
        out_dir: &Path,
    ) -> Result<(), String> {
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
        Ok(())
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
                .ops
                .send(
                    provider,
                    AppRequest::BlobGet {
                        hash: *hash,
                        offset,
                        len: len as u32,
                    },
                )
                .await
                .map_err(|e| e.message)?;
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
