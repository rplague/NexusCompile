//! Local task transport for manifest/bundle based compilation.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Handle;

use crate::compile::bundle::{TaskBundle, decode_bundle, materialize_bundle};
use crate::compile::cache::ResultCache;
use crate::compile::hash::hash_file;
use crate::compile_protocol::Hash32;
use crate::config::ConfigHandle;
use crate::local_protocol::{self, LocalRequest};
use crate::local_task_protocol::{
    LOCAL_TASK_PROTOCOL_VERSION, LocalTaskRequest, LocalTaskRequestBody, LocalTaskResponse,
    LocalTaskResponseBody,
};
use crate::log::{LogLevel, LogStruct};
use crate::remote::Coordinator;
use crate::toolchain::resolve_rustc;
use crate::workspace::Workspace;

pub fn bind(path: &Path) -> io::Result<UnixListener> {
    crate::local::bind(path)
}

pub async fn run(
    listener: UnixListener,
    coordinator: Arc<Coordinator>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    config: ConfigHandle,
    handle: Handle,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let coordinator = coordinator.clone();
                let cache = cache.clone();
                let workspace = workspace.clone();
                let config = config.clone();
                let handle = handle.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) =
                        handle_stream(stream, coordinator, cache, workspace, config, handle)
                    {
                        LogStruct::new(LogLevel::Warning, "本地任务请求失败", error.to_string())
                            .emit();
                    }
                });
            }
            Err(error) => {
                LogStruct::new(
                    LogLevel::Warning,
                    "本地任务 socket 接受失败",
                    error.to_string(),
                )
                .emit();
                break;
            }
        }
    }
}

struct Session {
    task_id: Hash32,
    task_dir: PathBuf,
    bundle_path: PathBuf,
    bundle_hash: Hash32,
    bundle_len: u64,
    bundle: File,
    received: u64,
}

fn handle_stream(
    stream: UnixStream,
    coordinator: Arc<Coordinator>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    config: ConfigHandle,
    handle: Handle,
) -> io::Result<()> {
    let mut stream = crate::local::prepare_stream(stream)?;
    let mut session: Option<Session> = None;

    loop {
        let request: LocalTaskRequest = local_protocol::read_message(&mut stream)?;
        if request.v != LOCAL_TASK_PROTOCOL_VERSION {
            local_protocol::write_message(
                &mut stream,
                &LocalTaskResponse::error("unsupported_version", "unsupported task protocol"),
            )?;
            return Ok(());
        }

        match request.body {
            LocalTaskRequestBody::Probe { task_id } => {
                let outputs = cache.cached_outputs(&task_id);
                let body = match outputs {
                    Some(outputs) => LocalTaskResponseBody::Hit { outputs },
                    None => LocalTaskResponseBody::Miss,
                };
                local_protocol::write_message(&mut stream, &LocalTaskResponse::new(body))?;
            }
            LocalTaskRequestBody::Start {
                task_id,
                bundle_hash,
                bundle_len,
            } => {
                if session.is_some() {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task already started"),
                    )?;
                    return Ok(());
                }
                let task_dir = workspace
                    .staging_dir(&task_id)
                    .map_err(|error| io::Error::other(error.to_string()))?;
                let bundle_path = task_dir.join("bundle");
                let bundle = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(&bundle_path)?;
                session = Some(Session {
                    task_id,
                    task_dir,
                    bundle_path,
                    bundle_hash,
                    bundle_len,
                    bundle,
                    received: 0,
                });
                local_protocol::write_message(
                    &mut stream,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )?;
            }
            LocalTaskRequestBody::Chunk {
                task_id,
                offset,
                data,
            } => {
                let Some(current) = session.as_mut() else {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task not started"),
                    )?;
                    return Ok(());
                };
                if current.task_id != task_id {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                if offset != current.received {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "non-contiguous bundle chunk"),
                    )?;
                    return Ok(());
                }
                let next = current.received.saturating_add(data.len() as u64);
                if next > current.bundle_len {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "bundle chunk overflow"),
                    )?;
                    return Ok(());
                }
                current.bundle.seek(SeekFrom::Start(offset))?;
                current.bundle.write_all(&data)?;
                current.received = next;
                local_protocol::write_message(
                    &mut stream,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )?;
            }
            LocalTaskRequestBody::Ready { task_id } => {
                let Some(current) = session.take() else {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task not started"),
                    )?;
                    return Ok(());
                };
                if current.task_id != task_id {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                if current.received != current.bundle_len {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "incomplete bundle"),
                    )?;
                    return Ok(());
                }
                let response = process_task(
                    &coordinator,
                    &cache,
                    &config,
                    &handle,
                    current.task_id,
                    &current.task_dir,
                    &current.bundle_path,
                    current.bundle_hash,
                    current.bundle_len,
                );
                local_protocol::write_message(&mut stream, &response)?;
                if matches!(response.body, LocalTaskResponseBody::Done { .. }) {
                    // Keep the task directory until the wrapper acknowledges.
                    session = Some(Session {
                        task_id: current.task_id,
                        task_dir: current.task_dir,
                        bundle_path: current.bundle_path,
                        bundle_hash: current.bundle_hash,
                        bundle_len: current.bundle_len,
                        bundle: current.bundle,
                        received: current.received,
                    });
                } else {
                    let _ = fs::remove_dir_all(&current.task_dir);
                }
            }
            LocalTaskRequestBody::Output {
                task_id,
                hash,
                offset,
                len,
            } => {
                if let Some(current) = session.as_ref()
                    && current.task_id != task_id
                {
                    local_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                match cache.read_chunk(&hash, offset, len as usize) {
                    Ok(data) => {
                        local_protocol::write_message(
                            &mut stream,
                            &LocalTaskResponse::new(LocalTaskResponseBody::Chunk {
                                hash,
                                offset,
                                data,
                            }),
                        )?;
                    }
                    Err(error) => {
                        local_protocol::write_message(
                            &mut stream,
                            &LocalTaskResponse::error("output_error", error.to_string()),
                        )?;
                        return Ok(());
                    }
                }
            }
            LocalTaskRequestBody::Ack { task_id } => {
                if let Some(current) = session.take() {
                    if current.task_id != task_id {
                        local_protocol::write_message(
                            &mut stream,
                            &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                        )?;
                        return Ok(());
                    }
                    let _ = fs::remove_dir_all(&current.task_dir);
                }
                local_protocol::write_message(
                    &mut stream,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )?;
                return Ok(());
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_task(
    coordinator: &Coordinator,
    cache: &ResultCache,
    config: &ConfigHandle,
    handle: &Handle,
    task_id: Hash32,
    task_dir: &Path,
    bundle_path: &Path,
    bundle_hash: Hash32,
    bundle_len: u64,
) -> LocalTaskResponse {
    let metadata = match fs::metadata(bundle_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return LocalTaskResponse::error("bundle_error", format!("read bundle: {error}"));
        }
    };
    if metadata.len() != bundle_len {
        return LocalTaskResponse::error("bundle_error", "bundle size mismatch");
    }
    let (actual_hash, _) = match hash_file(bundle_path) {
        Ok(hash) => hash,
        Err(error) => {
            return LocalTaskResponse::error("bundle_error", format!("hash bundle: {error}"));
        }
    };
    if actual_hash != bundle_hash {
        return LocalTaskResponse::error("bundle_error", "bundle hash mismatch");
    }
    let bytes = match fs::read(bundle_path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return LocalTaskResponse::error("bundle_error", format!("read bundle: {error}"));
        }
    };
    let bundle: TaskBundle = match decode_bundle(&bytes) {
        Ok(bundle) => bundle,
        Err(error) => return LocalTaskResponse::error("bundle_error", error),
    };
    let digest = match bundle.manifest.digest() {
        Ok(digest) => digest,
        Err(error) => return LocalTaskResponse::error("manifest_error", error.to_string()),
    };
    if digest != task_id {
        return LocalTaskResponse::error("manifest_error", "manifest digest mismatch");
    }

    let work = task_dir.join("work");
    let work_out = work.join("out");
    let work_extern = work.join("extern");
    let result = fs::create_dir_all(&work)
        .and_then(|_| materialize_bundle(&bundle, &work))
        .and_then(|_| fs::create_dir_all(&work_out))
        .and_then(|_| fs::create_dir_all(&work_extern))
        .and_then(|_| fs::create_dir_all(work.join("target")));
    if let Err(error) = result {
        return LocalTaskResponse::error("bundle_error", format!("materialize bundle: {error}"));
    }

    let config = config.snapshot();
    let rustc = match resolve_rustc(
        &config,
        &bundle.manifest.toolchain_id,
        &bundle.manifest.target,
    ) {
        Ok(rustc) => rustc,
        Err(error) => return LocalTaskResponse::error("toolchain_error", error),
    };
    let prepared = match crate::compile::bundle::prepare_task(&bundle, &work) {
        Ok(prepared) => prepared,
        Err(error) => return LocalTaskResponse::error("bundle_error", error),
    };
    let request = LocalRequest::new(
        prepared.cwd.to_string_lossy(),
        rustc,
        prepared.args,
        prepared.env.into_iter().collect(),
    );

    let response = handle.block_on(coordinator.execute(&request));
    let outputs = match response.body {
        crate::local_protocol::LocalBody::Compiled {
            exit_code,
            stdout,
            stderr,
            ..
        } => {
            if exit_code != 0 {
                let _ = fs::remove_dir_all(&work);
                return LocalTaskResponse::new(LocalTaskResponseBody::Failed {
                    exit_code,
                    stdout,
                    stderr,
                });
            }
            match cache.collect_outputs(&work_out) {
                Ok(outputs) if !outputs.is_empty() => outputs,
                Ok(_) => {
                    let _ = fs::remove_dir_all(&work);
                    return LocalTaskResponse::error("output_error", "rustc produced no outputs");
                }
                Err(error) => {
                    let _ = fs::remove_dir_all(&work);
                    return LocalTaskResponse::error("output_error", error.to_string());
                }
            }
        }
        crate::local_protocol::LocalBody::Error { code, message } => {
            let _ = fs::remove_dir_all(&work);
            return LocalTaskResponse::error(code, message);
        }
    };
    let _ = fs::remove_dir_all(&work);
    LocalTaskResponse::new(LocalTaskResponseBody::Done { outputs })
}
