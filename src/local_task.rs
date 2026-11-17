//! Local task transport for manifest/bundle based compilation.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Handle;

use crate::compile::bundle::TaskJob;
use crate::compile::cache::ResultCache;
use crate::compile::hash::hex_hash;
use crate::compile_protocol::Hash32;
use crate::local_task_protocol;
use crate::local_task_protocol::{
    LOCAL_TASK_PROTOCOL_VERSION, LocalTaskRequest, LocalTaskRequestBody, LocalTaskResponse,
    LocalTaskResponseBody,
};
use crate::log::{LogLevel, LogStruct, log_task_error};
use crate::remote::Coordinator;
use crate::workspace::Workspace;

const SOCKET_READ_TIMEOUT: Duration = Duration::from_secs(3600);
const SOCKET_MODE: u32 = 0o660;

pub fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path)?;
        }
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} exists and is not a socket", path.display()),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(SOCKET_MODE))?;
    Ok(listener)
}

fn prepare_stream(stream: UnixStream) -> io::Result<std::os::unix::net::UnixStream> {
    let stream = stream.into_std()?;
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(SOCKET_READ_TIMEOUT))?;
    stream.set_write_timeout(Some(SOCKET_READ_TIMEOUT))?;
    Ok(stream)
}

pub async fn run(
    listener: UnixListener,
    coordinator: Arc<Coordinator>,
    cache: Arc<ResultCache>,
    workspace: Arc<Workspace>,
    handle: Handle,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let coordinator = coordinator.clone();
                let cache = cache.clone();
                let workspace = workspace.clone();
                let handle = handle.clone();
                tokio::task::spawn_blocking(move || {
                    if let Err(error) = handle_stream(stream, coordinator, cache, workspace, handle)
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
    handle: Handle,
) -> io::Result<()> {
    let mut stream = prepare_stream(stream)?;
    let mut session: Option<Session> = None;

    loop {
        let request: LocalTaskRequest = local_task_protocol::read_message(&mut stream)?;
        if request.v != LOCAL_TASK_PROTOCOL_VERSION {
            local_task_protocol::write_message(
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
                local_task_protocol::write_message(&mut stream, &LocalTaskResponse::new(body))?;
            }
            LocalTaskRequestBody::Start {
                task_id,
                bundle_hash,
                bundle_len,
            } => {
                if session.is_some() {
                    local_task_protocol::write_message(
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
                local_task_protocol::write_message(
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
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task not started"),
                    )?;
                    return Ok(());
                };
                if current.task_id != task_id {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                if offset != current.received {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "non-contiguous bundle chunk"),
                    )?;
                    return Ok(());
                }
                let next = current.received.saturating_add(data.len() as u64);
                if next > current.bundle_len {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "bundle chunk overflow"),
                    )?;
                    return Ok(());
                }
                current.bundle.seek(SeekFrom::Start(offset))?;
                current.bundle.write_all(&data)?;
                current.received = next;
                local_task_protocol::write_message(
                    &mut stream,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )?;
            }
            LocalTaskRequestBody::Ready { task_id } => {
                let Some(current) = session.take() else {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task not started"),
                    )?;
                    return Ok(());
                };
                if current.task_id != task_id {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                if current.received != current.bundle_len {
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "incomplete bundle"),
                    )?;
                    return Ok(());
                }
                let response = process_task(
                    &coordinator,
                    &handle,
                    current.task_id,
                    &current.task_dir,
                    &current.bundle_path,
                    current.bundle_hash,
                    current.bundle_len,
                );
                if let LocalTaskResponseBody::Error { code, message } = &response.body {
                    log_task_error("local-task", &hex_hash(&current.task_id), code, message);
                }
                local_task_protocol::write_message(&mut stream, &response)?;
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
                    return Ok(());
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
                    local_task_protocol::write_message(
                        &mut stream,
                        &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                    )?;
                    return Ok(());
                }
                match cache.read_chunk(&hash, offset, len as usize) {
                    Ok(data) => {
                        local_task_protocol::write_message(
                            &mut stream,
                            &LocalTaskResponse::new(LocalTaskResponseBody::Chunk {
                                hash,
                                offset,
                                data,
                            }),
                        )?;
                    }
                    Err(error) => {
                        local_task_protocol::write_message(
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
                        local_task_protocol::write_message(
                            &mut stream,
                            &LocalTaskResponse::error("invalid_state", "task id mismatch"),
                        )?;
                        return Ok(());
                    }
                    let _ = fs::remove_dir_all(&current.task_dir);
                }
                local_task_protocol::write_message(
                    &mut stream,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )?;
                return Ok(());
            }
        }
    }
}

fn process_task(
    coordinator: &Coordinator,
    handle: &Handle,
    task_id: Hash32,
    task_dir: &Path,
    bundle_path: &Path,
    bundle_hash: Hash32,
    bundle_len: u64,
) -> LocalTaskResponse {
    let job = match TaskJob::from_bundle_file(
        bundle_path,
        task_id,
        bundle_hash,
        bundle_len,
        &task_dir.join("work"),
    ) {
        Ok(job) => job,
        Err(error) => return LocalTaskResponse::error(error.code, error.message),
    };

    let result = match handle.block_on(coordinator.execute(job)) {
        Ok(result) => result,
        Err(error) => return LocalTaskResponse::error(error.code, error.message),
    };

    if result.exit_code != 0 {
        return LocalTaskResponse::new(LocalTaskResponseBody::Failed {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        });
    }

    LocalTaskResponse::new(LocalTaskResponseBody::Done {
        outputs: result.outputs,
    })
}
