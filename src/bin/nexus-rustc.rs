use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use nexuscompile::compile::bundle::build_bundle;
use nexuscompile::compile::executor;
use nexuscompile::compile::hash::hash_bytes;
use nexuscompile::compile::manifest::collect_manifest;
use nexuscompile::compile_protocol::{Hash32, OutputRef};
use nexuscompile::local_task_protocol::{
    LOCAL_TASK_PROTOCOL_VERSION, LocalTaskRequest, LocalTaskRequestBody, LocalTaskResponse,
    LocalTaskResponseBody, TASK_CHUNK_SIZE,
};
use nexuscompile::paths;
use nexuscompile::util::args;
use nexuscompile::util::path::{resolve_path, safe_join};

const SOCKET_TIMEOUT_SECS: u64 = 3600;

enum TaskOutcome {
    Compiled {
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

fn main() {
    let mut raw_args = env::args_os().skip(1);
    let rustc = raw_args.next().unwrap_or_else(|| "rustc".into());
    let args: Vec<String> = raw_args
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let mut env_map: HashMap<String, String> = env::vars().collect();
    // Cargo's jobserver fds are not inherited by the sidecar, and passing the
    // stale handles makes rustc warn and disable its jobserver. The digest
    // already ignores CARGO_MAKEFLAGS.
    env_map.remove("CARGO_MAKEFLAGS");

    if executor::is_supported(&args, &env_map).is_ok()
        && let Some(outcome) = try_task_socket(&rustc, &args, &env_map)
    {
        match outcome {
            TaskOutcome::Compiled {
                exit_code,
                stdout,
                stderr,
            } => {
                let _ = io::stdout().write_all(&stdout);
                let _ = io::stdout().flush();
                let _ = io::stderr().write_all(&stderr);
                let _ = io::stderr().flush();
                std::process::exit(exit_code);
            }
        }
    }

    exec_rustc(&rustc, &args);
}

fn try_task_socket(
    rustc: &std::ffi::OsStr,
    args: &[String],
    env_map: &HashMap<String, String>,
) -> Option<TaskOutcome> {
    let cwd = env::current_dir().ok()?;
    let manifest = collect_manifest(&cwd, Path::new(rustc), args, env_map).ok()?;
    let task_id = manifest.digest().ok()?;
    let out_dir = args::value(args, "--out-dir").map(|value| resolve_path(&cwd, &value))?;
    let mut stream = UnixStream::connect(paths::task_socket_path()).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))
        .ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(SOCKET_TIMEOUT_SECS)))
        .ok()?;

    match task_response(&mut stream, LocalTaskRequestBody::Probe { task_id })? {
        LocalTaskResponseBody::Hit { outputs } => {
            download_outputs(&mut stream, task_id, &outputs, &out_dir)?;
            let _ = task_response(&mut stream, LocalTaskRequestBody::Ack { task_id });
            Some(TaskOutcome::Compiled {
                exit_code: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
        LocalTaskResponseBody::Miss => {
            let bundle = build_bundle(&cwd, args, env_map, &manifest).ok()?;
            let bundle_hash = hash_bytes(&bundle);
            let body = LocalTaskRequestBody::Start {
                task_id,
                bundle_hash,
                bundle_len: bundle.len() as u64,
            };
            if !matches!(
                task_response(&mut stream, body)?,
                LocalTaskResponseBody::Ack
            ) {
                return None;
            }

            let mut offset = 0;
            while offset < bundle.len() {
                let end = (offset + TASK_CHUNK_SIZE).min(bundle.len());
                let body = LocalTaskRequestBody::Chunk {
                    task_id,
                    offset: offset as u64,
                    data: bundle[offset..end].to_vec(),
                };
                if !matches!(
                    task_response(&mut stream, body)?,
                    LocalTaskResponseBody::Ack
                ) {
                    return None;
                }
                offset = end;
            }

            match task_response(&mut stream, LocalTaskRequestBody::Ready { task_id })? {
                LocalTaskResponseBody::Done { outputs } => {
                    download_outputs(&mut stream, task_id, &outputs, &out_dir)?;
                    let _ = task_response(&mut stream, LocalTaskRequestBody::Ack { task_id });
                    Some(TaskOutcome::Compiled {
                        exit_code: 0,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    })
                }
                LocalTaskResponseBody::Failed {
                    exit_code,
                    stdout,
                    stderr,
                } => Some(TaskOutcome::Compiled {
                    exit_code,
                    stdout,
                    stderr,
                }),
                LocalTaskResponseBody::Error { .. } => None,
                _ => None,
            }
        }
        LocalTaskResponseBody::Error { .. } => None,
        _ => None,
    }
}

fn task_response(
    stream: &mut UnixStream,
    body: LocalTaskRequestBody,
) -> Option<LocalTaskResponseBody> {
    nexuscompile::local_protocol::write_message(stream, &LocalTaskRequest::new(body)).ok()?;
    let response: LocalTaskResponse = nexuscompile::local_protocol::read_message(stream).ok()?;
    (response.v == LOCAL_TASK_PROTOCOL_VERSION).then_some(response.body)
}

fn download_outputs(
    stream: &mut UnixStream,
    task_id: Hash32,
    outputs: &[OutputRef],
    out_dir: &Path,
) -> Option<()> {
    for output in outputs {
        download_output(stream, task_id, output, out_dir)?;
    }
    Some(())
}

fn download_output(
    stream: &mut UnixStream,
    task_id: Hash32,
    output: &OutputRef,
    out_dir: &Path,
) -> Option<()> {
    let mut data = Vec::with_capacity(output.size.min(usize::MAX as u64) as usize);
    let mut offset = 0u64;
    while offset < output.size {
        let len = ((output.size - offset) as usize).min(TASK_CHUNK_SIZE);
        let body = LocalTaskRequestBody::Output {
            task_id,
            hash: output.hash,
            offset,
            len: len as u32,
        };
        match task_response(stream, body)? {
            LocalTaskResponseBody::Chunk {
                data: chunk,
                offset: chunk_offset,
                ..
            } if chunk_offset == offset && !chunk.is_empty() => {
                data.extend_from_slice(&chunk);
                offset += chunk.len() as u64;
            }
            _ => return None,
        }
    }
    if data.len() as u64 != output.size || hash_bytes(&data) != output.hash {
        return None;
    }

    let target = safe_join(out_dir, &output.path)?;
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).ok()?;
    }
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("output");
    let temp = target.with_file_name(format!(".{file_name}.nexuscompile.tmp"));
    fs::write(&temp, &data).ok()?;
    fs::set_permissions(&temp, fs::Permissions::from_mode(output.mode)).ok()?;
    fs::rename(&temp, &target).ok()?;
    Some(())
}

fn exec_rustc(rustc: &std::ffi::OsStr, args: &[String]) -> ! {
    use std::os::unix::process::CommandExt;

    let error = Command::new(rustc).args(args).exec();
    eprintln!("nexus-rustc: failed to exec rustc: {error}");
    std::process::exit(127);
}
