//! `nexus-rustc` wrapper: local task socket client and rustc fallback.

use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use nexuscompile::compile::bundle::build_bundle;
use nexuscompile::compile::hash::hash_bytes;
use nexuscompile::compile::manifest::collect_manifest;
use nexuscompile::compile::{ReviewError, review_invocation};
use nexuscompile::compile_protocol::{Hash32, OutputRef};
use nexuscompile::local_task_protocol::{
    LOCAL_TASK_PROTOCOL_VERSION, LocalTaskRequest, LocalTaskRequestBody, LocalTaskResponse,
    LocalTaskResponseBody, TASK_CHUNK_SIZE,
};
use nexuscompile::local_task_protocol::{read_message, write_message};
use nexuscompile::paths;
use nexuscompile::util::args;
use nexuscompile::util::path::{resolve_path, safe_join};

const SOCKET_TIMEOUT_SECS: u64 = 3600;
const DEBUG_ENV: &str = "NEXUSCOMPILE_WRAPPER_DEBUG";

static STAGING_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub enum WrapperError {
    Manifest(String),
    MissingOutDir,
    Io(io::Error),
    Protocol(String),
    Bundle(String),
    UnsafePath(PathBuf),
    HashMismatch,
}

impl std::fmt::Display for WrapperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Manifest(message) => write!(f, "manifest: {message}"),
            Self::MissingOutDir => f.write_str("missing --out-dir"),
            Self::Io(error) => write!(f, "io: {error}"),
            Self::Protocol(message) => write!(f, "protocol: {message}"),
            Self::Bundle(message) => write!(f, "bundle: {message}"),
            Self::UnsafePath(path) => write!(f, "unsafe output path: {}", path.display()),
            Self::HashMismatch => f.write_str("output hash mismatch"),
        }
    }
}

impl std::error::Error for WrapperError {}

impl From<io::Error> for WrapperError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
pub struct CompileOutcome {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CompileOutcome {
    fn success() -> Self {
        Self {
            exit_code: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }
}

struct Invocation {
    rustc: std::ffi::OsString,
    args: Vec<String>,
    env: HashMap<String, String>,
}

impl Invocation {
    fn from_env() -> Self {
        let mut raw_args = env::args_os().skip(1);
        let rustc = raw_args.next().unwrap_or_else(|| "rustc".into());
        let args = raw_args
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        let mut env: HashMap<String, String> = env::vars().collect();
        // Cargo's jobserver fds are not inherited by the sidecar. The digest
        // ignores CARGO_MAKEFLAGS, and the fallback path keeps the real
        // process environment unchanged.
        env.remove("CARGO_MAKEFLAGS");
        Self { rustc, args, env }
    }
}

fn main() {
    run();
}

/// Run the wrapper. Always exits or replaces the process with rustc.
pub fn run() -> ! {
    let invocation = Invocation::from_env();
    let incremental = invocation.env.get("CARGO_INCREMENTAL").map(String::as_str);
    if let Err(error) = review_invocation(&invocation.args, incremental) {
        debug_review(&error);
    } else {
        match try_remote(&invocation) {
            Ok(outcome) => replay(outcome),
            Err(error) => debug(&error),
        }
    }
    exec_rustc(&invocation.rustc, &invocation.args);
}

fn try_remote(invocation: &Invocation) -> Result<CompileOutcome, WrapperError> {
    let cwd = env::current_dir()?;
    let manifest = collect_manifest(
        &cwd,
        Path::new(&invocation.rustc),
        &invocation.args,
        &invocation.env,
    )
    .map_err(|error| WrapperError::Manifest(error.to_string()))?;
    let task_id = manifest
        .digest()
        .map_err(|error| WrapperError::Manifest(error.to_string()))?;
    let out_dir = args::value(&invocation.args, "--out-dir")
        .map(|value| resolve_path(&cwd, &value))
        .ok_or(WrapperError::MissingOutDir)?;

    let mut client = TaskSocketClient::connect(
        &paths::task_socket_path(),
        Duration::from_secs(SOCKET_TIMEOUT_SECS),
    )?;

    match client.probe(task_id)? {
        LocalTaskResponseBody::Hit { outputs } => {
            client.download_outputs(task_id, &outputs, &out_dir)?;
            let _ = client.ack(task_id);
            Ok(CompileOutcome::success())
        }
        LocalTaskResponseBody::Miss => {
            let bundle = build_bundle(&cwd, &invocation.args, &invocation.env, &manifest)
                .map_err(WrapperError::Bundle)?;
            let bundle_hash = hash_bytes(&bundle);
            client.start(
                task_id,
                bundle_hash,
                bundle.len() as u64,
                manifest.toolchain_id.clone(),
                manifest.target.clone(),
            )?;
            client.upload_bundle(task_id, &bundle)?;

            match client.ready(task_id)? {
                LocalTaskResponseBody::Done { outputs } => {
                    client.download_outputs(task_id, &outputs, &out_dir)?;
                    let _ = client.ack(task_id);
                    Ok(CompileOutcome::success())
                }
                LocalTaskResponseBody::Failed {
                    exit_code,
                    stdout,
                    stderr,
                } => Ok(CompileOutcome {
                    exit_code,
                    stdout,
                    stderr,
                }),
                other => Err(WrapperError::Protocol(format!(
                    "unexpected ready response: {other:?}"
                ))),
            }
        }
        other => Err(WrapperError::Protocol(format!(
            "unexpected probe response: {other:?}"
        ))),
    }
}

struct TaskSocketClient<S> {
    stream: S,
}

impl TaskSocketClient<UnixStream> {
    fn connect(path: &Path, timeout: Duration) -> Result<Self, WrapperError> {
        let stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        Ok(Self { stream })
    }
}

impl<S: Read + Write> TaskSocketClient<S> {
    fn request(
        &mut self,
        body: LocalTaskRequestBody,
    ) -> Result<LocalTaskResponseBody, WrapperError> {
        write_message(&mut self.stream, &LocalTaskRequest::new(body))?;
        let response: LocalTaskResponse = read_message(&mut self.stream)?;
        if response.v != LOCAL_TASK_PROTOCOL_VERSION {
            return Err(WrapperError::Protocol(format!(
                "protocol version {}",
                response.v
            )));
        }
        match response.body {
            LocalTaskResponseBody::Error { code, message } => {
                Err(WrapperError::Protocol(format!("{code}: {message}")))
            }
            body => Ok(body),
        }
    }

    fn probe(&mut self, task_id: Hash32) -> Result<LocalTaskResponseBody, WrapperError> {
        self.request(LocalTaskRequestBody::Probe { task_id })
    }

    fn start(
        &mut self,
        task_id: Hash32,
        bundle_hash: Hash32,
        bundle_len: u64,
        toolchain_id: String,
        target: String,
    ) -> Result<(), WrapperError> {
        self.expect_ack(LocalTaskRequestBody::Start {
            task_id,
            bundle_hash,
            bundle_len,
            toolchain_id,
            target,
        })
    }

    fn upload_bundle(&mut self, task_id: Hash32, bundle: &[u8]) -> Result<(), WrapperError> {
        let mut offset = 0;
        while offset < bundle.len() {
            let end = (offset + TASK_CHUNK_SIZE).min(bundle.len());
            self.expect_ack(LocalTaskRequestBody::Chunk {
                task_id,
                offset: offset as u64,
                data: bundle[offset..end].to_vec(),
            })?;
            offset = end;
        }
        Ok(())
    }

    fn ready(&mut self, task_id: Hash32) -> Result<LocalTaskResponseBody, WrapperError> {
        self.request(LocalTaskRequestBody::Ready { task_id })
    }

    fn ack(&mut self, task_id: Hash32) -> Result<(), WrapperError> {
        self.expect_ack(LocalTaskRequestBody::Ack { task_id })
    }

    fn expect_ack(&mut self, body: LocalTaskRequestBody) -> Result<(), WrapperError> {
        match self.request(body)? {
            LocalTaskResponseBody::Ack => Ok(()),
            other => Err(WrapperError::Protocol(format!(
                "expected ack, got {other:?}"
            ))),
        }
    }

    fn download_outputs(
        &mut self,
        task_id: Hash32,
        outputs: &[OutputRef],
        out_dir: &Path,
    ) -> Result<(), WrapperError> {
        let mut stager = OutputStager::new(out_dir, task_id)?;
        for output in outputs {
            let data = self.download_output(task_id, output)?;
            stager.stage(output, data)?;
        }
        stager.commit()
    }

    fn download_output(
        &mut self,
        task_id: Hash32,
        output: &OutputRef,
    ) -> Result<Vec<u8>, WrapperError> {
        let mut data = Vec::with_capacity(output.size.min(usize::MAX as u64) as usize);
        let mut offset = 0u64;
        while offset < output.size {
            let len = ((output.size - offset) as usize).min(TASK_CHUNK_SIZE);
            match self.request(LocalTaskRequestBody::Output {
                task_id,
                hash: output.hash,
                offset,
                len: len as u32,
            })? {
                LocalTaskResponseBody::Chunk {
                    data: chunk,
                    offset: chunk_offset,
                    ..
                } if chunk_offset == offset && !chunk.is_empty() => {
                    data.extend_from_slice(&chunk);
                    offset += chunk.len() as u64;
                }
                other => {
                    return Err(WrapperError::Protocol(format!(
                        "unexpected output response: {other:?}"
                    )));
                }
            }
        }
        if data.len() as u64 != output.size {
            return Err(WrapperError::Protocol("output size mismatch".into()));
        }
        if hash_bytes(&data) != output.hash {
            return Err(WrapperError::HashMismatch);
        }
        Ok(data)
    }
}

struct OutputStager {
    out_dir: PathBuf,
    temp_dir: PathBuf,
    files: Vec<(PathBuf, PathBuf)>,
}

impl OutputStager {
    fn new(out_dir: &Path, task_id: Hash32) -> Result<Self, WrapperError> {
        let counter = STAGING_COUNTER.fetch_add(1, Ordering::Relaxed);
        let temp_dir = out_dir.join(format!(
            ".nexuscompile-{}-{}-{counter}",
            std::process::id(),
            task_id.0[0]
        ));
        fs::create_dir_all(&temp_dir)?;
        Ok(Self {
            out_dir: out_dir.to_path_buf(),
            temp_dir,
            files: Vec::new(),
        })
    }

    fn stage(&mut self, output: &OutputRef, data: Vec<u8>) -> Result<(), WrapperError> {
        let target = safe_join(&self.out_dir, &output.path)
            .ok_or_else(|| WrapperError::UnsafePath(self.out_dir.join(&output.path)))?;
        let staged = self.temp_dir.join(format!("{:04}", self.files.len()));
        fs::write(&staged, &data)?;
        fs::set_permissions(&staged, fs::Permissions::from_mode(output.mode))?;
        self.files.push((staged, target));
        Ok(())
    }

    fn commit(self) -> Result<(), WrapperError> {
        for (staged, target) in &self.files {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(staged, target)?;
        }
        Ok(())
    }
}

impl Drop for OutputStager {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.temp_dir);
    }
}

fn replay(outcome: CompileOutcome) -> ! {
    let _ = io::stdout().write_all(&outcome.stdout);
    let _ = io::stdout().flush();
    let _ = io::stderr().write_all(&outcome.stderr);
    let _ = io::stderr().flush();
    std::process::exit(outcome.exit_code);
}

fn debug(error: &WrapperError) {
    if env::var_os(DEBUG_ENV).is_some() {
        eprintln!("nexus-rustc: {error}");
    }
}

fn debug_review(error: &ReviewError) {
    if env::var_os(DEBUG_ENV).is_some() {
        eprintln!(
            "nexus-rustc: unsupported: {}: {}",
            error.code, error.message
        );
    }
}

fn exec_rustc(rustc: &OsStr, args: &[String]) -> ! {
    use std::os::unix::process::CommandExt;

    let error = Command::new(rustc).args(args).exec();
    eprintln!("nexus-rustc: failed to exec rustc: {error}");
    std::process::exit(127);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Deref;
    use std::thread;

    static TEST_DIR_ID: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let id = TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("nexuscompile-{tag}-{}-{id}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Deref for TempDir {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn output(path: &str, data: &[u8], mode: u32) -> OutputRef {
        OutputRef {
            path: path.into(),
            hash: hash_bytes(data),
            size: data.len() as u64,
            mode,
        }
    }

    fn serve_hit(
        mut server: UnixStream,
        output: OutputRef,
        data: Vec<u8>,
        expect_ack: bool,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let request: LocalTaskRequest = read_message(&mut server).unwrap();
            assert!(matches!(request.body, LocalTaskRequestBody::Output { .. }));
            write_message(
                &mut server,
                &LocalTaskResponse::new(LocalTaskResponseBody::Chunk {
                    hash: output.hash,
                    offset: 0,
                    data,
                }),
            )
            .unwrap();

            if expect_ack {
                let request: LocalTaskRequest = read_message(&mut server).unwrap();
                assert!(matches!(request.body, LocalTaskRequestBody::Ack { .. }));
                write_message(
                    &mut server,
                    &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
                )
                .unwrap();
            }
        })
    }

    #[test]
    fn task_client_downloads_and_commits_output() {
        let dir = TempDir::new("wrapper-hit");
        let data = b"rlib bytes".to_vec();
        let output = output("libdemo.rlib", &data, 0o644);
        let (client, server) = UnixStream::pair().unwrap();
        let server = serve_hit(server, output.clone(), data.clone(), true);

        let mut client = TaskSocketClient { stream: client };
        client
            .download_outputs(Hash32([1; 32]), &[output], &dir)
            .unwrap();
        client.ack(Hash32([1; 32])).unwrap();
        server.join().unwrap();

        assert_eq!(fs::read(dir.join("libdemo.rlib")).unwrap(), data);
    }

    #[test]
    fn task_client_rejects_unsafe_path_without_partial_commit() {
        let dir = TempDir::new("wrapper-unsafe");
        let data = b"rlib bytes".to_vec();
        let output = output("../escape", &data, 0o644);
        let (client, server) = UnixStream::pair().unwrap();
        let server = serve_hit(server, output.clone(), data, false);

        let mut client = TaskSocketClient { stream: client };
        let error = client
            .download_outputs(Hash32([2; 32]), &[output], &dir)
            .unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, WrapperError::UnsafePath(_)));
        assert!(!dir.join("escape").exists());
    }

    #[test]
    fn task_client_rejects_hash_mismatch() {
        let dir = TempDir::new("wrapper-hash");
        let data = b"rlib bytes".to_vec();
        let output = output("libdemo.rlib", &data, 0o644);
        let (client, server) = UnixStream::pair().unwrap();
        let server = serve_hit(server, output.clone(), vec![b'x'; data.len()], false);

        let mut client = TaskSocketClient { stream: client };
        let error = client
            .download_outputs(Hash32([3; 32]), &[output], &dir)
            .unwrap_err();
        server.join().unwrap();

        assert!(matches!(error, WrapperError::HashMismatch));
    }

    #[test]
    fn task_client_upload_sends_start_and_chunks() {
        let (client, mut server) = UnixStream::pair().unwrap();
        let server = thread::spawn(move || {
            let request: LocalTaskRequest = read_message(&mut server).unwrap();
            assert!(matches!(request.body, LocalTaskRequestBody::Start { .. }));
            write_message(
                &mut server,
                &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
            )
            .unwrap();

            let request: LocalTaskRequest = read_message(&mut server).unwrap();
            assert!(matches!(request.body, LocalTaskRequestBody::Chunk { .. }));
            write_message(
                &mut server,
                &LocalTaskResponse::new(LocalTaskResponseBody::Ack),
            )
            .unwrap();
        });

        let mut client = TaskSocketClient { stream: client };
        client
            .start(
                Hash32([4; 32]),
                Hash32([5; 32]),
                3,
                "toolchain".into(),
                String::new(),
            )
            .unwrap();
        client.upload_bundle(Hash32([4; 32]), b"abc").unwrap();
        server.join().unwrap();
    }

    #[test]
    fn task_client_probe_reports_protocol_error() {
        let (client, mut server) = UnixStream::pair().unwrap();
        let server = thread::spawn(move || {
            let request: LocalTaskRequest = read_message(&mut server).unwrap();
            assert!(matches!(request.body, LocalTaskRequestBody::Probe { .. }));
            write_message(
                &mut server,
                &LocalTaskResponse::error("unavailable", "offline"),
            )
            .unwrap();
        });

        let mut client = TaskSocketClient { stream: client };
        let error = client.probe(Hash32([7; 32])).unwrap_err();
        server.join().unwrap();
        assert!(matches!(error, WrapperError::Protocol(_)));
    }

    #[test]
    fn staging_directories_are_unique() {
        let dir = TempDir::new("wrapper-stage");
        let first = OutputStager::new(&dir, Hash32([6; 32])).unwrap();
        let second = OutputStager::new(&dir, Hash32([6; 32])).unwrap();
        assert_ne!(first.temp_dir, second.temp_dir);
    }
}
