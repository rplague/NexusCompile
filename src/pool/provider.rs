//! Provider-side remote task execution.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use crate::compile::bundle::{decode_bundle, materialize_bundle};
use crate::compile::cache::ResultCache;
use crate::compile::hash::hash_file;
use crate::compile_protocol::{ErrorCode, Hash32, TaskId};
use crate::config::ServiceConfig;
use crate::local_protocol::truncate_diagnostic;
use crate::state::NodeState;
use crate::util::process;

pub fn spawn_task(
    state: Arc<Mutex<NodeState>>,
    cache: Arc<ResultCache>,
    config: ServiceConfig,
    task_id: TaskId,
    bundle_hash: Hash32,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        run_task_blocking(state, cache, config, task_id, bundle_hash);
    })
}

fn run_task_blocking(
    state: Arc<Mutex<NodeState>>,
    cache: Arc<ResultCache>,
    config: ServiceConfig,
    task_id: TaskId,
    bundle_hash: Hash32,
) {
    let task_dir = cache.task_dir(&task_id.0);
    let bundle_path = task_dir.join("bundle");
    let fail = |state: &Arc<Mutex<NodeState>>, code, message: String, stdout, stderr| {
        if let Ok(mut state) = state.lock() {
            state.fail_task_with_logs(task_id, code, message, stdout, stderr);
        }
        let _ = fs::remove_dir_all(&task_dir);
    };

    let actual = match hash_file(&bundle_path) {
        Ok((hash, _)) => hash,
        Err(error) => {
            fail(
                &state,
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
            &state,
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
                &state,
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
            fail(&state, ErrorCode::Internal, error, Vec::new(), Vec::new());
            return;
        }
    };
    let digest = match bundle.manifest.digest() {
        Ok(digest) => digest,
        Err(error) => {
            fail(
                &state,
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
            &state,
            ErrorCode::Internal,
            "manifest digest does not match task id".into(),
            Vec::new(),
            Vec::new(),
        );
        return;
    }

    let work = task_dir.join("work");
    let work_pkg = work.join("pkg");
    let work_out = work.join("out");
    let work_extern = work.join("extern");
    if let Err(error) = fs::create_dir_all(&work)
        .and_then(|_| materialize_bundle(&bundle, &work))
        .and_then(|_| fs::create_dir_all(&work_out))
        .and_then(|_| fs::create_dir_all(&work_extern))
    {
        fail(
            &state,
            ErrorCode::Internal,
            format!("materialize task: {error}"),
            Vec::new(),
            Vec::new(),
        );
        return;
    }

    let cwd = substitute(&bundle.manifest.cwd, &work);
    let cwd = if Path::new(&cwd).starts_with(&work) {
        PathBuf::from(cwd)
    } else {
        work_pkg.clone()
    };
    let args: Vec<String> = bundle
        .manifest
        .args
        .iter()
        .map(|arg| substitute(arg, &work))
        .collect();
    let mut env: HashMap<String, String> = bundle
        .manifest
        .env
        .iter()
        .map(|(key, value)| (key.clone(), substitute(value, &work)))
        .collect();
    env.insert("CARGO_MANIFEST_DIR".into(), work_pkg.display().to_string());
    env.insert("OUT_DIR".into(), work_out.display().to_string());
    env.insert("CARGO_TARGET_DIR".into(), work.display().to_string());

    let output = Command::new(&config.node.rustc)
        .args(&args)
        .current_dir(&cwd)
        .envs(&env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(output) => output,
        Err(error) => {
            fail(
                &state,
                ErrorCode::Internal,
                format!("spawn rustc: {error}"),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
    };

    if !output.status.success() {
        fail(
            &state,
            ErrorCode::Compile,
            format!("rustc exited with {}", process::exit_code(output.status)),
            truncate_diagnostic(output.stdout),
            truncate_diagnostic(output.stderr),
        );
        return;
    }

    let outputs = match cache.collect_outputs(&work_out) {
        Ok(outputs) if !outputs.is_empty() => outputs,
        Ok(_) => {
            fail(
                &state,
                ErrorCode::Internal,
                "rustc produced no outputs".into(),
                Vec::new(),
                Vec::new(),
            );
            return;
        }
        Err(error) => {
            fail(
                &state,
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
    if let Ok(mut state) = state.lock() {
        state.complete_task(task_id, outputs);
    }
}

fn substitute(value: &str, work: &Path) -> String {
    value
        .replace("<MANIFEST>", &work.join("pkg").display().to_string())
        .replace("<OUT>", &work.join("out").display().to_string())
        .replace("<EXTERN>", &work.join("extern").display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_placeholders() {
        let work = Path::new("/work");
        assert_eq!(
            substitute("<MANIFEST>/src/lib.rs", work),
            "/work/pkg/src/lib.rs"
        );
        assert_eq!(substitute("--out-dir=<OUT>", work), "--out-dir=/work/out");
        assert_eq!(substitute("<EXTERN>/dep", work), "/work/extern/dep");
    }
}
