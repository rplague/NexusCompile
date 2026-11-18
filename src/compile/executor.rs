//! Local rustc execution for supported lib-crate invocations.

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

use walkdir::WalkDir;

use crate::compile::bundle::TaskJob;
use crate::compile::cache::ResultCache;
use crate::compile::hash::hex_hash;
use crate::compile::{TaskError, TaskResult, TaskSource};
use crate::log::{LogLevel, LogStruct, log_compile_failure};
use crate::util::args;
use crate::util::process;
use crate::workspace::Workspace;

/// Execute one validated, materialized task.
pub fn execute_job(
    job: &TaskJob,
    rustc: &str,
    cache: &ResultCache,
    workspace: &Workspace,
    source: TaskSource,
) -> Result<TaskResult, TaskError> {
    let digest_prefix = &hex_hash(&job.task_id)[..8];
    let prepared_job = job.prepare()?;
    let task = &prepared_job.prepared;

    let staging = workspace
        .staging_dir(&job.task_id)
        .map_err(|error| TaskError::new("cache_error", error.to_string()))?;
    let staging_out = staging.join("out");
    if let Err(error) = fs::create_dir_all(&staging_out) {
        let _ = fs::remove_dir_all(&staging);
        return Err(TaskError::new("cache_error", error.to_string()));
    }

    let exec_args = match rewrite_out_dir(&task.args, &staging_out) {
        Some(args) => args,
        None => {
            let _ = fs::remove_dir_all(&staging);
            return Err(TaskError::new("unsupported", "unable to rewrite --out-dir"));
        }
    };

    let started = Instant::now();
    let topic = match source {
        TaskSource::Local => "本地编译",
        TaskSource::Provider => "远端编译",
    };
    LogStruct::new(
        LogLevel::Debug,
        topic,
        format!(
            "action={digest_prefix}, crate={}",
            args::value(&task.args, "--crate-name").unwrap_or_default()
        ),
    )
    .emit();

    let output = Command::new(rustc)
        .args(&exec_args)
        .current_dir(&task.cwd)
        .envs(&task.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(output) => output,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(TaskError::new("spawn_error", error.to_string()));
        }
    };
    let exit_code = process::exit_code(output.status);
    let duration_ms = started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);

    if exit_code != 0 {
        log_compile_failure(
            source.as_str(),
            &hex_hash(&job.task_id),
            &prepared_job.manifest.crate_name,
            rustc,
            &task.cwd.to_string_lossy(),
            exit_code,
            duration_ms,
            &task.args,
            &task.env,
            &output.stdout,
            &output.stderr,
            Some(&prepared_job.manifest),
            Some(&job.bundle_hash),
            Some(job.bundle_size),
            Some(prepared_job.file_count),
        );
        let _ = fs::remove_dir_all(&staging);
        return Ok(TaskResult {
            action_digest: job.task_id,
            exit_code,
            cache_hit: false,
            stdout: truncate_diagnostic(output.stdout),
            stderr: truncate_diagnostic(output.stderr),
            duration_ms,
            outputs: Vec::new(),
        });
    }

    if let Err(error) = rewrite_dep_info_files(&staging_out, &task.out_dir) {
        let _ = fs::remove_dir_all(&staging);
        return Err(TaskError::new("output_error", error.to_string()));
    }

    let outputs = match cache.collect_outputs(&staging_out) {
        Ok(outputs) if !outputs.is_empty() => outputs,
        Ok(_) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(TaskError::new("output_error", "rustc produced no outputs"));
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return Err(TaskError::new("output_error", error.to_string()));
        }
    };

    if let Err(error) = cache.materialize_outputs(&outputs, &task.out_dir) {
        let _ = fs::remove_dir_all(&staging);
        return Err(TaskError::new("output_error", error.to_string()));
    }

    let _ = cache.store(job.task_id, outputs.clone());
    let _ = fs::remove_dir_all(&staging);

    Ok(TaskResult {
        action_digest: job.task_id,
        exit_code: 0,
        cache_hit: false,
        stdout: truncate_diagnostic(output.stdout),
        stderr: truncate_diagnostic(output.stderr),
        duration_ms,
        outputs,
    })
}

pub fn truncate_diagnostic(mut bytes: Vec<u8>) -> Vec<u8> {
    const MAX_DIAGNOSTIC: usize = 6 << 20;
    if bytes.len() <= MAX_DIAGNOSTIC {
        return bytes;
    }
    bytes.truncate(MAX_DIAGNOSTIC);
    bytes.extend_from_slice(b"\n[nexus-compile: output truncated]\n");
    bytes
}

fn rewrite_out_dir(args: &[String], staging_out: &Path) -> Option<Vec<String>> {
    let mut result = Vec::with_capacity(args.len());
    let mut index = 0;
    let mut found = false;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--out-dir" {
            let value = args.get(index + 1)?;
            let _ = value;
            result.push(arg.clone());
            result.push(staging_out.display().to_string());
            found = true;
            index += 2;
            continue;
        }
        if let Some(_value) = arg.strip_prefix("--out-dir=") {
            result.push(format!("--out-dir={}", staging_out.display()));
            found = true;
        } else {
            result.push(arg.clone());
        }
        index += 1;
    }
    found.then_some(result)
}

fn rewrite_dep_info_files(staging_out: &Path, out_dir: &Path) -> std::io::Result<()> {
    let staging = staging_out.to_string_lossy().into_owned();
    let real = out_dir.to_string_lossy().into_owned();
    if staging == real {
        return Ok(());
    }

    for entry in WalkDir::new(staging_out).follow_links(false) {
        let entry = entry.map_err(std::io::Error::other)?;
        if !entry.file_type().is_file()
            || entry.path().extension().and_then(|ext| ext.to_str()) != Some("d")
        {
            continue;
        }
        let bytes = fs::read(entry.path())?;
        if !bytes
            .windows(staging.len())
            .any(|window| window == staging.as_bytes())
        {
            continue;
        }
        let text = String::from_utf8_lossy(&bytes).replace(&staging, &real);
        fs::write(entry.path(), text)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_out_dir_in_both_forms() {
        let staging = Path::new("/tmp/staging/out");
        let args = vec!["--out-dir".into(), "/tmp/real".into()];
        assert_eq!(
            rewrite_out_dir(&args, staging).unwrap(),
            vec!["--out-dir", "/tmp/staging/out"],
            "case: out dir split"
        );

        let args = vec!["--out-dir=/tmp/real".into()];
        assert_eq!(
            rewrite_out_dir(&args, staging).unwrap(),
            vec!["--out-dir=/tmp/staging/out"],
            "case: out dir joined"
        );
    }
}
