//! Local rustc execution for supported lib-crate invocations.

use std::collections::HashMap;
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
    if let Err(reason) = is_supported(&job.prepared.args, &job.prepared.env) {
        return Err(TaskError::new("unsupported", reason));
    }

    let digest_prefix = &hex_hash(&job.task_id)[..8];
    if let Some(cached) = cache.lookup_and_materialize(&job.task_id, &job.prepared.out_dir) {
        LogStruct::new(
            LogLevel::Debug,
            "缓存命中",
            format!("action={digest_prefix}"),
        )
        .emit();
        return Ok(TaskResult {
            action_digest: job.task_id,
            exit_code: 0,
            cache_hit: true,
            stdout: Vec::new(),
            stderr: Vec::new(),
            duration_ms: 0,
            outputs: cached.outputs,
        });
    }

    let staging = workspace
        .staging_dir(&job.task_id)
        .map_err(|error| TaskError::new("cache_error", error.to_string()))?;
    let staging_out = staging.join("out");
    if let Err(error) = fs::create_dir_all(&staging_out) {
        let _ = fs::remove_dir_all(&staging);
        return Err(TaskError::new("cache_error", error.to_string()));
    }

    let exec_args = match rewrite_out_dir(&job.prepared.args, &staging_out) {
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
            args::value(&job.prepared.args, "--crate-name").unwrap_or_default()
        ),
    )
    .emit();

    let output = Command::new(rustc)
        .args(&exec_args)
        .current_dir(&job.prepared.cwd)
        .envs(&job.prepared.env)
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
            &job.manifest.crate_name,
            rustc,
            &job.prepared.cwd.to_string_lossy(),
            exit_code,
            duration_ms,
            &job.prepared.args,
            &job.prepared.env,
            &output.stdout,
            &output.stderr,
            Some(&job.manifest),
            Some(&job.bundle_hash),
            Some(job.bundle_size),
            Some(job.bundle.files.len()),
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

    if let Err(error) = rewrite_dep_info_files(&staging_out, &job.prepared.out_dir) {
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

    if let Err(error) = cache.materialize_outputs(&outputs, &job.prepared.out_dir) {
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

/// Check whether this rustc invocation is supported by the local executor.
pub fn is_supported(args: &[String], env: &HashMap<String, String>) -> Result<(), String> {
    if env.get("CARGO_INCREMENTAL").map(String::as_str) != Some("0") {
        return Err("CARGO_INCREMENTAL must be 0".into());
    }

    let crate_name = args::value(args, "--crate-name").unwrap_or_default();
    if crate_name == "build_script_build" {
        return Err("build scripts stay local-direct".into());
    }

    let crate_type =
        args::value(args, "--crate-type").ok_or_else(|| "missing --crate-type".to_string())?;
    for kind in crate_type.split(',') {
        if !matches!(kind.trim(), "lib" | "rlib") {
            return Err(format!("unsupported crate type: {kind}"));
        }
    }

    if !args::value(args, "--emit")
        .map(|emit| emit.split(',').any(|kind| kind.trim() == "link"))
        .unwrap_or(false)
    {
        return Err("missing link in --emit".into());
    }

    if args::value(args, "--out-dir").is_none() {
        return Err("missing --out-dir".into());
    }

    if args.iter().any(|arg| arg == "--test" || arg == "--print") {
        return Err("test/print invocations stay local-direct".into());
    }

    for (index, arg) in args.iter().enumerate() {
        if arg == "-C" {
            if let Some(next) = args.get(index + 1)
                && next.contains("incremental")
            {
                return Err("incremental compilation is not supported".into());
            }
        } else if arg.starts_with("-C") && arg.contains("incremental") {
            return Err("incremental compilation is not supported".into());
        }
    }

    Ok(())
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

    fn base_env() -> HashMap<String, String> {
        HashMap::from([("CARGO_INCREMENTAL".into(), "0".into())])
    }

    fn base_args() -> Vec<String> {
        vec![
            "--crate-name".into(),
            "demo".into(),
            "--crate-type".into(),
            "lib".into(),
            "--out-dir".into(),
            "/tmp/out".into(),
            "--emit=dep-info,metadata,link".into(),
        ]
    }

    #[test]
    fn accepts_supported_lib_invocation() {
        assert!(is_supported(&base_args(), &base_env()).is_ok());
    }

    #[test]
    fn rejects_unsupported_invocations_and_rewrites_out_dir() {
        let mut env = base_env();
        env.insert("CARGO_INCREMENTAL".into(), "1".into());
        assert!(
            is_supported(&base_args(), &env).is_err(),
            "case: incremental"
        );

        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "lib").unwrap();
        args[index] = "bin".into();
        assert!(
            is_supported(&args, &base_env()).is_err(),
            "case: crate type"
        );

        let mut args = base_args();
        args.retain(|arg| arg != "--out-dir" && arg != "/tmp/out");
        assert!(is_supported(&args, &base_env()).is_err(), "case: out dir");

        let mut args = base_args();
        args.retain(|arg| arg != "--emit=dep-info,metadata,link");
        args.push("--emit=metadata".into());
        assert!(is_supported(&args, &base_env()).is_err(), "case: emit");

        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "demo").unwrap();
        args[index] = "build_script_build".into();
        assert!(
            is_supported(&args, &base_env()).is_err(),
            "case: build script"
        );

        let mut args = base_args();
        args.push("--test".into());
        assert!(
            is_supported(&args, &base_env()).is_err(),
            "case: test invocation"
        );

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
