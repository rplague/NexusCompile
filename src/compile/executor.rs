//! Local rustc execution for supported lib-crate invocations.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;

use walkdir::WalkDir;

use crate::compile::cache::ResultCache;
use crate::compile::hash::hex_hash;
use crate::compile::manifest::collect_manifest;
use crate::local_protocol::{
    LOCAL_PROTOCOL_VERSION, LocalBody, LocalRequest, LocalResponse, truncate_diagnostic,
};
use crate::log::{LogLevel, LogStruct, log_compile_failure};
use crate::util::args;
use crate::util::path::resolve_path;
use crate::util::process;
use crate::workspace::Workspace;

pub fn execute_local(
    request: &LocalRequest,
    cache: &ResultCache,
    workspace: &Workspace,
) -> LocalResponse {
    if request.v != LOCAL_PROTOCOL_VERSION {
        return LocalResponse::error(
            "unsupported_version",
            format!("unsupported local protocol version {}", request.v),
        );
    }

    let env: HashMap<String, String> = request.env.iter().cloned().collect();
    if let Err(reason) = is_supported(&request.args, &env) {
        return LocalResponse::error("unsupported", reason);
    }

    let cwd = Path::new(&request.cwd);
    let rustc = Path::new(&request.rustc);
    let out_dir = match args::value(&request.args, "--out-dir") {
        Some(value) => resolve_path(cwd, &value),
        None => return LocalResponse::error("unsupported", "missing --out-dir"),
    };

    let manifest = match collect_manifest(cwd, rustc, &request.args, &env) {
        Ok(manifest) => manifest,
        Err(error) => return LocalResponse::error("manifest_error", error.to_string()),
    };
    let action_digest = match manifest.digest() {
        Ok(digest) => digest,
        Err(error) => return LocalResponse::error("manifest_error", error.to_string()),
    };

    let digest_prefix = &hex_hash(&action_digest)[..8];
    if cache
        .lookup_and_materialize(&action_digest, &out_dir)
        .is_some()
    {
        LogStruct::new(
            LogLevel::Debug,
            "缓存命中",
            format!("action={digest_prefix}"),
        )
        .emit();
        return LocalResponse::new(LocalBody::Compiled {
            action_digest,
            exit_code: 0,
            cache_hit: true,
            stdout: Vec::new(),
            stderr: Vec::new(),
            duration_ms: 0,
        });
    }

    let staging = match workspace.staging_dir(&action_digest) {
        Ok(staging) => staging,
        Err(error) => return LocalResponse::error("cache_error", error.to_string()),
    };
    let staging_out = staging.join("out");
    if let Err(error) = fs::create_dir_all(&staging_out) {
        let _ = fs::remove_dir_all(&staging);
        return LocalResponse::error("cache_error", error.to_string());
    }

    let exec_args = match rewrite_out_dir(&request.args, &staging_out) {
        Some(args) => args,
        None => {
            let _ = fs::remove_dir_all(&staging);
            return LocalResponse::error("unsupported", "unable to rewrite --out-dir");
        }
    };

    let started = Instant::now();
    LogStruct::new(
        LogLevel::Debug,
        "本地编译",
        format!(
            "action={digest_prefix}, crate={}",
            args::value(&request.args, "--crate-name").unwrap_or_default()
        ),
    )
    .emit();

    let output = Command::new(&request.rustc)
        .args(&exec_args)
        .current_dir(cwd)
        .envs(&env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = match output {
        Ok(output) => output,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return LocalResponse::error("spawn_error", error.to_string());
        }
    };
    let exit_code = process::exit_code(output.status);

    if exit_code != 0 {
        log_compile_failure(
            "local",
            &hex_hash(&action_digest),
            &args::value(&request.args, "--crate-name").unwrap_or_default(),
            &request.rustc,
            &request.cwd,
            exit_code,
            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            &request.args,
            &env,
            &output.stdout,
            &output.stderr,
            Some(&manifest),
            None,
            None,
            None,
        );
        let _ = fs::remove_dir_all(&staging);
        return LocalResponse::new(LocalBody::Compiled {
            action_digest,
            exit_code,
            cache_hit: false,
            stdout: truncate_diagnostic(output.stdout),
            stderr: truncate_diagnostic(output.stderr),
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        });
    }

    if let Err(error) = rewrite_dep_info_files(&staging_out, &out_dir) {
        let _ = fs::remove_dir_all(&staging);
        return LocalResponse::error("output_error", error.to_string());
    }

    let outputs = match cache.collect_outputs(&staging_out) {
        Ok(outputs) => outputs,
        Err(error) => {
            let _ = fs::remove_dir_all(&staging);
            return LocalResponse::error("output_error", error.to_string());
        }
    };

    if outputs.is_empty() {
        let _ = fs::remove_dir_all(&staging);
        return LocalResponse::error("output_error", "rustc produced no outputs");
    }

    if let Err(error) = cache.materialize_outputs(&outputs, &out_dir) {
        let _ = fs::remove_dir_all(&staging);
        return LocalResponse::error("output_error", error.to_string());
    }

    let _ = cache.store(action_digest, outputs);
    let _ = fs::remove_dir_all(&staging);

    LocalResponse::new(LocalBody::Compiled {
        action_digest,
        exit_code: 0,
        cache_hit: false,
        stdout: truncate_diagnostic(output.stdout),
        stderr: truncate_diagnostic(output.stderr),
        duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
    })
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
    fn rejects_incremental_and_unsupported_crate_types() {
        let mut env = base_env();
        env.insert("CARGO_INCREMENTAL".into(), "1".into());
        assert!(is_supported(&base_args(), &env).is_err());

        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "lib").unwrap();
        args[index] = "bin".into();
        assert!(is_supported(&args, &base_env()).is_err());
    }

    #[test]
    fn rejects_missing_link_or_out_dir() {
        let mut args = base_args();
        args.retain(|arg| arg != "--out-dir" && arg != "/tmp/out");
        assert!(is_supported(&args, &base_env()).is_err());

        let mut args = base_args();
        args.retain(|arg| arg != "--emit=dep-info,metadata,link");
        args.push("--emit=metadata".into());
        assert!(is_supported(&args, &base_env()).is_err());
    }

    #[test]
    fn rejects_build_scripts_and_test_invocations() {
        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "demo").unwrap();
        args[index] = "build_script_build".into();
        assert!(is_supported(&args, &base_env()).is_err());

        let mut args = base_args();
        args.push("--test".into());
        assert!(is_supported(&args, &base_env()).is_err());
    }

    #[test]
    fn rewrites_both_out_dir_forms() {
        let staging = Path::new("/tmp/staging/out");
        let args = vec!["--out-dir".into(), "/tmp/real".into()];
        assert_eq!(
            rewrite_out_dir(&args, staging).unwrap(),
            vec!["--out-dir", "/tmp/staging/out"]
        );

        let args = vec!["--out-dir=/tmp/real".into()];
        assert_eq!(
            rewrite_out_dir(&args, staging).unwrap(),
            vec!["--out-dir=/tmp/staging/out"]
        );
    }
}
