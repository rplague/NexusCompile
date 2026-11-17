pub mod bundle;
pub mod cache;
pub mod cas;
pub mod executor;
pub mod hash;
pub mod manifest;

use std::fmt;

use crate::compile::manifest::ActionManifest;
use crate::compile_protocol::{Hash32, OutputRef};
use crate::util::args;

/// One completed or failed compile task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub action_digest: Hash32,
    pub exit_code: i32,
    pub cache_hit: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration_ms: u64,
    pub outputs: Vec<OutputRef>,
}

/// A classified task failure suitable for protocol mapping.
#[derive(Debug, Clone)]
pub struct TaskError {
    pub code: &'static str,
    pub message: String,
}

impl TaskError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TaskError {}

/// Origin of a task execution, used for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSource {
    Local,
    Provider,
}

impl TaskSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Provider => "provider",
        }
    }
}

/// Parsed identity and version information for one rustc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolchainInfo {
    pub id: String,
    pub release: String,
    pub host: String,
}

/// Toolchain identity required by a task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolchainRequirement {
    pub id: String,
    pub target: String,
}

/// Immutable task requirements used by local and remote placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRequirements {
    pub task_id: Hash32,
    pub bundle_hash: Hash32,
    pub toolchain: ToolchainRequirement,
    pub slots: u32,
}

/// Why a rustc invocation or task cannot be admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewError {
    pub code: &'static str,
    pub message: String,
}

impl ReviewError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Check whether this rustc invocation is supported by the task pipeline.
pub fn review_invocation(
    args: &[String],
    cargo_incremental: Option<&str>,
) -> Result<(), ReviewError> {
    if cargo_incremental != Some("0") {
        return Err(ReviewError::new(
            "unsupported",
            "CARGO_INCREMENTAL must be 0",
        ));
    }

    let crate_name = args::value(args, "--crate-name").unwrap_or_default();
    if crate_name == "build_script_build" {
        return Err(ReviewError::new(
            "unsupported",
            "build scripts stay local-direct",
        ));
    }

    let crate_type = args::value(args, "--crate-type")
        .ok_or_else(|| ReviewError::new("unsupported", "missing --crate-type"))?;
    for kind in crate_type.split(',') {
        if !matches!(kind.trim(), "lib" | "rlib") {
            return Err(ReviewError::new(
                "unsupported",
                format!("unsupported crate type: {kind}"),
            ));
        }
    }

    if !args::value(args, "--emit")
        .map(|emit| emit.split(',').any(|kind| kind.trim() == "link"))
        .unwrap_or(false)
    {
        return Err(ReviewError::new("unsupported", "missing link in --emit"));
    }

    if args::value(args, "--out-dir").is_none() {
        return Err(ReviewError::new("unsupported", "missing --out-dir"));
    }

    if args.iter().any(|arg| arg == "--test" || arg == "--print") {
        return Err(ReviewError::new(
            "unsupported",
            "test/print invocations stay local-direct",
        ));
    }

    for (index, arg) in args.iter().enumerate() {
        if arg == "-C" {
            if let Some(next) = args.get(index + 1)
                && next.contains("incremental")
            {
                return Err(ReviewError::new(
                    "unsupported",
                    "incremental compilation is not supported",
                ));
            }
        } else if arg.starts_with("-C") && arg.contains("incremental") {
            return Err(ReviewError::new(
                "unsupported",
                "incremental compilation is not supported",
            ));
        }
    }

    Ok(())
}

/// Build the immutable requirements for a fully decoded task manifest.
pub fn review_task(
    manifest: &ActionManifest,
    bundle_hash: Hash32,
) -> Result<TaskRequirements, ReviewError> {
    review_invocation(
        &manifest.args,
        manifest.env.get("CARGO_INCREMENTAL").map(String::as_str),
    )?;
    if manifest.toolchain_id.trim().is_empty() {
        return Err(ReviewError::new(
            "toolchain_required",
            "manifest has no toolchain id",
        ));
    }
    Ok(TaskRequirements {
        task_id: manifest
            .digest()
            .map_err(|error| ReviewError::new("manifest_error", error.to_string()))?,
        bundle_hash,
        toolchain: ToolchainRequirement {
            id: manifest.toolchain_id.clone(),
            target: manifest.target.clone(),
        },
        slots: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(review_invocation(&base_args(), Some("0")).is_ok());
    }

    #[test]
    fn rejects_unsupported_invocations() {
        assert!(review_invocation(&base_args(), None).is_err());
        assert!(review_invocation(&base_args(), Some("1")).is_err());

        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "lib").unwrap();
        args[index] = "bin".into();
        assert!(review_invocation(&args, Some("0")).is_err());

        let mut args = base_args();
        args.retain(|arg| arg != "--out-dir" && arg != "/tmp/out");
        assert!(review_invocation(&args, Some("0")).is_err());

        let mut args = base_args();
        args.retain(|arg| arg != "--emit=dep-info,metadata,link");
        args.push("--emit=metadata".into());
        assert!(review_invocation(&args, Some("0")).is_err());

        let mut args = base_args();
        let index = args.iter().position(|arg| arg == "demo").unwrap();
        args[index] = "build_script_build".into();
        assert!(review_invocation(&args, Some("0")).is_err());

        let mut args = base_args();
        args.push("--test".into());
        assert!(review_invocation(&args, Some("0")).is_err());
    }
}
