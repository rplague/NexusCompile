//! Transfer bundle for one remote compile task.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::compile::hash::{hash_file, hex_hash};
use crate::compile::manifest::ActionManifest;
use crate::compile::{TaskError, TaskRequirements, ToolchainRequirement, review_task};
use crate::compile_protocol::Hash32;
use crate::log::{LogLevel, LogStruct};
use crate::util::args;
use crate::util::fs::{file_mode, set_mode};
use crate::util::path::{resolve_path, safe_relative};

pub const BUNDLE_SCHEMA: u16 = 3;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleFile {
    pub path: String,
    pub mode: u32,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskBundle {
    pub schema: u16,
    pub manifest: ActionManifest,
    pub files: Vec<BundleFile>,
}

pub fn build_bundle(
    cwd: &Path,
    args: &[String],
    env: &HashMap<String, String>,
    manifest: &ActionManifest,
) -> Result<Vec<u8>, String> {
    let manifest_dir = env
        .get("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| cwd.to_path_buf());
    let out_dir = env
        .get("OUT_DIR")
        .cloned()
        .or_else(|| args::value(args, "--out-dir"))
        .ok_or_else(|| "missing --out-dir / OUT_DIR".to_string())?;
    let out_dir = resolve_path(cwd, &out_dir);

    let externs = extern_paths(cwd, args);
    let deps = link_paths(cwd, args);
    let source = args
        .iter()
        .rev()
        .find(|arg| !arg.starts_with('-') && resolve_path(cwd, arg).is_file())
        .map(|arg| resolve_path(cwd, arg));

    let mut files = Vec::with_capacity(manifest.inputs.len());
    for input in &manifest.inputs {
        let path = if let Some(rest) = input.path.strip_prefix("pkg/") {
            manifest_dir.join(rest)
        } else if let Some(rest) = input.path.strip_prefix("out/") {
            out_dir.join(rest)
        } else if let Some(rest) = input.path.strip_prefix("deps/") {
            let (kind, name) = rest
                .split_once('/')
                .ok_or_else(|| format!("invalid dependency path {}", input.path))?;
            deps.iter()
                .find(|(dep_kind, _)| dep_kind == kind)
                .map(|(_, path)| path.join(name))
                .ok_or_else(|| format!("missing dependency path {kind}"))?
        } else if let Some(name) = input.path.strip_prefix("extern/") {
            externs
                .get(name)
                .cloned()
                .ok_or_else(|| format!("missing extern {name}"))?
        } else if let Some(name) = input.path.strip_prefix("source/") {
            source
                .as_ref()
                .filter(|path| {
                    path.file_name()
                        .map(|file| file.to_string_lossy() == name)
                        .unwrap_or(false)
                })
                .cloned()
                .ok_or_else(|| format!("missing source {name}"))?
        } else {
            return Err(format!("unknown input path {}", input.path));
        };

        let data = fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let digest = crate::compile::hash::hash_bytes(&data);
        crate::compile::manifest::scan_log(
            "bundle",
            &path,
            &input.path,
            &digest,
            data.len() as u64,
        );
        let bundle_path = if input.path.starts_with("extern/") {
            path.file_name()
                .map(|name| format!("extern/{}", name.to_string_lossy()))
                .unwrap_or_else(|| input.path.clone())
        } else {
            input.path.clone()
        };
        files.push(BundleFile {
            path: bundle_path,
            mode: file_mode(&path),
            data,
        });
    }

    let bundle = TaskBundle {
        schema: BUNDLE_SCHEMA,
        manifest: manifest.clone(),
        files,
    };
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(&bundle, &mut bytes)
        .map_err(|e| format!("encode task bundle: {e}"))?;
    Ok(bytes)
}

pub fn decode_bundle(bytes: &[u8]) -> Result<TaskBundle, String> {
    let bundle: TaskBundle =
        ciborium::de::from_reader(bytes).map_err(|e| format!("decode task bundle: {e}"))?;
    if bundle.schema != BUNDLE_SCHEMA {
        return Err(format!("unsupported bundle schema {}", bundle.schema));
    }
    Ok(bundle)
}

pub fn materialize_bundle(bundle: &TaskBundle, root: &Path) -> io::Result<()> {
    fs::create_dir_all(root.join("deps/dependency"))?;
    fs::create_dir_all(root.join("deps/native"))?;
    for file in &bundle.files {
        let relative = safe_relative(&file.path)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unsafe bundle path"))?;
        let target = root.join(relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&target, &file.data)?;
        set_mode(&target, file.mode)?;
    }
    Ok(())
}

/// Rewrite `--extern` paths to the actual file names materialized from a bundle.
#[derive(Debug)]
pub struct PreparedTask {
    pub cwd: PathBuf,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub out_dir: PathBuf,
}

/// A task that has been accepted into the queue but not yet decoded.
#[derive(Debug)]
pub struct TaskJob {
    pub task_id: Hash32,
    pub bundle_hash: Hash32,
    pub bundle_size: u64,
    pub bundle_bytes: Vec<u8>,
    pub work_dir: PathBuf,
    pub requirements: TaskRequirements,
}

/// A decoded task, materialized into its work directory.
#[derive(Debug)]
pub struct PreparedJob {
    pub manifest: ActionManifest,
    pub prepared: PreparedTask,
    pub file_count: usize,
}

impl TaskJob {
    pub fn from_bundle_file(
        bundle_path: &Path,
        task_id: Hash32,
        bundle_hash: Hash32,
        bundle_len: u64,
        work_dir: PathBuf,
        toolchain_id: String,
        target: String,
    ) -> Result<Self, TaskError> {
        let metadata = fs::metadata(bundle_path)
            .map_err(|error| TaskError::new("bundle_error", format!("read bundle: {error}")))?;
        if metadata.len() != bundle_len {
            return Err(TaskError::new("bundle_error", "bundle size mismatch"));
        }
        let (actual_hash, _) = hash_file(bundle_path)
            .map_err(|error| TaskError::new("bundle_error", format!("hash bundle: {error}")))?;
        if actual_hash != bundle_hash {
            return Err(TaskError::new("bundle_error", "bundle hash mismatch"));
        }
        let bundle_bytes = fs::read(bundle_path)
            .map_err(|error| TaskError::new("bundle_error", format!("read bundle: {error}")))?;

        Ok(Self {
            task_id,
            bundle_hash,
            bundle_size: bundle_len,
            bundle_bytes,
            work_dir,
            requirements: TaskRequirements {
                task_id,
                bundle_hash,
                toolchain: ToolchainRequirement {
                    id: toolchain_id,
                    target,
                },
                slots: 1,
            },
        })
    }

    /// Decode, validate, and materialize the bundle. Called once a slot is assigned.
    pub fn prepare(&self) -> Result<PreparedJob, TaskError> {
        let bundle = decode_bundle(&self.bundle_bytes)
            .map_err(|error| TaskError::new("bundle_error", error))?;
        let digest = bundle
            .manifest
            .digest()
            .map_err(|error| TaskError::new("manifest_error", error.to_string()))?;
        LogStruct::new(
            LogLevel::Debug,
            "清单",
            format!(
                "digest={} manifest={}",
                hex_hash(&digest),
                serde_json::to_string(&bundle.manifest).unwrap_or_default()
            ),
        )
        .emit();
        if digest != self.task_id {
            return Err(TaskError::new("manifest_error", "manifest digest mismatch"));
        }
        review_task(&bundle.manifest, self.bundle_hash)
            .map_err(|error| TaskError::new(error.code, error.message))?;

        let work_out = self.work_dir.join("out");
        let work_extern = self.work_dir.join("extern");
        fs::create_dir_all(&self.work_dir)
            .and_then(|_| materialize_bundle(&bundle, &self.work_dir))
            .and_then(|_| fs::create_dir_all(&work_out))
            .and_then(|_| fs::create_dir_all(&work_extern))
            .and_then(|_| fs::create_dir_all(self.work_dir.join("target")))
            .map_err(|error| {
                TaskError::new("bundle_error", format!("materialize bundle: {error}"))
            })?;
        let prepared = prepare_task(&bundle, &self.work_dir)
            .map_err(|error| TaskError::new("bundle_error", error))?;

        let file_count = bundle.files.len();
        Ok(PreparedJob {
            manifest: bundle.manifest,
            prepared,
            file_count,
        })
    }
}

pub fn prepare_task(bundle: &TaskBundle, work: &Path) -> Result<PreparedTask, String> {
    let work_pkg = work.join("pkg");
    let work_out = work.join("out");
    let work_extern = work.join("extern");
    let candidate = substitute(&bundle.manifest.cwd, work);
    let cwd = if (bundle.manifest.cwd == "<MANIFEST>"
        || bundle.manifest.cwd.starts_with("<MANIFEST>/"))
        && Path::new(&candidate).is_dir()
    {
        PathBuf::from(candidate)
    } else {
        work_pkg.clone()
    };

    let mut args: Vec<String> = bundle
        .manifest
        .args
        .iter()
        .map(|arg| substitute(arg, work))
        .collect();
    normalize_extern_args(&mut args, &work_extern);

    let mut env: HashMap<String, String> = bundle
        .manifest
        .env
        .iter()
        .map(|(key, value)| (key.clone(), substitute(value, work)))
        .collect();
    env.insert("CARGO_MANIFEST_DIR".into(), work_pkg.display().to_string());
    env.insert("OUT_DIR".into(), work_out.display().to_string());
    env.insert("CARGO_TARGET_DIR".into(), work.display().to_string());

    let out_dir = args::value(&args, "--out-dir")
        .map(|value| resolve_path(&cwd, &value))
        .ok_or_else(|| "missing --out-dir".to_string())?;
    if !out_dir.is_dir() {
        return Err(format!("out-dir is missing: {}", out_dir.display()));
    }
    if !cwd.is_dir() {
        return Err(format!("cwd is missing: {}", cwd.display()));
    }
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "-L" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("-L")
        };
        if let Some(value) = value
            && let Some((_, path)) = value.split_once('=')
            && !resolve_path(&cwd, path).is_dir()
        {
            return Err(format!("dependency path is missing: {path}"));
        }
        index += 1;
    }
    if !args
        .iter()
        .rev()
        .any(|arg| !arg.starts_with('-') && resolve_path(&cwd, arg).is_file())
    {
        return Err("source file is missing".into());
    }
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "--extern" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("--extern=")
        };
        if let Some(value) = value
            && let Some((_, path)) = value.split_once('=')
            && !resolve_path(&cwd, path).is_file()
        {
            return Err(format!("extern file is missing: {path}"));
        }
        index += 1;
    }

    Ok(PreparedTask {
        cwd,
        args,
        env,
        out_dir,
    })
}

pub fn substitute(value: &str, work: &Path) -> String {
    value
        .replace("<MANIFEST>", &work.join("pkg").display().to_string())
        .replace("<OUT>", &work.join("out").display().to_string())
        .replace("<EXTERN>", &work.join("extern").display().to_string())
        .replace("<DEPS>", &work.join("deps").display().to_string())
        .replace("<TARGET>", &work.join("target").display().to_string())
}

pub fn normalize_extern_args(args: &mut [String], extern_dir: &Path) {
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--extern" {
            if let Some(value) = args.get_mut(index + 1) {
                *value = normalize_extern_value(value, extern_dir);
            }
            index += 2;
            continue;
        }
        if let Some(value) = args[index].strip_prefix("--extern=") {
            let normalized = normalize_extern_value(value, extern_dir);
            args[index] = format!("--extern={normalized}");
        }
        index += 1;
    }
}

fn normalize_extern_value(value: &str, extern_dir: &Path) -> String {
    let Some((name, path)) = value.split_once('=') else {
        return value.to_string();
    };
    if Path::new(path).is_file() {
        return value.to_string();
    }
    let Ok(entries) = fs::read_dir(extern_dir) else {
        return value.to_string();
    };
    let mut candidates: Vec<String> = entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|name| name.to_string()))
        .filter(|file| {
            file == name
                || file.starts_with(&format!("{name}."))
                || file.starts_with(&format!("{name}-"))
                || file.starts_with(&format!("lib{name}."))
                || file.starts_with(&format!("lib{name}-"))
        })
        .collect();
    candidates.sort();
    if let Some(file) = candidates.first() {
        return format!("{name}={}", extern_dir.join(file).display());
    }

    if let Ok(entries) = fs::read_dir(extern_dir) {
        let listing: Vec<String> = entries
            .flatten()
            .filter_map(|entry| entry.file_name().to_str().map(|name| name.to_string()))
            .collect();
        LogStruct::new(
            LogLevel::Debug,
            "外部依赖缺失",
            format!(
                "value={value} dir={} entries={listing:?}",
                extern_dir.display()
            ),
        )
        .emit();
    }
    value.to_string()
}

fn link_paths(cwd: &Path, args: &[String]) -> Vec<(String, PathBuf)> {
    let mut result = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "-L" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("-L")
        };
        if let Some(value) = value {
            let (kind, path) = value.split_once('=').unwrap_or(("unknown", value));
            result.push((kind.to_string(), resolve_path(cwd, path)));
        }
        index += 1;
    }
    result
}

fn extern_paths(cwd: &Path, args: &[String]) -> HashMap<String, PathBuf> {
    let mut result = HashMap::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--extern" {
            if let Some(value) = args.get(index + 1) {
                add_extern(cwd, value, &mut result);
            }
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--extern=") {
            add_extern(cwd, value, &mut result);
        }
        index += 1;
    }
    result
}

fn add_extern(cwd: &Path, value: &str, result: &mut HashMap<String, PathBuf>) {
    if let Some((name, path)) = value.split_once('=') {
        result.insert(name.to_string(), resolve_path(cwd, path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    fn manifest() -> ActionManifest {
        ActionManifest {
            schema: crate::compile::manifest::MANIFEST_SCHEMA,
            toolchain_id: "test-toolchain".into(),
            target: String::new(),
            profile: String::new(),
            cwd: "<TARGET>/pkg".into(),
            crate_name: "demo".into(),
            crate_type: "lib".into(),
            edition: Some("2021".into()),
            args: vec![
                "--crate-name".into(),
                "demo".into(),
                "--crate-type".into(),
                "lib".into(),
                "--out-dir".into(),
                "<OUT>".into(),
                "--emit=dep-info,metadata,link".into(),
                "src/lib.rs".into(),
            ],
            env: std::collections::BTreeMap::from([("CARGO_INCREMENTAL".into(), "0".into())]),
            inputs: Vec::new(),
            outputs: Vec::new(),
            incremental: false,
        }
    }

    fn bundle(schema: u16, path: &str) -> TaskBundle {
        TaskBundle {
            schema,
            manifest: manifest(),
            files: vec![BundleFile {
                path: path.into(),
                mode: 0o644,
                data: b"pub fn f() {}\n".to_vec(),
            }],
        }
    }

    #[test]
    fn prepare_task_and_substitute() {
        let root = TempDir::new("bundle");
        let work = root.join("work");
        fs::create_dir_all(work.join("pkg/src")).unwrap();
        fs::create_dir_all(work.join("out")).unwrap();
        fs::create_dir_all(work.join("extern")).unwrap();
        fs::write(work.join("pkg/src/lib.rs"), "pub fn f() {}\n").unwrap();

        let prepared = prepare_task(&bundle(BUNDLE_SCHEMA, "pkg/src/lib.rs"), &work).unwrap();
        assert_eq!(prepared.cwd, work.join("pkg"));
        assert_eq!(prepared.out_dir, work.join("out"));

        assert_eq!(
            substitute("<MANIFEST>/src/lib.rs", &work),
            work.join("pkg/src/lib.rs").display().to_string()
        );
        assert_eq!(
            substitute("--out-dir=<OUT>", &work),
            format!("--out-dir={}", work.join("out").display())
        );
        assert_eq!(
            substitute("<EXTERN>/dep", &work),
            work.join("extern/dep").display().to_string()
        );
        assert_eq!(
            substitute("--target-dir=<TARGET>", &work),
            format!("--target-dir={}", work.join("target").display())
        );
    }

    #[test]
    fn bundle_rejects_unsafe_paths_and_schema() {
        let root = TempDir::new("bundle-unsafe");
        let work = root.join("work");
        fs::create_dir_all(&work).unwrap();

        for path in ["../escape", "/tmp/escape", "a/../../escape"] {
            assert!(
                materialize_bundle(&bundle(BUNDLE_SCHEMA, path), &work).is_err(),
                "case: {path}"
            );
            assert!(!work.join("escape").exists(), "case: {path} escaped");
        }

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&bundle(BUNDLE_SCHEMA + 1, "pkg/src/lib.rs"), &mut bytes)
            .unwrap();
        assert!(decode_bundle(&bytes).is_err(), "case: unsupported schema");
    }
    #[test]
    fn task_job_prepare_rejects_unsupported_invocation() {
        let root = TempDir::new("task-job-unsupported");
        let mut task = bundle(BUNDLE_SCHEMA, "pkg/src/lib.rs");
        task.manifest.args[3] = "bin".into();
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&task, &mut bytes).unwrap();
        let bundle_path = root.join("bundle");
        fs::write(&bundle_path, &bytes).unwrap();
        let task_id = task.manifest.digest().unwrap();
        let hash = crate::compile::hash::hash_bytes(&bytes);
        let work = root.join("work");

        let job = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            hash,
            bytes.len() as u64,
            work.clone(),
            String::new(),
            String::new(),
        )
        .unwrap();
        let error = job.prepare().unwrap_err();
        assert_eq!(error.code, "unsupported");
        assert!(!work.exists());
    }

    #[test]
    fn task_job_defers_materialization_until_prepare() {
        let root = TempDir::new("task-job-defer");
        let bundle = bundle(BUNDLE_SCHEMA, "pkg/src/lib.rs");
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&bundle, &mut bytes).unwrap();
        let bundle_path = root.join("bundle");
        fs::write(&bundle_path, &bytes).unwrap();
        let task_id = bundle.manifest.digest().unwrap();
        let hash = crate::compile::hash::hash_bytes(&bytes);
        let work = root.join("work");

        let job = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            hash,
            bytes.len() as u64,
            work.clone(),
            "toolchain".into(),
            String::new(),
        )
        .unwrap();

        assert!(!work.exists(), "绑定入队时不应展开文件");
        let prepared = job.prepare().unwrap();
        assert!(work.exists(), "分配到 slot 后才展开");
        assert!(prepared.prepared.out_dir.is_dir());
    }

    #[test]
    fn task_job_validates_bundle_identity() {
        let root = TempDir::new("task-job");
        let bundle = bundle(BUNDLE_SCHEMA, "pkg/src/lib.rs");
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&bundle, &mut bytes).unwrap();
        let bundle_path = root.join("bundle");
        fs::write(&bundle_path, &bytes).unwrap();
        let task_id = bundle.manifest.digest().unwrap();
        let hash = crate::compile::hash::hash_bytes(&bytes);
        let len = bytes.len() as u64;
        let work = root.join("work");

        let job = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            hash,
            len,
            work.clone(),
            String::new(),
            String::new(),
        )
        .unwrap();
        assert_eq!(job.task_id, task_id);
        assert_eq!(job.bundle_bytes, bytes);

        let error = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            Hash32([9; 32]),
            len,
            work.clone(),
            String::new(),
            String::new(),
        )
        .unwrap_err();
        assert_eq!(error.code, "bundle_error");

        let error = TaskJob::from_bundle_file(
            &bundle_path,
            task_id,
            hash,
            len + 1,
            work,
            String::new(),
            String::new(),
        )
        .unwrap_err();
        assert_eq!(error.code, "bundle_error");
    }
}
