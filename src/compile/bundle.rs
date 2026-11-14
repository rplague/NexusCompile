//! Transfer bundle for one remote compile task.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::compile::manifest::ActionManifest;
use crate::util::args;
use crate::util::fs::{file_mode, set_mode};
use crate::util::path::{resolve_path, safe_relative};

pub const BUNDLE_SCHEMA: u16 = 2;

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
    let out_dir = args::value(args, "--out-dir")
        .or_else(|| env.get("OUT_DIR").cloned())
        .ok_or_else(|| "missing --out-dir / OUT_DIR".to_string())?;
    let out_dir = resolve_path(cwd, &out_dir);

    let externs = extern_paths(cwd, args);
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
    value.to_string()
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
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn prepare_task_uses_package_root_for_target_cwd() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "nexuscompile-bundle-{}-{unique}",
            std::process::id()
        ));
        let work = root.join("work");
        fs::create_dir_all(work.join("pkg/src")).unwrap();
        fs::create_dir_all(work.join("out")).unwrap();
        fs::create_dir_all(work.join("extern")).unwrap();
        fs::write(work.join("pkg/src/lib.rs"), "pub fn f() {}\n").unwrap();

        let manifest = ActionManifest {
            schema: crate::compile::manifest::MANIFEST_SCHEMA,
            toolchain_id: String::new(),
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
                "src/lib.rs".into(),
            ],
            env: Default::default(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            incremental: false,
        };
        let bundle = TaskBundle {
            schema: BUNDLE_SCHEMA,
            manifest,
            files: vec![BundleFile {
                path: "pkg/src/lib.rs".into(),
                mode: 0o644,
                data: b"pub fn f() {}\n".to_vec(),
            }],
        };
        let prepared = prepare_task(&bundle, &work).unwrap();
        assert_eq!(prepared.cwd, work.join("pkg"));
        assert_eq!(prepared.out_dir, work.join("out"));
        fs::remove_dir_all(root).unwrap();
    }
}
