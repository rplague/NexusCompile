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

pub const BUNDLE_SCHEMA: u16 = 1;

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
        files.push(BundleFile {
            path: input.path.clone(),
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
