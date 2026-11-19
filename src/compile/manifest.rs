//! Action manifest and canonical action digest for a single rustc invocation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::compile::ToolchainInfo;
use crate::compile::hash::hash_bytes;
use crate::compile_protocol::Hash32;
use crate::log::{LogLevel, LogStruct};
use crate::util::args;
use crate::util::path::resolve_path;

pub const MANIFEST_SCHEMA: u16 = 1;

#[derive(Debug, Clone)]
pub struct ManifestError(String);

impl ManifestError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ManifestError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBlob {
    pub path: String,
    pub hash: Hash32,
    pub size: u64,
    /// True when `hash` is the producing task id instead of the file content.
    pub identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionManifest {
    pub schema: u16,
    pub toolchain_id: String,
    pub target: String,
    pub profile: String,
    pub cwd: String,
    pub crate_name: String,
    pub crate_type: String,
    pub edition: Option<String>,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub inputs: Vec<InputBlob>,
    pub outputs: Vec<String>,
    pub incremental: bool,
}

impl ActionManifest {
    pub fn digest(&self) -> Result<Hash32, ManifestError> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(self, &mut bytes)
            .map_err(|e| ManifestError::new(format!("encode manifest: {e}")))?;
        Ok(hash_bytes(&bytes))
    }
}

pub fn collect_manifest(
    cwd: &Path,
    rustc: &Path,
    args: &[String],
    env: &HashMap<String, String>,
) -> Result<ActionManifest, ManifestError> {
    let manifest_dir = env
        .get("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| cwd.to_path_buf());
    let out_dir = args::value(args, "--out-dir")
        .or_else(|| env.get("OUT_DIR").cloned())
        .ok_or_else(|| ManifestError::new("missing --out-dir / OUT_DIR"))?;
    let out_dir = resolve_path(cwd, &out_dir);
    let target = env
        .get("TARGET")
        .cloned()
        .or_else(|| args::value(args, "--target"))
        .unwrap_or_default();
    let profile = env.get("PROFILE").cloned().unwrap_or_default();
    let crate_name = args::value(args, "--crate-name").unwrap_or_else(|| "unknown".into());
    let crate_type = args::value(args, "--crate-type").unwrap_or_default();
    let edition = args::value(args, "--edition");

    let toolchain_id = toolchain_id(rustc, &target)?;
    let canonical_args = canonicalize_args(args, &manifest_dir, &out_dir, env);
    let canonical_env = canonicalize_env(env, &manifest_dir, &out_dir);
    let inputs = collect_inputs(cwd, &manifest_dir, &out_dir, args, env)?;
    let canonical_cwd = normalize_path_token(&cwd.to_string_lossy(), &out_dir, "<OUT>");
    let canonical_cwd = normalize_path_token(&canonical_cwd, &manifest_dir, "<MANIFEST>");

    Ok(ActionManifest {
        schema: MANIFEST_SCHEMA,
        toolchain_id,
        target,
        profile,
        cwd: canonical_cwd,
        crate_name: crate_name.clone(),
        crate_type,
        edition,
        args: canonical_args,
        env: canonical_env,
        inputs,
        outputs: vec![
            format!("<OUT>/lib{crate_name}.rlib"),
            format!("<OUT>/lib{crate_name}.rmeta"),
            format!("<OUT>/{crate_name}.d"),
        ],
        incremental: false,
    })
}

fn collect_inputs(
    cwd: &Path,
    manifest_dir: &Path,
    out_dir: &Path,
    args: &[String],
    env: &HashMap<String, String>,
) -> Result<Vec<InputBlob>, ManifestError> {
    let mut inputs = BTreeMap::<String, InputBlob>::new();

    hash_tree(manifest_dir, "pkg", Some(out_dir), &mut inputs)?;

    if let Some(out_dir) = env.get("OUT_DIR") {
        let path = resolve_path(cwd, out_dir);
        if path.exists() {
            hash_tree(&path, "out", None, &mut inputs)?;
        }
    }

    for (index, arg) in args.iter().enumerate() {
        if arg == "--extern" {
            if let Some(value) = args.get(index + 1) {
                add_extern(cwd, value, &mut inputs)?;
            }
        } else if let Some(value) = arg.strip_prefix("--extern=") {
            add_extern(cwd, value, &mut inputs)?;
        }
    }

    let extern_names = extern_names(args);
    let crate_name = args::value(args, "--crate-name").unwrap_or_default();
    for (kind, path) in link_paths(cwd, args) {
        add_dependency_files(&crate_name, &kind, &path, &extern_names, out_dir, &mut inputs)?;
    }

    if let Some(source) = args
        .iter()
        .rev()
        .find(|arg| !arg.starts_with('-') && resolve_path(cwd, arg).is_file())
    {
        let path = resolve_path(cwd, source);
        let logical = format!(
            "source/{}",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        add_file(&path, &logical, &mut inputs)?;
    }

    Ok(inputs.into_values().collect())
}

fn add_extern(
    cwd: &Path,
    value: &str,
    inputs: &mut BTreeMap<String, InputBlob>,
) -> Result<(), ManifestError> {
    let (name, path) = value.split_once('=').unwrap_or((value, value));
    let path = resolve_path(cwd, path);
    if !path.exists() {
        return Ok(());
    }
    let logical = format!("extern/{name}");
    add_artifact(&path, &logical, inputs)
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

fn extern_names(args: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let value = if args[index] == "--extern" {
            index += 1;
            args.get(index).map(String::as_str)
        } else {
            args[index].strip_prefix("--extern=")
        };
        if let Some(value) = value {
            let (name, _) = value.split_once('=').unwrap_or((value, value));
            result.push(name.to_string());
        }
        index += 1;
    }
    result
}

fn dependency_closure(out_dir: &Path, externs: &[String]) -> Option<HashSet<String>> {
    let profile_dir = out_dir.parent()?;
    let fingerprint_dir = profile_dir.join(".fingerprint");
    if !fingerprint_dir.is_dir() {
        return None;
    }

    let mut edges = HashMap::<String, Vec<String>>::new();
    for entry in std::fs::read_dir(&fingerprint_dir).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&path) else {
            continue;
        };
        for file_entry in files.flatten() {
            let file = file_entry.path();
            let Some(file_name) = file.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(crate_name) = file_name
                .strip_prefix("lib-")
                .and_then(|name| name.strip_suffix(".json"))
            else {
                continue;
            };
            let Ok(text) = std::fs::read_to_string(&file) else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            let deps = value
                .get("deps")
                .and_then(|value| value.as_array())
                .map(|deps| {
                    deps.iter()
                        .filter_map(|dep| dep.get(1).and_then(|name| name.as_str()))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            edges.insert(crate_name.to_string(), deps);
        }
    }

    if edges.is_empty() {
        return None;
    }

    let mut closure = HashSet::new();
    let mut queue: Vec<String> = externs.to_vec();
    while let Some(name) = queue.pop() {
        if !closure.insert(name.clone()) {
            continue;
        }
        if let Some(deps) = edges.get(&name) {
            queue.extend(deps.iter().cloned());
        }
    }
    Some(closure)
}

/// Which artifacts of a `-L` directory must be bundled.
#[derive(Debug)]
enum LinkSelection {
    /// Conservative superset: keep every artifact in the directory.
    All,
    /// Keep artifacts whose file name references one of these extern crates.
    Closure(HashSet<String>),
}

fn artifact_files(dir: &Path) -> Result<Vec<PathBuf>, ManifestError> {
    const EXTENSIONS: [&str; 6] = ["rmeta", "rlib", "so", "a", "dylib", "dll"];
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|error| ManifestError::new(format!("read deps {}: {error}", dir.display())))?
    {
        let entry = entry.map_err(|error| ManifestError::new(error.to_string()))?;
        let file = entry.path();
        if !file.is_file() {
            continue;
        }
        let extension = file
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if EXTENSIONS.contains(&extension) {
            files.push(file);
        }
    }
    files.sort();
    Ok(files)
}

fn artifact_matches(file: &Path, crate_name: &str) -> bool {
    let name = file.file_name().unwrap_or_default().to_string_lossy();
    name.starts_with(&format!("lib{crate_name}-")) || name.starts_with(&format!("lib{crate_name}."))
}

/// Select the artifacts of a `-L dependency` directory.
///
/// `dependency_closure` keys crates by the extern name the dependent uses.
/// A dependency renamed in `Cargo.toml` (`package = "..."`) is only visible
/// under its alias there, so the closure may name crates that match no
/// artifact and silently drop whole subtrees. rustc needs a superset of what
/// it loads, so an unresolvable closure falls back to the whole directory.
fn dependency_selection(out_dir: &Path, externs: &[String], files: &[PathBuf]) -> LinkSelection {
    let Some(closure) = dependency_closure(out_dir, externs) else {
        return LinkSelection::All;
    };
    let unresolved: Vec<&String> = closure
        .iter()
        .filter(|crate_name| !files.iter().any(|file| artifact_matches(file, crate_name)))
        .collect();
    if unresolved.is_empty() {
        return LinkSelection::Closure(closure);
    }
    LogStruct::new(
        LogLevel::Warning,
        "依赖闭包回退",
        format!(
            "closure={} unresolved={unresolved:?} reason=renamed-or-unknown-extern",
            closure.len()
        ),
    )
    .emit();
    LinkSelection::All
}

fn add_dependency_files(
    crate_name: &str,
    kind: &str,
    path: &Path,
    externs: &[String],
    out_dir: &Path,
    inputs: &mut BTreeMap<String, InputBlob>,
) -> Result<(), ManifestError> {
    if !path.is_dir() {
        return Ok(());
    }
    let files = artifact_files(path)?;
    // Only `-L dependency` carries the transitive crate artifacts. Other kinds
    // (native, framework, ...) point at directories whose contents are already
    // part of the action inputs, so they keep the direct-extern filter.
    let selection = if kind == "dependency" {
        dependency_selection(out_dir, externs, &files)
    } else {
        LinkSelection::Closure(externs.iter().cloned().collect())
    };

    let mut matched = 0usize;
    for file in &files {
        if let LinkSelection::Closure(closure) = &selection
            && !closure
                .iter()
                .any(|crate_name| artifact_matches(file, crate_name))
        {
            continue;
        }
        matched += 1;
        let name = file.file_name().unwrap_or_default().to_string_lossy();
        let logical = format!("deps/{kind}/{name}");
        add_artifact(file, &logical, inputs)?;
    }

    if let Some(log_path) = std::env::var_os("NEXUSCOMPILE_SCAN_LOG") {
        let selection_name = match &selection {
            LinkSelection::All => "all",
            LinkSelection::Closure(_) => "closure",
        };
        let line = format!(
            "crate={crate_name} kind={kind} externs={externs:?} selection={selection_name} files={} matched={matched}\n",
            files.len()
        );
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
        {
            use std::io::Write;
            let _ = file.write_all(line.as_bytes());
        }
    }
    Ok(())
}

fn hash_tree(
    base: &Path,
    prefix: &str,
    skip_dir: Option<&Path>,
    inputs: &mut BTreeMap<String, InputBlob>,
) -> Result<(), ManifestError> {
    if !base.exists() {
        return Ok(());
    }

    let walker = WalkDir::new(base)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| {
            if entry.depth() == 0 || !entry.file_type().is_dir() {
                return true;
            }
            let name = entry.file_name().to_string_lossy();
            if matches!(name.as_ref(), "target" | ".git" | "node_modules") {
                return false;
            }
            if let Some(skip) = skip_dir
                && entry.path().starts_with(skip)
            {
                return false;
            }
            true
        });

    for entry in walker {
        let entry =
            entry.map_err(|e| ManifestError::new(format!("walk {}: {e}", base.display())))?;
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }

        let relative = path
            .strip_prefix(base)
            .map_err(|e| ManifestError::new(format!("strip prefix: {e}")))?;
        let logical = format!("{prefix}/{}", relative.to_string_lossy());
        add_file(path, &logical, inputs)?;
    }
    Ok(())
}

fn add_file(
    path: &Path,
    logical: &str,
    inputs: &mut BTreeMap<String, InputBlob>,
) -> Result<(), ManifestError> {
    if inputs.contains_key(logical) {
        return Ok(());
    }
    let (hash, size) = hash_file(path)?;
    scan_log("manifest", path, logical, &hash, size);
    inputs.insert(
        logical.to_string(),
        InputBlob {
            path: logical.to_string(),
            hash,
            size,
            identity: false,
        },
    );
    Ok(())
}

/// Record an upstream artifact by the task id that produced it, when available.
fn add_artifact(
    path: &Path,
    logical: &str,
    inputs: &mut BTreeMap<String, InputBlob>,
) -> Result<(), ManifestError> {
    if inputs.contains_key(logical) {
        return Ok(());
    }
    if let Some(identity) = read_identity(path) {
        inputs.insert(
            logical.to_string(),
            InputBlob {
                path: logical.to_string(),
                hash: identity,
                size: 0,
                identity: true,
            },
        );
        return Ok(());
    }
    add_file(path, logical, inputs)
}

fn read_identity(path: &Path) -> Option<Hash32> {
    let sidecar = PathBuf::from(format!("{}.nexus", path.display()));
    let text = std::fs::read_to_string(sidecar).ok()?;
    let text = text.trim();
    if text.len() != 64 || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (slot, pair) in bytes.iter_mut().zip(text.as_bytes().chunks(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Hash32(bytes))
}

/// Append one line describing a file at a specific phase, for diagnosing
/// mismatches between hashing and bundling.
pub(crate) fn scan_log(phase: &str, path: &Path, logical: &str, hash: &Hash32, size: u64) {
    use std::os::unix::fs::MetadataExt;

    let Some(log_path) = std::env::var_os("NEXUSCOMPILE_SCAN_LOG") else {
        return;
    };
    let (ino, mtime) = std::fs::metadata(path)
        .map(|meta| {
            (
                meta.ino(),
                format!("{}.{:09}", meta.mtime(), meta.mtime_nsec()),
            )
        })
        .unwrap_or((0, "none".to_string()));
    let line = format!(
        "phase={phase} path={logical} size={size} hash={} ino={ino} mtime={mtime}\n",
        crate::compile::hash::hex_hash(hash)
    );
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    {
        use std::io::Write;
        let _ = file.write_all(line.as_bytes());
    }
}

fn hash_file(path: &Path) -> Result<(Hash32, u64), ManifestError> {
    let mut file = File::open(path)
        .map_err(|e| ManifestError::new(format!("open {}: {e}", path.display())))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut size = 0u64;

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|e| ManifestError::new(format!("read {}: {e}", path.display())))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size.saturating_add(read as u64);
    }

    Ok((Hash32(hasher.finalize().into()), size))
}

fn canonicalize_args(
    args: &[String],
    manifest_dir: &Path,
    out_dir: &Path,
    env: &HashMap<String, String>,
) -> Vec<String> {
    let target_dir = env.get("CARGO_TARGET_DIR").map(PathBuf::from);

    let mut result = Vec::with_capacity(args.len());
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];

        if arg == "--extern" {
            result.push(arg.clone());
            if let Some(value) = args.get(index + 1) {
                result.push(canonicalize_extern(value));
                index += 2;
                continue;
            }
        } else if let Some(value) = arg.strip_prefix("--extern=") {
            result.push(format!("--extern={}", canonicalize_extern(value)));
        } else if arg == "-L" {
            result.push(arg.clone());
            if let Some(value) = args.get(index + 1) {
                result.push(canonicalize_link_path(value));
                index += 2;
                continue;
            }
        } else if let Some(value) = arg.strip_prefix("-L") {
            result.push(format!("-L{}", canonicalize_link_path(value)));
        } else {
            let mut value = normalize_path_token(arg, out_dir, "<OUT>");
            if let Some(target_dir) = &target_dir {
                value = normalize_path_token(&value, target_dir, "<TARGET>");
            }
            value = normalize_path_token(&value, manifest_dir, "<MANIFEST>");
            result.push(value);
        }
        index += 1;
    }
    result
}

fn canonicalize_extern(value: &str) -> String {
    match value.split_once('=') {
        Some((name, _)) => format!("{name}=<EXTERN>/{name}"),
        None => format!("<EXTERN>/{value}"),
    }
}

fn canonicalize_link_path(value: &str) -> String {
    let (kind, _) = value.split_once('=').unwrap_or(("unknown", value));
    format!("{kind}=<DEPS>/{kind}")
}

fn canonicalize_env(
    env: &HashMap<String, String>,
    manifest_dir: &Path,
    out_dir: &Path,
) -> BTreeMap<String, String> {
    let target_dir = env.get("CARGO_TARGET_DIR").map(PathBuf::from);
    let mut result = BTreeMap::new();

    for (key, value) in env {
        if !env_allowed(key) {
            continue;
        }
        let mut normalized = normalize_path_token(value, out_dir, "<OUT>");
        if let Some(target_dir) = &target_dir {
            normalized = normalize_path_token(&normalized, target_dir, "<TARGET>");
        }
        normalized = normalize_path_token(&normalized, manifest_dir, "<MANIFEST>");
        result.insert(key.clone(), normalized);
    }
    result
}

fn env_allowed(key: &str) -> bool {
    matches!(
        key,
        "OUT_DIR"
            | "RUSTFLAGS"
            | "TARGET"
            | "HOST"
            | "PROFILE"
            | "OPT_LEVEL"
            | "DEBUG"
            | "SOURCE_DATE_EPOCH"
            | "CARGO_INCREMENTAL"
            | "CARGO_MANIFEST_DIR"
            | "CARGO_MANIFEST_PATH"
            | "CARGO_TARGET_DIR"
    ) || key.starts_with("CARGO_CFG_")
        || key.starts_with("CARGO_PKG_")
        || (key.starts_with("CARGO_") && !key.contains("WRAPPER") && key != "CARGO_MAKEFLAGS")
}

fn normalize_path_token(value: &str, path: &Path, placeholder: &str) -> String {
    let path = path.to_string_lossy();
    if path.is_empty() {
        return value.to_string();
    }
    if value == path {
        return placeholder.to_string();
    }
    let prefix = format!("{path}/");
    if let Some(rest) = value.strip_prefix(&prefix) {
        return format!("{placeholder}/{rest}");
    }
    value.to_string()
}

pub fn toolchain_info(rustc: &Path, target: &str) -> Result<ToolchainInfo, ManifestError> {
    static CACHE: OnceLock<Mutex<HashMap<String, ToolchainInfo>>> = OnceLock::new();
    let key = format!("{}|{target}", rustc.display());
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(value) = cache.lock().ok().and_then(|cache| cache.get(&key).cloned()) {
        return Ok(value);
    }

    let output = Command::new(rustc)
        .arg("-vV")
        .output()
        .map_err(|e| ManifestError::new(format!("run rustc -vV: {e}")))?;
    if !output.status.success() {
        return Err(ManifestError::new("rustc -vV failed"));
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let release = text
        .lines()
        .find_map(|line| line.strip_prefix("release: "))
        .unwrap_or_default()
        .to_string();
    let host = text
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap_or_default()
        .to_string();

    let mut hasher = Sha256::new();
    hasher.update(&output.stdout);
    hasher.update(target.as_bytes());
    let info = ToolchainInfo {
        id: hex_digest(&hasher.finalize()),
        release,
        host,
    };
    if let Ok(mut cache) = cache.lock() {
        cache.insert(key, info.clone());
    }
    Ok(info)
}

pub fn toolchain_id(rustc: &Path, target: &str) -> Result<String, ManifestError> {
    toolchain_info(rustc, target).map(|info| info.id)
}

/// Whether the target standard library is installed for this rustc.
pub fn target_installed(rustc: &Path, target: &str) -> Result<bool, ManifestError> {
    if target.is_empty() {
        return Ok(true);
    }

    static CACHE: OnceLock<Mutex<HashMap<String, bool>>> = OnceLock::new();
    let key = format!("{}|{target}", rustc.display());
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(value) = cache.lock().ok().and_then(|cache| cache.get(&key).copied()) {
        return Ok(value);
    }

    let output = Command::new(rustc)
        .arg("--print")
        .arg("target-libdir")
        .arg("--target")
        .arg(target)
        .output()
        .map_err(|e| ManifestError::new(format!("run rustc --print target-libdir: {e}")))?;
    let installed = output.status.success()
        && Path::new(String::from_utf8_lossy(&output.stdout).trim()).is_dir();
    if let Ok(mut cache) = cache.lock() {
        cache.insert(key, installed);
    }
    Ok(installed)
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut result, "{byte:02x}");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;
    use std::fs;

    #[test]
    fn hash_file_matches_hash_bytes() {
        let dir = TempDir::new("hash-match");
        let file = dir.join("f");
        fs::write(&file, b"{\"v\":1}").unwrap();
        let (hash, size) = hash_file(&file).unwrap();
        assert_eq!(size, 7);
        assert_eq!(hash, crate::compile::hash::hash_bytes(b"{\"v\":1}"));
    }

    #[test]
    fn artifact_identity_overrides_content_hash() {
        let root = TempDir::new("artifact-identity");
        let artifact = root.join("libfoo.rlib");
        let logical = "deps/dependency/libfoo.rlib";
        fs::write(&artifact, b"one").unwrap();
        fs::write(root.join("libfoo.rlib.nexus"), "aa".repeat(32)).unwrap();

        let mut inputs = BTreeMap::new();
        add_artifact(&artifact, logical, &mut inputs).unwrap();
        let blob = inputs.get(logical).unwrap();
        assert!(blob.identity);
        assert_eq!(blob.size, 0);
        assert_eq!(blob.hash, Hash32([0xaa; 32]));

        fs::write(&artifact, b"two").unwrap();
        let mut again = BTreeMap::new();
        add_artifact(&artifact, logical, &mut again).unwrap();
        assert_eq!(inputs, again);

        fs::remove_file(root.join("libfoo.rlib.nexus")).unwrap();
        let mut fallback = BTreeMap::new();
        add_artifact(&artifact, logical, &mut fallback).unwrap();
        assert!(!fallback.get(logical).unwrap().identity);
    }

    fn base_env(root: &Path, out: &Path) -> HashMap<String, String> {
        HashMap::from([
            ("CARGO_MANIFEST_DIR".into(), root.display().to_string()),
            ("OUT_DIR".into(), out.display().to_string()),
            ("TARGET".into(), "x86_64-unknown-linux-gnu".into()),
            ("PROFILE".into(), "debug".into()),
            ("CARGO_INCREMENTAL".into(), "0".into()),
        ])
    }

    fn collect_with(
        root: &Path,
        out: &Path,
        extern_path: &Path,
        links: &[(&str, &Path)],
    ) -> ActionManifest {
        let env = base_env(root, out);
        let mut args = vec![
            "--crate-name".into(),
            "demo".into(),
            "--crate-type".into(),
            "lib".into(),
            "--out-dir".into(),
            out.display().to_string(),
            "--emit=dep-info,metadata,link".into(),
            "--extern".into(),
            format!("dep={}", extern_path.display()),
        ];
        for (kind, path) in links {
            args.push("-L".into());
            args.push(format!("{kind}={}", path.display()));
        }
        collect_manifest(root, Path::new("rustc"), &args, &env).unwrap()
    }

    fn collect(root: &Path, out: &Path, extern_path: &Path) -> ActionManifest {
        collect_with(root, out, extern_path, &[])
    }

    fn fingerprint(profile: &Path, unit: &str, deps: &str) {
        // The manifest reads the crate name from `lib-<crate>.json`.
        let crate_name = unit.split_once('-').map_or(unit, |(name, _)| name);
        let dir = profile.join(".fingerprint").join(unit);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(format!("lib-{crate_name}.json")),
            format!("{{\"deps\":{deps}}}"),
        )
        .unwrap();
    }

    #[test]
    fn manifest_hashes_package_out_dir_and_extern() {
        let root = TempDir::new("manifest");
        let out = root.join("out");
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("target")).unwrap();
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::create_dir_all(&out).unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn demo() {}").unwrap();
        fs::write(root.join("target/ignored"), "ignored").unwrap();
        fs::write(root.join(".git/ignored"), "ignored").unwrap();
        fs::write(out.join("generated.rs"), "pub fn generated() {}").unwrap();
        let extern_path = root.join("libdep.rlib");
        fs::write(&extern_path, "rlib bytes").unwrap();

        let manifest = collect(&root, &out, &extern_path);
        let paths: Vec<&str> = manifest
            .inputs
            .iter()
            .map(|input| input.path.as_str())
            .collect();
        assert!(paths.contains(&"pkg/src/lib.rs"));
        assert!(paths.contains(&"out/generated.rs"));
        assert!(paths.contains(&"extern/dep"));
        assert!(!paths.iter().any(|path| path.contains("target/ignored")));
        assert!(!paths.iter().any(|path| path.contains(".git/ignored")));
        assert!(!manifest.incremental);

        let args = manifest.args.join(" ");
        assert!(args.contains("<OUT>"));
        assert!(args.contains("<EXTERN>/dep"));
        assert!(!args.contains(&root.display().to_string()));

        let first = manifest.digest().unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn changed() {}").unwrap();
        let second = collect(&root, &out, &extern_path).digest().unwrap();
        assert_ne!(first, second, "case: source change");
    }

    #[test]
    fn target_installed_checks_sysroot() {
        assert!(target_installed(Path::new("rustc"), "").unwrap());
        assert!(!target_installed(Path::new("rustc"), "definitely-not-a-target").unwrap());
    }

    #[test]
    fn renamed_dependency_falls_back_to_full_link_dir() {
        let root = TempDir::new("manifest-alias");
        let profile = root.join("profile");
        let out = profile.join("deps");
        let links = root.join("linkdeps");
        fs::create_dir_all(&out).unwrap();
        fs::create_dir_all(&links).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn demo() {}").unwrap();
        // `dep` depends on a renamed crate: the fingerprint records the alias,
        // the artifact carries the target name.
        fingerprint(&profile, "dep-0000000000000001", "[[1,\"yamux012\",false,2]]");
        fs::write(links.join("libdep-0000000000000001.rmeta"), b"dep").unwrap();
        fs::write(links.join("libyamux-0000000000000001.rmeta"), b"yamux").unwrap();
        let extern_path = root.join("libdep.rlib");
        fs::write(&extern_path, b"rlib").unwrap();

        let manifest = collect_with(&root, &out, &extern_path, &[("dependency", &links)]);
        let paths: Vec<&str> = manifest.inputs.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"deps/dependency/libdep-0000000000000001.rmeta"));
        assert!(
            paths.contains(&"deps/dependency/libyamux-0000000000000001.rmeta"),
            "renamed dependency artifact must stay in the link dir"
        );
    }

    #[test]
    fn closure_still_filters_unrelated_artifacts() {
        let root = TempDir::new("manifest-closure");
        let profile = root.join("profile");
        let out = profile.join("deps");
        let links = root.join("linkdeps");
        fs::create_dir_all(&out).unwrap();
        fs::create_dir_all(&links).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn demo() {}").unwrap();
        fingerprint(&profile, "dep-0000000000000001", "[[1,\"dep_child\",false,2]]");
        fingerprint(&profile, "dep_child-0000000000000001", "[]");
        fs::write(links.join("libdep-0000000000000001.rmeta"), b"dep").unwrap();
        fs::write(links.join("libdep_child-0000000000000001.rmeta"), b"child").unwrap();
        fs::write(links.join("libunrelated-0000000000000001.rmeta"), b"no").unwrap();
        let extern_path = root.join("libdep.rlib");
        fs::write(&extern_path, b"rlib").unwrap();

        let manifest = collect_with(&root, &out, &extern_path, &[("dependency", &links)]);
        let paths: Vec<&str> = manifest.inputs.iter().map(|i| i.path.as_str()).collect();
        assert!(paths.contains(&"deps/dependency/libdep_child-0000000000000001.rmeta"));
        assert!(
            !paths
                .iter()
                .any(|path| path.starts_with("deps/dependency/libunrelated")),
            "unrelated artifacts must stay filtered"
        );
    }

    #[test]
    fn missing_fingerprints_keep_all_link_artifacts() {
        let root = TempDir::new("manifest-no-fingerprint");
        let profile = root.join("profile");
        let out = profile.join("deps");
        let links = root.join("linkdeps");
        fs::create_dir_all(&out).unwrap();
        fs::create_dir_all(&links).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn demo() {}").unwrap();
        fs::write(links.join("libany-0000000000000001.rmeta"), b"any").unwrap();
        let extern_path = root.join("libdep.rlib");
        fs::write(&extern_path, b"rlib").unwrap();

        let manifest = collect_with(&root, &out, &extern_path, &[("dependency", &links)]);
        let paths: Vec<&str> = manifest.inputs.iter().map(|i| i.path.as_str()).collect();
        assert!(
            paths.contains(&"deps/dependency/libany-0000000000000001.rmeta"),
            "without a fingerprint graph the whole link dir is kept"
        );
    }
}
