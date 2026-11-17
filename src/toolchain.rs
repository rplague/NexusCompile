//! Install-time toolchain configuration and runtime compilation checks.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};

use crate::compile::manifest::{target_installed, toolchain_id};
use crate::config::{ServiceConfig, ToolchainEntry};
use crate::paths;

const EX_CONFIG: i32 = 78;

pub fn run_cli(args: &[String]) -> Option<i32> {
    let command = args.first()?.as_str();
    match command {
        "doctor" => Some(run_doctor(&args[1..])),
        "configure-toolchain" => Some(run_configure(&args[1..])),
        "configure-toolchains" => Some(run_configure_toolchains(&args[1..])),
        _ => None,
    }
}

fn run_doctor(args: &[String]) -> i32 {
    match doctor(args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("doctor: {error}");
            EX_CONFIG
        }
    }
}

fn run_configure(args: &[String]) -> i32 {
    match configure(args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("configure-toolchain: {error}");
            1
        }
    }
}

fn run_configure_toolchains(args: &[String]) -> i32 {
    match configure_toolchains(args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("configure-toolchains: {error}");
            1
        }
    }
}

fn doctor(args: &[String]) -> Result<(), String> {
    let mut skip_socket = false;
    let mut config_path = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--skip-socket" => skip_socket = true,
            "--config" => {
                index += 1;
                config_path = args.get(index).map(PathBuf::from);
                if config_path.is_none() {
                    return Err("--config requires a path".into());
                }
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
    }

    let config_path = config_path.unwrap_or_else(paths::config_path);
    let config = load_service_config(&config_path)?;
    for warning in config.warnings() {
        println!("doctor: warning: {warning}");
    }

    for entry in configured_toolchains(&config) {
        let rustc = entry.rustc.trim();
        if rustc.is_empty() {
            return Err("configured toolchain has an empty rustc path".into());
        }
        let version = check_rustc(rustc)?;
        check_target(rustc, entry.target.trim())?;
        println!(
            "doctor: ok rustc={} release={} target={}",
            rustc,
            version.trim(),
            if entry.target.is_empty() {
                "host"
            } else {
                entry.target.trim()
            }
        );
    }

    check_tool("cc")?;
    check_tool("ld")?;
    check_cache(&config)?;
    if !skip_socket {
        check_socket()?;
    }
    Ok(())
}

pub fn configured_toolchains(config: &ServiceConfig) -> Vec<ToolchainEntry> {
    if !config.node.toolchains.is_empty() {
        return config.node.toolchains.clone();
    }
    vec![ToolchainEntry {
        channel: String::new(),
        rustc: config.node.rustc.clone(),
        target: String::new(),
    }]
}

pub fn resolve_rustc(
    config: &ServiceConfig,
    requested_toolchain_id: &str,
    target: &str,
) -> Result<String, String> {
    for entry in configured_toolchains(config) {
        let rustc = entry.rustc.trim();
        if rustc.is_empty() {
            continue;
        }
        if !entry.target.is_empty() && !target.is_empty() && entry.target != target {
            continue;
        }
        if !requested_toolchain_id.is_empty() {
            let expected = match toolchain_id(Path::new(rustc), target) {
                Ok(expected) => expected,
                Err(_) => continue,
            };
            if expected != requested_toolchain_id {
                continue;
            }
            if !target_installed(Path::new(rustc), target).unwrap_or(false) {
                return Err(format!("target {target} is not installed"));
            }
            return Ok(rustc.to_string());
        }
        if !target_installed(Path::new(rustc), target).unwrap_or(false) {
            continue;
        }
        return Ok(rustc.to_string());
    }

    if requested_toolchain_id.is_empty() {
        Err("no usable toolchain configured".into())
    } else {
        Err("toolchain mismatch".into())
    }
}

fn load_service_config(path: &Path) -> Result<ServiceConfig, String> {
    if !path.exists() {
        return Err(format!("read {}: file not found", path.display()));
    }
    ServiceConfig::try_from_toml_file(path, false).map_err(|error| error.to_string())
}

fn check_rustc(rustc: &str) -> Result<String, String> {
    let output = run(Command::new(rustc).arg("-vV"))?;
    if !output.status.success() {
        return Err(format!(
            "{} -vV failed: {}",
            rustc,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let release = text
        .lines()
        .find_map(|line| line.strip_prefix("release: "))
        .ok_or_else(|| format!("{} -vV has no release field", rustc))?;
    Ok(release.to_string())
}

fn check_target(rustc: &str, target: &str) -> Result<(), String> {
    let mut command = Command::new(rustc);
    command.args(["--print", "target-libdir"]);
    if !target.is_empty() {
        command.arg("--target").arg(target);
    }
    let output = run(&mut command)?;
    if !output.status.success() {
        return Err(format!(
            "{} --print target-libdir failed: {}",
            rustc,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !Path::new(&path).is_dir() {
        return Err(format!("target libdir is missing: {path}"));
    }
    Ok(())
}

fn check_tool(tool: &str) -> Result<(), String> {
    let output = run(Command::new(tool).arg("--version"))?;
    if !output.status.success() {
        return Err(format!(
            "{tool} --version failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

fn check_cache(config: &ServiceConfig) -> Result<(), String> {
    let path = if config.cache.dir.trim().is_empty() {
        paths::cache_path()
    } else {
        PathBuf::from(config.cache.dir.trim())
    };
    let path = if path.is_relative() {
        env::current_dir()
            .map_err(|error| format!("resolve current directory: {error}"))?
            .join(path)
    } else {
        path
    };
    fs::create_dir_all(&path)
        .map_err(|error| format!("create cache {}: {error}", path.display()))?;
    check_writable_dir(&path, "cache")
}

fn check_socket() -> Result<(), String> {
    let socket = paths::task_socket_path();
    let parent = socket
        .parent()
        .ok_or_else(|| format!("socket has no parent: {}", socket.display()))?;
    if !parent.is_dir() {
        return Err(format!("socket directory is missing: {}", parent.display()));
    }
    check_writable_dir(parent, "socket directory")
}

fn check_writable_dir(path: &Path, label: &str) -> Result<(), String> {
    let marker = path.join(format!(".doctor-{}", std::process::id()));
    fs::write(&marker, b"ok")
        .map_err(|error| format!("write {label} {}: {error}", path.display()))?;
    let _ = fs::remove_file(marker);
    Ok(())
}

fn configure(args: &[String]) -> Result<(), String> {
    let mut config_path = None;
    let mut rustc = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                config_path = args.get(index).map(PathBuf::from);
                if config_path.is_none() {
                    return Err("--config requires a path".into());
                }
            }
            "--rustc" => {
                index += 1;
                rustc = args.get(index).cloned();
                if rustc.is_none() {
                    return Err("--rustc requires a path".into());
                }
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
    }

    let config_path = config_path.unwrap_or_else(paths::config_path);
    let rustc = rustc.ok_or_else(|| "missing --rustc".to_string())?;
    if rustc.trim().is_empty() {
        return Err("--rustc is empty".into());
    }

    let mut document = load(&config_path)?;
    let original_permissions = fs::metadata(&config_path)
        .ok()
        .map(|metadata| metadata.permissions());
    let node = table_mut(&mut document, "node");
    node["rustc"] = value(rustc.trim());
    node.remove("toolchains");
    write_atomic(&config_path, &document.to_string(), original_permissions)
}

fn configure_toolchains(args: &[String]) -> Result<(), String> {
    let mut config_path = None;
    let mut state_path = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--config" => {
                index += 1;
                config_path = args.get(index).map(PathBuf::from);
                if config_path.is_none() {
                    return Err("--config requires a path".into());
                }
            }
            "--state" => {
                index += 1;
                state_path = args.get(index).map(PathBuf::from);
                if state_path.is_none() {
                    return Err("--state requires a path".into());
                }
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
    }

    let config_path = config_path.unwrap_or_else(paths::config_path);
    let state_path = state_path.ok_or_else(|| "missing --state".to_string())?;
    let entries = read_toolchain_state(&state_path)?;
    write_toolchains(&config_path, &entries)
}

fn read_toolchain_state(path: &Path) -> Result<Vec<ToolchainEntry>, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut entries = Vec::new();

    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 3 {
            return Err(format!(
                "invalid state {} line {}: expected channel|rustc|target",
                path.display(),
                index + 1
            ));
        }
        let channel = parts[0].trim();
        let rustc = parts[1].trim();
        let target = parts[2].trim();
        if channel.is_empty() || rustc.is_empty() {
            return Err(format!(
                "invalid state {} line {}: empty channel or rustc",
                path.display(),
                index + 1
            ));
        }
        entries.push(ToolchainEntry {
            channel: channel.to_string(),
            rustc: rustc.to_string(),
            target: target.to_string(),
        });
    }

    if entries.is_empty() {
        return Err(format!("no toolchains in {}", path.display()));
    }
    Ok(entries)
}

fn write_toolchains(path: &Path, entries: &[ToolchainEntry]) -> Result<(), String> {
    let mut document = load(path)?;
    let original_permissions = fs::metadata(path)
        .ok()
        .map(|metadata| metadata.permissions());
    let node = table_mut(&mut document, "node");
    node["rustc"] = value(entries[0].rustc.clone());

    let mut array = ArrayOfTables::new();
    for entry in entries {
        let mut table = Table::new();
        table["channel"] = value(entry.channel.clone());
        table["rustc"] = value(entry.rustc.clone());
        table["target"] = value(entry.target.clone());
        array.push(table);
    }
    node.insert("toolchains", Item::ArrayOfTables(array));

    write_atomic(path, &document.to_string(), original_permissions)
}

fn load(path: &Path) -> Result<DocumentMut, String> {
    match fs::read_to_string(path) {
        Ok(content) => content
            .parse::<DocumentMut>()
            .map_err(|error| format!("parse {}: {error}", path.display())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(DocumentMut::new()),
        Err(error) => Err(format!("read {}: {error}", path.display())),
    }
}

fn table_mut<'a>(document: &'a mut DocumentMut, name: &str) -> &'a mut Table {
    if !document.contains_key(name) {
        document.insert(name, Item::Table(Table::new()));
    }
    document[name]
        .as_table_mut()
        .expect("managed config table must be a table")
}

fn write_atomic(
    path: &Path,
    content: &str,
    permissions: Option<fs::Permissions>,
) -> Result<(), String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }

    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    let temp = path.with_file_name(format!(".{name}.nexuscompile.tmp"));
    fs::write(&temp, content).map_err(|error| format!("write {}: {error}", temp.display()))?;
    if let Some(permissions) = permissions {
        fs::set_permissions(&temp, permissions)
            .map_err(|error| format!("chmod {}: {error}", temp.display()))?;
    }
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(format!("rename {}: {error}", path.display()))
        }
    }
}

fn run(command: &mut Command) -> Result<Output, String> {
    command
        .output()
        .map_err(|error| format!("run {:?}: {error}", command))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn configure_preserves_existing_content() {
        let root = TempDir::new("toolchain");
        let path = root.join("config.toml");
        fs::write(
            &path,
            "# keep me\nport = 6100\n\n[node]\nmax_usage_percent = 70\nrustc = \"old\"\n",
        )
        .unwrap();

        configure(&[
            "--config".into(),
            path.display().to_string(),
            "--rustc".into(),
            "/opt/rust/bin/rustc".into(),
        ])
        .unwrap();

        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("# keep me"));
        assert!(text.contains("port = 6100"));
        assert!(text.contains("max_usage_percent = 70"));
        assert!(text.contains("rustc = \"/opt/rust/bin/rustc\""));
        assert_eq!(text.matches("rustc =").count(), 1);

    }

    #[test]
    fn configure_creates_node_table() {
        let root = TempDir::new("toolchain");
        let path = root.join("config.toml");

        configure(&[
            "--config".into(),
            path.display().to_string(),
            "--rustc".into(),
            "/opt/rust/bin/rustc".into(),
        ])
        .unwrap();

        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("[node]"));
        assert!(text.contains("rustc = \"/opt/rust/bin/rustc\""));

    }

    #[test]
    fn configure_toolchains_writes_registry() {
        let root = TempDir::new("toolchain");
        let config = root.join("config.toml");
        let state = root.join("state");
        fs::write(
            &state,
            "stable|/opt/stable/bin/rustc|x86_64-unknown-linux-gnu\n             1.99.0|/opt/199/bin/rustc|\n",
        )
        .unwrap();

        configure_toolchains(&[
            "--config".into(),
            config.display().to_string(),
            "--state".into(),
            state.display().to_string(),
        ])
        .unwrap();

        let parsed: ServiceConfig = toml::from_str(&fs::read_to_string(&config).unwrap()).unwrap();
        assert_eq!(parsed.node.rustc, "/opt/stable/bin/rustc");
        assert_eq!(parsed.node.toolchains.len(), 2);
        assert_eq!(parsed.node.toolchains[0].channel, "stable");
        assert_eq!(parsed.node.toolchains[0].target, "x86_64-unknown-linux-gnu");
        assert_eq!(parsed.node.toolchains[1].channel, "1.99.0");
        assert!(parsed.node.toolchains[1].target.is_empty());

    }

    #[test]
    fn state_rejects_malformed_lines() {
        let root = TempDir::new("toolchain");
        let state = root.join("state");
        fs::write(&state, "stable|/opt/stable/bin/rustc\n").unwrap();
        assert!(read_toolchain_state(&state).is_err());
    }

    #[test]
    fn resolver_matches_default_toolchain() {
        let config = ServiceConfig::default();
        let id = toolchain_id(Path::new("rustc"), "").unwrap();
        assert_eq!(resolve_rustc(&config, &id, "").unwrap(), "rustc");
        assert!(resolve_rustc(&config, "not-a-toolchain", "").is_err());
    }

    #[test]
    fn resolver_uses_configured_toolchain_list() {
        let mut config = ServiceConfig::default();
        config.node.toolchains = vec![
            ToolchainEntry {
                channel: "stable".into(),
                rustc: "rustc".into(),
                target: String::new(),
            },
            ToolchainEntry {
                channel: "1.99.0".into(),
                rustc: "/definitely/missing/rustc".into(),
                target: String::new(),
            },
        ];
        let id = toolchain_id(Path::new("rustc"), "").unwrap();
        assert_eq!(resolve_rustc(&config, &id, "").unwrap(), "rustc");
    }
}
