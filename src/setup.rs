//! Per-user Cargo configuration used by the package installer.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use toml_edit::{DocumentMut, Item, Table, value};

pub const WRAPPER_PATH: &str = "/usr/bin/nexus-rustc";
pub const DEFAULT_SOCKET: &str = "/run/nexuscompile/nexus.sock";

/// Handle `NexusCompile setup-user ...` / `unsetup-user ...`.
///
/// Returns `None` when the arguments are not a setup command so `main` can
/// continue starting the server.
pub fn run_cli(args: &[String]) -> Option<i32> {
    let command = args.first()?.as_str();
    if command != "setup-user" && command != "unsetup-user" {
        return None;
    }

    let Some(config) = flag(args, "--config") else {
        eprintln!("{command}: missing --config <path>");
        return Some(2);
    };

    let result = if command == "setup-user" {
        let socket = flag(args, "--socket").unwrap_or_else(|| DEFAULT_SOCKET.to_string());
        setup_user(Path::new(&config), &socket).map(|_| ())
    } else {
        unsetup_user(Path::new(&config))
    };

    match result {
        Ok(()) => Some(0),
        Err(error) => {
            eprintln!("{command}: {error}");
            Some(1)
        }
    }
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .cloned()
}

pub fn setup_user(path: &Path, socket: &str) -> io::Result<()> {
    let mut document = load(path)?;
    let original_permissions = fs::metadata(path).ok().map(|meta| meta.permissions());

    if path.exists() {
        backup_once(path)?;
    }

    {
        let build = table_mut(&mut document, "build");
        build["rustc-wrapper"] = value(WRAPPER_PATH);
        build["incremental"] = value(false);

        let env = table_mut(&mut document, "env");
        env["CARGO_INCREMENTAL"] = value("0");
        env["NEXUSCOMPILE_SOCKET"] = value(socket);
    }

    write_atomic(path, &document.to_string(), original_permissions)
}

pub fn unsetup_user(path: &Path) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let mut document = load(path)?;
    let original_permissions = fs::metadata(path).ok().map(|meta| meta.permissions());
    let mut changed = false;

    if let Some(build) = document.get_mut("build").and_then(Item::as_table_mut) {
        if build
            .get("rustc-wrapper")
            .and_then(Item::as_str)
            .is_some_and(|value| value == WRAPPER_PATH)
        {
            build.remove("rustc-wrapper");
            changed = true;
        }
        if build
            .get("incremental")
            .and_then(Item::as_bool)
            .is_some_and(|value| !value)
        {
            build.remove("incremental");
            changed = true;
        }
        if build.is_empty() {
            document.remove("build");
        }
    }

    if let Some(env) = document.get_mut("env").and_then(Item::as_table_mut) {
        if env
            .get("CARGO_INCREMENTAL")
            .and_then(Item::as_str)
            .is_some_and(|value| value == "0")
        {
            env.remove("CARGO_INCREMENTAL");
            changed = true;
        }
        if env
            .get("NEXUSCOMPILE_SOCKET")
            .and_then(Item::as_str)
            .is_some_and(|value| value.starts_with("/run/nexuscompile/"))
        {
            env.remove("NEXUSCOMPILE_SOCKET");
            changed = true;
        }
        if env.is_empty() {
            document.remove("env");
        }
    }

    if changed {
        write_atomic(path, &document.to_string(), original_permissions)?;
    }
    Ok(())
}

fn load(path: &Path) -> io::Result<DocumentMut> {
    match fs::read_to_string(path) {
        Ok(content) => content
            .parse::<DocumentMut>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(DocumentMut::new()),
        Err(error) => Err(error),
    }
}

fn table_mut<'a>(document: &'a mut DocumentMut, name: &str) -> &'a mut Table {
    if !document.contains_key(name) {
        document.insert(name, Item::Table(Table::new()));
    }
    document[name]
        .as_table_mut()
        .expect("managed Cargo config table must be a table")
}

fn backup_once(path: &Path) -> io::Result<()> {
    let backup = backup_path(path);
    if backup.exists() {
        return Ok(());
    }
    fs::copy(path, backup)?;
    Ok(())
}

fn backup_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    path.with_file_name(format!("{name}.nexuscompile.bak"))
}

fn write_atomic(
    path: &Path,
    content: &str,
    permissions: Option<fs::Permissions>,
) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    let temp = path.with_file_name(format!(".{name}.nexuscompile.tmp"));
    fs::write(&temp, content)?;
    if let Some(permissions) = permissions {
        fs::set_permissions(&temp, permissions)?;
    }
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "nexuscompile-setup-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn setup_creates_unique_keys_and_backup() {
        let dir = temp_dir();
        let path = dir.join("config.toml");
        fs::write(&path, "# keep me\n[net]\nretry = 2\n").unwrap();

        setup_user(&path, DEFAULT_SOCKET).unwrap();
        let first = fs::read_to_string(&path).unwrap();
        assert!(first.contains("# keep me"));
        assert_eq!(first.matches("rustc-wrapper").count(), 1);
        assert_eq!(first.matches("CARGO_INCREMENTAL").count(), 1);
        assert!(dir.join("config.toml.nexuscompile.bak").is_file());

        setup_user(&path, DEFAULT_SOCKET).unwrap();
        let second = fs::read_to_string(&path).unwrap();
        assert_eq!(second.matches("[build]").count(), 1);
        assert_eq!(second.matches("[env]").count(), 1);
        assert_eq!(second.matches("rustc-wrapper").count(), 1);
        assert_eq!(second.matches("NEXUSCOMPILE_SOCKET").count(), 1);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unsetup_removes_only_managed_keys() {
        let dir = temp_dir();
        let path = dir.join("config.toml");
        fs::write(
            &path,
            r#"[build]
rustc-wrapper = "/usr/bin/nexus-rustc"
incremental = false
jobs = 4

[env]
CARGO_INCREMENTAL = "0"
NEXUSCOMPILE_SOCKET = "/run/nexuscompile/nexus.sock"
RUST_BACKTRACE = "1"
"#,
        )
        .unwrap();

        unsetup_user(&path).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(!text.contains("rustc-wrapper"));
        assert!(!text.contains("incremental"));
        assert!(!text.contains("CARGO_INCREMENTAL"));
        assert!(!text.contains("NEXUSCOMPILE_SOCKET"));
        assert!(text.contains("jobs = 4"));
        assert!(text.contains("RUST_BACKTRACE"));
        assert!(text.contains("[build]"));
        assert!(text.contains("[env]"));

        fs::remove_dir_all(dir).unwrap();
    }
}
