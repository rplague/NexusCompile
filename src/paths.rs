//! Unified path resolution.
//!
//! - systemd deployment: standard directories are anchored by the
//!   `NEXUSCOMPILE_HOME`, `NEXUSCOMPILE_CONFIG`, and `NEXUSCOMPILE_LOG_PATH`
//!   environment variables.
//! - Local `cargo run`: falls back to the current directory when no environment
//!   variables are set (`./config.toml`, `./log`), preserving existing behavior.

use std::env;
use std::path::PathBuf;

/// Read a non-empty environment variable as a path.
fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Configuration file path.
///
/// Priority: `NEXUSCOMPILE_CONFIG` -> `$NEXUSCOMPILE_HOME/config.toml` -> `./config.toml`.
pub fn config_path() -> PathBuf {
    if let Some(path) = env_path("NEXUSCOMPILE_CONFIG") {
        return path;
    }
    if let Some(home) = env_path("NEXUSCOMPILE_HOME") {
        return home.join("config.toml");
    }
    PathBuf::from("./config.toml")
}

/// Log directory.
///
/// Priority: `NEXUSCOMPILE_LOG_PATH` -> `$NEXUSCOMPILE_HOME` -> `./`.
pub fn log_path() -> PathBuf {
    if let Some(path) = env_path("NEXUSCOMPILE_LOG_PATH") {
        return path;
    }
    if let Some(home) = env_path("NEXUSCOMPILE_HOME") {
        return home;
    }
    PathBuf::from("./")
}

/// Result cache directory.
///
/// Priority: `NEXUSCOMPILE_CACHE` → `$NEXUSCOMPILE_HOME/cache` → `./cache`.
pub fn cache_path() -> PathBuf {
    if let Some(path) = env_path("NEXUSCOMPILE_CACHE") {
        return path;
    }
    if let Some(home) = env_path("NEXUSCOMPILE_HOME") {
        return home.join("cache");
    }
    PathBuf::from("./cache")
}

/// Local wrapper -> sidecar task socket.
pub fn task_socket_path() -> PathBuf {
    if let Some(path) = env_path("NEXUSCOMPILE_TASK_SOCKET") {
        return path;
    }
    if let Some(path) = env_path("NEXUSCOMPILE_SOCKET") {
        return path.with_file_name("task.sock");
    }
    if let Some(runtime) = env_path("XDG_RUNTIME_DIR") {
        return runtime.join("nexus-compile-task.sock");
    }
    env::temp_dir().join(format!("nexus-compile-{}-task.sock", safe_user()))
}

fn safe_user() -> String {
    let user = env::var("USER").unwrap_or_else(|_| "unknown".into());
    user.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
