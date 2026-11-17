use crate::log::{LogLevel, LogStruct};
use crate::paths;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};

macro_rules! default_u32_fn {
    ($name:ident, $value:expr) => {
        fn $name() -> u32 {
            $value
        }
    };
}

default_u32_fn!(default_port, 5100);
default_u32_fn!(default_target_slots, 8);

fn default_service_name() -> String {
    "rustc-pool".into()
}

fn default_max_usage_percent() -> u8 {
    80
}

fn default_sample_interval_secs() -> u64 {
    1
}

fn default_update_interval_secs() -> u64 {
    30
}

fn default_ttl_secs() -> u64 {
    60
}

fn default_target_reevaluate_secs() -> u64 {
    5
}

fn default_result_cache_mb() -> u64 {
    512
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolchainEntry {
    #[serde(default)]
    pub channel: String,
    pub rustc: String,
    #[serde(default)]
    pub target: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    #[serde(default = "default_max_usage_percent")]
    pub max_usage_percent: u8,
    #[serde(default = "default_sample_interval_secs")]
    pub sample_interval_secs: u64,
    #[serde(default = "default_rustc")]
    pub rustc: String,
    #[serde(default)]
    pub toolchains: Vec<ToolchainEntry>,
}

fn default_rustc() -> String {
    "rustc".into()
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            max_usage_percent: default_max_usage_percent(),
            sample_interval_secs: default_sample_interval_secs(),
            rustc: default_rustc(),
            toolchains: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SubscriptionConfig {
    #[serde(default = "default_target_slots")]
    pub target_slots: u32,
    #[serde(default = "default_update_interval_secs")]
    pub update_interval_secs: u64,
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_target_reevaluate_secs")]
    pub target_reevaluate_secs: u64,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            target_slots: default_target_slots(),
            update_interval_secs: default_update_interval_secs(),
            ttl_secs: default_ttl_secs(),
            target_reevaluate_secs: default_target_reevaluate_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    #[serde(default)]
    pub dir: String,
    #[serde(default = "default_result_cache_mb")]
    pub result_cache_mb: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            dir: String::new(),
            result_cache_mb: default_result_cache_mb(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServiceConfig {
    #[serde(default = "default_port")]
    pub port: u32,
    #[serde(default = "default_service_name")]
    pub service_name: String,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub subscription: SubscriptionConfig,
    #[serde(default)]
    pub cache: CacheConfig,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            service_name: default_service_name(),
            node: NodeConfig::default(),
            subscription: SubscriptionConfig::default(),
            cache: CacheConfig::default(),
        }
    }
}

/// Kind of configuration loading failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigErrorKind {
    Io,
    Parse,
    Invalid,
}

/// Configuration loading failure that does not terminate the process.
#[derive(Debug, Clone)]
pub struct ConfigError {
    pub kind: ConfigErrorKind,
    pub message: String,
}

impl ConfigError {
    fn new(kind: ConfigErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn io(message: impl Into<String>) -> Self {
        Self::new(ConfigErrorKind::Io, message)
    }

    fn parse(message: impl Into<String>) -> Self {
        Self::new(ConfigErrorKind::Parse, message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

impl ServiceConfig {
    /// Load a configuration file without terminating the process.
    ///
    /// An existing file is read-only: missing fields fall back to defaults in
    /// memory and are never written back. A missing file is created with
    /// defaults only when `create_if_missing` is true.
    pub fn try_from_toml_file(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<ServiceConfig, ConfigError> {
        let path = path.as_ref();

        match fs::read_to_string(path) {
            Ok(content) => toml::from_str::<ServiceConfig>(&content)
                .map_err(|error| ConfigError::parse(format!("parse {}: {error}", path.display()))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !create_if_missing {
                    LogStruct::new(
                        LogLevel::Warning,
                        "配置文件不存在，使用默认配置",
                        path.display().to_string(),
                    )
                    .emit();
                    return Ok(ServiceConfig::default());
                }

                let config = ServiceConfig::default();
                config.save_to_file(path)?;
                LogStruct::new(
                    LogLevel::Important,
                    "配置文件已创建",
                    path.display().to_string(),
                )
                .emit();
                Ok(config)
            }
            Err(error) => Err(ConfigError::io(format!("read {}: {error}", path.display()))),
        }
    }

    /// Serialize this configuration and replace `path` atomically.
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let serialized = toml::to_string_pretty(self)
            .map_err(|error| ConfigError::io(format!("serialize config: {error}")))?;
        write_atomic(path.as_ref(), &serialized)
    }

    /// Non-fatal configuration warnings. These do not reject the file.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();

        if self.port == 0 || self.port > u16::MAX as u32 {
            warnings.push(format!("port 超出范围 1..={}: {}", u16::MAX, self.port));
        }
        if self.service_name.trim().is_empty() {
            warnings.push("service_name 为空".into());
        }
        if self.node.max_usage_percent > 100 {
            warnings.push(format!(
                "node.max_usage_percent 大于 100: {}",
                self.node.max_usage_percent
            ));
        }
        if self.node.sample_interval_secs == 0 {
            warnings.push("node.sample_interval_secs 为 0".into());
        }
        if self.subscription.update_interval_secs == 0 {
            warnings.push("subscription.update_interval_secs 为 0".into());
        }
        if self.subscription.ttl_secs == 0 {
            warnings.push("subscription.ttl_secs 为 0".into());
        }
        if self.subscription.target_reevaluate_secs == 0 {
            warnings.push("subscription.target_reevaluate_secs 为 0".into());
        }
        if self.subscription.ttl_secs <= self.subscription.update_interval_secs {
            warnings.push(format!(
                "subscription.ttl_secs ({}) 应大于 update_interval_secs ({})",
                self.subscription.ttl_secs, self.subscription.update_interval_secs
            ));
        }
        if self.subscription.target_reevaluate_secs > self.subscription.ttl_secs {
            warnings.push(format!(
                "subscription.target_reevaluate_secs ({}) 大于 ttl_secs ({})",
                self.subscription.target_reevaluate_secs, self.subscription.ttl_secs
            ));
        }

        for entry in &self.node.toolchains {
            if entry.rustc.trim().is_empty() {
                warnings.push("node.toolchains[].rustc 为空".into());
            }
        }

        warnings
    }
}

fn write_atomic(path: &Path, content: &str) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| ConfigError::io(format!("create {}: {error}", parent.display())))?;
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    let temp = path.with_file_name(format!(".{file_name}.tmp"));

    fs::write(&temp, content)
        .map_err(|error| ConfigError::io(format!("write {}: {error}", temp.display())))?;

    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(ConfigError::io(format!(
                "rename {}: {error}",
                path.display()
            )))
        }
    }
}

#[derive(Clone)]
pub struct ConfigHandle {
    inner: Arc<RwLock<ServiceConfig>>,
    path: Option<Arc<PathBuf>>,
}

impl ConfigHandle {
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(config)),
            path: None,
        }
    }

    pub fn try_from_toml_file(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref().to_path_buf();
        let config = ServiceConfig::try_from_toml_file(&path, create_if_missing)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(config)),
            path: Some(Arc::new(path)),
        })
    }

    pub fn load_or_create_default() -> Result<Self, ConfigError> {
        Self::try_from_toml_file(paths::config_path(), true)
    }

    /// Re-read the bound file and replace the in-memory config.
    ///
    /// Returns an error without touching the current config when the file
    /// cannot be read or parsed.
    pub fn reload_from_file(&self) -> Result<(), ConfigError> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        if !path.exists() {
            return Err(ConfigError::io(format!(
                "config file is missing: {}",
                path.display()
            )));
        }
        let config = ServiceConfig::try_from_toml_file(path.as_path(), false)?;
        *self.inner.write().expect("RwLock 被污染") = config;
        Ok(())
    }

    /// Path this handle was loaded from, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref().map(PathBuf::as_path)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, ServiceConfig> {
        self.inner.read().expect("RwLock 被污染")
    }

    pub fn snapshot(&self) -> ServiceConfig {
        self.read().clone()
    }

    /// Save a snapshot to an explicit path. Intended for installer CLI use.
    pub fn save_to_path(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let snapshot = self.snapshot();
        snapshot.save_to_file(path)
    }

    // ========== Convenience read-only methods ==========

    pub fn port(&self) -> u32 {
        self.read().port
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn legacy_config_uses_defaults_without_rewriting() {
        let config: ServiceConfig = toml::from_str("port = 5100").unwrap();
        assert_eq!(config.port, 5100);
        assert_eq!(config.service_name, "rustc-pool");
        assert_eq!(config.node.max_usage_percent, 80);
        assert_eq!(config.node.sample_interval_secs, 1);
        assert_eq!(config.subscription.target_slots, 8);
        assert_eq!(config.subscription.update_interval_secs, 30);
        assert_eq!(config.subscription.ttl_secs, 60);
        assert_eq!(config.subscription.target_reevaluate_secs, 5);
        assert_eq!(config.cache.result_cache_mb, 512);
        assert!(config.cache.dir.is_empty());

        let root = TempDir::new("config");
        let path = root.join("config.toml");
        let original = r#"# keep me
port = 6100

[node]
cores_per_slot = 2.0
mem_per_slot_mb = 4096
reserve_cores = 1.0
reserve_mem_mb = 2048
sample_interval_secs = 3
rustc = "rustc-custom"
toolchain = "old-toolchain"
target = "old-target"

[subscription]
target_slots = 12
static_peers = ["peer-a"]

[cache]
dir = "/tmp/cache"
result_cache_mb = 128
"#;
        fs::write(&path, original).unwrap();

        let config = ServiceConfig::try_from_toml_file(&path, true).unwrap();
        assert_eq!(config.port, 6100);
        assert_eq!(config.service_name, "rustc-pool");
        assert_eq!(config.node.max_usage_percent, 80);
        assert_eq!(config.node.sample_interval_secs, 3);
        assert_eq!(config.node.rustc, "rustc-custom");
        assert_eq!(config.subscription.target_slots, 12);
        assert_eq!(config.cache.dir, "/tmp/cache");
        assert_eq!(config.cache.result_cache_mb, 128);
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert!(!root.join("config.toml.bak").exists());
    }

    #[test]
    fn missing_file_is_created_only_when_requested() {
        let root = TempDir::new("config");
        let path = root.join("config.toml");

        let config = ServiceConfig::try_from_toml_file(&path, false).unwrap();
        assert_eq!(config.port, 5100);
        assert!(!path.exists());

        let created = ServiceConfig::try_from_toml_file(&path, true).unwrap();
        assert_eq!(created.port, 5100);
        assert!(path.is_file());
    }

    #[test]
    fn invalid_config_does_not_replace_or_rename() {
        let root = TempDir::new("config-invalid");
        let path = root.join("config.toml");
        let original = "[node\n";
        fs::write(&path, original).unwrap();

        let error = ServiceConfig::try_from_toml_file(&path, true).unwrap_err();
        assert_eq!(error.kind, ConfigErrorKind::Parse, "case: parse error");
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        assert!(!root.join("config.toml.bak").exists());

        let root = TempDir::new("config-reload");
        let path = root.join("config.toml");
        fs::write(&path, "port = 6100\n").unwrap();
        let handle = ConfigHandle::try_from_toml_file(&path, true).unwrap();
        assert_eq!(handle.port(), 6100);

        fs::write(&path, "port = [\n").unwrap();
        assert!(handle.reload_from_file().is_err(), "case: reload bad");
        assert_eq!(handle.port(), 6100);

        fs::write(&path, "port = 6200\n").unwrap();
        handle.reload_from_file().unwrap();
        assert_eq!(handle.port(), 6200, "case: reload good");
    }

    #[test]
    fn toolchain_list_roundtrips() {
        let mut config = ServiceConfig::default();
        config.node.toolchains = vec![
            ToolchainEntry {
                channel: "stable".into(),
                rustc: "/opt/stable/bin/rustc".into(),
                target: "x86_64-unknown-linux-gnu".into(),
            },
            ToolchainEntry {
                channel: "1.99.0".into(),
                rustc: "/opt/199/bin/rustc".into(),
                target: String::new(),
            },
        ];

        let text = toml::to_string_pretty(&config).unwrap();
        let parsed: ServiceConfig = toml::from_str(&text).unwrap();
        assert_eq!(parsed.node.toolchains, config.node.toolchains);
    }
}
