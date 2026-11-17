//! Service configuration model, loading, validation and handle.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use serde::{Deserialize, Serialize};

use crate::log::{LogLevel, LogStruct};
use crate::paths;

mod load;
pub use load::LoadedConfig;

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

fn default_rustc() -> String {
    "rustc".into()
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

impl ServiceConfig {
    /// Cache root, using the path resolver when `cache.dir` is empty.
    pub fn cache_dir(&self) -> PathBuf {
        let dir = self.cache.dir.trim();
        if dir.is_empty() {
            paths::cache_path()
        } else {
            PathBuf::from(dir)
        }
    }
}


/// Kind of configuration loading failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigErrorKind {
    Io,
    Parse,
}

/// Configuration loading failure that does not terminate the process.
#[derive(Debug, Clone)]
pub struct ConfigError {
    pub kind: ConfigErrorKind,
    pub message: String,
}

impl ConfigError {
    pub(crate) fn io(message: impl Into<String>) -> Self {
        Self::new(ConfigErrorKind::Io, message)
    }

    pub(crate) fn parse(message: impl Into<String>) -> Self {
        Self::new(ConfigErrorKind::Parse, message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}


#[derive(Clone)]
pub struct ConfigHandle {
    inner: Arc<RwLock<ServiceConfig>>,
}

impl ConfigHandle {
    pub fn new(config: ServiceConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(config)),
        }
    }

    pub fn try_from_toml_file(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let loaded = ServiceConfig::load_or_create(path, create_if_missing)?;
        if loaded.created {
            LogStruct::new(
                LogLevel::Important,
                "配置文件已创建",
                path.display().to_string(),
            )
            .emit();
        } else if loaded.missing {
            LogStruct::new(
                LogLevel::Warning,
                "配置文件不存在，使用默认配置",
                path.display().to_string(),
            )
            .emit();
        }
        Ok(Self::new(loaded.config))
    }

    pub fn load_or_create_default() -> Result<Self, ConfigError> {
        Self::try_from_toml_file(paths::config_path(), true)
    }

    /// Read the current configuration.
    ///
    /// Lock poisoning is not recoverable in this process; the process is
    /// expected to restart rather than continue with partially observed state.
    pub fn read(&self) -> RwLockReadGuard<'_, ServiceConfig> {
        self.inner.read().expect("RwLock 被污染")
    }

    pub fn snapshot(&self) -> ServiceConfig {
        self.read().clone()
    }

    pub fn port(&self) -> u32 {
        self.read().port
    }
}


/// One non-fatal configuration warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigWarning {
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for ConfigWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl ServiceConfig {
    /// Non-fatal configuration warnings. These do not reject the file.
    pub fn warnings(&self) -> Vec<ConfigWarning> {
        let mut warnings = Vec::new();

        if self.port == 0 || self.port > u16::MAX as u32 {
            warnings.push(ConfigWarning {
                code: "port",
                message: format!("port 超出范围 1..={}: {}", u16::MAX, self.port),
            });
        }
        if self.service_name.trim().is_empty() {
            warnings.push(ConfigWarning {
                code: "service_name",
                message: "service_name 为空".into(),
            });
        }
        if self.node.max_usage_percent > 100 {
            warnings.push(ConfigWarning {
                code: "max_usage_percent",
                message: format!(
                    "node.max_usage_percent 大于 100: {}",
                    self.node.max_usage_percent
                ),
            });
        }
        if self.node.sample_interval_secs == 0 {
            warnings.push(ConfigWarning {
                code: "sample_interval_secs",
                message: "node.sample_interval_secs 为 0".into(),
            });
        }
        if self.subscription.update_interval_secs == 0 {
            warnings.push(ConfigWarning {
                code: "update_interval_secs",
                message: "subscription.update_interval_secs 为 0".into(),
            });
        }
        if self.subscription.ttl_secs == 0 {
            warnings.push(ConfigWarning {
                code: "ttl_secs",
                message: "subscription.ttl_secs 为 0".into(),
            });
        }
        if self.subscription.target_reevaluate_secs == 0 {
            warnings.push(ConfigWarning {
                code: "target_reevaluate_secs",
                message: "subscription.target_reevaluate_secs 为 0".into(),
            });
        }
        if self.subscription.ttl_secs <= self.subscription.update_interval_secs {
            warnings.push(ConfigWarning {
                code: "ttl_vs_update",
                message: format!(
                    "subscription.ttl_secs ({}) 应大于 update_interval_secs ({})",
                    self.subscription.ttl_secs, self.subscription.update_interval_secs
                ),
            });
        }
        if self.subscription.target_reevaluate_secs > self.subscription.ttl_secs {
            warnings.push(ConfigWarning {
                code: "reevaluate_vs_ttl",
                message: format!(
                    "subscription.target_reevaluate_secs ({}) 大于 ttl_secs ({})",
                    self.subscription.target_reevaluate_secs, self.subscription.ttl_secs
                ),
            });
        }

        for entry in &self.node.toolchains {
            if entry.rustc.trim().is_empty() {
                warnings.push(ConfigWarning {
                    code: "toolchain_rustc",
                    message: "node.toolchains[].rustc 为空".into(),
                });
            }
        }

        warnings
    }
}


#[cfg(test)]
mod tests {
    use std::fs;

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

    let root = TempDir::new("config-load");
    let path = root.join("config.toml");
    fs::write(&path, "port = 6100\n").unwrap();
    let loaded = ServiceConfig::load_or_create(&path, false).unwrap();
    assert!(!loaded.created);
    assert!(!loaded.missing);
    assert_eq!(loaded.config.port, 6100);

    let missing = ServiceConfig::load_or_create(root.join("missing.toml"), false).unwrap();
    assert!(missing.missing);
    assert!(!missing.created);
    assert_eq!(missing.config.port, 5100);
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

#[test]
fn warnings_have_stable_codes() {
    let config = ServiceConfig {
        port: 0,
        subscription: SubscriptionConfig {
            ttl_secs: 0,
            ..Default::default()
        },
        ..Default::default()
    };

    let warnings = config.warnings();
    assert!(warnings.iter().any(|warning| warning.code == "port"));
    assert!(warnings.iter().any(|warning| warning.code == "ttl_secs"));
    assert!(warnings.iter().all(|warning| !warning.message.is_empty()));
}
