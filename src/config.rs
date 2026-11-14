use crate::log::{LogLevel, LogStruct};
use crate::paths;
use serde::{Deserialize, Serialize};
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

impl ServiceConfig {
    pub fn from_toml_file(path: impl AsRef<Path>, create_if_missing: bool) -> ServiceConfig {
        let path = path.as_ref();

        if !create_if_missing && !path.exists() {
            LogStruct::new(LogLevel::Warning, "配置文件不存在，使用默认配置", "").emit();
            return ServiceConfig::default();
        }

        match fs::read_to_string(path) {
            Ok(content) => match toml::from_str::<ServiceConfig>(&content) {
                Ok(config) => {
                    if let Err(error) = rewrite_if_needed(path, &content, &config) {
                        LogStruct::new(
                            LogLevel::Warning,
                            "配置迁移失败，继续使用内存配置",
                            error.to_string(),
                        )
                        .emit();
                    }
                    return config;
                }
                Err(e) => {
                    LogStruct::new(LogLevel::Error, "配置文件解析失败", e.to_string()).emit();
                    if let Err(rename_err) = rename_bad_config(path) {
                        LogStruct::new(
                            LogLevel::Critical,
                            "重命名损坏的配置文件失败",
                            rename_err.to_string(),
                        )
                        .emit();
                        std::process::exit(1);
                    }
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                LogStruct::new(LogLevel::Critical, "无法读取配置文件", e.to_string()).emit();
                std::process::exit(1);
            }
        }

        let default_config = ServiceConfig::default();
        if let Err(error) = write_config(path, &default_config, false) {
            LogStruct::new(
                LogLevel::Critical,
                "写入默认配置文件失败",
                error.to_string(),
            )
            .emit();
            std::process::exit(1);
        }
        default_config
    }
}

fn rewrite_if_needed(path: &Path, current: &str, config: &ServiceConfig) -> io::Result<()> {
    let serialized = toml::to_string_pretty(config)
        .map_err(|error| io::Error::other(format!("序列化配置失败: {error}")))?;
    if current == serialized {
        return Ok(());
    }
    write_config(path, config, true)
}

fn write_config(path: &Path, config: &ServiceConfig, backup: bool) -> io::Result<()> {
    let serialized = toml::to_string_pretty(config)
        .map_err(|error| io::Error::other(format!("序列化配置失败: {error}")))?;

    if backup && path.exists() {
        backup_config(path)?;
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let temp = temp_path(path);
    fs::write(&temp, serialized)?;
    match fs::rename(&temp, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error)
        }
    }
}

fn backup_config(path: &Path) -> io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    let mut backup = path.with_file_name(format!("{file_name}.bak"));
    let mut counter = 1;
    while backup.exists() {
        backup = path.with_file_name(format!("{file_name}.bak.{counter}"));
        counter += 1;
    }
    fs::copy(path, backup)?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    path.with_file_name(format!(".{file_name}.tmp"))
}

fn rename_bad_config(path: &Path) -> io::Result<()> {
    let mut backup_path = path.with_extension("bak");
    let mut counter = 1;
    while backup_path.exists() {
        backup_path = path.with_extension(format!("bak.{}", counter));
        counter += 1;
    }
    fs::rename(path, &backup_path)?;
    Ok(())
}

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

    pub fn from_toml_file(path: impl AsRef<Path>, create_if_missing: bool) -> Self {
        let config = ServiceConfig::from_toml_file(path, create_if_missing);
        Self::new(config)
    }

    pub fn load_or_create_default() -> Self {
        Self::from_toml_file(paths::config_path(), true)
    }

    pub fn read(&self) -> RwLockReadGuard<'_, ServiceConfig> {
        self.inner.read().expect("RwLock 被污染")
    }

    pub fn snapshot(&self) -> ServiceConfig {
        self.read().clone()
    }

    // ========== Convenience read-only methods ==========

    pub fn port(&self) -> u32 {
        self.read().port
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
        let path = std::env::temp_dir().join(format!(
            "nexuscompile-config-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn legacy_port_only_config_uses_defaults() {
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
    }

    #[test]
    fn legacy_config_is_rebuilt_with_new_fields() {
        let root = temp_dir();
        let path = root.join("config.toml");
        fs::write(
            &path,
            r#"
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
"#,
        )
        .unwrap();

        let config = ServiceConfig::from_toml_file(&path, true);
        assert_eq!(config.port, 6100);
        assert_eq!(config.service_name, "rustc-pool");
        assert_eq!(config.node.max_usage_percent, 80);
        assert_eq!(config.node.sample_interval_secs, 3);
        assert_eq!(config.node.rustc, "rustc-custom");
        assert_eq!(config.subscription.target_slots, 12);
        assert_eq!(config.cache.dir, "/tmp/cache");
        assert_eq!(config.cache.result_cache_mb, 128);

        let migrated = fs::read_to_string(&path).unwrap();
        assert!(migrated.contains("service_name = \"rustc-pool\""));
        assert!(migrated.contains("max_usage_percent = 80"));
        assert!(!migrated.contains("cores_per_slot"));
        assert!(!migrated.contains("mem_per_slot_mb"));
        assert!(!migrated.contains("reserve_cores"));
        assert!(!migrated.contains("reserve_mem_mb"));
        assert!(!migrated.contains("static_peers"));
        assert!(!migrated.contains("toolchain ="));
        assert!(!migrated.contains("target ="));
        assert!(root.join("config.toml.bak").is_file());

        fs::remove_dir_all(root).unwrap();
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
