use crate::log::{LogLevel, LogStruct};
use crate::paths;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

macro_rules! default_u32_fn {
    ($name:ident, $value:expr) => {
        fn $name() -> u32 {
            $value
        }
    };
}

default_u32_fn!(default_port, 5100);
default_u32_fn!(default_target_slots, 8);

fn default_cores_per_slot() -> f32 {
    1.0
}

fn default_mem_per_slot_mb() -> u64 {
    2048
}

fn default_reserve_cores() -> f32 {
    0.5
}

fn default_reserve_mem_mb() -> u64 {
    1024
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    #[serde(default = "default_cores_per_slot")]
    pub cores_per_slot: f32,
    #[serde(default = "default_mem_per_slot_mb")]
    pub mem_per_slot_mb: u64,
    #[serde(default = "default_reserve_cores")]
    pub reserve_cores: f32,
    #[serde(default = "default_reserve_mem_mb")]
    pub reserve_mem_mb: u64,
    #[serde(default = "default_sample_interval_secs")]
    pub sample_interval_secs: u64,
    #[serde(default = "default_rustc")]
    pub rustc: String,
    #[serde(default)]
    pub toolchain: String,
    #[serde(default)]
    pub target: String,
}

fn default_rustc() -> String {
    "rustc".into()
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            cores_per_slot: default_cores_per_slot(),
            mem_per_slot_mb: default_mem_per_slot_mb(),
            reserve_cores: default_reserve_cores(),
            reserve_mem_mb: default_reserve_mem_mb(),
            sample_interval_secs: default_sample_interval_secs(),
            rustc: default_rustc(),
            toolchain: String::new(),
            target: String::new(),
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
    #[serde(default)]
    pub static_peers: Vec<String>,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            target_slots: default_target_slots(),
            update_interval_secs: default_update_interval_secs(),
            ttl_secs: default_ttl_secs(),
            target_reevaluate_secs: default_target_reevaluate_secs(),
            static_peers: Vec::new(),
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
            Ok(content) => match toml::from_str(&content) {
                Ok(config) => return config,
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
        let toml_string = match toml::to_string_pretty(&default_config) {
            Ok(s) => s,
            Err(e) => {
                LogStruct::new(LogLevel::Critical, "序列化默认配置失败", e.to_string()).emit();
                std::process::exit(1);
            }
        };
        if let Err(e) = fs::write(path, toml_string) {
            LogStruct::new(LogLevel::Critical, "写入默认配置文件失败", e.to_string()).emit();
            std::process::exit(1);
        }
        default_config
    }
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

    pub fn save_to_file(&self, path: impl AsRef<Path>) {
        let path = path.as_ref();
        let snapshot = self.snapshot();
        let toml_string = match toml::to_string_pretty(&snapshot) {
            Ok(s) => s,
            Err(e) => {
                LogStruct::new(LogLevel::Critical, "序列化配置失败", e.to_string()).emit();
                std::process::exit(1);
            }
        };

        let temp_path = path.with_extension("tmp");

        if let Err(e) = fs::write(&temp_path, toml_string) {
            LogStruct::new(
                LogLevel::Critical,
                "写入临时配置文件失败",
                format!("路径: {}, 错误: {}", temp_path.display(), e),
            )
            .emit();
            std::process::exit(1);
        }

        if let Err(e) = fs::rename(&temp_path, path) {
            let _ = fs::remove_file(&temp_path);
            LogStruct::new(
                LogLevel::Critical,
                "重命名配置文件失败",
                format!(
                    "从 {} 到 {}, 错误: {}",
                    temp_path.display(),
                    path.display(),
                    e
                ),
            )
            .emit();
            std::process::exit(1);
        }
    }

    pub fn save_to_default(&self) {
        self.save_to_file(paths::config_path());
    }

    pub fn read(&self) -> RwLockReadGuard<'_, ServiceConfig> {
        self.inner.read().expect("RwLock 被污染")
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, ServiceConfig> {
        self.inner.write().expect("RwLock 被污染")
    }

    pub fn replace_config(&self, new_config: ServiceConfig) {
        *self.write() = new_config;
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

    #[test]
    fn legacy_port_only_config_uses_defaults() {
        let config: ServiceConfig = toml::from_str("port = 5100").unwrap();
        assert_eq!(config.port, 5100);
        assert_eq!(config.node.mem_per_slot_mb, 2048);
        assert_eq!(config.node.cores_per_slot, 1.0);
        assert_eq!(config.subscription.target_slots, 8);
        assert_eq!(config.subscription.update_interval_secs, 30);
        assert_eq!(config.subscription.ttl_secs, 60);
        assert_eq!(config.cache.result_cache_mb, 512);
        assert!(config.cache.dir.is_empty());
    }
}
