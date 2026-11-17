use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::paths;

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
