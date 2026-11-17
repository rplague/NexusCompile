use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::paths;

use super::error::ConfigError;
use super::model::ServiceConfig;

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
        let config = ServiceConfig::try_from_toml_file(path, create_if_missing)?;
        Ok(Self::new(config))
    }

    pub fn load_or_create_default() -> Result<Self, ConfigError> {
        Self::try_from_toml_file(paths::config_path(), true)
    }

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
