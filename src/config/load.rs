use std::fs;
use std::io;
use std::path::Path;

use crate::log::{LogLevel, LogStruct};

use super::error::ConfigError;
use super::model::ServiceConfig;

/// Result of loading a configuration file.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: ServiceConfig,
    pub created: bool,
    pub missing: bool,
}

impl ServiceConfig {
    /// Load a configuration file without logging or terminating the process.
    pub fn load_or_create(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<LoadedConfig, ConfigError> {
        let path = path.as_ref();

        match fs::read_to_string(path) {
            Ok(content) => {
                let config = toml::from_str::<ServiceConfig>(&content).map_err(|error| {
                    ConfigError::parse(format!("parse {}: {error}", path.display()))
                })?;
                Ok(LoadedConfig {
                    config,
                    created: false,
                    missing: false,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !create_if_missing {
                    return Ok(LoadedConfig {
                        config: ServiceConfig::default(),
                        created: false,
                        missing: true,
                    });
                }

                let config = ServiceConfig::default();
                config.save_to_file(path)?;
                Ok(LoadedConfig {
                    config,
                    created: true,
                    missing: false,
                })
            }
            Err(error) => Err(ConfigError::io(format!("read {}: {error}", path.display()))),
        }
    }

    /// Load a configuration file, logging missing/default creation outcomes.
    pub fn try_from_toml_file(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<ServiceConfig, ConfigError> {
        let path = path.as_ref();
        let loaded = Self::load_or_create(path, create_if_missing)?;
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
        Ok(loaded.config)
    }

    /// Serialize this configuration and replace `path` atomically.
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let path = path.as_ref();
        let serialized = toml::to_string_pretty(self)
            .map_err(|error| ConfigError::io(format!("serialize config: {error}")))?;
        crate::util::fs::write_atomic(path, serialized.as_bytes(), None)
            .map_err(|error| ConfigError::io(format!("write {}: {error}", path.display())))
    }
}
