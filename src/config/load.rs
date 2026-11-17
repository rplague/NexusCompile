use std::fs;
use std::io;
use std::path::Path;

use super::ConfigError;
use super::ServiceConfig;

/// Result of loading a configuration file.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub config: ServiceConfig,
    pub created: bool,
    pub missing: bool,
}

fn parse_config(path: &Path, content: &str) -> Result<ServiceConfig, ConfigError> {
    toml::from_str::<ServiceConfig>(content)
        .map_err(|error| ConfigError::parse(format!("parse {}: {error}", path.display())))
}

impl ServiceConfig {
    /// Load a configuration file without logging or terminating the process.
    pub fn load_or_create(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<LoadedConfig, ConfigError> {
        let path = path.as_ref();

        match fs::read_to_string(path) {
            Ok(content) => Ok(LoadedConfig {
                config: parse_config(path, &content)?,
                created: false,
                missing: false,
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !create_if_missing {
                    return Ok(LoadedConfig {
                        config: ServiceConfig::default(),
                        created: false,
                        missing: true,
                    });
                }

                let config = ServiceConfig::default();
                match config.save_to_new_file(path) {
                    Ok(()) => Ok(LoadedConfig {
                        config,
                        created: true,
                        missing: false,
                    }),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let content = fs::read_to_string(path).map_err(|error| {
                            ConfigError::io(format!("read {}: {error}", path.display()))
                        })?;
                        Ok(LoadedConfig {
                            config: parse_config(path, &content)?,
                            created: false,
                            missing: false,
                        })
                    }
                    Err(error) => Err(ConfigError::io(format!(
                        "create {}: {error}",
                        path.display()
                    ))),
                }
            }
            Err(error) => Err(ConfigError::io(format!("read {}: {error}", path.display()))),
        }
    }

    /// Load a configuration file without logging side effects.
    pub fn try_from_toml_file(
        path: impl AsRef<Path>,
        create_if_missing: bool,
    ) -> Result<ServiceConfig, ConfigError> {
        Self::load_or_create(path, create_if_missing).map(|loaded| loaded.config)
    }

    /// Serialize this configuration and replace `path` atomically.
    pub fn save_to_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let path = path.as_ref();
        let serialized = toml::to_string_pretty(self)
            .map_err(|error| ConfigError::io(format!("serialize config: {error}")))?;
        crate::util::fs::write_atomic(path, serialized.as_bytes(), None)
            .map_err(|error| ConfigError::io(format!("write {}: {error}", path.display())))
    }

    /// Serialize this configuration and create `path` only if it is absent.
    pub fn save_to_new_file(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        let serialized = toml::to_string_pretty(self)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        crate::util::fs::write_atomic_new(path, serialized.as_bytes(), None)
    }
}
