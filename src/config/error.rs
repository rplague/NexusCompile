use std::fmt;

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
    pub(crate) fn new(kind: ConfigErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

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
