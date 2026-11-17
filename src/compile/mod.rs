pub mod bundle;
pub mod cache;
pub mod cas;
pub mod executor;
pub mod hash;
pub mod manifest;

use std::fmt;

use crate::compile_protocol::{Hash32, OutputRef};

/// One completed or failed compile task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    pub action_digest: Hash32,
    pub exit_code: i32,
    pub cache_hit: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration_ms: u64,
    pub outputs: Vec<OutputRef>,
}

/// A classified task failure suitable for protocol mapping.
#[derive(Debug, Clone)]
pub struct TaskError {
    pub code: &'static str,
    pub message: String,
}

impl TaskError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TaskError {}

/// Origin of a task execution, used for diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSource {
    Local,
    Provider,
}

impl TaskSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Provider => "provider",
        }
    }
}
