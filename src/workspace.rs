//! Scratch space for staging builds and provider task directories.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::compile::hash::hex_hash;
use crate::compile_protocol::Hash32;

#[derive(Debug, Clone)]
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn task_dir(&self, action_digest: &Hash32) -> PathBuf {
        self.root.join("work").join(hex_hash(action_digest))
    }

    pub fn staging_dir(&self, action_digest: &Hash32) -> io::Result<PathBuf> {
        let dir = self
            .root
            .join("staging")
            .join(hex_hash(action_digest))
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    pub fn remove_task_dir(&self, action_digest: &Hash32) {
        let _ = fs::remove_dir_all(self.task_dir(action_digest));
    }

    /// Remove stale staging directories; call once at startup.
    pub fn cleanup_staging(&self) -> io::Result<()> {
        let staging = self.root.join("staging");
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(staging)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn staging_and_task_dirs_are_separate() {
        let root = TempDir::new("workspace");
        let workspace = Workspace::new(&root);
        let digest = Hash32([1; 32]);
        let staging = workspace.staging_dir(&digest).unwrap();
        let task = workspace.task_dir(&digest);
        assert!(staging.starts_with(root.join("staging")));
        assert!(task.starts_with(root.join("work")));
        assert_ne!(staging, task);
        workspace.remove_task_dir(&digest);
        assert!(!task.exists());
    }
}
