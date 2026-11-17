use std::fs;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn file_mode(path: &Path) -> u32 {
    fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o777)
        .unwrap_or(0o644)
}

pub fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

/// Write `bytes` through a uniquely named sibling file and atomically replace
/// `path`. The temporary file is removed when the write fails.
pub fn write_atomic(
    path: &Path,
    bytes: &[u8],
    permissions: Option<fs::Permissions>,
) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let temp = temp_path(path);
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        if let Some(permissions) = permissions {
            fs::set_permissions(&temp, permissions)?;
        }
        fs::rename(&temp, path)
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }

    result
}

fn temp_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("file");
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.with_file_name(format!(
        ".{file_name}.{}.{counter}.nexuscompile.tmp",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn atomic_write_replaces_content_and_preserves_permissions() {
        let dir = TempDir::new("util-fs");
        let path = dir.join("config.toml");
        fs::write(&path, "old").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        write_atomic(&path, b"new", Some(fs::Permissions::from_mode(0o600))).unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(file_mode(&path), 0o600);
        assert_eq!(
            fs::read_dir(&dir).unwrap().filter_map(Result::ok).count(),
            1,
            "temporary file must be renamed or cleaned up"
        );
    }

    #[test]
    fn atomic_write_cleans_temp_on_failure() {
        let dir = TempDir::new("util-fs-fail");
        let blocker = dir.join("blocker");
        fs::write(&blocker, "file").unwrap();
        let path = blocker.join("child.toml");

        assert!(write_atomic(&path, b"data", None).is_err());
        assert_eq!(fs::read_to_string(&blocker).unwrap(), "file");
    }
}
