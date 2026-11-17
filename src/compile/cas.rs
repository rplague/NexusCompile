//! Local content-addressed blob storage.

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::compile::hash::{hash_bytes, hash_file, hex_hash};
use crate::compile_protocol::{BlobRef, Hash32};
use crate::util::fs::set_mode;

#[derive(Debug, Clone)]
pub struct Cas {
    root: PathBuf,
}

impl Cas {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub fn blob_path(&self, hash: &Hash32) -> PathBuf {
        self.blobs_dir().join(hex_hash(hash))
    }

    pub fn has(&self, hash: &Hash32) -> bool {
        self.blob_path(hash).is_file()
    }

    pub fn put_file(&self, source: &Path) -> io::Result<BlobRef> {
        let (hash, size) = hash_file(source)?;
        let target = self.blob_path(&hash);
        if target.is_file() {
            return Ok(BlobRef { hash, size });
        }

        fs::create_dir_all(self.blobs_dir())?;
        fs::create_dir_all(self.tmp_dir())?;
        let temp = self.tmp_dir().join(format!("blob-{}.tmp", Uuid::new_v4()));
        fs::copy(source, &temp)?;
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let _ = fs::remove_file(&temp);
                if target.is_file() {
                    return Ok(BlobRef { hash, size });
                }
                return Err(error);
            }
        }
        Ok(BlobRef { hash, size })
    }

    pub fn put_bytes(&self, bytes: &[u8]) -> io::Result<BlobRef> {
        let hash = hash_bytes(bytes);
        let size = bytes.len() as u64;
        let target = self.blob_path(&hash);
        if target.is_file() {
            return Ok(BlobRef { hash, size });
        }

        fs::create_dir_all(self.blobs_dir())?;
        fs::create_dir_all(self.tmp_dir())?;
        let temp = self.tmp_dir().join(format!("blob-{}.tmp", Uuid::new_v4()));
        fs::write(&temp, bytes)?;
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let _ = fs::remove_file(&temp);
                if target.is_file() {
                    return Ok(BlobRef { hash, size });
                }
                return Err(error);
            }
        }
        Ok(BlobRef { hash, size })
    }

    pub fn read_chunk(&self, hash: &Hash32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        use std::io::{Seek, SeekFrom};
        let mut file = File::open(self.blob_path(hash))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; len];
        let mut read_total = 0usize;
        while read_total < len {
            let read = file.read(&mut data[read_total..])?;
            if read == 0 {
                break;
            }
            read_total += read;
        }
        data.truncate(read_total);
        Ok(data)
    }

    pub fn get_bytes(&self, hash: &Hash32) -> io::Result<Vec<u8>> {
        let path = self.blob_path(hash);
        let bytes = fs::read(&path)?;
        if hash_bytes(&bytes) != *hash {
            let _ = fs::remove_file(&path);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "blob hash mismatch",
            ));
        }
        Ok(bytes)
    }

    pub fn materialize(&self, hash: &Hash32, target: &Path, mode: u32) -> io::Result<()> {
        let source_path = self.blob_path(hash);
        let mut source = File::open(&source_path)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }

        let file_name = target
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "blob".into());
        let temp = target.with_file_name(format!(".{file_name}.nexus-tmp-{}", Uuid::new_v4()));
        let mut output = File::create(&temp)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 64 * 1024];

        loop {
            let read = match source.read(&mut buffer) {
                Ok(read) => read,
                Err(error) => {
                    let _ = fs::remove_file(&temp);
                    return Err(error);
                }
            };
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            if let Err(error) = output.write_all(&buffer[..read]) {
                let _ = fs::remove_file(&temp);
                return Err(error);
            }
        }

        if Hash32(hasher.finalize().into()) != *hash {
            let _ = fs::remove_file(&temp);
            let _ = fs::remove_file(&source_path);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "blob hash mismatch",
            ));
        }

        set_mode(&temp, mode)?;
        fs::rename(&temp, target)?;
        Ok(())
    }

    pub fn remove(&self, hash: &Hash32) -> io::Result<()> {
        match fs::remove_file(self.blob_path(hash)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn clean_tmp(&self) -> io::Result<()> {
        let tmp = self.tmp_dir();
        if tmp.exists() {
            fs::remove_dir_all(&tmp)?;
        }
        fs::create_dir_all(tmp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    #[test]
    fn put_get_and_deduplicate() {
        let root = TempDir::new("cas");
        let cas = Cas::new(&root);
        let first = cas.put_bytes(b"hello").unwrap();
        let second = cas.put_bytes(b"hello").unwrap();
        assert_eq!(first, second);
        assert_eq!(cas.get_bytes(&first.hash).unwrap(), b"hello");
        assert!(cas.has(&first.hash));
    }

    #[test]
    fn materialize_verifies_and_copies() {
        let root = TempDir::new("cas");
        let cas = Cas::new(&root);
        let blob = cas.put_bytes(b"payload").unwrap();
        let target = root.join("out/file.rlib");
        cas.materialize(&blob.hash, &target, 0o644).unwrap();
        assert_eq!(fs::read(target).unwrap(), b"payload");
    }

    #[test]
    fn corrupt_blob_is_rejected() {
        let root = TempDir::new("cas");
        let cas = Cas::new(&root);
        let blob = cas.put_bytes(b"payload").unwrap();
        fs::write(cas.blob_path(&blob.hash), b"broken").unwrap();
        assert!(cas.get_bytes(&blob.hash).is_err());
        assert!(!cas.has(&blob.hash));
    }
}
