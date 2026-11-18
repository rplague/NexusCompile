//! Local result cache keyed by action digest.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::compile::cas::Cas;
use crate::compile::hash::hex_hash;
use crate::compile_protocol::Hash32;
use crate::util::fs::file_mode;
use crate::util::path::{safe_join, safe_relative};

pub const RESULT_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub const CACHE_SCHEMA: u16 = 2;

pub use crate::compile_protocol::OutputRef as OutputFile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedManifest {
    pub schema: u16,
    pub action_digest: Hash32,
    pub outputs: Vec<OutputFile>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    action_digest: Hash32,
    outputs: Vec<OutputFile>,
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct Inner {
    entries: HashMap<Hash32, CacheEntry>,
    blob_refs: HashMap<Hash32, u32>,
    total_bytes: u64,
}

impl Inner {
    fn add_entry(&mut self, entry: CacheEntry) {
        for output in &entry.outputs {
            let count = self.blob_refs.entry(output.hash).or_insert(0);
            if *count == 0 {
                self.total_bytes = self.total_bytes.saturating_add(output.size);
            }
            *count = count.saturating_add(1);
        }
        self.entries.insert(entry.action_digest, entry);
    }
}

#[derive(Debug)]
pub struct ResultCache {
    root: PathBuf,
    cas: Cas,
    max_bytes: u64,
    inner: Mutex<Inner>,
    enabled: bool,
}

impl ResultCache {
    pub fn open(root: impl Into<PathBuf>, max_bytes: u64) -> io::Result<Self> {
        let root = absolute_path(root.into())?;
        let cas = Cas::new(root.clone());
        fs::create_dir_all(root.join("actions"))?;
        fs::create_dir_all(cas.blobs_dir())?;
        cas.clean_tmp()?;

        let mut inner = Inner::default();
        for entry in fs::read_dir(root.join("actions"))? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let manifest_path = path.join("manifest.cbor");
            let manifest = match read_manifest(&manifest_path) {
                Ok(manifest) => manifest,
                Err(_) => {
                    let _ = fs::remove_dir_all(&path);
                    continue;
                }
            };
            if manifest.schema != CACHE_SCHEMA {
                let _ = fs::remove_dir_all(&path);
                continue;
            }
            if path.file_name().and_then(|name| name.to_str())
                != Some(hex_hash(&manifest.action_digest).as_str())
            {
                let _ = fs::remove_dir_all(&path);
                continue;
            }
            if manifest
                .outputs
                .iter()
                .any(|output| safe_relative(&output.path).is_none() || !cas.has(&output.hash))
            {
                let _ = fs::remove_dir_all(&path);
                continue;
            }

            let entry = CacheEntry {
                action_digest: manifest.action_digest,
                outputs: manifest.outputs,
                expires_at: Instant::now() + RESULT_TTL,
            };
            inner.add_entry(entry);
        }

        Ok(Self {
            root,
            cas,
            max_bytes,
            inner: Mutex::new(inner),
            enabled: max_bytes > 0,
        })
    }

    pub fn disabled(root: impl Into<PathBuf>) -> Self {
        let root = absolute_path(root.into()).unwrap_or_else(|_| std::env::temp_dir());
        let cas = Cas::new(root.clone());
        let _ = fs::create_dir_all(root.join("tmp"));
        Self {
            root,
            cas,
            max_bytes: 0,
            inner: Mutex::new(Inner::default()),
            enabled: false,
        }
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn read_chunk(&self, hash: &Hash32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.cas.read_chunk(hash, offset, len)
    }

    /// 命中即续期；查询时顺手删掉过期条目。
    pub fn lookup(&self, action_digest: &Hash32) -> Option<Vec<OutputFile>> {
        if !self.enabled {
            return None;
        }

        let now = Instant::now();
        let outputs = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let expired: Vec<Hash32> = inner
                .entries
                .iter()
                .filter(|(_, entry)| entry.expires_at <= now)
                .map(|(digest, _)| *digest)
                .collect();
            for digest in expired {
                self.remove_entry_locked(&mut inner, &digest);
            }

            let entry = inner.entries.get_mut(action_digest)?;
            entry.expires_at = now + RESULT_TTL;
            entry.outputs.clone()
        };

        if outputs.iter().any(|output| !self.cas.has(&output.hash)) {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            self.remove_entry_locked(&mut inner, action_digest);
            return None;
        }

        Some(outputs)
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner
            .lock()
            .map(|inner| inner.total_bytes)
            .unwrap_or(0)
    }

    pub fn store(&self, action_digest: Hash32, outputs: Vec<OutputFile>) -> io::Result<bool> {
        if !self.enabled {
            return Ok(false);
        }
        if outputs.is_empty()
            || outputs
                .iter()
                .any(|output| safe_relative(&output.path).is_none())
        {
            return Ok(false);
        }

        if self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .contains_key(&action_digest)
        {
            return Ok(true);
        }

        let manifest = CachedManifest {
            schema: CACHE_SCHEMA,
            action_digest,
            outputs: outputs.clone(),
        };
        write_manifest(&self.root, &manifest)?;

        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.entries.contains_key(&action_digest) {
            return Ok(true);
        }
        inner.add_entry(CacheEntry {
            action_digest,
            outputs,
            expires_at: Instant::now() + RESULT_TTL,
        });
        Ok(true)
    }

    pub fn collect_outputs(&self, staging_out: &Path) -> io::Result<Vec<OutputFile>> {
        let mut outputs = Vec::new();
        for entry in WalkDir::new(staging_out).follow_links(false) {
            let entry = entry.map_err(io::Error::other)?;
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            let relative = path
                .strip_prefix(staging_out)
                .map_err(io::Error::other)?
                .to_string_lossy()
                .replace('\\', "/");
            if safe_relative(&relative).is_none() {
                continue;
            }
            let blob = self.cas.put_file(path)?;
            outputs.push(OutputFile {
                path: relative,
                hash: blob.hash,
                size: blob.size,
                mode: file_mode(path),
            });
        }
        outputs.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(outputs)
    }

    pub fn materialize_outputs(&self, outputs: &[OutputFile], out_dir: &Path) -> io::Result<()> {
        for output in outputs {
            let target = safe_join(out_dir, &output.path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unsafe output path"))?;
            self.cas.materialize(&output.hash, &target, output.mode)?;
        }
        Ok(())
    }

    /// 超过配额时按最久未用（到期时间最早）删除。
    pub fn evict(&self) {
        if !self.enabled {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        while inner.total_bytes > self.max_bytes {
            let victim = inner
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(digest, _)| *digest);
            let Some(victim) = victim else {
                break;
            };
            self.remove_entry_locked(&mut inner, &victim);
        }
    }

    fn action_dir(&self, action_digest: &Hash32) -> PathBuf {
        self.root.join("actions").join(hex_hash(action_digest))
    }

    fn remove_entry_locked(&self, inner: &mut Inner, action_digest: &Hash32) -> Option<CacheEntry> {
        let entry = inner.entries.remove(action_digest)?;
        let _ = fs::remove_dir_all(self.action_dir(action_digest));
        for output in &entry.outputs {
            let remove_blob = match inner.blob_refs.get_mut(&output.hash) {
                Some(count) => {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        inner.blob_refs.remove(&output.hash);
                        inner.total_bytes = inner.total_bytes.saturating_sub(output.size);
                        true
                    } else {
                        false
                    }
                }
                None => false,
            };
            if remove_blob {
                let _ = self.cas.remove(&output.hash);
            }
        }
        Some(entry)
    }
}

fn absolute_path(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn read_manifest(path: &Path) -> io::Result<CachedManifest> {
    let bytes = fs::read(path)?;
    ciborium::de::from_reader(bytes.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn write_manifest(root: &Path, manifest: &CachedManifest) -> io::Result<()> {
    let dir = root.join("actions").join(hex_hash(&manifest.action_digest));
    fs::create_dir_all(&dir)?;
    let temp = dir.join(format!("manifest-{}.tmp", Uuid::new_v4()));
    let mut bytes = Vec::new();
    ciborium::ser::into_writer(manifest, &mut bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    fs::write(&temp, bytes)?;
    fs::rename(&temp, dir.join("manifest.cbor"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TempDir;

    fn cache(root: &Path, max_bytes: u64) -> ResultCache {
        ResultCache::open(root, max_bytes).unwrap()
    }

    fn store_payload(cache: &ResultCache, digest: u8, payload: &[u8]) -> Hash32 {
        let workspace = crate::workspace::Workspace::new(cache.root());
        let staging = workspace.staging_dir(&Hash32([digest; 32])).unwrap();
        fs::write(staging.join("libdemo.rlib"), payload).unwrap();
        let outputs = cache.collect_outputs(&staging).unwrap();
        let action_digest = Hash32([digest; 32]);
        cache.store(action_digest, outputs).unwrap();
        fs::remove_dir_all(staging).unwrap();
        action_digest
    }

    #[test]
    fn store_reopen_and_materialize() {
        let root = TempDir::new("cache");
        let digest = {
            let cache = cache(&root, 10 * 1024 * 1024);
            let digest = store_payload(&cache, 1, b"compiled bytes");
            let out = root.join("out");
            let outputs = cache.lookup(&digest).expect("hit");
            assert_eq!(outputs.len(), 1);
            cache.materialize_outputs(&outputs, &out).unwrap();
            assert_eq!(
                fs::read(out.join("libdemo.rlib")).unwrap(),
                b"compiled bytes"
            );
            assert!(cache.total_bytes() > 0);
            digest
        };

        let reopened = cache(&root, 10 * 1024 * 1024);
        assert!(reopened.lookup(&digest).is_some());
    }

    #[test]
    fn quota_eviction_removes_oldest() {
        let root = TempDir::new("cache-quota");
        let cache = cache(&root, 10);
        let first = store_payload(&cache, 3, b"123456");
        let second = store_payload(&cache, 4, b"abcdef");
        assert!(cache.total_bytes() > 10);

        cache.evict();

        assert!(cache.total_bytes() <= 10);
        let hits = [cache.lookup(&first), cache.lookup(&second)]
            .into_iter()
            .flatten()
            .count();
        assert_eq!(hits, 1);
    }

    #[test]
    fn expired_entry_is_removed_on_lookup() {
        let root = TempDir::new("cache-expired");
        let cache = cache(&root, 10 * 1024 * 1024);
        let digest = store_payload(&cache, 6, b"stale");
        {
            let mut inner = cache.inner.lock().unwrap();
            inner.entries.get_mut(&digest).unwrap().expires_at =
                Instant::now() - Duration::from_secs(1);
        }
        assert!(cache.lookup(&digest).is_none());
        assert!(cache.inner.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn unsafe_output_path_is_rejected() {
        let root = TempDir::new("cache");
        let cache = cache(&root, 10 * 1024 * 1024);
        let workspace = crate::workspace::Workspace::new(cache.root());
        let staging = workspace.staging_dir(&Hash32([9; 32])).unwrap();

        for path in ["../escape", "/tmp/escape", "a/../../escape"] {
            let outputs = vec![OutputFile {
                path: path.into(),
                hash: Hash32([1; 32]),
                size: 1,
                mode: 0o644,
            }];
            assert!(
                cache.materialize_outputs(&outputs, &staging).is_err(),
                "case: {path}"
            );
        }
    }

    #[test]
    fn blob_validation_invalidates_entry() {
        let root = TempDir::new("cache");
        let cache = cache(&root, 10 * 1024 * 1024);
        let digest = store_payload(&cache, 5, b"gone");

        let outputs = cache.lookup(&digest).unwrap();
        assert_eq!(outputs.len(), 1);

        let blobs: Vec<PathBuf> = fs::read_dir(cache.cas.blobs_dir())
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .collect();
        for blob in blobs {
            fs::remove_file(blob).unwrap();
        }

        assert!(cache.lookup(&digest).is_none());
        assert!(cache.lookup(&digest).is_none());
    }
}
