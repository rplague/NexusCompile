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
pub const CACHE_SCHEMA: u16 = 1;

pub use crate::compile_protocol::OutputRef as OutputFile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedManifest {
    pub schema: u16,
    pub action_digest: Hash32,
    pub outputs: Vec<OutputFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedResult {
    pub outputs: Vec<OutputFile>,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    action_digest: Hash32,
    outputs: Vec<OutputFile>,
    pin_count: u32,
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
                pin_count: 0,
                expires_at: Instant::now() + RESULT_TTL,
            };
            inner.add_entry(entry);
        }

        let cache = Self {
            root,
            cas,
            max_bytes,
            inner: Mutex::new(inner),
            enabled: max_bytes > 0,
        };
        {
            let mut inner = cache.inner.lock().unwrap_or_else(|e| e.into_inner());
            cache.evict_to_fit_locked(&mut inner, None);
        }
        cache.remove_orphan_blobs();
        Ok(cache)
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

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    pub fn read_chunk(&self, hash: &Hash32, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.cas.read_chunk(hash, offset, len)
    }

    pub fn pin(&self, action_digest: &Hash32) -> bool {
        if !self.enabled {
            return false;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = inner.entries.get_mut(action_digest) else {
            return false;
        };
        entry.pin_count = entry.pin_count.saturating_add(1);
        entry.expires_at = Instant::now() + RESULT_TTL;
        true
    }

    pub fn release(&self, action_digest: &Hash32) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = inner.entries.get_mut(action_digest) {
            entry.pin_count = entry.pin_count.saturating_sub(1);
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner
            .lock()
            .map(|inner| inner.total_bytes)
            .unwrap_or(0)
    }

    pub fn task_dir(&self, action_digest: &Hash32) -> PathBuf {
        self.root.join("tmp").join(hex_hash(action_digest))
    }

    pub fn staging_dir(&self, action_digest: &Hash32) -> io::Result<PathBuf> {
        let dir = self
            .root
            .join("tmp")
            .join(hex_hash(action_digest))
            .join(Uuid::new_v4().to_string());
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    pub fn lookup_and_materialize(
        &self,
        action_digest: &Hash32,
        out_dir: &Path,
    ) -> Option<CachedResult> {
        if !self.enabled {
            return None;
        }
        self.sweep();

        let outputs = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let Some(entry) = inner.entries.get_mut(action_digest) else {
                return None;
            };
            if entry.expires_at <= now && entry.pin_count == 0 {
                self.remove_entry_locked(&mut inner, action_digest);
                return None;
            }
            entry.pin_count = entry.pin_count.saturating_add(1);
            entry.expires_at = now + RESULT_TTL;
            entry.outputs.clone()
        };

        let materialized = self.materialize_outputs(&outputs, out_dir);
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = inner.entries.get_mut(action_digest) {
            entry.pin_count = entry.pin_count.saturating_sub(1);
        }

        if materialized.is_err() {
            self.remove_entry_locked(&mut inner, action_digest);
            return None;
        }

        Some(CachedResult { outputs })
    }

    pub fn cached_outputs(&self, action_digest: &Hash32) -> Option<Vec<OutputFile>> {
        if !self.enabled {
            return None;
        }
        self.sweep();

        let outputs = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let Some(entry) = inner.entries.get_mut(action_digest) else {
                return None;
            };
            if entry.expires_at <= now && entry.pin_count == 0 {
                self.remove_entry_locked(&mut inner, action_digest);
                return None;
            }
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
            pin_count: 0,
            expires_at: Instant::now() + RESULT_TTL,
        });
        self.evict_to_fit_locked(&mut inner, Some(action_digest));

        let stored = inner.entries.contains_key(&action_digest);
        if !stored {
            let _ = fs::remove_dir_all(self.action_dir(&action_digest));
        }
        Ok(stored)
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

    pub fn sweep(&self) {
        if !self.enabled {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let expired: Vec<Hash32> = inner
            .entries
            .iter()
            .filter(|(_, entry)| entry.pin_count == 0 && entry.expires_at <= now)
            .map(|(digest, _)| *digest)
            .collect();
        for digest in expired {
            self.remove_entry_locked(&mut inner, &digest);
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

    fn evict_to_fit_locked(&self, inner: &mut Inner, exclude: Option<Hash32>) {
        while inner.total_bytes > self.max_bytes {
            let candidates: Vec<Hash32> = inner
                .entries
                .iter()
                .filter(|(digest, entry)| entry.pin_count == 0 && Some(**digest) != exclude)
                .map(|(digest, _)| *digest)
                .collect();
            if candidates.is_empty() {
                break;
            }
            let index = (Uuid::new_v4().as_u128() as usize) % candidates.len();
            self.remove_entry_locked(inner, &candidates[index]);
        }

        if inner.total_bytes > self.max_bytes
            && let Some(digest) = exclude
            && inner
                .entries
                .get(&digest)
                .map(|entry| entry.pin_count == 0)
                .unwrap_or(false)
        {
            self.remove_entry_locked(inner, &digest);
        }
    }

    fn remove_orphan_blobs(&self) {
        let referenced: std::collections::HashSet<String> = self
            .inner
            .lock()
            .map(|inner| inner.blob_refs.keys().map(hex_hash).collect())
            .unwrap_or_default();
        let Ok(entries) = fs::read_dir(self.cas.blobs_dir()) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !referenced.contains(&name) {
                let _ = fs::remove_file(entry.path());
            }
        }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "nexuscompile-cache-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn cache(root: &Path, max_bytes: u64) -> ResultCache {
        ResultCache::open(root, max_bytes).unwrap()
    }

    fn store_payload(cache: &ResultCache, digest: u8, payload: &[u8]) -> Hash32 {
        let staging = cache.staging_dir(&Hash32([digest; 32])).unwrap();
        fs::write(staging.join("libdemo.rlib"), payload).unwrap();
        let outputs = cache.collect_outputs(&staging).unwrap();
        let action_digest = Hash32([digest; 32]);
        cache.store(action_digest, outputs).unwrap();
        fs::remove_dir_all(staging).unwrap();
        action_digest
    }

    #[test]
    fn store_and_materialize_cache_hit() {
        let root = temp_dir();
        let cache = cache(&root, 10 * 1024 * 1024);
        let digest = store_payload(&cache, 1, b"compiled bytes");

        let out = root.join("out");
        let hit = cache.lookup_and_materialize(&digest, &out).unwrap();
        assert_eq!(hit.outputs.len(), 1);
        assert_eq!(
            fs::read(out.join("libdemo.rlib")).unwrap(),
            b"compiled bytes"
        );
        assert!(cache.total_bytes() > 0);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ttl_survives_reopen_and_resets() {
        let root = temp_dir();
        let digest = {
            let cache = cache(&root, 10 * 1024 * 1024);
            store_payload(&cache, 2, b"persistent")
        };
        let reopened = cache(&root, 10 * 1024 * 1024);
        assert!(
            reopened
                .lookup_and_materialize(&digest, &root.join("out2"))
                .is_some()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn respects_quota_and_evicts_unpinned_entries() {
        let root = temp_dir();
        let cache = cache(&root, 10);
        let first = store_payload(&cache, 3, b"123456");
        let second = store_payload(&cache, 4, b"abcdef");
        assert!(cache.total_bytes() <= 10);
        let hits = [
            cache.lookup_and_materialize(&first, &root.join("out3")),
            cache.lookup_and_materialize(&second, &root.join("out4")),
        ]
        .into_iter()
        .flatten()
        .count();
        assert_eq!(hits, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cached_outputs_peek_validates_blobs() {
        let root = temp_dir();
        let cache = cache(&root, 10 * 1024 * 1024);
        let digest = store_payload(&cache, 6, b"peek");

        let outputs = cache.cached_outputs(&digest).unwrap();
        assert_eq!(outputs.len(), 1);

        let blobs: Vec<PathBuf> = fs::read_dir(cache.cas.blobs_dir())
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .collect();
        for blob in blobs {
            fs::remove_file(blob).unwrap();
        }

        assert!(cache.cached_outputs(&digest).is_none());
        assert!(
            cache
                .lookup_and_materialize(&digest, &root.join("out"))
                .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unsafe_output_path_is_rejected() {
        let root = temp_dir();
        let cache = cache(&root, 10 * 1024 * 1024);
        let staging = cache.staging_dir(&Hash32([9; 32])).unwrap();
        let outputs = vec![OutputFile {
            path: "../escape".into(),
            hash: Hash32([1; 32]),
            size: 1,
            mode: 0o644,
        }];
        assert!(cache.materialize_outputs(&outputs, &staging).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn missing_blob_invalidates_entry() {
        let root = temp_dir();
        let cache = cache(&root, 10 * 1024 * 1024);
        let digest = store_payload(&cache, 5, b"gone");
        let blobs: Vec<PathBuf> = fs::read_dir(cache.cas.blobs_dir())
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .collect();
        for blob in blobs {
            fs::remove_file(blob).unwrap();
        }
        assert!(
            cache
                .lookup_and_materialize(&digest, &root.join("out5"))
                .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }
}
