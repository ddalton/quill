use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::cas::CasLayout;
use crate::digest::Digest;

/// Metadata for a single locally-pushed tag.
///
/// A tag listed here is served from local CAS unconditionally — Quill never
/// contacts upstream to revalidate it. See plan §5.6.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTagMeta {
    pub digest: String,
    pub pushed_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum LocalTagsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Default, Serialize, Deserialize)]
struct OnDiskTags(HashMap<String, LocalTagMeta>);

/// In-memory cache of `_local_tags.json` files, keyed by `(repo, tag)`.
///
/// On startup, the caller walks the cache root and calls `load_repo` for each
/// repo discovered. On every `PUT manifest` for a tag, the caller invokes `set`
/// which updates both the in-memory map and the on-disk sidecar atomically.
pub struct LocalTagsStore {
    layout: CasLayout,
    map: DashMap<(String, String), LocalTagMeta>,
    /// Per-repo write lock to serialize sidecar writes against concurrent updates.
    write_locks: DashMap<String, Mutex<()>>,
}

impl LocalTagsStore {
    pub fn new(layout: CasLayout) -> Self {
        Self {
            layout,
            map: DashMap::new(),
            write_locks: DashMap::new(),
        }
    }

    pub fn get(&self, repo: &str, tag: &str) -> Option<LocalTagMeta> {
        self.map
            .get(&(repo.to_string(), tag.to_string()))
            .map(|e| e.clone())
    }

    pub fn list_for_repo(&self, repo: &str) -> Vec<(String, LocalTagMeta)> {
        self.map
            .iter()
            .filter(|kv| kv.key().0 == repo)
            .map(|kv| (kv.key().1.clone(), kv.value().clone()))
            .collect()
    }

    /// Load `_local_tags.json` for one repo into memory. Idempotent.
    pub fn load_repo(&self, repo: &str) -> Result<(), LocalTagsError> {
        let path = self.layout.local_tags_path(repo);
        if !path.exists() {
            return Ok(());
        }
        let body = std::fs::read_to_string(&path)?;
        let on_disk: OnDiskTags = serde_json::from_str(&body)?;
        for (tag, meta) in on_disk.0 {
            self.map.insert((repo.to_string(), tag), meta);
        }
        Ok(())
    }

    /// Set or replace a locally-pushed tag. Atomic on the sidecar via temp+rename.
    pub fn set(
        &self,
        repo: &str,
        tag: &str,
        digest: &Digest,
    ) -> Result<(), LocalTagsError> {
        let lock_entry = self
            .write_locks
            .entry(repo.to_string())
            .or_default();
        let _guard = lock_entry.lock();

        let meta = LocalTagMeta {
            digest: digest.to_string(),
            pushed_at: Utc::now(),
        };
        self.map
            .insert((repo.to_string(), tag.to_string()), meta.clone());

        let snapshot: HashMap<String, LocalTagMeta> = self
            .list_for_repo(repo)
            .into_iter()
            .collect();
        write_atomic(&self.layout.local_tags_path(repo), &OnDiskTags(snapshot))?;
        Ok(())
    }

    /// Remove a locally-pushed tag. Returns true if removed.
    pub fn remove(&self, repo: &str, tag: &str) -> Result<bool, LocalTagsError> {
        let lock_entry = self
            .write_locks
            .entry(repo.to_string())
            .or_default();
        let _guard = lock_entry.lock();

        let removed = self
            .map
            .remove(&(repo.to_string(), tag.to_string()))
            .is_some();
        if removed {
            let snapshot: HashMap<String, LocalTagMeta> = self
                .list_for_repo(repo)
                .into_iter()
                .collect();
            write_atomic(&self.layout.local_tags_path(repo), &OnDiskTags(snapshot))?;
        }
        Ok(removed)
    }
}

fn write_atomic(path: &PathBuf, value: &OnDiskTags) -> Result<(), LocalTagsError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let body = serde_json::to_vec_pretty(value)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn set_and_get_roundtrips() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        layout.ensure_repo_dirs("foo/bar").unwrap();
        let store = LocalTagsStore::new(layout);
        let d = Digest::parse(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        store.set("foo/bar", "v1-patched", &d).unwrap();
        assert!(store.get("foo/bar", "v1-patched").is_some());
        assert!(store.get("foo/bar", "other").is_none());
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        layout.ensure_repo_dirs("foo/bar").unwrap();
        let d = Digest::parse(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        {
            let store = LocalTagsStore::new(layout.clone());
            store.set("foo/bar", "v1-patched", &d).unwrap();
        }
        let reloaded = LocalTagsStore::new(layout);
        reloaded.load_repo("foo/bar").unwrap();
        assert!(reloaded.get("foo/bar", "v1-patched").is_some());
    }
}
