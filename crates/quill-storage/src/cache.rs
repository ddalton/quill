use std::path::PathBuf;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::digest::Digest;

#[derive(Debug, Clone)]
pub struct BlobMeta {
    pub path: PathBuf,
    pub size: u64,
    pub cached_at: Instant,
}

/// Lock-free metadata cache, keyed by (repo, digest).
///
/// On a hit, this saves the per-request `stat()` syscall. TTL bounds staleness
/// when blobs are deleted out from under us.
pub struct BlobMetaCache {
    entries: DashMap<(String, Digest), BlobMeta>,
    ttl: Duration,
}

impl BlobMetaCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
        }
    }

    pub fn get(&self, repo: &str, digest: &Digest) -> Option<BlobMeta> {
        let key = (repo.to_string(), digest.clone());
        let entry = self.entries.get(&key)?;
        if entry.cached_at.elapsed() < self.ttl {
            Some(entry.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, repo: &str, digest: &Digest, meta: BlobMeta) {
        self.entries
            .insert((repo.to_string(), digest.clone()), meta);
    }

    pub fn invalidate(&self, repo: &str, digest: &Digest) {
        self.entries.remove(&(repo.to_string(), digest.clone()));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
