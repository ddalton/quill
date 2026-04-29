use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tracing::warn;

use quill_pullthrough::PullThroughTable;
use quill_storage::{CasLayout, Digest, LocalStorage, LocalTagsStore, UploadStore};
use quill_upstream::UpstreamRouter;

/// State of an `(repo, tag)` entry in the upstream cache.
#[derive(Debug, Clone)]
pub enum TagCacheState {
    /// Within freshness TTL — serve local immediately.
    Fresh(Digest),
    /// Past freshness TTL — caller should HEAD upstream to revalidate.
    Stale(Digest),
    /// Not in cache.
    Miss,
}

/// In-memory cache of upstream-resolved tags. (repo, tag) -> (digest, fetched_at).
/// Distinguishes Fresh / Stale / Miss so the route handler can decide whether
/// to revalidate via HEAD. Persisted to `_upstream_tags.json` per repo so that
/// tags survive restarts.
pub struct UpstreamTagCache {
    entries: DashMap<(String, String), (Digest, std::time::Instant)>,
    ttl: Duration,
    layout: Option<CasLayout>,
}

impl UpstreamTagCache {
    pub fn new(ttl: Duration, layout: Option<CasLayout>) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            layout,
        }
    }

    pub fn load_repo(&self, repo: &str) {
        let layout = match &self.layout {
            Some(l) => l,
            None => return,
        };
        let path = layout.upstream_tags_path(repo);
        let body = match std::fs::read_to_string(&path) {
            Ok(b) => b,
            Err(_) => return,
        };
        let map: HashMap<String, String> = match serde_json::from_str(&body) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, repo, "failed to parse _upstream_tags.json");
                return;
            }
        };
        for (tag, digest_str) in map {
            if let Ok(digest) = Digest::parse(&digest_str) {
                self.entries.insert(
                    (repo.to_string(), tag),
                    (digest, std::time::Instant::now()),
                );
            }
        }
    }

    pub fn lookup(&self, repo: &str, tag: &str) -> TagCacheState {
        match self
            .entries
            .get(&(repo.to_string(), tag.to_string()))
            .map(|e| e.clone())
        {
            None => TagCacheState::Miss,
            Some((digest, fetched_at)) => {
                if fetched_at.elapsed() < self.ttl {
                    TagCacheState::Fresh(digest)
                } else {
                    TagCacheState::Stale(digest)
                }
            }
        }
    }

    /// Convenience for callers that don't care about freshness — returns
    /// `Some(digest)` if the entry exists at all (fresh or stale), `None` on miss.
    pub fn get_any(&self, repo: &str, tag: &str) -> Option<Digest> {
        match self.lookup(repo, tag) {
            TagCacheState::Fresh(d) | TagCacheState::Stale(d) => Some(d),
            TagCacheState::Miss => None,
        }
    }

    pub fn insert(&self, repo: &str, tag: &str, digest: Digest) {
        self.entries.insert(
            (repo.to_string(), tag.to_string()),
            (digest, std::time::Instant::now()),
        );
        self.persist_repo(repo);
    }

    /// Refresh the timestamp without changing the digest (used after a successful
    /// upstream HEAD that confirmed the digest is still current).
    pub fn touch(&self, repo: &str, tag: &str) {
        let key = (repo.to_string(), tag.to_string());
        if let Some(mut entry) = self.entries.get_mut(&key) {
            entry.1 = std::time::Instant::now();
        }
    }

    /// Iterate all currently-cached `(tag, digest)` pairs for a repo. Used by
    /// `tags/list` to merge with locally-pushed tags.
    pub fn list_for_repo(&self, repo: &str) -> Vec<String> {
        self.entries
            .iter()
            .filter(|kv| kv.key().0 == repo)
            .map(|kv| kv.key().1.clone())
            .collect()
    }

    fn persist_repo(&self, repo: &str) {
        let layout = match &self.layout {
            Some(l) => l,
            None => return,
        };
        let snapshot: HashMap<String, String> = self
            .entries
            .iter()
            .filter(|kv| kv.key().0 == repo)
            .map(|kv| (kv.key().1.clone(), kv.value().0.to_string()))
            .collect();
        let path = layout.upstream_tags_path(repo);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("json.tmp");
        if let Ok(body) = serde_json::to_vec_pretty(&snapshot) {
            if std::fs::write(&tmp, body).is_ok() {
                let _ = std::fs::rename(&tmp, &path);
            }
        }
    }
}

/// Top-level state shared across registry handlers.
#[derive(Clone)]
pub struct RegistryState {
    pub storage: Arc<LocalStorage>,
    pub local_tags: Arc<LocalTagsStore>,
    pub uploads: Arc<UploadStore>,
    pub pullthrough: Arc<PullThroughTable>,
    pub upstreams: Arc<UpstreamRouter>,
    pub upstream_tag_cache: Arc<UpstreamTagCache>,
}

impl RegistryState {
    pub fn new(
        storage: Arc<LocalStorage>,
        local_tags: Arc<LocalTagsStore>,
        uploads: Arc<UploadStore>,
        pullthrough: Arc<PullThroughTable>,
        upstreams: Arc<UpstreamRouter>,
        upstream_tag_cache: Arc<UpstreamTagCache>,
    ) -> Self {
        Self {
            storage,
            local_tags,
            uploads,
            pullthrough,
            upstreams,
            upstream_tag_cache,
        }
    }
}
