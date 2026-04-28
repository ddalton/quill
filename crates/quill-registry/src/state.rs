use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use quill_pullthrough::PullThroughTable;
use quill_storage::{Digest, LocalStorage, LocalTagsStore, UploadStore};
use quill_upstream::UpstreamRouter;

/// In-memory cache of upstream-resolved tags. (repo, tag) -> (digest, fetched_at).
/// Phase 4 will add TTL-based revalidation; for Phase 3 we cache for the
/// configured TTL and serve directly until then.
pub struct UpstreamTagCache {
    entries: DashMap<(String, String), (Digest, std::time::Instant)>,
    ttl: Duration,
}

impl UpstreamTagCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
        }
    }

    pub fn get(&self, repo: &str, tag: &str) -> Option<Digest> {
        let entry = self
            .entries
            .get(&(repo.to_string(), tag.to_string()))?;
        if entry.1.elapsed() < self.ttl {
            Some(entry.0.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, repo: &str, tag: &str, digest: Digest) {
        self.entries.insert(
            (repo.to_string(), tag.to_string()),
            (digest, std::time::Instant::now()),
        );
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
