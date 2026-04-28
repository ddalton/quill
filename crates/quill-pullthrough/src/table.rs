use std::sync::Arc;

use dashmap::DashMap;

use quill_storage::Digest;

use crate::entry::PullThroughEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerRole {
    /// This caller created the entry and must drive the upstream fetch.
    Producer,
    /// Another caller is already producing; this caller just reads the tempfile.
    Subscriber,
}

/// Lock-free table of in-flight pull-through fetches, keyed by digest.
/// Single-flight: concurrent identical requests collapse to one upstream fetch.
pub struct PullThroughTable {
    entries: DashMap<Digest, Arc<PullThroughEntry>>,
}

impl PullThroughTable {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Atomically insert-or-attach. If `Producer` is returned, the caller must
    /// drive the upstream fetch and ultimately call [`Self::finish`]. If
    /// `Subscriber`, the caller reads the tempfile until the producer signals.
    pub fn get_or_insert(
        &self,
        digest: Digest,
        new_entry: impl FnOnce() -> Arc<PullThroughEntry>,
    ) -> (Arc<PullThroughEntry>, ProducerRole) {
        match self.entries.entry(digest) {
            dashmap::Entry::Occupied(o) => (Arc::clone(o.get()), ProducerRole::Subscriber),
            dashmap::Entry::Vacant(v) => {
                let entry = new_entry();
                v.insert(Arc::clone(&entry));
                (entry, ProducerRole::Producer)
            }
        }
    }

    /// Producer-only: remove the entry from the table after completion. Does
    /// not affect any in-flight subscribers — they hold their own `Arc`s.
    pub fn finish(&self, digest: &Digest) {
        self.entries.remove(digest);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for PullThroughTable {
    fn default() -> Self {
        Self::new()
    }
}
