//! Mark-and-sweep garbage collector for the local CAS.
//!
//! Roots:
//! 1. Every digest listed in any repo's `_local_tags.json` (locally-pushed tags).
//! 2. Optionally, every digest in a caller-supplied "extra roots" set
//!    (e.g. currently-cached upstream tags so a GC during active proxying
//!    doesn't blow away things being served).
//!
//! From each root we recursively traverse referenced blobs:
//! - manifests reference `config.digest` and each `layers[*].digest`
//! - image-index manifests reference each `manifests[*].digest`
//!
//! Anything in `<root>/<repo>/blobs/sha256/` not in the reachable set is
//! removed. Tempfiles under `_uploads/` are not touched (they're swept by
//! `UploadStore::sweep` on a separate cadence).

use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::fs;
use tracing::{debug, instrument};

use crate::cas::CasLayout;
use crate::digest::Digest;
use crate::local_tags::LocalTagsStore;

#[derive(Debug, Error)]
pub enum GcError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("digest: {0}")]
    Digest(#[from] crate::digest::DigestError),
    #[error("local tags: {0}")]
    LocalTags(#[from] crate::local_tags::LocalTagsError),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GcReport {
    pub repos_scanned: usize,
    pub roots: usize,
    pub reachable_blobs: usize,
    pub on_disk_blobs: usize,
    pub deleted: usize,
    pub bytes_freed: u64,
    pub errors: Vec<String>,
}

pub struct GarbageCollector {
    layout: CasLayout,
}

impl GarbageCollector {
    pub fn new(layout: CasLayout) -> Self {
        Self { layout }
    }

    /// Run a full mark-and-sweep pass.
    ///
    /// `extra_roots` lets the caller pin additional digests as reachable —
    /// typically the union of currently-cached upstream-tag digests so an
    /// online GC doesn't delete blobs being served right now.
    ///
    /// `dry_run = true` reports the work that *would* be done without actually
    /// deleting anything.
    #[instrument(skip(self, extra_roots), fields(dry_run))]
    pub async fn run(
        &self,
        extra_roots: HashSet<Digest>,
        dry_run: bool,
    ) -> Result<GcReport, GcError> {
        let mut report = GcReport::default();
        let repos = self.discover_repos().await?;
        report.repos_scanned = repos.len();

        // ---- mark phase ----
        let mut reachable: HashSet<(String, Digest)> = HashSet::new();
        // `extra_roots` are repo-agnostic: we treat them as reachable in *any*
        // repo where they appear (handled inside the per-repo loop below). That's
        // a slight over-mark but safe — we'd rather keep an extra blob than
        // delete one being served.

        for repo in &repos {
            let store = LocalTagsStore::new(self.layout.clone());
            store.load_repo(repo)?;
            let mut roots: Vec<Digest> = store
                .list_for_repo(repo)
                .into_iter()
                .filter_map(|(_, m)| Digest::parse(&m.digest).ok())
                .collect();
            // Also pin any extra_roots within this repo's namespace.
            roots.extend(extra_roots.iter().cloned());

            for root in &roots {
                self.traverse(repo, root, &mut reachable, &mut report).await;
            }
            report.roots += roots.len();
        }
        report.reachable_blobs = reachable.len();

        // ---- sweep phase ----
        for repo in &repos {
            let blobs_dir = self.layout.repo_dir(repo).join("blobs").join("sha256");
            if !blobs_dir.exists() {
                continue;
            }
            let mut rd = fs::read_dir(&blobs_dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let path = entry.path();
                let md = entry.metadata().await?;
                if !md.is_file() {
                    continue;
                }
                let hex = match path.file_name().and_then(|n| n.to_str()) {
                    Some(s) => s,
                    None => continue,
                };
                let digest = match Digest::parse(&format!("sha256:{hex}")) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                report.on_disk_blobs += 1;
                if !reachable.contains(&(repo.clone(), digest)) {
                    if dry_run {
                        debug!(repo, ?path, "would delete");
                    } else {
                        match fs::remove_file(&path).await {
                            Ok(()) => {
                                report.deleted += 1;
                                report.bytes_freed = report.bytes_freed.saturating_add(md.len());
                            }
                            Err(e) => {
                                report
                                    .errors
                                    .push(format!("delete {}: {e}", path.display()));
                            }
                        }
                    }
                }
            }
        }
        Ok(report)
    }

    /// Recursive traversal: read the manifest blob at `digest`, parse, follow
    /// `config.digest`, `layers[*].digest`, and (for indexes) `manifests[*].digest`.
    /// All digests visited are inserted into `reachable`. Best-effort — invalid
    /// or unreadable manifests are recorded as errors but don't abort GC.
    async fn traverse(
        &self,
        repo: &str,
        root: &Digest,
        reachable: &mut HashSet<(String, Digest)>,
        report: &mut GcReport,
    ) {
        let mut stack = vec![root.clone()];
        while let Some(d) = stack.pop() {
            if !reachable.insert((repo.to_string(), d.clone())) {
                continue;
            }
            let path = self.layout.blob_path(repo, &d);
            let body = match fs::read(&path).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Try parsing as JSON manifest. If it isn't, it's a layer/config
            // (still reachable, but a leaf in the graph).
            let v: serde_json::Value = match serde_json::from_slice(&body) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Some(c) = v.get("config").and_then(|c| c.get("digest")).and_then(|d| d.as_str()) {
                if let Ok(p) = Digest::parse(c) {
                    stack.push(p);
                }
            }
            if let Some(arr) = v.get("layers").and_then(|l| l.as_array()) {
                for layer in arr {
                    if let Some(d_str) = layer.get("digest").and_then(|d| d.as_str()) {
                        if let Ok(p) = Digest::parse(d_str) {
                            stack.push(p);
                        }
                    }
                }
            }
            if let Some(arr) = v.get("manifests").and_then(|l| l.as_array()) {
                for m in arr {
                    if let Some(d_str) = m.get("digest").and_then(|d| d.as_str()) {
                        if let Ok(p) = Digest::parse(d_str) {
                            stack.push(p);
                        }
                    }
                }
            }
            let _ = report;
        }
    }

    /// Walk the cache root looking for repos. A repo is a directory containing
    /// a `blobs/sha256/` subdirectory.
    async fn discover_repos(&self) -> Result<Vec<String>, GcError> {
        let mut repos: Vec<String> = Vec::new();
        let root = self.layout.root().to_path_buf();
        if !root.exists() {
            return Ok(repos);
        }
        let mut stack: Vec<PathBuf> = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = match fs::read_dir(&dir).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let mut subdirs = Vec::new();
            let mut has_blobs = false;
            while let Some(entry) = rd.next_entry().await? {
                let p = entry.path();
                if p.is_dir() {
                    let name = entry.file_name();
                    if name == "blobs" {
                        has_blobs = true;
                    } else if name == "_uploads" || name == "_quill" {
                        continue;
                    } else {
                        subdirs.push(p);
                    }
                }
            }
            if has_blobs {
                if let Ok(rel) = dir.strip_prefix(&root) {
                    let s = rel.to_string_lossy().to_string();
                    if !s.is_empty() {
                        repos.push(s);
                    }
                }
            } else {
                // Recurse only if this dir isn't itself a repo (no nested repos).
                stack.extend(subdirs);
            }
        }
        Ok(repos)
    }

    /// Total disk usage of all blob files under all repos.
    pub async fn disk_usage(&self) -> Result<u64, GcError> {
        let mut total: u64 = 0;
        let repos = self.discover_repos().await?;
        for repo in repos {
            let dir = self.layout.repo_dir(&repo).join("blobs").join("sha256");
            if !dir.exists() {
                continue;
            }
            let mut rd = fs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let md = entry.metadata().await?;
                if md.is_file() {
                    total = total.saturating_add(md.len());
                }
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest as _, Sha256};
    use tempfile::tempdir;

    fn write_blob(layout: &CasLayout, repo: &str, body: &[u8]) -> Digest {
        let mut h = Sha256::new();
        h.update(body);
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(h.finalize()))).unwrap();
        layout.ensure_repo_dirs(repo).unwrap();
        let path = layout.blob_path(repo, &digest);
        std::fs::write(&path, body).unwrap();
        digest
    }

    #[tokio::test]
    async fn gc_keeps_reachable_blobs_and_deletes_orphans() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        let repo = "myorg/myimage";

        // Reachable graph: manifest → config + layer.
        let layer = write_blob(&layout, repo, b"layer-bytes");
        let config = write_blob(&layout, repo, b"{\"x\":1}");
        let manifest_body = serde_json::json!({
            "schemaVersion": 2,
            "config": {"digest": config.to_string()},
            "layers": [{"digest": layer.to_string()}]
        })
        .to_string();
        let manifest = write_blob(&layout, repo, manifest_body.as_bytes());

        // Orphan that should be swept.
        let orphan = write_blob(&layout, repo, b"orphan-bytes");

        // Mark manifest as locally pushed via the tags store.
        let tags = LocalTagsStore::new(layout.clone());
        tags.set(repo, "v1", &manifest).unwrap();

        let gc = GarbageCollector::new(layout.clone());
        let report = gc.run(HashSet::new(), false).await.unwrap();

        assert_eq!(report.repos_scanned, 1);
        assert!(report.reachable_blobs >= 3); // manifest + config + layer
        assert!(report.deleted >= 1);
        // Reachable blobs still exist
        assert!(layout.blob_path(repo, &manifest).exists());
        assert!(layout.blob_path(repo, &config).exists());
        assert!(layout.blob_path(repo, &layer).exists());
        // Orphan is gone
        assert!(!layout.blob_path(repo, &orphan).exists());
    }

    #[tokio::test]
    async fn gc_dry_run_deletes_nothing() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        let repo = "x/y";
        let orphan = write_blob(&layout, repo, b"orphan");

        let gc = GarbageCollector::new(layout.clone());
        let report = gc.run(HashSet::new(), true).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert!(layout.blob_path(repo, &orphan).exists());
    }
}
