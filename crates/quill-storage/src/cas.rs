use std::path::{Path, PathBuf};

use crate::digest::Digest;

/// Zot-compatible CAS layout under a single root.
///
/// ```text
/// <root>/
///   <repo>/
///     blobs/
///       sha256/
///         <hex>
///     index.json
///     _local_tags.json
///     _uploads/
///       <session-or-tempfile>
/// ```
#[derive(Debug, Clone)]
pub struct CasLayout {
    root: PathBuf,
}

impl CasLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn repo_dir(&self, repo: &str) -> PathBuf {
        self.root.join(repo)
    }

    pub fn blob_path(&self, repo: &str, digest: &Digest) -> PathBuf {
        self.repo_dir(repo)
            .join("blobs")
            .join(digest.algorithm())
            .join(digest.hex())
    }

    pub fn index_path(&self, repo: &str) -> PathBuf {
        self.repo_dir(repo).join("index.json")
    }

    pub fn local_tags_path(&self, repo: &str) -> PathBuf {
        self.repo_dir(repo).join("_local_tags.json")
    }

    pub fn uploads_dir(&self, repo: &str) -> PathBuf {
        self.repo_dir(repo).join("_uploads")
    }

    pub fn ensure_repo_dirs(&self, repo: &str) -> std::io::Result<()> {
        let repo_dir = self.repo_dir(repo);
        std::fs::create_dir_all(repo_dir.join("blobs").join("sha256"))?;
        std::fs::create_dir_all(repo_dir.join("_uploads"))?;
        Ok(())
    }
}
