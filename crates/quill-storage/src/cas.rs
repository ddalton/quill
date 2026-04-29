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

    pub fn upstream_tags_path(&self, repo: &str) -> PathBuf {
        self.repo_dir(repo).join("_upstream_tags.json")
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

    pub fn list_repos(&self) -> std::io::Result<Vec<String>> {
        let mut repos = Vec::new();
        self.walk_repos(&self.root, &mut repos)?;
        repos.sort();
        Ok(repos)
    }

    fn walk_repos(&self, dir: &Path, out: &mut Vec<String>) -> std::io::Result<()> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str.starts_with('.') || name_str.starts_with('_') {
                continue;
            }
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if path.join("blobs").is_dir() {
                if let Ok(rel) = path.strip_prefix(&self.root) {
                    out.push(rel.to_string_lossy().into_owned());
                }
            }
            self.walk_repos(&path, out)?;
        }
        Ok(())
    }
}
