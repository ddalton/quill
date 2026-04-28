use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use sha2::{Digest as Sha2Digest, Sha256};
use thiserror::Error;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::cache::{BlobMeta, BlobMetaCache};
use crate::cas::CasLayout;
use crate::digest::Digest;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("blob not found: {0}")]
    NotFound(String),
    #[error("digest mismatch: expected {expected}, got {got}")]
    DigestMismatch { expected: String, got: String },
}

/// Local-filesystem CAS storage with an in-memory metadata cache.
///
/// All blob writes go through a tempfile + atomic rename. The metadata cache
/// is consulted on every read to skip the `stat()` syscall.
pub struct LocalStorage {
    layout: CasLayout,
    meta_cache: Arc<BlobMetaCache>,
}

impl LocalStorage {
    pub fn new(layout: CasLayout, meta_ttl: Duration) -> Self {
        Self {
            layout,
            meta_cache: Arc::new(BlobMetaCache::new(meta_ttl)),
        }
    }

    pub fn layout(&self) -> &CasLayout {
        &self.layout
    }

    pub fn meta_cache(&self) -> &Arc<BlobMetaCache> {
        &self.meta_cache
    }

    /// Resolve blob metadata, consulting the cache first. On miss, `stat()` and
    /// populate the cache. Returns `Ok(None)` if the blob does not exist.
    pub async fn blob_meta(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<Option<BlobMeta>, StorageError> {
        if let Some(m) = self.meta_cache.get(repo, digest) {
            return Ok(Some(m));
        }
        let path = self.layout.blob_path(repo, digest);
        match fs::metadata(&path).await {
            Ok(md) if md.is_file() => {
                let meta = BlobMeta {
                    path,
                    size: md.len(),
                    cached_at: Instant::now(),
                };
                self.meta_cache.insert(repo, digest, meta.clone());
                Ok(Some(meta))
            }
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    pub async fn open_blob(&self, meta: &BlobMeta) -> Result<File, StorageError> {
        File::open(&meta.path).await.map_err(StorageError::Io)
    }

    /// Buffer-and-verify write: hash the bytes, only commit if the digest matches.
    /// Use this for small blobs (manifests, configs). Layer blobs go through the
    /// streaming pull-through path which has its own producer/consumer machinery.
    pub async fn put_blob_buffered(
        &self,
        repo: &str,
        expected: &Digest,
        bytes: Bytes,
    ) -> Result<BlobMeta, StorageError> {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let got_hex = hex::encode(hasher.finalize());
        if got_hex != expected.hex() {
            return Err(StorageError::DigestMismatch {
                expected: expected.to_string(),
                got: format!("sha256:{}", got_hex),
            });
        }
        let final_path = self.layout.blob_path(repo, expected);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let tmp = self.tempfile_path(repo);
        if let Some(parent) = tmp.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut f = File::create(&tmp).await?;
        f.write_all(&bytes).await?;
        f.flush().await?;
        f.sync_data().await?;
        drop(f);
        fs::rename(&tmp, &final_path).await?;
        let meta = BlobMeta {
            path: final_path,
            size: bytes.len() as u64,
            cached_at: Instant::now(),
        };
        self.meta_cache.insert(repo, expected, meta.clone());
        Ok(meta)
    }

    /// Read a small blob fully into memory. For manifests / configs only.
    pub async fn read_blob_to_bytes(&self, meta: &BlobMeta) -> Result<Bytes, StorageError> {
        let mut buf = Vec::with_capacity(meta.size as usize);
        let mut f = self.open_blob(meta).await?;
        f.read_to_end(&mut buf).await?;
        Ok(Bytes::from(buf))
    }

    /// Delete a blob from CAS and invalidate the metadata cache.
    pub async fn delete_blob(
        &self,
        repo: &str,
        digest: &Digest,
    ) -> Result<(), StorageError> {
        let path = self.layout.blob_path(repo, digest);
        match fs::remove_file(&path).await {
            Ok(()) => {
                self.meta_cache.invalidate(repo, digest);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                self.meta_cache.invalidate(repo, digest);
                Err(StorageError::NotFound(digest.to_string()))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }

    pub fn tempfile_path(&self, repo: &str) -> PathBuf {
        let suffix: u64 = rand_u64();
        self.layout
            .uploads_dir(repo)
            .join(format!("tmp-{:016x}", suffix))
    }

    pub async fn ensure_repo(&self, repo: &str) -> Result<(), StorageError> {
        let layout = self.layout.clone();
        let repo = repo.to_string();
        tokio::task::spawn_blocking(move || layout.ensure_repo_dirs(&repo))
            .await
            .map_err(|e| StorageError::Io(io::Error::other(e)))??;
        Ok(())
    }
}

fn rand_u64() -> u64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    nanos.rotate_left(13) ^ pid.wrapping_mul(0x9E3779B97F4A7C15)
}

#[allow(dead_code)]
fn _path_helper(p: &Path) -> &Path {
    p
}
