//! Resumable upload session storage for the OCI push flow.
//!
//! Each `POST /v2/<repo>/blobs/uploads/` allocates a session whose payload
//! lives at `<root>/<repo>/_uploads/<session>.data` and whose metadata at
//! `<root>/<repo>/_uploads/<session>.meta.json`. `PATCH` appends bytes; `PUT`
//! finalizes (verifies digest, atomic rename into CAS).
//!
//! sha256 hasher state is kept in memory only — quill restart aborts in-flight
//! uploads. Clients retry the upload from offset 0. Resumable hasher state
//! across process restarts is a Phase 4 polish (Sha256 has no portable
//! serialize, but `digest::compat::Sha256VarCore` private state can be
//! reconstructed from the bytes by re-hashing the on-disk payload at recovery
//! time).

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest as Sha2Digest, Sha256};
use thiserror::Error;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn};

use crate::cas::CasLayout;
use crate::digest::Digest;

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("digest mismatch: expected {expected}, got {got}")]
    DigestMismatch { expected: String, got: String },
    #[error("invalid range: {0}")]
    InvalidRange(String),
    #[error("session already finalized")]
    AlreadyFinalized,
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// On-disk persisted session metadata. The `Sha256` running state is *not*
/// included — it is recomputed from the on-disk data file on recovery, or the
/// session is abandoned if recovery is not desired.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadMeta {
    pub session: String,
    pub repo: String,
    pub bytes_received: u64,
    pub started_at: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// In-memory per-session state including the live hasher.
struct UploadState {
    meta: UploadMeta,
    hasher: Sha256,
    /// Append-only file handle for the data payload.
    data_path: PathBuf,
    finalized: bool,
}

pub struct UploadStore {
    layout: CasLayout,
    /// (repo, session) -> live state under a Mutex (PATCH calls within a session
    /// must serialize against each other; the per-session mutex is fine because
    /// real clients PATCH sequentially).
    sessions: DashMap<(String, String), Arc<Mutex<UploadState>>>,
}

impl UploadStore {
    pub fn new(layout: CasLayout) -> Self {
        Self {
            layout,
            sessions: DashMap::new(),
        }
    }

    /// Create a fresh session. Returns the opaque session id.
    pub async fn create_session(&self, repo: &str) -> Result<String, UploadError> {
        let session = new_session_id();
        let uploads_dir = self.layout.uploads_dir(repo);
        fs::create_dir_all(&uploads_dir).await?;
        let data_path = uploads_dir.join(format!("{session}.data"));
        // Touch the file so an empty PUT (no PATCH) still works.
        File::create(&data_path).await?;
        let now = Utc::now();
        let meta = UploadMeta {
            session: session.clone(),
            repo: repo.to_string(),
            bytes_received: 0,
            started_at: now,
            last_seen: now,
        };
        self.write_meta(repo, &meta).await?;
        let state = UploadState {
            meta,
            hasher: Sha256::new(),
            data_path,
            finalized: false,
        };
        self.sessions
            .insert((repo.to_string(), session.clone()), Arc::new(Mutex::new(state)));
        debug!(repo, session, "created upload session");
        Ok(session)
    }

    /// Append a chunk to a session. Returns the new total bytes received.
    pub async fn append(
        &self,
        repo: &str,
        session: &str,
        chunk: &[u8],
    ) -> Result<u64, UploadError> {
        // Snapshot what we need so we don't hold the dashmap shard during async I/O.
        let key = (repo.to_string(), session.to_string());
        let state_arc = self
            .sessions
            .get(&key)
            .ok_or_else(|| UploadError::NotFound(session.to_string()))?
            .clone();

        let data_path = {
            let s = state_arc.lock();
            if s.finalized {
                return Err(UploadError::AlreadyFinalized);
            }
            s.data_path.clone()
        };

        // Append to disk first (durable side-effect).
        let mut file = OpenOptions::new()
            .append(true)
            .open(&data_path)
            .await?;
        file.write_all(chunk).await?;
        file.flush().await?;
        drop(file);

        // Then update the in-memory hasher and counters under the lock.
        let new_total = {
            let mut s = state_arc.lock();
            s.hasher.update(chunk);
            s.meta.bytes_received = s.meta.bytes_received.saturating_add(chunk.len() as u64);
            s.meta.last_seen = Utc::now();
            s.meta.bytes_received
        };
        // Persist updated meta (best-effort; if it fails, the session can still
        // recover from the data file's actual length on restart).
        if let Err(e) = self.write_meta_for(repo, session, new_total).await {
            warn!(error = %e, "failed to persist upload meta");
        }
        Ok(new_total)
    }

    /// Finalize a session: verify digest and atomic-rename the data file into CAS.
    /// On success the session is removed from the in-memory map and disk metadata
    /// is unlinked. On digest mismatch the session stays so the client can retry.
    pub async fn finalize(
        &self,
        repo: &str,
        session: &str,
        last_chunk: Option<&[u8]>,
        expected: &Digest,
    ) -> Result<(PathBuf, u64), UploadError> {
        // Append final chunk, if any.
        if let Some(chunk) = last_chunk {
            if !chunk.is_empty() {
                self.append(repo, session, chunk).await?;
            }
        }

        let key = (repo.to_string(), session.to_string());
        let state_arc = self
            .sessions
            .get(&key)
            .ok_or_else(|| UploadError::NotFound(session.to_string()))?
            .clone();

        let (data_path, hex, total) = {
            let mut s = state_arc.lock();
            if s.finalized {
                return Err(UploadError::AlreadyFinalized);
            }
            // Clone the hasher so a digest mismatch doesn't poison the session.
            let h = s.hasher.clone();
            let hex = hex::encode(h.finalize());
            (s.data_path.clone(), hex, s.meta.bytes_received)
        };

        if hex != expected.hex() {
            return Err(UploadError::DigestMismatch {
                expected: expected.to_string(),
                got: format!("sha256:{hex}"),
            });
        }

        // Atomic rename into CAS.
        let final_path = self.layout.blob_path(repo, expected);
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::rename(&data_path, &final_path).await?;

        {
            let mut s = state_arc.lock();
            s.finalized = true;
            // touch fields to silence unused-write lint; the real signal is the dashmap remove below.
            let _ = &s.meta;
        }
        // Cleanup
        let _ = fs::remove_file(self.meta_path(repo, session)).await;
        self.sessions.remove(&key);
        debug!(repo, session, %expected, total, "upload finalized");
        Ok((final_path, total))
    }

    /// Best-effort abort: unlink data + meta files and drop the session.
    pub async fn abort(&self, repo: &str, session: &str) -> Result<(), UploadError> {
        let key = (repo.to_string(), session.to_string());
        if let Some((_, state_arc)) = self.sessions.remove(&key) {
            let data_path = state_arc.lock().data_path.clone();
            let _ = fs::remove_file(&data_path).await;
        }
        let _ = fs::remove_file(self.meta_path(repo, session)).await;
        Ok(())
    }

    /// Sweep on-disk session files older than the threshold. Called at startup.
    /// Sessions only known on disk (no in-memory state) are unlinked.
    pub async fn sweep(&self, max_age: Duration) -> Result<usize, UploadError> {
        let mut removed = 0;
        let cache_root = self.layout.root().to_path_buf();
        if !cache_root.exists() {
            return Ok(0);
        }
        // Walk repos under root looking for `_uploads/` dirs.
        let mut stack = vec![cache_root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = match fs::read_dir(&dir).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            while let Some(entry) = rd.next_entry().await? {
                let p = entry.path();
                if !p.is_dir() {
                    continue;
                }
                let fname = entry.file_name();
                if fname == "blobs" {
                    continue;
                }
                if fname == "_uploads" {
                    if let Ok(removed_here) = sweep_uploads_dir(&p, max_age).await {
                        removed += removed_here;
                    }
                } else if fname != "_quill" {
                    stack.push(p);
                }
            }
        }
        Ok(removed)
    }

    fn meta_path(&self, repo: &str, session: &str) -> PathBuf {
        self.layout
            .uploads_dir(repo)
            .join(format!("{session}.meta.json"))
    }

    async fn write_meta(&self, repo: &str, meta: &UploadMeta) -> Result<(), UploadError> {
        let path = self.meta_path(repo, &meta.session);
        let body = serde_json::to_vec_pretty(meta)?;
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, body).await?;
        fs::rename(&tmp, &path).await?;
        Ok(())
    }

    async fn write_meta_for(
        &self,
        repo: &str,
        session: &str,
        bytes_received: u64,
    ) -> Result<(), UploadError> {
        // Read-modify-write of just the counters; cheap given the file is tiny.
        let path = self.meta_path(repo, session);
        let mut meta: UploadMeta = match fs::read(&path).await {
            Ok(b) => serde_json::from_slice(&b)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        meta.bytes_received = bytes_received;
        meta.last_seen = Utc::now();
        self.write_meta(repo, &meta).await
    }
}

async fn sweep_uploads_dir(dir: &PathBuf, max_age: Duration) -> Result<usize, UploadError> {
    let mut removed = 0;
    let mut rd = fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let md = entry.metadata().await?;
        let mtime = md.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        let age = SystemTime::now()
            .duration_since(mtime)
            .unwrap_or(Duration::ZERO);
        if age > max_age {
            let _ = fs::remove_file(entry.path()).await;
            removed += 1;
        }
    }
    Ok(removed)
}

fn new_session_id() -> String {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id() as u64;
    let mix = nanos.rotate_left(13) ^ pid.wrapping_mul(0x9E3779B97F4A7C15);
    format!("{:016x}{:016x}", mix, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn roundtrip_full_upload() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        layout.ensure_repo_dirs("foo/bar").unwrap();
        let store = UploadStore::new(layout.clone());

        let session = store.create_session("foo/bar").await.unwrap();
        let _ = store.append("foo/bar", &session, b"hello ").await.unwrap();
        let _ = store.append("foo/bar", &session, b"world").await.unwrap();

        let mut h = Sha256::new();
        h.update(b"hello world");
        let want = Digest::parse(&format!("sha256:{}", hex::encode(h.finalize()))).unwrap();

        let (final_path, total) = store
            .finalize("foo/bar", &session, None, &want)
            .await
            .unwrap();
        assert_eq!(total, b"hello world".len() as u64);
        let body = std::fs::read(&final_path).unwrap();
        assert_eq!(&body, b"hello world");
    }

    #[tokio::test]
    async fn digest_mismatch_keeps_session() {
        let dir = tempdir().unwrap();
        let layout = CasLayout::new(dir.path());
        layout.ensure_repo_dirs("foo/bar").unwrap();
        let store = UploadStore::new(layout.clone());

        let session = store.create_session("foo/bar").await.unwrap();
        let _ = store.append("foo/bar", &session, b"hello").await.unwrap();

        let wrong = Digest::parse(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        assert!(matches!(
            store.finalize("foo/bar", &session, None, &wrong).await,
            Err(UploadError::DigestMismatch { .. })
        ));
        // Session still appendable: hasher was cloned, so state is intact.
        let _ = store.append("foo/bar", &session, b" world").await.unwrap();
    }
}
