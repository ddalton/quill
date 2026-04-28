use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use sha2::{Digest as Sha2Digest, Sha256};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, instrument, warn};

use quill_storage::Digest;

use crate::entry::{ProducerOutcome, PullThroughEntry, PullThroughError};

/// Run the producer task that streams `body` into the tempfile referenced by
/// `entry`. On success, the tempfile is renamed to `final_path` and `Ok(())` is
/// returned via the entry's completion channel.
///
/// `final_path` must be on the same filesystem as `entry.tempfile_path` for
/// the atomic rename to be a single syscall.
#[instrument(skip(body, entry), fields(digest = %entry.digest, tempfile = ?entry.tempfile_path))]
pub async fn run_producer<S, E>(
    entry: Arc<PullThroughEntry>,
    final_path: PathBuf,
    mut body: S,
) -> Result<(), PullThroughError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin + Send,
    E: std::fmt::Display + Send,
{
    if let Some(parent) = entry.tempfile_path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            entry.finish(ProducerOutcome::Failed(PullThroughError::Io(e.to_string())));
            return Err(PullThroughError::Io(e.to_string()));
        }
    }

    let mut file = match File::create(&entry.tempfile_path).await {
        Ok(f) => f,
        Err(e) => {
            entry.finish(ProducerOutcome::Failed(PullThroughError::Io(e.to_string())));
            return Err(PullThroughError::Io(e.to_string()));
        }
    };

    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let cleanup_path = entry.tempfile_path.clone();

    let outcome: Result<u64, PullThroughError> = async {
        while let Some(chunk_res) = body.next().await {
            let chunk = match chunk_res {
                Ok(b) => b,
                Err(e) => return Err(PullThroughError::Upstream(e.to_string())),
            };
            file.write_all(&chunk)
                .await
                .map_err(|e| PullThroughError::Io(e.to_string()))?;
            hasher.update(&chunk);
            total = total.saturating_add(chunk.len() as u64);
            entry.advance(total);
        }
        file.flush()
            .await
            .map_err(|e| PullThroughError::Io(e.to_string()))?;
        file.sync_data()
            .await
            .map_err(|e| PullThroughError::Io(e.to_string()))?;
        drop(file);

        let got_hex = hex::encode(hasher.finalize_reset());
        if got_hex != entry.digest.hex() {
            return Err(PullThroughError::DigestMismatch {
                expected: entry.digest.to_string(),
                got: format!("sha256:{got_hex}"),
            });
        }

        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| PullThroughError::Io(e.to_string()))?;
        }
        tokio::fs::rename(&cleanup_path, &final_path)
            .await
            .map_err(|e| PullThroughError::Io(e.to_string()))?;
        debug!(bytes = total, "pull-through promoted tempfile to CAS");
        Ok(total)
    }
    .await;

    match outcome {
        Ok(final_size) => {
            entry.finish(ProducerOutcome::Success { final_size });
            Ok(())
        }
        Err(e) => {
            // Best-effort tempfile cleanup; ignore if it's already gone (rename win
            // race or the tempfile was never created).
            if let Err(rm_err) = tokio::fs::remove_file(&cleanup_path).await {
                if rm_err.kind() != std::io::ErrorKind::NotFound {
                    warn!(error = %rm_err, "tempfile cleanup failed");
                }
            }
            error!(error = %e, "producer failed");
            entry.finish(ProducerOutcome::Failed(e.clone()));
            Err(e)
        }
    }
}

#[allow(dead_code)]
fn _phantom_use(_d: &Digest) {}
