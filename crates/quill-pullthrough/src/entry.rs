use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{watch, Notify};

use quill_storage::Digest;

#[derive(Debug, Error, Clone)]
pub enum PullThroughError {
    #[error("upstream: {0}")]
    Upstream(String),
    #[error("digest mismatch: expected {expected}, got {got}")]
    DigestMismatch { expected: String, got: String },
    #[error("io: {0}")]
    Io(String),
    #[error("producer dropped without completion")]
    ProducerGone,
}

/// What the producer signals on its completion channel.
#[derive(Debug, Clone)]
pub enum ProducerOutcome {
    Success { final_size: u64 },
    Failed(PullThroughError),
}

/// One in-flight upstream fetch. Cloned out of the table by both the producer
/// and any number of subscribers; the underlying state is shared via `Arc`.
pub struct PullThroughEntry {
    pub digest: Digest,
    pub tempfile_path: PathBuf,
    /// Bytes the producer has written to the tempfile and made visible.
    written: AtomicU64,
    /// Set when the producer task finishes, success or failure.
    done: AtomicBool,
    /// Notified each time the producer advances `written` or terminates.
    progress: Arc<Notify>,
    /// Receiver side resolves when the producer terminates with the outcome.
    completion_rx: watch::Receiver<Option<ProducerOutcome>>,
    completion_tx: watch::Sender<Option<ProducerOutcome>>,
}

impl PullThroughEntry {
    pub fn new(digest: Digest, tempfile_path: PathBuf) -> Self {
        let (tx, rx) = watch::channel(None);
        Self {
            digest,
            tempfile_path,
            written: AtomicU64::new(0),
            done: AtomicBool::new(false),
            progress: Arc::new(Notify::new()),
            completion_rx: rx,
            completion_tx: tx,
        }
    }

    pub fn high_water_mark(&self) -> u64 {
        self.written.load(Ordering::Acquire)
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    pub fn progress(&self) -> Arc<Notify> {
        Arc::clone(&self.progress)
    }

    pub fn completion(&self) -> watch::Receiver<Option<ProducerOutcome>> {
        self.completion_rx.clone()
    }

    /// Producer-only: advance the high-water mark and wake all subscribers.
    pub fn advance(&self, new_total: u64) {
        self.written.store(new_total, Ordering::Release);
        self.progress.notify_waiters();
    }

    /// Producer-only: signal completion. Wakes subscribers blocked on either
    /// `progress` (because they may have been mid-await) or `completion`.
    pub fn finish(&self, outcome: ProducerOutcome) {
        self.done.store(true, Ordering::Release);
        // Best-effort send; if the watch has no receivers we don't care.
        let _ = self.completion_tx.send(Some(outcome));
        self.progress.notify_waiters();
    }

    /// Subscriber-side: latest outcome, if the producer has finished.
    pub fn outcome(&self) -> Option<ProducerOutcome> {
        self.completion_rx.borrow().clone()
    }
}
