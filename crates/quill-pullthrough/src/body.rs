use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::ready;
use futures::Future;
use http_body::{Body, Frame};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::entry::{ProducerOutcome, PullThroughEntry, PullThroughError};

/// HTTP body that tails the producer's tempfile up to the high-water mark.
///
/// State machine:
/// - **Idle**: no I/O in flight; if `offset < high_water`, transition to
///   `Reading`; else if producer is done, emit terminal; else wait on progress.
/// - **Reading**: a tempfile read future is in flight; resolve to a `Bytes` chunk.
/// - **Waiting**: caught up to the producer; await `progress.notified()`.
/// - **Done / Errored**: terminal states, body emits `None` thereafter.
pub struct PullThroughBody {
    entry: Arc<PullThroughEntry>,
    state: State,
    offset: u64,
    chunk_capacity: usize,
    /// Where the producer renames the tempfile once it has verified the digest.
    final_path: Option<PathBuf>,
    /// Kept open between reads. The producer renames the tempfile into CAS when it
    /// finishes; reopening it by path for every chunk then failed with "No such file"
    /// for any subscriber that had not caught up, and its client saw an unexpected EOF.
    /// An open handle keeps reading the same file across the rename.
    file: Option<File>,
}

type ReadResult = Result<(Option<File>, Bytes), PullThroughError>;

#[allow(clippy::large_enum_variant)]
enum State {
    Idle,
    Reading(Pin<Box<dyn Future<Output = ReadResult> + Send>>),
    Waiting(Pin<Box<dyn Future<Output = ()> + Send>>),
    Done,
    Errored,
}

impl PullThroughBody {
    pub fn new(entry: Arc<PullThroughEntry>) -> Self {
        Self {
            entry,
            state: State::Idle,
            offset: 0,
            chunk_capacity: 64 * 1024,
            final_path: None,
            file: None,
        }
    }

    pub fn with_chunk_capacity(mut self, cap: usize) -> Self {
        self.chunk_capacity = cap;
        self
    }

    /// The CAS path the producer renames the tempfile to. A subscriber that first opens
    /// the blob after that rename reads it from here.
    pub fn with_final_path(mut self, final_path: PathBuf) -> Self {
        self.final_path = Some(final_path);
        self
    }
}

impl Body for PullThroughBody {
    type Data = Bytes;
    type Error = PullThroughError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        loop {
            match &mut self.state {
                State::Done | State::Errored => return Poll::Ready(None),

                State::Idle => {
                    let high = self.entry.high_water_mark();
                    if self.offset < high {
                        let file = self.file.take();
                        let path = self.entry.tempfile_path.clone();
                        let final_path = self.final_path.clone();
                        let offset = self.offset;
                        let cap = self.chunk_capacity;
                        let fut = Box::pin(read_chunk(file, path, final_path, offset, cap, high));
                        self.state = State::Reading(fut);
                        continue;
                    }
                    if let Some(outcome) = self.entry.outcome() {
                        return Self::handle_terminal(self, outcome);
                    }
                    let progress = self.entry.progress();
                    let fut = Box::pin(async move { progress.notified().await });
                    self.state = State::Waiting(fut);
                    continue;
                }

                State::Reading(fut) => {
                    let result = ready!(fut.as_mut().poll(cx));
                    match result {
                        Ok((file, bytes)) => {
                            self.file = file;
                            self.offset = self.offset.saturating_add(bytes.len() as u64);
                            self.state = State::Idle;
                            if bytes.is_empty() {
                                continue;
                            }
                            return Poll::Ready(Some(Ok(Frame::data(bytes))));
                        }
                        Err(e) => {
                            self.state = State::Errored;
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }

                State::Waiting(fut) => {
                    let _ = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    continue;
                }
            }
        }
    }
}

impl PullThroughBody {
    fn handle_terminal(
        mut self: Pin<&mut Self>,
        outcome: ProducerOutcome,
    ) -> Poll<Option<Result<Frame<Bytes>, PullThroughError>>> {
        match outcome {
            ProducerOutcome::Success { final_size } => {
                if self.offset >= final_size {
                    self.state = State::Done;
                    Poll::Ready(None)
                } else {
                    // Producer signalled success but we haven't yet drained to
                    // the final size. Loop back to Idle to issue another read.
                    self.state = State::Idle;
                    Poll::Pending
                }
            }
            ProducerOutcome::Failed(e) => {
                self.state = State::Errored;
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

async fn read_chunk(
    file: Option<File>,
    path: PathBuf,
    final_path: Option<PathBuf>,
    offset: u64,
    cap: usize,
    high: u64,
) -> ReadResult {
    let to_read = high.saturating_sub(offset).min(cap as u64) as usize;
    if to_read == 0 {
        return Ok((file, Bytes::new()));
    }
    let mut f = match file {
        Some(f) => f,
        None => open_blob(&path, final_path.as_deref()).await?,
    };
    f.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| PullThroughError::Io(e.to_string()))?;
    let mut buf = vec![0u8; to_read];
    let n = f
        .read(&mut buf)
        .await
        .map_err(|e| PullThroughError::Io(e.to_string()))?;
    buf.truncate(n);
    Ok((Some(f), Bytes::from(buf)))
}

/// The tempfile, or the CAS path if the producer has already renamed it there.
async fn open_blob(tempfile: &Path, final_path: Option<&Path>) -> Result<File, PullThroughError> {
    match (File::open(tempfile).await, final_path) {
        (Ok(f), _) => Ok(f),
        (Err(e), Some(final_path)) if e.kind() == std::io::ErrorKind::NotFound => File::open(final_path)
            .await
            .map_err(|e| PullThroughError::Io(e.to_string())),
        (Err(e), _) => Err(PullThroughError::Io(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer::run_producer;
    use futures::channel::mpsc;
    use quill_storage::Digest;
    use sha2::{Digest as _, Sha256};
    use std::path::PathBuf;

    /// 200 KiB: more than three of the body's 64 KiB reads.
    fn blob() -> (Vec<u8>, Digest) {
        let data: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect();
        let digest = Digest::parse(&format!("sha256:{}", hex::encode(Sha256::digest(&data)))).unwrap();
        (data, digest)
    }

    fn paths(test: &str) -> (PathBuf, PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("quill-pt-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("uploads/pt-blob"), dir.join("blobs/blob"), dir)
    }

    async fn next_data(body: &mut PullThroughBody) -> Option<Result<Bytes, PullThroughError>> {
        futures::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx))
            .await
            .map(|r| r.map(|f| f.into_data().unwrap()))
    }

    async fn drain(body: &mut PullThroughBody, out: &mut Vec<u8>) -> Result<(), PullThroughError> {
        while let Some(chunk) = next_data(body).await {
            out.extend_from_slice(&chunk?);
        }
        Ok(())
    }

    /// The producer renames the tempfile into CAS when it finishes. A subscriber that has
    /// not caught up by then must still get the whole blob, not a "No such file" error
    /// that its client sees as an unexpected EOF.
    #[tokio::test]
    async fn subscriber_behind_the_producer_gets_the_whole_blob_after_the_rename() {
        let (data, digest) = blob();
        let (tempfile, final_path, dir) = paths("behind");
        let entry = Arc::new(PullThroughEntry::new(digest, tempfile));
        let chunks: Vec<Result<Bytes, std::io::Error>> =
            data.chunks(50 * 1024).map(|c| Ok(Bytes::copy_from_slice(c))).collect();
        run_producer(entry.clone(), final_path.clone(), futures::stream::iter(chunks)).await.unwrap();
        assert!(final_path.exists());

        let mut body = PullThroughBody::new(entry).with_final_path(final_path);
        let mut out = Vec::new();
        drain(&mut body, &mut out).await.unwrap();
        assert_eq!(out, data);
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Same race for a subscriber already part-way through when the rename happens.
    #[tokio::test]
    async fn subscriber_mid_stream_survives_the_rename() {
        let (data, digest) = blob();
        let (tempfile, final_path, dir) = paths("mid");
        let entry = Arc::new(PullThroughEntry::new(digest, tempfile));
        let (tx, rx) = mpsc::unbounded::<Result<Bytes, std::io::Error>>();
        let producer = tokio::spawn(run_producer(entry.clone(), final_path.clone(), rx));

        tx.unbounded_send(Ok(Bytes::copy_from_slice(&data[..100 * 1024]))).unwrap();
        let mut body = PullThroughBody::new(entry.clone()).with_final_path(final_path.clone());
        let mut out = next_data(&mut body).await.unwrap().unwrap().to_vec();
        assert!(!out.is_empty() && out.len() < data.len());

        tx.unbounded_send(Ok(Bytes::copy_from_slice(&data[100 * 1024..]))).unwrap();
        drop(tx);
        producer.await.unwrap().unwrap();
        assert!(final_path.exists() && !entry.tempfile_path.exists());

        drain(&mut body, &mut out).await.unwrap();
        assert_eq!(out, data);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
