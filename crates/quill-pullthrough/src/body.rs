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
}

#[allow(clippy::large_enum_variant)]
enum State {
    Idle,
    Reading(Pin<Box<dyn Future<Output = Result<Bytes, PullThroughError>> + Send>>),
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
        }
    }

    pub fn with_chunk_capacity(mut self, cap: usize) -> Self {
        self.chunk_capacity = cap;
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
                        let path = self.entry.tempfile_path.clone();
                        let offset = self.offset;
                        let cap = self.chunk_capacity;
                        let fut = Box::pin(read_chunk(path, offset, cap, high));
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
                        Ok(bytes) => {
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
    path: std::path::PathBuf,
    offset: u64,
    cap: usize,
    high: u64,
) -> Result<Bytes, PullThroughError> {
    let to_read = high.saturating_sub(offset).min(cap as u64) as usize;
    if to_read == 0 {
        return Ok(Bytes::new());
    }
    let mut f = File::open(&path)
        .await
        .map_err(|e| PullThroughError::Io(e.to_string()))?;
    f.seek(std::io::SeekFrom::Start(offset))
        .await
        .map_err(|e| PullThroughError::Io(e.to_string()))?;
    let mut buf = vec![0u8; to_read];
    let n = f
        .read(&mut buf)
        .await
        .map_err(|e| PullThroughError::Io(e.to_string()))?;
    buf.truncate(n);
    Ok(Bytes::from(buf))
}
