//! Streaming pull-through cache (PLAN.md §5).
//!
//! On a cache miss, exactly one producer fetches from upstream into a tempfile
//! while emitting `Notify` progress events; one or more consumers tail the
//! tempfile and stream bytes back to their HTTP clients. On digest verification,
//! the tempfile is atomically renamed into CAS and all consumers see clean EOF.

pub mod body;
pub mod entry;
pub mod producer;
pub mod table;

pub use body::PullThroughBody;
pub use entry::{PullThroughEntry, PullThroughError, ProducerOutcome};
pub use producer::run_producer;
pub use table::{ProducerRole, PullThroughTable};
