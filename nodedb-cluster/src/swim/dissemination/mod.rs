//! Piggyback dissemination queue.
//!
//! SWIM spreads membership deltas by attaching a small number of recent
//! rumours to every probe datagram. This module owns the queue, the
//! decay rule (Lifeguard §4.4 "least-disseminated first, drop after
//! lambda * log(n) sends"), and the thin wrapper that keeps the queue
//! consistent with [`super::MembershipList::apply`] outcomes.

pub mod apply;
pub mod entry;
pub mod queue;

pub use apply::apply_and_disseminate;
pub use entry::PendingUpdate;
pub use queue::DisseminationQueue;
