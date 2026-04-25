//! WAL appender for surrogate hwm flushes.
//!
//! Decouples the registry's flush path from a concrete `WalManager`
//! handle so tests can substitute a no-op (or in-memory) appender
//! without spinning up a real WAL. Production wires
//! [`WalSurrogateAppender`].
//!
//! The appender is invoked from [`crate::control::surrogate::assign`]
//! immediately after the catalog hwm row has been persisted. Order is
//! load-bearing for crash recovery: post-restart we read the catalog
//! row first; the WAL record is only consulted if the catalog is
//! behind (S2 will wire the actual replay path).

use std::sync::Arc;

use crate::wal::WalManager;

/// Pluggable WAL appender. Tests substitute `NoopWalAppender`;
/// production wires [`WalSurrogateAppender`] (a thin wrapper over
/// `Arc<WalManager>`).
pub trait SurrogateWalAppender: Send + Sync {
    /// Append a `SurrogateAlloc` record carrying the new high-water
    /// surrogate value. Called by the surrogate-flush path after the
    /// catalog row has been updated.
    fn record_alloc_to_wal(&self, hi: u32) -> crate::Result<()>;
}

/// Production appender — wraps `Arc<WalManager>` and forwards to
/// `WalManager::append_surrogate_alloc`.
pub struct WalSurrogateAppender {
    wal: Arc<WalManager>,
}

impl WalSurrogateAppender {
    pub fn new(wal: Arc<WalManager>) -> Self {
        Self { wal }
    }
}

impl SurrogateWalAppender for WalSurrogateAppender {
    fn record_alloc_to_wal(&self, hi: u32) -> crate::Result<()> {
        self.wal.append_surrogate_alloc(hi).map(|_| ())
    }
}

/// No-op appender. Used by tests that exercise assign / flush logic
/// without a WAL (and by the bootstrap shim before the WAL is wired).
pub struct NoopWalAppender;

impl SurrogateWalAppender for NoopWalAppender {
    fn record_alloc_to_wal(&self, _hi: u32) -> crate::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_appender_succeeds() {
        let a = NoopWalAppender;
        a.record_alloc_to_wal(123).unwrap();
        a.record_alloc_to_wal(u32::MAX).unwrap();
    }
}
