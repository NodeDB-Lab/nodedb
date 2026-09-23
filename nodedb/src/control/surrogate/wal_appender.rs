// SPDX-License-Identifier: BUSL-1.1

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

use nodedb_types::{DatabaseId, TenantId};

use crate::wal::WalManager;
use crate::wal::manager::NO_APPLY_KEY;

/// Pluggable WAL appender. Tests substitute `NoopWalAppender`;
/// production wires [`WalSurrogateAppender`] (a thin wrapper over
/// `Arc<WalManager>`).
pub trait SurrogateWalAppender: Send + Sync {
    /// Append a `SurrogateAlloc` record carrying the new high-water
    /// surrogate value. Called by the surrogate-flush path after the
    /// catalog row has been updated.
    fn record_alloc_to_wal(&self, hi: u32) -> crate::Result<()>;

    /// Append a `SurrogateBind` record carrying the
    /// `(surrogate, collection, pk_bytes)` triple. Called by
    /// `SurrogateAssigner::assign` after the catalog two-table txn so
    /// the binding is durable before the write-lock is released. The
    /// `(database_id, tenant_id)` scope is stamped into the record header
    /// so replay re-keys the binding under the same scope.
    fn record_bind_to_wal(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        surrogate: u32,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<()>;
}

/// Production appender — wraps `Arc<WalManager>` and forwards to
/// `WalAppender::append_surrogate_alloc`.
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
        self.wal
            .appender(NO_APPLY_KEY)
            .append_surrogate_alloc(hi)
            .map(|_| ())
    }

    fn record_bind_to_wal(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        surrogate: u32,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<()> {
        self.wal.appender(NO_APPLY_KEY).append_surrogate_bind(
            database_id,
            tenant_id,
            surrogate,
            collection,
            pk_bytes,
        )?;
        // Force the record to disk before the assigner releases its
        // write-lock. A crash after `assign` returns must always see
        // the binding on replay; group-commit batching alone does not
        // give us that guarantee.
        self.wal.sync()
    }
}

/// No-op appender. Used by tests that exercise assign / flush logic
/// without a WAL (and by the bootstrap shim before the WAL is wired).
pub struct NoopWalAppender;

impl SurrogateWalAppender for NoopWalAppender {
    fn record_alloc_to_wal(&self, _hi: u32) -> crate::Result<()> {
        Ok(())
    }

    fn record_bind_to_wal(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _surrogate: u32,
        _collection: &str,
        _pk_bytes: &[u8],
    ) -> crate::Result<()> {
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
