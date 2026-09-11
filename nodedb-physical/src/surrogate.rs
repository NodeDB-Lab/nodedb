// SPDX-License-Identifier: Apache-2.0

//! Surrogate-allocation contract used by the shared SqlPlan → PhysicalPlan
//! converter. Origin's WAL-durable, Raft-replicated allocator implements this
//! trait; Lite supplies its own local-monotonic implementation.
//!
//! Synchronous-only: the converter runs on the Control Plane in `Send + Sync`
//! code paths. Origin's async surrogate-fetch work stays internal to its impl
//! and is hidden behind this sync facade.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

/// Errors a [`SurrogateAssigner`] may return.
///
/// The error surface is deliberately narrow — the converter does not need to
/// distinguish more cases. Origin's rich allocator errors collapse to one of
/// these at the trait boundary; the original error is preserved in
/// [`SurrogateAssignError::Backend`]'s message.
#[derive(Debug, thiserror::Error)]
pub enum SurrogateAssignError {
    #[error("surrogate registry lock poisoned")]
    LockPoisoned,
    #[error("surrogate backend: {0}")]
    Backend(String),
}

/// Allocate stable, cross-engine surrogates for `(collection, pk_bytes)`.
///
/// Implementations must be:
/// - **idempotent**: repeated calls for the same `(collection, pk_bytes)`
///   return the same `Surrogate`;
/// - **monotonic**: every allocated value is greater than every previously
///   allocated value within the same allocator;
/// - **`Send + Sync`**: the converter holds a reference across `await`
///   points on the Control Plane.
pub trait SurrogateAssigner: Send + Sync {
    /// Highest surrogate ever issued by this assigner. `0` on a fresh
    /// allocator. Used by CLONE DATABASE to capture an AS-OF cutoff.
    fn current_hwm(&self) -> u32;

    /// Resolve `(database_id, tenant_id, collection, pk_bytes)` to a stable
    /// surrogate. Allocate on the first call; return the persisted value on
    /// every subsequent call (UPSERT preserves the surrogate).
    fn assign(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        pk_bytes: &[u8],
    ) -> Result<Surrogate, SurrogateAssignError>;

    /// Allocate a FRESH, never-before-issued surrogate for a row that has no
    /// content primary key — i.e. a collection whose primary key is the
    /// auto-generated `_rowid` (no `PRIMARY KEY` was declared at CREATE), or
    /// a timeseries row.
    ///
    /// Every call allocates a new value. There is no `pk_bytes` to
    /// content-address on, so repeated calls never collapse onto one
    /// surrogate.
    ///
    /// The returned `String` is the bound identity. The caller uses it
    /// verbatim and never re-derives it.
    fn assign_fresh(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> Result<(Surrogate, String), SurrogateAssignError>;
}
