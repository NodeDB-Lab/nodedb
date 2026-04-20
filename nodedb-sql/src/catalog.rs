//! `SqlCatalog` trait + descriptor-resolution error type.
//!
//! The SQL planner resolves collection metadata through the
//! `SqlCatalog` trait. Both Origin (via the host-side
//! `CredentialStore` + `SystemCatalog`) and Lite (via the embedded
//! redb catalog) implement it. The trait lives in its own file so
//! `types.rs` stays under the 500-line limit and so the error
//! surface has headroom for additional variants.

use thiserror::Error;

use crate::types::CollectionInfo;

/// Errors surfaced by `SqlCatalog` implementations.
///
/// Only one variant today — callers pattern-match directly and
/// map the retryable case to `SqlError::RetryableSchemaChanged`
/// via the `From` impl in `error.rs`. The enum shape is kept
/// despite having a single variant so future variants can be
/// added without a breaking change.
#[derive(Debug, Clone, Error)]
pub enum SqlCatalogError {
    /// A DDL drain is in progress on the descriptor at the
    /// version the planner wanted to acquire a lease on. Callers
    /// should retry the whole plan after a short backoff — by
    /// then either the drain has completed (new descriptor
    /// version available in the cache) or the retry budget is
    /// exhausted and a typed error surfaces to the client.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged {
        /// Human-readable identifier for the descriptor, e.g.
        /// `"collection orders"`. Used in log / trace output.
        descriptor: String,
    },

    /// Collection is soft-deleted (`DROP COLLECTION` run, retention
    /// window still active). Distinct from `Ok(None)` = absent so the
    /// planner can surface an actionable error with an `UNDROP`
    /// hint rather than a generic "unknown table".
    #[error(
        "collection '{name}' was dropped and is within its retention window; \
         restore with UNDROP COLLECTION before {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        name: String,
        /// Wall-clock nanoseconds when retention elapses and the
        /// collection is hard-deleted by the GC sweeper.
        retention_expires_at_ns: u64,
    },
}

/// Trait for looking up collection metadata during planning.
///
/// Both Origin (via CredentialStore) and Lite (via the embedded
/// redb catalog) implement this trait.
///
/// The return type is `Result<Option<CollectionInfo>, _>` with
/// a three-way semantics:
///
/// - `Ok(Some(info))` — the collection exists and is usable.
///   An Origin implementation will have acquired a descriptor
///   lease at the current version before returning; subsequent
///   planning against the same collection within the lease
///   window is drain-safe.
/// - `Ok(None)` — the collection does not exist. Callers should
///   surface this as `SqlError::UnknownTable`.
/// - `Err(SqlCatalogError::RetryableSchemaChanged { .. })` —
///   the collection exists but a DDL drain is in progress.
///   Callers propagate this up so the pgwire layer can retry
///   the whole statement.
pub trait SqlCatalog {
    fn get_collection(&self, name: &str) -> Result<Option<CollectionInfo>, SqlCatalogError>;
}
