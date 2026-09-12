// SPDX-License-Identifier: BUSL-1.1

//! Wire mirror of the Data-Plane verdict code.
//!
//! The executing shard's `ErrorCode` (owned by the `nodedb` crate, which
//! depends on this one) crosses the node hop verbatim as this enum, so the
//! coordinator rebuilds the exact code and renders the same SQLSTATE a
//! single-node execution would. Variants are appended, never reordered.

/// Data-Plane verdict carried across a node hop.
///
/// One variant per `nodedb::bridge::envelope::ErrorCode` variant; the `nodedb`
/// side converts both ways with exhaustive matches, so a new code fails to
/// compile there until it is mirrored here.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum DataPlaneErrorCode {
    DeadlineExceeded,
    RejectedConstraint {
        constraint: String,
        detail: String,
    },
    RejectedPrevalidation {
        reason: String,
    },
    RetryableRefusal {
        reason: String,
    },
    NotFound,
    RejectedAuthz {
        resource: String,
    },
    ConflictRetry,
    CrdtFrontierMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    FanOutExceeded,
    ResourcesExhausted,
    RejectedDanglingEdge {
        missing_node: String,
    },
    DuplicateWrite,
    AppendOnlyViolation {
        collection: String,
    },
    BalanceViolation {
        collection: String,
        detail: String,
    },
    PeriodLocked {
        collection: String,
    },
    RetentionViolation {
        collection: String,
    },
    LegalHoldActive {
        collection: String,
    },
    StateTransitionViolation {
        collection: String,
        detail: String,
    },
    TransitionCheckViolation {
        collection: String,
        detail: String,
    },
    TypeGuardViolation {
        collection: String,
        detail: String,
    },
    TypeMismatch {
        collection: String,
        detail: String,
    },
    OverflowError {
        collection: String,
    },
    InsufficientBalance {
        collection: String,
        detail: String,
    },
    RateExceeded {
        gate: String,
        retry_after_ms: u64,
    },
    CollectionDraining {
        collection: String,
    },
    /// `max_depth` is `u64` on the wire: a `usize` archives at the sender's
    /// pointer width and would not decode on a differently sized peer.
    RecursionDepthExceeded {
        cte_name: String,
        max_depth: u64,
    },
    UndefinedColumn {
        column: String,
    },
    Internal {
        detail: String,
    },
    Unsupported {
        detail: String,
    },
    /// `entry_index` is `u64` on the wire, for the same reason as `max_depth`.
    RollbackFailed {
        entry_index: u64,
        detail: String,
    },
    OllpRetryRequired,
    /// `limit` is `u64` on the wire, for the same reason as `max_depth`.
    TxnOverlayMemoryExceeded {
        limit: u64,
    },
    DivisionByZero,
    /// A period-lock reference row exists but does not carry the
    /// configured `status_column` — a misconfigured column name, not a
    /// locked period.
    PeriodLockMisconfigured {
        collection: String,
        ref_table: String,
        status_column: String,
        row_identity: String,
    },
}
