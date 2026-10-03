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
    CounterFault {
        collection: String,
        fault: DataPlaneCounterFault,
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
    /// Expression evaluation called a function no evaluator implements.
    UndefinedFunction {
        name: String,
    },
    /// A function received an argument it cannot compute on (SQLSTATE
    /// `22000`). `detail` names the function and the value.
    DataException {
        detail: String,
    },
    /// A period-lock reference row exists but does not carry the
    /// configured `status_column` — a misconfigured column name, not a
    /// locked period.
    PeriodLockMisconfigured {
        collection: String,
        ref_table: String,
        status_column: String,
        row_identity: String,
    },
    /// The bridge dispatcher refused the request at a capacity limit; nothing
    /// was enqueued. `reason` names the limit and its counts.
    DispatchCapacity {
        reason: String,
    },
    /// The request's deadline passed before the core started it; nothing
    /// ran.
    ExpiredBeforeExecution,
    /// A sync frame the validator refused for good. Nothing applied. The
    /// four provenance fields name the stream position the high-water mark
    /// advanced to.
    SyncRejected {
        violation: nodedb_types::sync::violation::ViolationType,
        applied_seq: u64,
        producer_id: u64,
        epoch: u64,
        stream_id: u64,
        seq: u64,
    },
    /// A sync frame the idempotency gate held back. Nothing applied, and
    /// the stream's mark did not move.
    SyncNotApplied {
        hold: DataPlaneSyncHold,
        applied_seq: u64,
    },
    /// The request itself is malformed (SQLSTATE `42601`).
    BadRequest {
        detail: String,
    },
    /// The whole transaction aborted before any read-set was validated
    /// (SQLSTATE `40000`).
    TransactionRollback {
        detail: String,
    },
    /// The statement cannot run in the current transaction state (SQLSTATE
    /// `25001`).
    ActiveSqlTransaction {
        detail: String,
    },
    /// A DROP refused because other objects depend on `object` (SQLSTATE
    /// `2BP01`).
    DependentObjectsExist {
        object: String,
        detail: String,
    },
    /// A full-text search named a field that cannot serve it (SQLSTATE
    /// `42703` or `42804`, by fault).
    TextColumn {
        collection: String,
        column: String,
        fault: DataPlaneTextColumnFault,
    },
}

/// Wire mirror of `nodedb_types::text_search::TextColumnFault`.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum DataPlaneTextColumnFault {
    Undeclared,
    NotText { data_type: String },
    NotAColumn,
    NotIndexed,
}

/// Wire mirror of `nodedb::bridge::envelope::SyncHold`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum DataPlaneSyncHold {
    Duplicate,
    Fenced,
    Gap { expected: u64 },
}

/// Wire mirror of `nodedb_physical::kv_atomic::CounterFault`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum DataPlaneCounterFault {
    NotAnInteger,
    NotAFloat,
    IntegerOverflow,
    NonFinite,
}
