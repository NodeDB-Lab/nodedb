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
///
/// The enum is recursive: `RollbackFailed` carries the code of its cause.
/// The recursive field omits its derived bounds, and the bounds below are
/// the ones its `Box` needs.
#[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(
    __C: rkyv::validation::ArchiveContext,
    __C::Error: rkyv::rancor::Source,
)))]
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
    /// `cause` is the code of the failed reverse write, `None` when no
    /// reverse write ran.
    RollbackFailed {
        entry_index: u64,
        detail: String,
        #[rkyv(omit_bounds)]
        cause: Option<Box<DataPlaneErrorCode>>,
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
    /// A computed value lies outside the range of its result type (SQLSTATE
    /// `22003`).
    NumericValueOutOfRange {
        detail: String,
    },
    /// A label write needs a node label past the partition's node-label cap
    /// (SQLSTATE `54000`). `limit` is `u64` on the wire, for the same reason
    /// as `max_depth`.
    NodeLabelLimit {
        node: String,
        label: String,
        limit: u64,
    },
    /// Text does not parse as the column's type (SQLSTATE `22P02`).
    InvalidTextRepresentation {
        detail: String,
    },
    /// A value of the wrong kind for the column's type (SQLSTATE `42804`).
    DatatypeMismatch {
        detail: String,
    },
    /// Text does not parse as a timestamp (SQLSTATE `22007`).
    InvalidDatetimeFormat {
        detail: String,
    },
    /// An instant outside the timestamp range (SQLSTATE `22008`).
    DatetimeFieldOverflow {
        detail: String,
    },
    /// The request names an object that does not exist (SQLSTATE `42704`).
    UndefinedObject {
        object: String,
    },
    /// The object is not in the state the request needs (SQLSTATE `55000`).
    ObjectNotInPrerequisiteState {
        object: String,
        detail: String,
    },
    /// The serving core fail-stopped and applied nothing. Another replica, or
    /// the same one after a restart, serves the request.
    CoreFailStopped {
        core_id: u64,
        detail: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A nested rollback cause survives an rkyv encode and a checked decode.
    #[test]
    fn nested_rollback_cause_roundtrips_through_rkyv() {
        let code = DataPlaneErrorCode::RollbackFailed {
            entry_index: 2,
            detail: "outer".into(),
            cause: Some(Box::new(DataPlaneErrorCode::RollbackFailed {
                entry_index: 1,
                detail: "inner".into(),
                cause: Some(Box::new(DataPlaneErrorCode::Internal {
                    detail: "commit".into(),
                })),
            })),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&code).expect("encode");
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(&bytes);
        let decoded =
            rkyv::from_bytes::<DataPlaneErrorCode, rkyv::rancor::Error>(&aligned).expect("decode");
        assert_eq!(decoded, code);
    }
}
