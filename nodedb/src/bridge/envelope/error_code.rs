// SPDX-License-Identifier: BUSL-1.1

//! Deterministic Data-Plane error codes, and their conversions to and from
//! the crate's typed error.

/// Deterministic error codes returned by the Data Plane.
///
/// Final outcomes are explicit, never opaque strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// Constraint violation at commit time.
    ///
    /// `constraint` names the kind (`unique`, `not_null`, ...). `detail`
    /// carries the human-readable explanation (e.g. which primary-key
    /// value conflicted) so pgwire drivers can surface it to the user.
    RejectedConstraint { constraint: String, detail: String },
    /// Pre-validation fast-reject. Permanent: the same bytes fail identically.
    RejectedPrevalidation { reason: String },
    /// Nothing was applied, and the identical frame at the same sequence is
    /// expected to succeed once a transient precondition resolves.
    ///
    /// Kept distinct from [`Self::RejectedPrevalidation`] so a caller that owns
    /// a retry channel can tell a retry apart from a permanent refusal instead
    /// of collapsing both into a terminal rejection.
    RetryableRefusal { reason: String },
    /// Document/collection not found.
    NotFound,
    /// Authorization failure.
    ///
    /// `resource` says what was refused and by what — "RLS write policy on
    /// 'orders' rejected the row", "permission denied: collection 'x'". It is
    /// carried rather than dropped because the caller cannot act on, or even
    /// recognise, a bare "authorization denied": a row-level-security refusal
    /// and a missing GRANT need opposite responses, and only this string tells
    /// them apart once the verdict has crossed the bridge.
    RejectedAuthz { resource: String },
    /// Write conflict — client should retry.
    ConflictRetry,
    /// A CRDT apply no longer matches the domain-bound frontier it previewed.
    CrdtFrontierMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// Fan-out limit exceeded for graph/scatter queries.
    FanOutExceeded,
    /// Memory budget exhausted — DataFusion should spill.
    ResourcesExhausted,
    /// Edge creation rejected: source or destination node does not exist.
    RejectedDanglingEdge { missing_node: String },
    /// Duplicate write detected via idempotency key.
    DuplicateWrite,
    /// Append-only collection: UPDATE/DELETE not allowed.
    AppendOnlyViolation { collection: String },
    /// BALANCED constraint: debit/credit sums don't match.
    BalanceViolation { collection: String, detail: String },
    /// Period is closed/locked: writes rejected.
    PeriodLocked { collection: String },
    /// A period-lock reference row exists but does not carry the
    /// configured `status_column` — a misconfigured column name, refused
    /// as a config error rather than admitted or treated as locked.
    PeriodLockMisconfigured {
        collection: String,
        ref_table: String,
        status_column: String,
        row_identity: String,
    },
    /// Retention period not expired: DELETE rejected.
    RetentionViolation { collection: String },
    /// Legal hold active: DELETE rejected.
    LegalHoldActive { collection: String },
    /// State transition not in allowed list.
    StateTransitionViolation { collection: String, detail: String },
    /// Transition check predicate returned false, or failed to evaluate —
    /// `detail` distinguishes the two.
    TransitionCheckViolation { collection: String, detail: String },
    /// Type guard violation: field type mismatch or REQUIRED absent.
    TypeGuardViolation { collection: String, detail: String },
    /// Value type does not match expected type for operation (e.g. INCR on a string).
    TypeMismatch { collection: String, detail: String },
    /// Arithmetic overflow (e.g. i64::MAX + 1 on INCR).
    OverflowError { collection: String },
    /// Insufficient balance for transfer (source lacks required amount).
    InsufficientBalance { collection: String, detail: String },
    /// Rate limit exceeded for a rate gate / cooldown.
    RateExceeded { gate: String, retry_after_ms: u64 },
    /// The collection is currently draining for hard-delete. New scans
    /// are refused until the drain resolves (or is cleared). Maps to
    /// `NodeDbError::collection_draining` (code 1102) at the
    /// Control-Plane boundary.
    CollectionDraining { collection: String },
    /// WITH RECURSIVE CTE exceeded the configured maximum recursion depth.
    /// The client should either add a stricter termination condition or
    /// increase `max_recursion_depth` in the server configuration.
    RecursionDepthExceeded { cte_name: String, max_depth: usize },
    /// A column reference resolved against nothing in scope. Raised where the
    /// Data Plane evaluates an expression whose column set is only known at
    /// execution time (a value-generating `WITH RECURSIVE` step), so the
    /// planner cannot catch it. Maps to `42703` (undefined_column), the same
    /// SQLSTATE a planner-side `SqlError::UnknownColumn` produces — a missing
    /// column reports identically wherever it is detected.
    UndefinedColumn { column: String },
    /// Internal error (io_uring failure, corruption, etc.)
    Internal { detail: String },
    /// Operation is not supported on this engine, or not yet implemented for
    /// this op-type. Distinguished from `Internal` so pgwire surfaces it as
    /// `0A000` (feature_not_supported) rather than `XX000`.
    Unsupported { detail: String },
    /// Transaction rollback failed: at least one undo entry could not be
    /// applied. The shard state is unknown — the client must treat this as a
    /// fatal error and the operator must restart the shard (WAL replay restores
    /// correct state on startup). Never silently continues.
    RollbackFailed { entry_index: usize, detail: String },
    /// The active Calvin executor detected that the declared predicate no
    /// longer matches the engine state at execution time (OLLP mismatch).
    /// No write was applied. The OLLP orchestrator retries with a fresh
    /// pre-execution scan.
    ///
    /// Numeric value: `OLLP_RETRY_REQUIRED_CODE` (0xCAAD) — single source of
    /// truth defined in `control/cluster/calvin/executor/ollp/orchestrator.rs`.
    OllpRetryRequired,
    /// The per-transaction staging overlay exceeded its per-core byte budget.
    /// Surfaces as `program_limit_exceeded` (54000) so clients know the
    /// transaction is too large to stage rather than that it hit an internal
    /// fault.
    TxnOverlayMemoryExceeded { limit: usize },
    /// Expression evaluation divided or took a modulus by zero. Distinct
    /// from `Internal` so it survives the Data Plane
    /// → pgwire boundary (including `result_stream::stream_response_channel`,
    /// which special-cases this variant the same way it already
    /// special-cases `NotFound`) and reaches the client as SQLSTATE `22012`
    /// rather than the generic `XX000` every `Internal` maps to.
    DivisionByZero,
}

impl From<crate::Error> for ErrorCode {
    fn from(e: crate::Error) -> Self {
        match e {
            crate::Error::DeadlineExceeded { .. } => Self::DeadlineExceeded,
            crate::Error::RejectedConstraint {
                constraint, detail, ..
            } => Self::RejectedConstraint { constraint, detail },
            crate::Error::RejectedPrevalidation { reason, .. } => {
                Self::RejectedPrevalidation { reason }
            }
            crate::Error::RetryableRefusal { reason } => Self::RetryableRefusal { reason },
            crate::Error::CollectionNotFound { .. } | crate::Error::DocumentNotFound { .. } => {
                Self::NotFound
            }
            crate::Error::RejectedAuthz { resource, .. } => Self::RejectedAuthz { resource },
            crate::Error::ConflictRetry { .. } => Self::ConflictRetry,
            crate::Error::FanOutExceeded { .. } => Self::FanOutExceeded,
            crate::Error::MemoryExhausted { .. } => Self::ResourcesExhausted,
            crate::Error::Backpressure { .. } => Self::ResourcesExhausted,
            crate::Error::AppendOnlyViolation { collection, .. } => {
                Self::AppendOnlyViolation { collection }
            }
            crate::Error::BalanceViolation {
                collection, detail, ..
            } => Self::BalanceViolation { collection, detail },
            // A materialized-sum target that cannot be addressed breaks the
            // balance invariant the target collection maintains, so it crosses
            // the bridge as the same class of violation the Control Plane
            // already renders it as — not as a generic `Internal`, which would
            // reach the client as SQLSTATE `XX000` and lose the target
            // collection, join column, and join value the message names.
            crate::Error::MaterializedSumTargetNotFound {
                target_collection,
                join_column,
                join_value,
            } => Self::BalanceViolation {
                collection: target_collection,
                detail: format!(
                    "no row with primary key '{join_value}', referenced by join column \
                     '{join_column}'"
                ),
            },
            crate::Error::PeriodLocked { collection, .. } => Self::PeriodLocked { collection },
            crate::Error::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            } => Self::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                row_identity,
            },
            crate::Error::RetentionViolation { collection, .. } => {
                Self::RetentionViolation { collection }
            }
            crate::Error::LegalHoldActive { collection, .. } => {
                Self::LegalHoldActive { collection }
            }
            crate::Error::StateTransitionViolation {
                collection, detail, ..
            } => Self::StateTransitionViolation { collection, detail },
            crate::Error::TransitionCheckViolation { collection, detail } => {
                Self::TransitionCheckViolation { collection, detail }
            }
            crate::Error::TypeGuardViolation {
                collection, detail, ..
            } => Self::TypeGuardViolation { collection, detail },
            crate::Error::TypeMismatch {
                collection, detail, ..
            } => Self::TypeMismatch { collection, detail },
            crate::Error::OverflowError { collection, .. } => Self::OverflowError { collection },
            crate::Error::InsufficientBalance {
                collection, detail, ..
            } => Self::InsufficientBalance { collection, detail },
            crate::Error::RateExceeded {
                gate,
                retry_after_ms,
                ..
            } => Self::RateExceeded {
                gate,
                retry_after_ms,
            },
            crate::Error::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded { limit }
            }
            crate::Error::DivisionByZero => Self::DivisionByZero,
            crate::Error::UndefinedColumn { column } => Self::UndefinedColumn { column },
            // Same condition an undefined column reports at plan time, raised
            // here by the strict encoder for a transport the planner never
            // sees (native client, `COPY FROM`, CRDT delta merge).
            crate::Error::UnknownStrictField { column, .. } => Self::UndefinedColumn { column },
            // Already a Data-Plane verdict: hand back the same code rather
            // than re-wrapping it as `Internal` and losing its SQLSTATE.
            crate::Error::DataPlane(code) => code,
            other => Self::Internal {
                detail: other.to_string(),
            },
        }
    }
}
