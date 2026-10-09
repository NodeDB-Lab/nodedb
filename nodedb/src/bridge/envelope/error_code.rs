// SPDX-License-Identifier: BUSL-1.1

//! Deterministic Data-Plane error codes. `error_code_from` holds the
//! conversions into them.

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
    /// The core fail-stopped: its state is unknown until a restart rebuilds
    /// it from the WAL. It refused the request and applied nothing. Another
    /// replica, or this one after its restart, serves the request.
    CoreFailStopped { core_id: usize, detail: String },
    /// A sync frame the validator refused for good. Nothing applied. The
    /// stream's high-water mark advanced to `provenance`, so the frame is
    /// never admitted again, and `applied_seq` is the mark after the refusal.
    SyncRejected {
        violation: nodedb_types::sync::violation::ViolationType,
        applied_seq: u64,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    },
    /// A sync frame the idempotency gate held back. Nothing applied, and
    /// the stream's mark did not move. `applied_seq` is that mark.
    SyncNotApplied {
        hold: super::SyncHold,
        applied_seq: u64,
    },
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
    /// A KV counter atomic read a stored value it cannot parse as a
    /// number, or computed a result out of range.
    CounterFault {
        collection: String,
        fault: super::CounterFault,
    },
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
    /// A full-text search named a field that cannot serve it, detected when
    /// the Data Plane resolves the field's index.
    TextColumn {
        collection: String,
        column: String,
        fault: nodedb_types::text_search::TextColumnFault,
    },
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
    ///
    /// `entry_index` is the forward-order position of the failed entry in
    /// the undo log. `detail` names the reverse write that failed. `cause`
    /// is the error that reverse write returned, or `None` when the engine
    /// state did not match the entry and no reverse write ran.
    RollbackFailed {
        entry_index: usize,
        detail: String,
        cause: Option<Box<ErrorCode>>,
    },
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
    /// Expression evaluation called a function no evaluator implements.
    /// Surfaces as SQLSTATE `42883` (`undefined_function`), never as a
    /// silent `NULL`.
    UndefinedFunction { name: String },
    /// A function received an argument it cannot compute on: vectors of
    /// different dimensions, an argument of the wrong type, a malformed
    /// JSONPath. Surfaces as SQLSTATE `22000` (`data_exception`).
    DataException { detail: String },
    /// A computed value lies outside the range of its result type, such as
    /// an exact integer SUM past the decimal range. SQLSTATE `22003`
    /// (`numeric_value_out_of_range`).
    NumericValueOutOfRange { detail: String },
    /// The bridge dispatcher refused the request at a capacity limit, so
    /// nothing was enqueued or applied. Transient: the same request succeeds
    /// once capacity frees. `reason` names the limit and its counts.
    DispatchCapacity { reason: String },
    /// The request's deadline passed before the core started it, so nothing
    /// ran. Distinct from [`Self::DeadlineExceeded`], which a core also
    /// answers for a task it stopped part way. Surfaces as the same
    /// query-cancelled error.
    ExpiredBeforeExecution,
    /// The request itself is malformed: an FTS query with no positive term,
    /// a value the target cannot hold. The same verdict the Control Plane
    /// gives `crate::Error::BadRequest`: SQLSTATE `42601` (syntax_error).
    BadRequest { detail: String },
    /// The whole transaction aborted before any read-set was validated, and
    /// the client retries it. SQLSTATE `40000` (transaction_rollback).
    TransactionRollback { detail: String },
    /// The statement cannot run in the current transaction state, such as
    /// inside an explicit transaction block. SQLSTATE `25001`
    /// (active_sql_transaction).
    ActiveSqlTransaction { detail: String },
    /// A DROP refused because other objects depend on its target. `object`
    /// names the target. SQLSTATE `2BP01` (dependent_objects_still_exist).
    DependentObjectsExist { object: String, detail: String },
    /// A label write needs a node label past the partition's node-label cap.
    /// Nothing of the statement applied. `node` and `label` name the refused
    /// write, and `limit` is the cap. SQLSTATE `54000` (program_limit_exceeded).
    NodeLabelLimit {
        node: String,
        label: String,
        limit: usize,
    },
    /// Text does not parse as the type of the column it is written to.
    /// `detail` names the column, the text and the type. SQLSTATE `22P02`
    /// (invalid_text_representation).
    InvalidTextRepresentation { detail: String },
    /// A value of the wrong kind for the column it is written to. `detail`
    /// names the column, the value and the type. SQLSTATE `42804`
    /// (datatype_mismatch).
    DatatypeMismatch { detail: String },
    /// Text does not parse as the `TIMESTAMP` or `TIMESTAMPTZ` it is written
    /// to. SQLSTATE `22007` (invalid_datetime_format).
    InvalidDatetimeFormat { detail: String },
    /// An instant outside the range a `TIMESTAMP` or `TIMESTAMPTZ` holds.
    /// SQLSTATE `22008` (datetime_field_overflow).
    DatetimeFieldOverflow { detail: String },
    /// The request names an object that does not exist, such as a document
    /// at a version where it had no state. `object` describes it, and the
    /// message reads "`object` does not exist". SQLSTATE `42704`
    /// (undefined_object).
    UndefinedObject { object: String },
    /// The object exists but is not in the state the request needs, such as
    /// a version whose history compaction discarded. SQLSTATE `55000`
    /// (object_not_in_prerequisite_state).
    ObjectNotInPrerequisiteState { object: String, detail: String },
}
