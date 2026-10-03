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
    /// Expression evaluation called a function no evaluator implements.
    /// Surfaces as SQLSTATE `42883` (`undefined_function`), never as a
    /// silent `NULL`.
    UndefinedFunction { name: String },
    /// A function received an argument it cannot compute on: vectors of
    /// different dimensions, an argument of the wrong type, a malformed
    /// JSONPath. Surfaces as SQLSTATE `22000` (`data_exception`).
    DataException { detail: String },
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
}

/// An expression evaluation failure, as the Data Plane reports it.
///
/// Exhaustive, so a new evaluator error picks its own code rather than
/// defaulting to one.
impl From<nodedb_query::EvalError> for ErrorCode {
    fn from(e: nodedb_query::EvalError) -> Self {
        match e {
            nodedb_query::EvalError::DivisionByZero => Self::DivisionByZero,
            nodedb_query::EvalError::UnknownFunction { name } => Self::UndefinedFunction { name },
            e @ (nodedb_query::EvalError::VectorDimensionMismatch { .. }
            | nodedb_query::EvalError::ArgumentType { .. }
            | nodedb_query::EvalError::InvalidJsonPath { .. }
            | nodedb_query::EvalError::NumericOverflow { .. }) => Self::DataException {
                detail: e.to_string(),
            },
        }
    }
}

/// A KV write that binds a row to `Surrogate::ZERO` fails prevalidation, the
/// same refusal the dispatch-time check returns.
impl From<crate::engine::kv::UnboundKvWrite> for ErrorCode {
    fn from(e: crate::engine::kv::UnboundKvWrite) -> Self {
        Self::RejectedPrevalidation {
            reason: e.to_string(),
        }
    }
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
            crate::Error::CollectionNotFound { .. }
            | crate::Error::CollectionDeactivated { .. }
            | crate::Error::DocumentNotFound { .. } => Self::NotFound,
            crate::Error::RejectedAuthz { resource, .. } => Self::RejectedAuthz { resource },
            // The Control Plane gives both `40001` (serialization_failure).
            crate::Error::ConflictRetry { .. } | crate::Error::CalvinSerializationConflict => {
                Self::ConflictRetry
            }
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
            // A capacity refusal enqueued nothing, and the same request
            // succeeds once capacity frees.
            capacity @ crate::Error::DispatchCapacity { .. } => Self::DispatchCapacity {
                reason: capacity.to_string(),
            },
            crate::Error::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded { limit }
            }
            crate::Error::DivisionByZero => Self::DivisionByZero,
            crate::Error::UndefinedFunction { name } => Self::UndefinedFunction { name },
            crate::Error::DataException { detail } => Self::DataException { detail },
            // `42601` (syntax_error), as the Control Plane gives both.
            crate::Error::BadRequest { detail } | crate::Error::PlanError { detail } => {
                Self::BadRequest { detail }
            }
            // `0A000` (feature_not_supported), as the Control Plane gives both.
            crate::Error::FeatureNotSupported { detail } => Self::Unsupported { detail },
            unsupported @ crate::Error::CrossCollectionNotColocated { .. } => Self::Unsupported {
                detail: unsupported.to_string(),
            },
            crate::Error::UndefinedColumn { column } => Self::UndefinedColumn { column },
            crate::Error::TextColumn {
                collection,
                column,
                fault,
            } => Self::TextColumn {
                collection,
                column,
                fault,
            },
            // Same condition an undefined column reports at plan time, raised
            // here by the strict encoder for a transport the planner never
            // sees (native client, `COPY FROM`, CRDT delta merge).
            crate::Error::UnknownStrictField { column, .. } => Self::UndefinedColumn { column },
            // Already a Data-Plane verdict: hand back the same code rather
            // than re-wrapping it as `Internal` and losing its SQLSTATE.
            crate::Error::DataPlane(code) => code,
            // Class `22`, the class the Control Plane gives both.
            e @ (crate::Error::OffsetRegression { .. }
            | crate::Error::BackupTenantMismatch { .. }
            | crate::Error::InvalidLimitValue { .. }) => Self::DataException {
                detail: e.to_string(),
            },
            // `40000`, as the Control Plane gives it.
            e @ crate::Error::CalvinParticipantError => Self::TransactionRollback {
                detail: e.to_string(),
            },
            // `40001`: the client retries the statement.
            e @ crate::Error::RetryableSchemaChanged { .. } => Self::RetryableRefusal {
                reason: e.to_string(),
            },
            // `25001`, as the Control Plane gives all three.
            e @ (crate::Error::CrdtApplyForbiddenInTransaction
            | crate::Error::NotInTransactionBlock { .. }
            | crate::Error::CrossShardInExplicitTransaction) => Self::ActiveSqlTransaction {
                detail: e.to_string(),
            },
            // `2BP01`, as the Control Plane gives both. The detail is the
            // public message the Control Plane renders.
            crate::Error::DependentObjectsExist {
                root_kind,
                root_name,
                dependent_count,
                dependents,
                ..
            } => {
                let (object, detail) = crate::error_classify::dependent_objects_text(
                    root_kind,
                    &root_name,
                    dependent_count,
                    &dependents,
                );
                Self::DependentObjectsExist { object, detail }
            }
            crate::Error::RoleInUse { role, dependents } => {
                let object = format!("role \"{role}\"");
                let detail = crate::Error::RoleInUse { role, dependents }.to_string();
                Self::DependentObjectsExist { object, detail }
            }
            crate::Error::CrdtAdmissionRetriesExhausted { .. } => Self::ConflictRetry,
            // Retryable refusals whose class (`55P03`) no Data-Plane code has.
            // The retry contract survives: nothing was applied.
            e @ (crate::Error::NoLeader { .. }
            | crate::Error::GroupQuorumUnavailable { .. }
            | crate::Error::GroupMarksUnavailable { .. }
            | crate::Error::BackupCaptureMoved { .. }
            | crate::Error::AuthorizationStateBehind { .. }
            | crate::Error::LinearizableReadRefused { .. }
            | crate::Error::StaleReadNotLeader { .. }) => Self::RetryableRefusal {
                reason: e.to_string(),
            },
            // Class `57`: the client retries once the leader settles.
            e @ crate::Error::NotLeader { .. } => Self::DispatchCapacity {
                reason: e.to_string(),
            },
            crate::Error::CrdtAdmissionTimeout { .. } => Self::DeadlineExceeded,
            e @ crate::Error::VShardAdmissionCapacityExceeded { .. } => Self::RateExceeded {
                gate: e.to_string(),
                retry_after_ms: 0,
            },
            // Class `53`: a configured resource ceiling.
            crate::Error::QuotaOvercommit { .. }
            | crate::Error::TenantVectorDimExceeded { .. }
            | crate::Error::TenantGraphDepthExceeded { .. } => Self::ResourcesExhausted,
            // Class `28` has no Data-Plane code. The nearest is the access
            // refusal, which keeps it a client error the client cannot retry.
            e @ (crate::Error::BackupKeyMismatch | crate::Error::SessionTokenExpired) => {
                Self::RejectedAuthz {
                    resource: e.to_string(),
                }
            }
            // Client errors of class `42`, and client errors whose class
            // (`25006`, `55`) no Data-Plane code has. `BadRequest` is the
            // class their public code has.
            e @ (crate::Error::CrdtAdmissionInvalidPlan { .. }
            | crate::Error::CrdtAdmissionCallerFence
            | crate::Error::CrdtApplyRequiresAdmission
            | crate::Error::CloneWriteRequiresMaterialize { .. }
            | crate::Error::ObjectNotInPrerequisiteState { .. }
            | crate::Error::MirrorReadOnly { .. }
            | crate::Error::UndefinedObject { .. }
            | crate::Error::AmbiguousColumn { .. }
            | crate::Error::ExecutionLimitExceeded { .. }
            | crate::Error::LimitExceeded { .. }
            | crate::Error::Promql(_)
            | crate::Error::SequencerUnavailable
            | crate::Error::SessionCapExceeded { .. }
            | crate::Error::SessionIdleTimeout
            | crate::Error::SessionKilledByAdmin
            | crate::Error::SessionUserDropped
            | crate::Error::OidcProviderTenantUnbound
            | crate::Error::OidcProviderTenantUnavailable { .. }
            | crate::Error::ExternalRoleUndefined { .. }
            | crate::Error::OidcNoDefaultDatabase { .. }
            | crate::Error::RoleInheritanceCycle { .. }
            | crate::Error::RoleInheritanceDepthExceeded { .. }) => Self::BadRequest {
                detail: e.to_string(),
            },
            // Retry exhaustion takes the code of its cause.
            crate::Error::OllpExhausted { cause, .. } => match cause {
                crate::OllpExhaustedCause::PredicateDrift => Self::ConflictRetry,
                crate::OllpExhaustedCause::PreAdmission(inner) => Self::from(*inner),
                crate::OllpExhaustedCause::AdmissionRefused { detail } => Self::RateExceeded {
                    gate: detail,
                    retry_after_ms: 0,
                },
            },
            // Server-side faults and system defects. `Shaping`,
            // `RemoteTyped` and `Ddl` carry a public numeric code that has no
            // Data-Plane twin, and none is raised on the Data Plane.
            e @ (crate::Error::MaterializedSumResolutionMissing { .. }
            | crate::Error::RetryableLeaderChange { .. }
            | crate::Error::CommittedResultUnavailable { .. }
            | crate::Error::ProposalOutcomeUnknown { .. }
            | crate::Error::MetadataLeaderUnavailable
            | crate::Error::Wal(_)
            | crate::Error::Dispatch { .. }
            | crate::Error::Storage { .. }
            | crate::Error::ColdStorage { .. }
            | crate::Error::Serialization { .. }
            | crate::Error::Codec { .. }
            | crate::Error::SegmentCorrupted { .. }
            | crate::Error::Crdt(_)
            | crate::Error::Io(_)
            | crate::Error::Config { .. }
            | crate::Error::Encryption { .. }
            | crate::Error::Bridge { .. }
            | crate::Error::VersionCompat { .. }
            | crate::Error::RestoreTargetNotEmpty { .. }
            | crate::Error::RestoreVerificationFailed { .. }
            | crate::Error::Internal { .. }
            | crate::Error::Shaping(_)
            | crate::Error::RemoteTyped { .. }
            | crate::Error::Ddl(_)
            | crate::Error::DescriptorVersionAnomaly { .. }
            | crate::Error::CollectionPurgeRowMissing { .. }
            | crate::Error::CollectionUnstamped { .. }
            | crate::Error::CatalogIntegrityViolation { .. }
            | crate::Error::CascadeCycle { .. }) => Self::Internal {
                detail: e.to_string(),
            },
        }
    }
}
