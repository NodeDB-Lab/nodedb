// SPDX-License-Identifier: BUSL-1.1

use crate::types::{DatabaseId, RequestId, TenantId, VShardId};

/// Internal error classes for NodeDB Origin.
///
/// Every error is actionable — clients can programmatically handle each variant.
/// Cross-plane errors surface deterministic codes, never opaque strings.
///
/// At the public API boundary, `Error` converts to [`nodedb_types::error::NodeDbError`] via `From`,
/// so external consumers never see infrastructure details like `WalError` or
/// `CrdtError`.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // --- Write path errors ---
    #[error("constraint violation on {collection}: {detail}")]
    RejectedConstraint {
        collection: String,
        constraint: String,
        detail: String,
    },

    #[error("transaction staging overlay exceeded its {limit}-byte per-core budget")]
    TxnOverlayMemoryExceeded { limit: usize },

    #[error("authorization denied for tenant {tenant_id} on {resource}")]
    RejectedAuthz {
        tenant_id: TenantId,
        resource: String,
    },

    #[error(
        "offset regression on stream '{stream}' group '{group}' partition {partition_id}: \
         attempted LSN {attempted_lsn} < current committed LSN {current_lsn}"
    )]
    OffsetRegression {
        stream: String,
        group: String,
        partition_id: u32,
        current_lsn: u64,
        attempted_lsn: u64,
    },

    #[error("request {request_id} exceeded deadline")]
    DeadlineExceeded { request_id: RequestId },

    #[error("write conflict on {collection}/{document_id}, retry with idempotency key")]
    ConflictRetry {
        collection: String,
        document_id: String,
    },

    /// A cross-shard Calvin transaction's global OCC verdict was ABORT: a
    /// participant's read-set validation failed at the cross-shard commit
    /// barrier, so the writes were dropped. A serialization failure — the
    /// client should retry the whole transaction. Maps to SQLSTATE `40001`
    /// (serialization_failure) on both the pgwire and native transports.
    #[error(
        "cross-shard transaction aborted: serialization conflict (read-set validation failed); retry"
    )]
    CalvinSerializationConflict,

    #[error("CRDT delta pre-validation rejected: {constraint} — {reason}")]
    RejectedPrevalidation { constraint: String, reason: String },

    #[error("append-only violation on {collection}: {detail}")]
    AppendOnlyViolation { collection: String, detail: String },

    #[error("balance violation on {collection}: {detail}")]
    BalanceViolation { collection: String, detail: String },

    #[error("period locked on {collection}: {detail}")]
    PeriodLocked { collection: String, detail: String },

    #[error("retention violation on {collection}: {detail}")]
    RetentionViolation { collection: String, detail: String },

    #[error("legal hold active on {collection}: {detail}")]
    LegalHoldActive { collection: String, detail: String },

    #[error("state transition violation on {collection}: {detail}")]
    StateTransitionViolation { collection: String, detail: String },

    #[error("transition check violation on {collection}: {detail}")]
    TransitionCheckViolation { collection: String, detail: String },

    #[error("type guard violation on {collection}: {detail}")]
    TypeGuardViolation { collection: String, detail: String },

    #[error("type mismatch on {collection} key {key}: {detail}")]
    TypeMismatch {
        collection: String,
        key: String,
        detail: String,
    },

    #[error("arithmetic overflow on {collection} key {key}")]
    OverflowError { collection: String, key: String },

    #[error("insufficient balance on {collection} key {key}: {detail}")]
    InsufficientBalance {
        collection: String,
        key: String,
        detail: String,
    },

    #[error("rate limit exceeded for {gate}: {detail}")]
    RateExceeded {
        gate: String,
        detail: String,
        retry_after_ms: u64,
    },

    // --- Read path errors ---
    #[error("collection {collection} not found for tenant {tenant_id}")]
    CollectionNotFound {
        tenant_id: TenantId,
        collection: String,
    },

    #[error("document {document_id} not found in {collection}")]
    DocumentNotFound {
        collection: String,
        document_id: String,
    },

    #[error(
        "collection '{collection}' is soft-deleted for tenant {tenant_id}; \
         UNDROP before {retention_expires_at_ns} ns"
    )]
    CollectionDeactivated {
        tenant_id: TenantId,
        collection: String,
        retention_expires_at_ns: u64,
    },

    // --- Routing errors ---
    #[error("vshard {vshard_id} has no serving leader")]
    NoLeader { vshard_id: VShardId },

    #[error("not leader for vshard {vshard_id}; leader is node {leader_node} at {leader_addr}")]
    NotLeader {
        vshard_id: VShardId,
        leader_node: u64,
        leader_addr: String,
    },

    #[error("query fan-out exceeded: {shards_touched} shards > limit {limit}")]
    FanOutExceeded { shards_touched: u16, limit: u16 },

    /// A cross-collection write op (`MERGE`, `UPDATE ... FROM`, KV
    /// `TransferItem`) reads a SOURCE collection and writes a TARGET collection
    /// in a single Data-Plane handler that reads the source from the target
    /// core's LOCAL store. Each collection name hashes to its own vShard
    /// independently, so on a multi-core node source and target can land on
    /// DIFFERENT cores; the source read would then hit an empty store and
    /// return a silently wrong (empty) result. Until cross-core source-shipping
    /// lands, the op is refused rather than run. On a single-core node every
    /// vShard maps to core 0, so this never fires (the op is genuinely
    /// co-resident).
    #[error(
        "cross-collection {op} requires source '{source_collection}' and target \
         '{target_collection}' on the same core; they map to different cores \
         (co-location/source-shipping not yet supported) — refusing to run rather \
         than return a silently wrong result"
    )]
    CrossCollectionNotColocated {
        op: &'static str,
        source_collection: String,
        target_collection: String,
    },

    /// Database is temporarily frozen because a clone materializer is reading
    /// from it as the source.  The write must be retried after the materializer
    /// sweep completes.  Maps to SQLSTATE `40001` (serialization_failure) so
    /// clients that already handle write conflicts will retry automatically.
    #[error("database {database_id} is frozen for clone materialization; retry shortly")]
    SourceFrozen { database_id: DatabaseId },

    // --- Client input errors ---
    #[error("bad request: {detail}")]
    BadRequest { detail: String },

    /// The proposed quota allocation would push the sum past the configured
    /// ceiling. The `field` names the over-budget dimension.
    #[error("quota overcommit on field '{field}': {detail}")]
    QuotaOvercommit { field: String, detail: String },

    #[error("query plan error: {detail}")]
    PlanError { detail: String },

    /// The planner tried to acquire a descriptor lease at a version
    /// being drained by an in-flight DDL. The pgwire layer catches
    /// this variant and retries the whole statement up to
    /// `PLAN_RETRY_BUDGET` times with backoff. If every retry
    /// fails, the error surfaces to the client.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged { descriptor: String },

    /// The Raft entry the proposer was waiting on at `(group_id, log_index)`
    /// was overwritten by a leader-election no-op (the previous leader
    /// stepped down before the user entry committed; the new leader
    /// committed an empty entry at the same index, truncating the
    /// uncommitted data).
    ///
    /// **Critical**: this is the silent-data-loss bug killer — without
    /// surfacing this case, `tracker.complete(Ok([]))` on the no-op
    /// would tell the proposer their INSERT succeeded when in fact
    /// the row was never replicated. Callers (gateway, async raft
    /// proposer) MUST treat this as retryable and re-propose.
    #[error(
        "raft entry at group {group_id} index {log_index} was overwritten by leader change; retry needed"
    )]
    RetryableLeaderChange { group_id: u64, log_index: u64 },

    #[error("execution limit exceeded: {detail}")]
    ExecutionLimitExceeded { detail: String },

    #[error("operation limit exceeded: {limit_name} = {value} exceeds cap {max}")]
    LimitExceeded {
        limit_name: &'static str,
        value: u64,
        max: u64,
    },

    // --- Infrastructure errors ---
    #[error("WAL error: {0}")]
    Wal(#[from] nodedb_wal::WalError),

    #[error("dispatch error: {detail}")]
    Dispatch { detail: String },

    #[error("storage error ({engine}): {detail}")]
    Storage { engine: String, detail: String },

    #[error("cold storage error: {detail}")]
    ColdStorage { detail: String },

    #[error("serialization error ({format}): {detail}")]
    Serialization { format: String, detail: String },

    #[error("codec error: {detail}")]
    Codec { detail: String },

    #[error("segment corrupted: {detail}")]
    SegmentCorrupted { detail: String },

    #[error("memory budget exhausted for engine {engine}")]
    MemoryExhausted { engine: String },

    /// Memory pressure at Emergency level — the named engine is over 95% budget.
    /// The write is rejected; the caller must retry when pressure subsides.
    /// Maps to SQLSTATE 53200 (out_of_memory / insufficient_resources).
    #[error("backpressure: engine {engine} is at Emergency pressure; retry later")]
    Backpressure { engine: nodedb_mem::EngineId },

    #[error("CRDT engine error: {0}")]
    Crdt(#[from] nodedb_crdt::CrdtError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("configuration error: {detail}")]
    Config { detail: String },

    #[error("encryption error: {detail}")]
    Encryption { detail: String },

    #[error("bridge error: {detail}")]
    Bridge { detail: String },

    #[error("version compatibility: {detail}")]
    VersionCompat { detail: String },

    #[error("internal error: {detail}")]
    Internal { detail: String },

    #[error(
        "descriptor version anomaly for '{descriptor}': replicated version {carried} \
         is inconsistent with local prior {prior} (expected {prior} or prior+1)"
    )]
    DescriptorVersionAnomaly {
        descriptor: String,
        carried: u64,
        prior: u64,
    },

    /// A deterministic typed error code returned by the Data Plane across the
    /// SPSC bridge. Preserves the [`crate::bridge::envelope::ErrorCode`] instead
    /// of flattening it to an opaque string, so Control-Plane callers (e.g. the
    /// CRDT sync delta path) can classify the failure by type rather than by
    /// substring-matching a human message. Upholds the crate contract that
    /// cross-plane errors surface deterministic codes, never opaque strings.
    #[error("data plane error: {0:?}")]
    DataPlane(crate::bridge::envelope::ErrorCode),

    #[error("promql error: {0}")]
    Promql(#[from] crate::control::promql::PromqlError),

    /// DROP / PURGE refused because other catalog objects still
    /// reference the target. Operator must either drop them first or
    /// retry with `CASCADE`. `dependents` lists `(kind, name)` pairs.
    #[error(
        "cannot drop {root_kind} '{root_name}' for tenant {tenant_id}: \
         {dependent_count} dependent object(s) exist; use CASCADE to drop them atomically"
    )]
    DependentObjectsExist {
        tenant_id: u64,
        root_kind: &'static str,
        root_name: String,
        dependent_count: usize,
        dependents: Vec<(String, String)>,
    },

    /// MV-graph cycle detected (or graph exceeded `MAX_DEPTH`) during
    /// cascade enumeration. Treated as a blocker rather than silently
    /// truncating.
    #[error(
        "cascade cycle or depth limit ({depth}) exceeded while enumerating \
         dependents of '{root}' for tenant {tenant_id}"
    )]
    CascadeCycle {
        tenant_id: u64,
        root: String,
        depth: usize,
    },

    /// A cross-shard write was attempted inside an explicit transaction block.
    ///
    /// Calvin cross-shard atomicity requires auto-commit (single-statement).
    /// Options:
    ///   1. Remove BEGIN/COMMIT to use auto-commit.
    ///   2. SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch.
    #[error(
        "cross-shard write inside explicit transaction block is not supported. \
         Calvin cross-shard atomicity requires auto-commit (single-statement). \
         Options: 1) Remove BEGIN/COMMIT to use auto-commit. \
         2) SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch."
    )]
    CrossShardInExplicitTransaction,

    /// The Calvin sequencer inbox is unavailable — this node is running in
    /// embedded/local mode without a cluster deployment.
    #[error(
        "cross-shard transactions require a cluster deployment with the Calvin sequencer; \
         this node is running in embedded/local mode"
    )]
    SequencerUnavailable,

    /// New login rejected because the active-session registry is at capacity.
    #[error("session cap ({cap}) exceeded — rejecting new login")]
    SessionCapExceeded { cap: usize },

    /// Session closed because the per-database idle timeout elapsed.
    #[error("session closed: idle timeout exceeded")]
    SessionIdleTimeout,

    /// Session closed because the OIDC token expired.
    #[error("session closed: OIDC token expired")]
    SessionTokenExpired,

    /// Session terminated by an administrator via `KILL SESSION` DDL.
    #[error("session terminated by administrator")]
    SessionKilledByAdmin,

    /// Session closed because the associated user was dropped.
    #[error("session closed: user account was dropped")]
    SessionUserDropped,

    /// OIDC bearer token presented but no matching provider is configured.
    #[error(
        "OIDC token rejected: unknown provider — no configured provider matches 'iss' claim '{iss}'"
    )]
    OidcUnknownProvider { iss: String },

    /// OIDC bearer token rejected: claim mapping produced no default database.
    #[error("OIDC token rejected: claim mapping produced no default database for subject '{sub}'")]
    OidcNoDefaultDatabase { sub: String },

    /// Vector insert or index rejected: the vector dimension exceeds the
    /// tenant's `max_vector_dim` quota.
    #[error("vector dimension {dim} exceeds tenant quota max_vector_dim={limit}")]
    TenantVectorDimExceeded { dim: u32, limit: u32 },

    /// Graph traversal rejected: the requested depth exceeds the tenant's
    /// `max_graph_depth` quota.
    #[error("graph traversal depth {depth} exceeds tenant quota max_graph_depth={limit}")]
    TenantGraphDepthExceeded { depth: u32, limit: u32 },

    /// A GRANT ROLE would create a cycle in the role inheritance graph.
    ///
    /// NodeDB enforces a DAG at write time so `resolve_inheritance` never
    /// needs runtime cycle detection.
    #[error(
        "role inheritance cycle: granting '{parent}' as parent of '{child}' would create a cycle"
    )]
    RoleInheritanceCycle { child: String, parent: String },

    /// A GRANT ROLE would push the inheritance chain past
    /// `MAX_ROLE_INHERITANCE_DEPTH`. Rejected at catalog-write time.
    #[error("role inheritance depth {depth} exceeds the maximum allowed depth of {limit}")]
    RoleInheritanceDepthExceeded { depth: usize, limit: usize },

    /// The OLLP dependent-read retry loop exhausted its retry budget.
    ///
    /// The predicate's matching set kept changing across retries. Consider
    /// rephrasing as a static-key UPDATE if possible.
    #[error(
        "OLLP dependent-read exhausted {retries} retries; the predicate's matching set kept \
         changing across retries. Consider rephrasing as a static-key UPDATE if possible."
    )]
    OllpExhausted { retries: u8 },

    /// A write was attempted on a mirror database that has not yet been promoted.
    ///
    /// Mirrors are read-only replicas of a source database. All writes are rejected
    /// until the mirror is promoted via `ALTER DATABASE <name> PROMOTE`.
    #[error("database '{database}' is a read-only mirror; promote it before writing")]
    MirrorReadOnly { database: String },

    /// A strong-consistency read was attempted on a mirror database.
    ///
    /// Mirrors cannot serve strong reads because they are not the Raft leader
    /// for the source's commit log. The client should redirect to the source cluster.
    #[error(
        "database '{database}' is a mirror; redirect strong reads to source cluster '{source_cluster}'"
    )]
    StaleReadNotLeader {
        database: String,
        source_cluster: String,
        /// Human-readable detail including actual lag if available.
        detail: String,
    },
}

impl From<nodedb_physical::physical_plan::wire::WireError> for Error {
    fn from(e: nodedb_physical::physical_plan::wire::WireError) -> Self {
        Error::Internal {
            detail: e.to_string(),
        }
    }
}

impl From<nodedb_physical::ConvertError> for Error {
    fn from(e: nodedb_physical::ConvertError) -> Self {
        use nodedb_physical::ConvertError;
        match e {
            ConvertError::PlanError(detail) => Error::PlanError { detail },
            ConvertError::BadRequest(detail) => Error::BadRequest { detail },
            ConvertError::LimitExceeded {
                limit_name,
                value,
                max,
            } => Error::LimitExceeded {
                limit_name,
                value,
                max,
            },
            ConvertError::Surrogate(s) => Error::Internal {
                detail: s.to_string(),
            },
            ConvertError::Serialization(detail) => Error::Serialization {
                format: "msgpack".into(),
                detail,
            },
            ConvertError::Other(detail) => Error::Internal { detail },
        }
    }
}

/// Result alias for NodeDB operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_constraint() {
        let e = Error::RejectedConstraint {
            collection: "users".into(),
            constraint: "users_email_unique".into(),
            detail: "duplicate email".into(),
        };
        assert!(e.to_string().contains("constraint violation"));
        assert!(e.to_string().contains("users"));
    }

    #[test]
    fn error_display_deadline() {
        let e = Error::DeadlineExceeded {
            request_id: RequestId::new(42),
        };
        assert!(e.to_string().contains("req:42"));
        assert!(e.to_string().contains("deadline"));
    }

    #[test]
    fn error_display_fan_out() {
        let e = Error::FanOutExceeded {
            shards_touched: 32,
            limit: 16,
        };
        assert!(e.to_string().contains("32"));
        assert!(e.to_string().contains("16"));
    }

    #[test]
    fn crdt_error_converts() {
        let crdt_err = nodedb_crdt::CrdtError::ConstraintViolation {
            constraint: "test".into(),
            collection: "col".into(),
            detail: "detail".into(),
        };
        let e: Error = crdt_err.into();
        assert!(matches!(e, Error::Crdt(_)));
    }

    #[test]
    fn internal_error_to_nodedb_error() {
        let e = Error::Wal(nodedb_wal::WalError::Sealed);
        let public: nodedb_types::error::NodeDbError = e.into();
        assert!(public.is_storage());
        assert!(public.to_string().contains("NDB-4100"));
    }

    #[test]
    fn constraint_to_nodedb_error() {
        let e = Error::RejectedConstraint {
            collection: "users".into(),
            constraint: "unique_email".into(),
            detail: "dup".into(),
        };
        let public: nodedb_types::error::NodeDbError = e.into();
        assert!(public.is_constraint_violation());
    }

    #[test]
    fn io_error_to_nodedb_error() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let public: nodedb_types::error::NodeDbError = e.into();
        assert!(public.is_storage());
    }
}
