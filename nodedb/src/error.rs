use crate::types::{RequestId, TenantId, VShardId};
use nodedb_types::error::NodeDbError;

/// Internal error classes for NodeDB Origin.
///
/// Every error is actionable — clients can programmatically handle each variant.
/// Cross-plane errors surface deterministic codes, never opaque strings.
///
/// At the public API boundary, `Error` converts to [`NodeDbError`] via `From`,
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

    #[error("authorization denied for tenant {tenant_id} on {resource}")]
    RejectedAuthz {
        tenant_id: TenantId,
        resource: String,
    },

    #[error("request {request_id} exceeded deadline")]
    DeadlineExceeded { request_id: RequestId },

    #[error("write conflict on {collection}/{document_id}, retry with idempotency key")]
    ConflictRetry {
        collection: String,
        document_id: String,
    },

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

    // --- Client input errors ---
    #[error("bad request: {detail}")]
    BadRequest { detail: String },

    #[error("query plan error: {detail}")]
    PlanError { detail: String },

    /// The planner tried to acquire a descriptor lease at a version
    /// being drained by an in-flight DDL. The pgwire layer catches
    /// this variant and retries the whole statement up to
    /// `PLAN_RETRY_BUDGET` times with backoff. If every retry
    /// fails, the error surfaces to the client.
    #[error("retryable schema change on {descriptor}")]
    RetryableSchemaChanged { descriptor: String },

    #[error("execution limit exceeded: {detail}")]
    ExecutionLimitExceeded { detail: String },

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
}

/// Result alias for NodeDB operations.
pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// From impls for domain-specific errors
// ---------------------------------------------------------------------------

impl From<crate::control::pubsub::TopicError> for Error {
    fn from(e: crate::control::pubsub::TopicError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::ilp::IlpError> for Error {
    fn from(e: crate::engine::timeseries::ilp::IlpError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::columnar_segment::SegmentError> for Error {
    fn from(e: crate::engine::timeseries::columnar_segment::SegmentError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::query::QueryError> for Error {
    fn from(e: crate::engine::timeseries::query::QueryError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::crl::CrlError> for Error {
    fn from(e: crate::control::security::crl::CrlError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::jwt::JwtError> for Error {
    fn from(e: crate::control::security::jwt::JwtError) -> Self {
        Self::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: e.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// From<Error> for NodeDbError — the public API boundary conversion
// ---------------------------------------------------------------------------

impl From<Error> for NodeDbError {
    fn from(e: Error) -> Self {
        match e {
            // Write path
            Error::RejectedConstraint {
                collection, detail, ..
            } => NodeDbError::constraint_violation(collection, detail),
            Error::RejectedAuthz { resource, .. } => NodeDbError::authorization_denied(resource),
            Error::DeadlineExceeded { .. } => NodeDbError::deadline_exceeded(),
            Error::ConflictRetry {
                collection,
                document_id,
            } => NodeDbError::write_conflict(collection, document_id),
            Error::RejectedPrevalidation { constraint, reason } => {
                NodeDbError::prevalidation_rejected(constraint, reason)
            }
            Error::AppendOnlyViolation {
                collection, detail, ..
            } => NodeDbError::append_only_violation(collection, detail),
            Error::BalanceViolation {
                collection, detail, ..
            } => NodeDbError::balance_violation(collection, detail),
            Error::PeriodLocked {
                collection, detail, ..
            } => NodeDbError::period_locked(collection, detail),
            Error::RetentionViolation {
                collection, detail, ..
            } => NodeDbError::retention_violation(collection, detail),
            Error::LegalHoldActive {
                collection, detail, ..
            } => NodeDbError::legal_hold_active(collection, detail),
            Error::StateTransitionViolation {
                collection, detail, ..
            } => NodeDbError::state_transition_violation(collection, detail),
            Error::TransitionCheckViolation {
                collection, detail, ..
            } => NodeDbError::transition_check_violation(collection, detail),
            Error::TypeGuardViolation {
                collection, detail, ..
            } => NodeDbError::type_guard_violation(collection, detail),
            Error::TypeMismatch {
                collection, detail, ..
            } => NodeDbError::type_mismatch(collection, detail),
            Error::OverflowError { collection, key } => {
                NodeDbError::overflow(collection, format!("key {key}"))
            }
            Error::InsufficientBalance {
                collection,
                key,
                detail,
            } => NodeDbError::insufficient_balance(collection, format!("key {key}: {detail}")),
            Error::RateExceeded { gate, detail, .. } => NodeDbError::rate_exceeded(gate, detail),

            // Read path
            Error::CollectionNotFound { collection, .. } => {
                NodeDbError::collection_not_found(collection)
            }
            Error::DocumentNotFound {
                collection,
                document_id,
            } => NodeDbError::document_not_found(collection, document_id),

            // Routing / Cluster
            Error::NoLeader { vshard_id } => {
                NodeDbError::no_leader(format!("vshard {vshard_id} has no serving leader"))
            }
            Error::NotLeader { leader_addr, .. } => NodeDbError::not_leader(leader_addr),
            Error::FanOutExceeded {
                shards_touched,
                limit,
            } => NodeDbError::fan_out_exceeded(shards_touched, limit),

            // Client input
            Error::BadRequest { detail } => NodeDbError::bad_request(detail),
            Error::PlanError { detail } => NodeDbError::plan_error(detail),
            Error::RetryableSchemaChanged { descriptor } => {
                NodeDbError::plan_error(format!("retryable schema change on {descriptor}"))
            }
            Error::ExecutionLimitExceeded { detail } => NodeDbError::bad_request(detail),

            // Infrastructure — flatten to opaque public variants
            Error::Wal(wal_err) => NodeDbError::wal(wal_err),
            Error::Dispatch { detail } => NodeDbError::dispatch(detail),
            Error::Storage { detail, .. } => NodeDbError::storage(detail),
            Error::ColdStorage { detail } => NodeDbError::cold_storage(detail),
            Error::Serialization { format, detail } => NodeDbError::serialization(format, detail),
            Error::Codec { detail } => NodeDbError::codec(detail),
            Error::SegmentCorrupted { detail } => NodeDbError::segment_corrupted(detail),
            Error::MemoryExhausted { engine } => NodeDbError::memory_exhausted(engine),
            Error::Crdt(crdt_err) => NodeDbError::internal(crdt_err),
            Error::Io(io_err) => NodeDbError::storage(io_err),
            Error::Config { detail } => NodeDbError::config(detail),
            Error::Encryption { detail } => NodeDbError::encryption(detail),
            Error::Bridge { detail } => NodeDbError::bridge(detail),
            Error::VersionCompat { detail } => NodeDbError::cluster(detail),
            Error::Internal { detail } => NodeDbError::internal(detail),
        }
    }
}

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
        let public: NodeDbError = e.into();
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
        let public: NodeDbError = e.into();
        assert!(public.is_constraint_violation());
    }

    #[test]
    fn io_error_to_nodedb_error() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "gone"));
        let public: NodeDbError = e.into();
        assert!(public.is_storage());
    }
}
