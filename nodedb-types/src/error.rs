//! Standardized error types for the NodeDB public API.
//!
//! [`NodeDbError`] is a **struct** (not an enum) that separates:
//! - `code` — stable numeric code for programmatic handling (`NDB-1000`)
//! - `message` — human-readable explanation
//! - `details` — machine-matchable [`ErrorDetails`] enum with structured data
//! - `cause` — optional chained error for debugging
//!
//! # Wire format
//!
//! Serializes to:
//! ```json
//! {
//!   "code": "NDB-1000",
//!   "message": "constraint violation on users: duplicate email",
//!   "details": { "kind": "constraint_violation", "collection": "users" }
//! }
//! ```
//!
//! # Error code ranges
//!
//! | Range       | Category      |
//! |-------------|---------------|
//! | 1000–1099   | Write path    |
//! | 1100–1199   | Read path     |
//! | 1200–1299   | Query         |
//! | 2000–2099   | Auth/Security |
//! | 3000–3099   | Sync          |
//! | 4000–4099   | Storage       |
//! | 4100–4199   | WAL           |
//! | 4200–4299   | Serialization |
//! | 5000–5099   | Config        |
//! | 6000–6099   | Cluster       |
//! | 7000–7099   | Memory        |
//! | 8000–8099   | Encryption    |
//! | 9000–9099   | Internal      |

use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Error codes
// ---------------------------------------------------------------------------

/// Stable numeric error codes for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    // Write path (1000–1099)
    pub const CONSTRAINT_VIOLATION: Self = Self(1000);
    pub const WRITE_CONFLICT: Self = Self(1001);
    pub const DEADLINE_EXCEEDED: Self = Self(1002);
    pub const PREVALIDATION_REJECTED: Self = Self(1003);
    pub const APPEND_ONLY_VIOLATION: Self = Self(1010);
    pub const BALANCE_VIOLATION: Self = Self(1011);
    pub const PERIOD_LOCKED: Self = Self(1012);
    pub const STATE_TRANSITION_VIOLATION: Self = Self(1013);
    pub const TRANSITION_CHECK_VIOLATION: Self = Self(1014);
    pub const RETENTION_VIOLATION: Self = Self(1015);
    pub const LEGAL_HOLD_ACTIVE: Self = Self(1016);
    pub const TYPE_MISMATCH: Self = Self(1020);
    pub const OVERFLOW: Self = Self(1021);
    pub const INSUFFICIENT_BALANCE: Self = Self(1022);
    pub const RATE_EXCEEDED: Self = Self(1023);

    // Read path (1100–1199)
    pub const COLLECTION_NOT_FOUND: Self = Self(1100);
    pub const DOCUMENT_NOT_FOUND: Self = Self(1101);

    // Query (1200–1299)
    pub const PLAN_ERROR: Self = Self(1200);
    pub const FAN_OUT_EXCEEDED: Self = Self(1201);
    pub const SQL_NOT_ENABLED: Self = Self(1202);

    // Auth / Security (2000–2099)
    pub const AUTHORIZATION_DENIED: Self = Self(2000);
    pub const AUTH_EXPIRED: Self = Self(2001);

    // Sync (3000–3099)
    pub const SYNC_CONNECTION_FAILED: Self = Self(3000);
    pub const SYNC_DELTA_REJECTED: Self = Self(3001);
    pub const SHAPE_SUBSCRIPTION_FAILED: Self = Self(3002);

    // Storage (4000–4099)
    pub const STORAGE: Self = Self(4000);
    pub const SEGMENT_CORRUPTED: Self = Self(4001);
    pub const COLD_STORAGE: Self = Self(4002);

    // WAL (4100–4199)
    pub const WAL: Self = Self(4100);

    // Serialization (4200–4299)
    pub const SERIALIZATION: Self = Self(4200);
    pub const CODEC: Self = Self(4201);

    // Config (5000–5099)
    pub const CONFIG: Self = Self(5000);
    pub const BAD_REQUEST: Self = Self(5001);

    // Cluster (6000–6099)
    pub const NO_LEADER: Self = Self(6000);
    pub const NOT_LEADER: Self = Self(6001);
    pub const MIGRATION_IN_PROGRESS: Self = Self(6002);
    pub const NODE_UNREACHABLE: Self = Self(6003);
    pub const CLUSTER: Self = Self(6010);

    // Memory (7000–7099)
    pub const MEMORY_EXHAUSTED: Self = Self(7000);

    // Encryption (8000–8099)
    pub const ENCRYPTION: Self = Self(8000);

    // Internal (9000–9099)
    pub const INTERNAL: Self = Self(9000);
    pub const BRIDGE: Self = Self(9001);
    pub const DISPATCH: Self = Self(9002);
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NDB-{:04}", self.0)
    }
}

// ---------------------------------------------------------------------------
// ErrorDetails — machine-matchable structured data
// ---------------------------------------------------------------------------

/// Structured error details for programmatic matching.
///
/// Clients match on the variant to determine the error category, then
/// extract structured fields. The `message` on [`NodeDbError`] carries
/// the human-readable explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ErrorDetails {
    // Write path
    ConstraintViolation {
        collection: String,
    },
    WriteConflict {
        collection: String,
        document_id: String,
    },
    DeadlineExceeded,
    PrevalidationRejected {
        constraint: String,
    },
    AppendOnlyViolation {
        collection: String,
    },
    BalanceViolation {
        collection: String,
    },
    PeriodLocked {
        collection: String,
    },
    StateTransitionViolation {
        collection: String,
    },
    TransitionCheckViolation {
        collection: String,
    },
    RetentionViolation {
        collection: String,
    },
    LegalHoldActive {
        collection: String,
    },
    TypeMismatch {
        collection: String,
    },
    Overflow {
        collection: String,
    },
    InsufficientBalance {
        collection: String,
    },
    RateExceeded {
        gate: String,
    },

    // Read path
    CollectionNotFound {
        collection: String,
    },
    DocumentNotFound {
        collection: String,
        document_id: String,
    },

    // Query
    PlanError,
    FanOutExceeded {
        shards_touched: u16,
        limit: u16,
    },
    SqlNotEnabled,

    // Auth
    AuthorizationDenied {
        resource: String,
    },
    AuthExpired,

    // Sync
    SyncConnectionFailed,
    SyncDeltaRejected {
        compensation: Option<crate::sync::compensation::CompensationHint>,
    },
    ShapeSubscriptionFailed {
        shape_id: String,
    },

    // Storage (opaque infrastructure)
    Storage,
    SegmentCorrupted,
    ColdStorage,
    Wal,

    // Serialization
    Serialization {
        format: String,
    },
    Codec,

    // Config
    Config,
    BadRequest,

    // Cluster
    NoLeader,
    NotLeader {
        leader_addr: String,
    },
    MigrationInProgress,
    NodeUnreachable,
    Cluster,

    // Memory
    MemoryExhausted {
        engine: String,
    },

    // Encryption
    Encryption,

    // Bridge / Dispatch / Internal
    Bridge,
    Dispatch,
    Internal,
}

// ---------------------------------------------------------------------------
// NodeDbError — the public error struct
// ---------------------------------------------------------------------------

/// Public error type returned by all `NodeDb` trait methods.
///
/// Separates machine-readable data ([`ErrorCode`] + [`ErrorDetails`]) from
/// the human-readable `message`. Optional `cause` preserves the error chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDbError {
    code: ErrorCode,
    message: String,
    details: ErrorDetails,
    #[serde(skip_serializing_if = "Option::is_none")]
    cause: Option<Box<NodeDbError>>,
}

impl fmt::Display for NodeDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for NodeDbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_deref().map(|e| e as &dyn std::error::Error)
    }
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

impl NodeDbError {
    /// The stable numeric error code.
    pub fn code(&self) -> ErrorCode {
        self.code
    }

    /// Human-readable error message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Machine-matchable error details.
    pub fn details(&self) -> &ErrorDetails {
        &self.details
    }

    /// The chained cause, if any.
    pub fn cause(&self) -> Option<&NodeDbError> {
        self.cause.as_deref()
    }

    /// Attach a cause to this error.
    pub fn with_cause(mut self, cause: NodeDbError) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Whether this error is retriable by the client.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::WriteConflict { .. }
                | ErrorDetails::DeadlineExceeded
                | ErrorDetails::NoLeader
                | ErrorDetails::NotLeader { .. }
                | ErrorDetails::MigrationInProgress
                | ErrorDetails::NodeUnreachable
                | ErrorDetails::SyncConnectionFailed
                | ErrorDetails::Bridge
                | ErrorDetails::MemoryExhausted { .. }
        )
    }

    /// Whether this error indicates the client sent invalid input.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::BadRequest
                | ErrorDetails::ConstraintViolation { .. }
                | ErrorDetails::AppendOnlyViolation { .. }
                | ErrorDetails::BalanceViolation { .. }
                | ErrorDetails::PeriodLocked { .. }
                | ErrorDetails::StateTransitionViolation { .. }
                | ErrorDetails::TransitionCheckViolation { .. }
                | ErrorDetails::RetentionViolation { .. }
                | ErrorDetails::LegalHoldActive { .. }
                | ErrorDetails::CollectionNotFound { .. }
                | ErrorDetails::DocumentNotFound { .. }
                | ErrorDetails::AuthorizationDenied { .. }
                | ErrorDetails::AuthExpired
                | ErrorDetails::Config
                | ErrorDetails::SqlNotEnabled
        )
    }
}

// ---------------------------------------------------------------------------
// Category checks
// ---------------------------------------------------------------------------

impl NodeDbError {
    pub fn is_constraint_violation(&self) -> bool {
        matches!(self.details, ErrorDetails::ConstraintViolation { .. })
    }
    pub fn is_not_found(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::CollectionNotFound { .. } | ErrorDetails::DocumentNotFound { .. }
        )
    }
    pub fn is_auth_denied(&self) -> bool {
        matches!(self.details, ErrorDetails::AuthorizationDenied { .. })
    }
    pub fn is_storage(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::Storage
                | ErrorDetails::SegmentCorrupted
                | ErrorDetails::ColdStorage
                | ErrorDetails::Wal
        )
    }
    pub fn is_internal(&self) -> bool {
        matches!(self.details, ErrorDetails::Internal)
    }
    pub fn is_type_mismatch(&self) -> bool {
        matches!(self.details, ErrorDetails::TypeMismatch { .. })
    }
    pub fn is_overflow(&self) -> bool {
        matches!(self.details, ErrorDetails::Overflow { .. })
    }
    pub fn is_insufficient_balance(&self) -> bool {
        matches!(self.details, ErrorDetails::InsufficientBalance { .. })
    }
    pub fn is_rate_exceeded(&self) -> bool {
        matches!(self.details, ErrorDetails::RateExceeded { .. })
    }
    pub fn is_cluster(&self) -> bool {
        matches!(
            self.details,
            ErrorDetails::NoLeader
                | ErrorDetails::NotLeader { .. }
                | ErrorDetails::MigrationInProgress
                | ErrorDetails::NodeUnreachable
                | ErrorDetails::Cluster
        )
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

impl NodeDbError {
    // ── Write path ──

    pub fn constraint_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::CONSTRAINT_VIOLATION,
            message: format!("constraint violation on {collection}: {detail}"),
            details: ErrorDetails::ConstraintViolation { collection },
            cause: None,
        }
    }

    pub fn write_conflict(collection: impl Into<String>, document_id: impl Into<String>) -> Self {
        let collection = collection.into();
        let document_id = document_id.into();
        Self {
            code: ErrorCode::WRITE_CONFLICT,
            message: format!(
                "write conflict on {collection}/{document_id}, retry with idempotency key"
            ),
            details: ErrorDetails::WriteConflict {
                collection,
                document_id,
            },
            cause: None,
        }
    }

    pub fn deadline_exceeded() -> Self {
        Self {
            code: ErrorCode::DEADLINE_EXCEEDED,
            message: "request exceeded deadline".into(),
            details: ErrorDetails::DeadlineExceeded,
            cause: None,
        }
    }

    pub fn prevalidation_rejected(
        constraint: impl Into<String>,
        reason: impl fmt::Display,
    ) -> Self {
        let constraint = constraint.into();
        Self {
            code: ErrorCode::PREVALIDATION_REJECTED,
            message: format!("pre-validation rejected: {constraint} — {reason}"),
            details: ErrorDetails::PrevalidationRejected { constraint },
            cause: None,
        }
    }

    // ── Accounting enforcement ──

    pub fn append_only_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::APPEND_ONLY_VIOLATION,
            message: format!("append-only violation on {collection}: {detail}"),
            details: ErrorDetails::AppendOnlyViolation { collection },
            cause: None,
        }
    }

    pub fn balance_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::BALANCE_VIOLATION,
            message: format!("balance violation on {collection}: {detail}"),
            details: ErrorDetails::BalanceViolation { collection },
            cause: None,
        }
    }

    pub fn period_locked(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::PERIOD_LOCKED,
            message: format!("period locked on {collection}: {detail}"),
            details: ErrorDetails::PeriodLocked { collection },
            cause: None,
        }
    }

    pub fn state_transition_violation(
        collection: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::STATE_TRANSITION_VIOLATION,
            message: format!("state transition violation on {collection}: {detail}"),
            details: ErrorDetails::StateTransitionViolation { collection },
            cause: None,
        }
    }

    pub fn transition_check_violation(
        collection: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::TRANSITION_CHECK_VIOLATION,
            message: format!("transition check violation on {collection}: {detail}"),
            details: ErrorDetails::TransitionCheckViolation { collection },
            cause: None,
        }
    }

    pub fn retention_violation(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::RETENTION_VIOLATION,
            message: format!("retention violation on {collection}: {detail}"),
            details: ErrorDetails::RetentionViolation { collection },
            cause: None,
        }
    }

    pub fn legal_hold_active(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::LEGAL_HOLD_ACTIVE,
            message: format!("legal hold active on {collection}: {detail}"),
            details: ErrorDetails::LegalHoldActive { collection },
            cause: None,
        }
    }

    pub fn type_mismatch(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::TYPE_MISMATCH,
            message: format!("type mismatch on {collection}: {detail}"),
            details: ErrorDetails::TypeMismatch { collection },
            cause: None,
        }
    }

    pub fn overflow(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::OVERFLOW,
            message: format!("arithmetic overflow on {collection}: {detail}"),
            details: ErrorDetails::Overflow { collection },
            cause: None,
        }
    }

    pub fn insufficient_balance(collection: impl Into<String>, detail: impl fmt::Display) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::INSUFFICIENT_BALANCE,
            message: format!("insufficient balance on {collection}: {detail}"),
            details: ErrorDetails::InsufficientBalance { collection },
            cause: None,
        }
    }

    pub fn rate_exceeded(gate: impl Into<String>, detail: impl fmt::Display) -> Self {
        let gate = gate.into();
        Self {
            code: ErrorCode::RATE_EXCEEDED,
            message: format!("rate limit exceeded for {gate}: {detail}"),
            details: ErrorDetails::RateExceeded { gate },
            cause: None,
        }
    }

    // ── Read path ──

    pub fn collection_not_found(collection: impl Into<String>) -> Self {
        let collection = collection.into();
        Self {
            code: ErrorCode::COLLECTION_NOT_FOUND,
            message: format!("collection '{collection}' not found"),
            details: ErrorDetails::CollectionNotFound { collection },
            cause: None,
        }
    }

    pub fn document_not_found(
        collection: impl Into<String>,
        document_id: impl Into<String>,
    ) -> Self {
        let collection = collection.into();
        let document_id = document_id.into();
        Self {
            code: ErrorCode::DOCUMENT_NOT_FOUND,
            message: format!("document '{document_id}' not found in '{collection}'"),
            details: ErrorDetails::DocumentNotFound {
                collection,
                document_id,
            },
            cause: None,
        }
    }

    // ── Query ──

    pub fn plan_error(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::PLAN_ERROR,
            message: format!("query plan error: {detail}"),
            details: ErrorDetails::PlanError,
            cause: None,
        }
    }

    pub fn fan_out_exceeded(shards_touched: u16, limit: u16) -> Self {
        Self {
            code: ErrorCode::FAN_OUT_EXCEEDED,
            message: format!("query fan-out exceeded: {shards_touched} shards > limit {limit}"),
            details: ErrorDetails::FanOutExceeded {
                shards_touched,
                limit,
            },
            cause: None,
        }
    }

    pub fn sql_not_enabled() -> Self {
        Self {
            code: ErrorCode::SQL_NOT_ENABLED,
            message: "SQL not enabled (compile with 'sql' feature)".into(),
            details: ErrorDetails::SqlNotEnabled,
            cause: None,
        }
    }

    // ── Auth ──

    pub fn authorization_denied(resource: impl Into<String>) -> Self {
        let resource = resource.into();
        Self {
            code: ErrorCode::AUTHORIZATION_DENIED,
            message: format!("authorization denied on {resource}"),
            details: ErrorDetails::AuthorizationDenied { resource },
            cause: None,
        }
    }

    pub fn auth_expired(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::AUTH_EXPIRED,
            message: format!("auth expired: {detail}"),
            details: ErrorDetails::AuthExpired,
            cause: None,
        }
    }

    // ── Sync ──

    pub fn sync_connection_failed(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SYNC_CONNECTION_FAILED,
            message: format!("sync connection failed: {detail}"),
            details: ErrorDetails::SyncConnectionFailed,
            cause: None,
        }
    }

    pub fn sync_delta_rejected(
        reason: impl fmt::Display,
        compensation: Option<crate::sync::compensation::CompensationHint>,
    ) -> Self {
        Self {
            code: ErrorCode::SYNC_DELTA_REJECTED,
            message: format!("sync delta rejected: {reason}"),
            details: ErrorDetails::SyncDeltaRejected { compensation },
            cause: None,
        }
    }

    pub fn shape_subscription_failed(
        shape_id: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let shape_id = shape_id.into();
        Self {
            code: ErrorCode::SHAPE_SUBSCRIPTION_FAILED,
            message: format!("shape subscription failed for '{shape_id}': {detail}"),
            details: ErrorDetails::ShapeSubscriptionFailed { shape_id },
            cause: None,
        }
    }

    // ── Storage ──

    pub fn storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::STORAGE,
            message: format!("storage error: {detail}"),
            details: ErrorDetails::Storage,
            cause: None,
        }
    }

    pub fn segment_corrupted(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SEGMENT_CORRUPTED,
            message: format!("segment corrupted: {detail}"),
            details: ErrorDetails::SegmentCorrupted,
            cause: None,
        }
    }

    pub fn cold_storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::COLD_STORAGE,
            message: format!("cold storage error: {detail}"),
            details: ErrorDetails::ColdStorage,
            cause: None,
        }
    }

    pub fn wal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::WAL,
            message: format!("WAL error: {detail}"),
            details: ErrorDetails::Wal,
            cause: None,
        }
    }

    // ── Serialization ──

    pub fn serialization(format: impl Into<String>, detail: impl fmt::Display) -> Self {
        let format = format.into();
        Self {
            code: ErrorCode::SERIALIZATION,
            message: format!("serialization error ({format}): {detail}"),
            details: ErrorDetails::Serialization { format },
            cause: None,
        }
    }

    pub fn codec(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CODEC,
            message: format!("codec error: {detail}"),
            details: ErrorDetails::Codec,
            cause: None,
        }
    }

    // ── Config ──

    pub fn config(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CONFIG,
            message: format!("configuration error: {detail}"),
            details: ErrorDetails::Config,
            cause: None,
        }
    }

    pub fn bad_request(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BAD_REQUEST,
            message: format!("bad request: {detail}"),
            details: ErrorDetails::BadRequest,
            cause: None,
        }
    }

    // ── Cluster ──

    pub fn no_leader(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NO_LEADER,
            message: format!("no serving leader: {detail}"),
            details: ErrorDetails::NoLeader,
            cause: None,
        }
    }

    pub fn not_leader(leader_addr: impl Into<String>) -> Self {
        let leader_addr = leader_addr.into();
        Self {
            code: ErrorCode::NOT_LEADER,
            message: format!("not leader; redirect to leader at {leader_addr}"),
            details: ErrorDetails::NotLeader { leader_addr },
            cause: None,
        }
    }

    pub fn migration_in_progress(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::MIGRATION_IN_PROGRESS,
            message: format!("migration in progress: {detail}"),
            details: ErrorDetails::MigrationInProgress,
            cause: None,
        }
    }

    pub fn node_unreachable(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NODE_UNREACHABLE,
            message: format!("node unreachable: {detail}"),
            details: ErrorDetails::NodeUnreachable,
            cause: None,
        }
    }

    pub fn cluster(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CLUSTER,
            message: format!("cluster error: {detail}"),
            details: ErrorDetails::Cluster,
            cause: None,
        }
    }

    // ── Memory ──

    pub fn memory_exhausted(engine: impl Into<String>) -> Self {
        let engine = engine.into();
        Self {
            code: ErrorCode::MEMORY_EXHAUSTED,
            message: format!("memory budget exhausted for engine {engine}"),
            details: ErrorDetails::MemoryExhausted { engine },
            cause: None,
        }
    }

    // ── Encryption ──

    pub fn encryption(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::ENCRYPTION,
            message: format!("encryption error: {detail}"),
            details: ErrorDetails::Encryption,
            cause: None,
        }
    }

    // ── Bridge / Dispatch / Internal ──

    pub fn bridge(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BRIDGE,
            message: format!("bridge error: {detail}"),
            details: ErrorDetails::Bridge,
            cause: None,
        }
    }

    pub fn dispatch(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::DISPATCH,
            message: format!("dispatch error: {detail}"),
            details: ErrorDetails::Dispatch,
            cause: None,
        }
    }

    pub fn internal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::INTERNAL,
            message: format!("internal error: {detail}"),
            details: ErrorDetails::Internal,
            cause: None,
        }
    }
}

/// Result alias for NodeDb operations.
pub type NodeDbResult<T> = std::result::Result<T, NodeDbError>;

impl From<std::io::Error> for NodeDbError {
    fn from(e: std::io::Error) -> Self {
        Self::storage(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::CONSTRAINT_VIOLATION.to_string(), "NDB-1000");
        assert_eq!(ErrorCode::INTERNAL.to_string(), "NDB-9000");
        assert_eq!(ErrorCode::WAL.to_string(), "NDB-4100");
    }

    #[test]
    fn error_display_includes_code() {
        let e = NodeDbError::constraint_violation("users", "duplicate email");
        let msg = e.to_string();
        assert!(msg.contains("NDB-1000"));
        assert!(msg.contains("constraint violation"));
        assert!(msg.contains("users"));
    }

    #[test]
    fn error_code_accessor() {
        let e = NodeDbError::write_conflict("orders", "abc");
        assert_eq!(e.code(), ErrorCode::WRITE_CONFLICT);
        assert_eq!(e.code().0, 1001);
    }

    #[test]
    fn details_matching() {
        let e = NodeDbError::collection_not_found("users");
        assert!(matches!(
            e.details(),
            ErrorDetails::CollectionNotFound { collection } if collection == "users"
        ));
        assert!(e.is_not_found());
    }

    #[test]
    fn error_cause_chaining() {
        let inner = NodeDbError::storage("disk full");
        let outer = NodeDbError::internal("write failed").with_cause(inner);
        assert!(outer.cause().is_some());
        assert!(outer.cause().unwrap().is_storage());
        assert!(outer.cause().unwrap().message().contains("disk full"));
    }

    #[test]
    fn sync_delta_rejected() {
        let e = NodeDbError::sync_delta_rejected(
            "unique violation",
            Some(
                crate::sync::compensation::CompensationHint::UniqueViolation {
                    field: "email".into(),
                    conflicting_value: "a@b.com".into(),
                },
            ),
        );
        assert!(e.to_string().contains("NDB-3001"));
        assert!(e.to_string().contains("sync delta rejected"));
    }

    #[test]
    fn sql_not_enabled() {
        let e = NodeDbError::sql_not_enabled();
        assert!(e.to_string().contains("SQL not enabled"));
        assert_eq!(e.code(), ErrorCode::SQL_NOT_ENABLED);
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: NodeDbError = io_err.into();
        assert!(e.is_storage());
        assert_eq!(e.code(), ErrorCode::STORAGE);
    }

    #[test]
    fn retriable_errors() {
        assert!(NodeDbError::write_conflict("x", "y").is_retriable());
        assert!(NodeDbError::deadline_exceeded().is_retriable());
        assert!(!NodeDbError::bad_request("bad").is_retriable());
    }

    #[test]
    fn client_errors() {
        assert!(NodeDbError::bad_request("bad").is_client_error());
        assert!(!NodeDbError::internal("oops").is_client_error());
    }

    #[test]
    fn json_serialization() {
        let e = NodeDbError::collection_not_found("users");
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(json["code"], 1100);
        assert!(json["message"].as_str().unwrap().contains("users"));
        assert_eq!(json["details"]["kind"], "collection_not_found");
        assert_eq!(json["details"]["collection"], "users");
    }
}
