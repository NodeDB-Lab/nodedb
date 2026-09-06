// SPDX-License-Identifier: Apache-2.0

//! Single source of truth for the `ErrorCode` <-> `ErrorDetails` pairing.
//!
//! [`error_code_table!`] lists each pairing once and generates
//! [`ErrorDetails::code`] plus [`primary_details_for_code`], which
//! [`details_for_code`] wraps for `wire_details` and `remote_typed`.

use super::code::ErrorCode;
use super::details::ErrorDetails;

macro_rules! error_code_table {
    (
        msg = $msg:ident;
        $( $code:ident => $variant:ident $( { $($field:ident : $value:expr),* $(,)? } )? ),* $(,)?
    ) => {
        impl ErrorDetails {
            /// The numeric [`ErrorCode`] this variant reconstructs from on
            /// the wire. Exhaustive: a new variant not listed in
            /// [`error_code_table!`] fails to compile.
            pub(crate) fn code(&self) -> ErrorCode {
                match self {
                    $(
                        ErrorDetails::$variant $( { $($field: _),* } )? => ErrorCode::$code,
                    )*
                }
            }
        }

        /// Reconstruct the primary `ErrorDetails` variant for a wire `code`,
        /// using `$msg` for placeholder detail fields. `None` if `code`
        /// has no dedicated variant.
        pub(crate) fn primary_details_for_code(code: ErrorCode, $msg: &str) -> Option<ErrorDetails> {
            let details = match code {
                $(
                    ErrorCode::$code => Some(ErrorDetails::$variant $( { $($field: $value),* } )?),
                )*
                _ => None,
            };
            debug_assert!(
                details.as_ref().is_none_or(|d| d.code() == code),
                "code_table entry for {code} does not round-trip through ErrorDetails::code()",
            );
            details
        }
    };
}

error_code_table! {
    msg = message;

    // Write path.
    CONSTRAINT_VIOLATION => ConstraintViolation { collection: String::new() },
    WRITE_CONFLICT => WriteConflict { collection: String::new(), document_id: String::new() },
    DEADLINE_EXCEEDED => DeadlineExceeded,
    PREVALIDATION_REJECTED => PrevalidationRejected { constraint: String::new() },
    APPEND_ONLY_VIOLATION => AppendOnlyViolation { collection: String::new() },
    BALANCE_VIOLATION => BalanceViolation { collection: String::new() },
    PERIOD_LOCKED => PeriodLocked { collection: String::new() },
    STATE_TRANSITION_VIOLATION => StateTransitionViolation { collection: String::new() },
    TRANSITION_CHECK_VIOLATION => TransitionCheckViolation { collection: String::new() },
    TYPE_GUARD_VIOLATION => TypeGuardViolation { collection: String::new() },
    RETENTION_VIOLATION => RetentionViolation { collection: String::new() },
    LEGAL_HOLD_ACTIVE => LegalHoldActive { collection: String::new() },
    TYPE_MISMATCH => TypeMismatch { collection: String::new() },
    OVERFLOW => Overflow { collection: String::new() },
    INSUFFICIENT_BALANCE => InsufficientBalance { collection: String::new() },
    RATE_EXCEEDED => RateExceeded { gate: String::new() },

    // Read path.
    COLLECTION_NOT_FOUND => CollectionNotFound { collection: String::new() },
    DATABASE_NOT_FOUND => DatabaseNotFound { database: String::new() },
    UNDEFINED_OBJECT => UndefinedObject { object: String::new() },
    ALREADY_EXISTS => AlreadyExists { object: String::new() },
    OBJECT_NOT_READY => ObjectNotReady { object: String::new() },
    NOT_FOUND => NotFound { detail: message.to_owned() },
    DOCUMENT_NOT_FOUND => DocumentNotFound { collection: String::new(), document_id: String::new() },
    COLLECTION_DRAINING => CollectionDraining { collection: String::new() },
    COLLECTION_DEACTIVATED => CollectionDeactivated {
        collection: String::new(),
        retention_expires_at_ns: 0,
        undrop_hint: String::new(),
    },

    // Query.
    PLAN_ERROR => PlanError { phase: "remote".into(), detail: message.to_owned() },
    FAN_OUT_EXCEEDED => FanOutExceeded { shards_touched: 0, limit: 0 },
    SQL_NOT_ENABLED => SqlNotEnabled,
    UNDEFINED_FUNCTION => UndefinedFunction { name: String::new() },
    UNDEFINED_COLUMN => UndefinedColumn { table: "remote".into(), column: message.to_owned() },
    DIVISION_BY_ZERO => DivisionByZero,
    INVALID_LIMIT_VALUE => InvalidLimitValue { clause: "remote".into(), value: message.to_owned() },

    // Auth / tenant quota.
    AUTHORIZATION_DENIED => AuthorizationDenied { resource: String::new() },
    AUTH_EXPIRED => AuthExpired,
    TENANT_VECTOR_DIM_EXCEEDED => TenantVectorDimExceeded { dim: 0, limit: 0 },
    TENANT_GRAPH_DEPTH_EXCEEDED => TenantGraphDepthExceeded { depth: 0, limit: 0 },

    // Protocol handshake.
    HANDSHAKE_FAILED => HandshakeFailed { server_code: 0 },

    // Sync.
    SYNC_CONNECTION_FAILED => SyncConnectionFailed,
    SYNC_DELTA_REJECTED => SyncDeltaRejected { compensation: None },
    SHAPE_SUBSCRIPTION_FAILED => ShapeSubscriptionFailed { shape_id: String::new() },

    // Storage / infrastructure.
    STORAGE => Storage { component: "remote".into(), op: String::new(), detail: message.to_owned() },
    SEGMENT_CORRUPTED => SegmentCorrupted {
        segment_id: 0,
        corruption: "remote".into(),
        detail: message.to_owned(),
    },
    COLD_STORAGE => ColdStorage { backend: "remote".into(), op: String::new(), detail: message.to_owned() },
    WAL => Wal { stage: "remote".into(), detail: message.to_owned() },

    // Serialization.
    SERIALIZATION => Serialization { format: String::new() },
    CODEC => Codec { codec: "remote".into(), op: String::new(), detail: message.to_owned() },

    // Config.
    CONFIG => Config,
    BAD_REQUEST => BadRequest,

    // Cluster.
    NO_LEADER => NoLeader,
    NOT_LEADER => NotLeader { leader_addr: String::new() },
    MIGRATION_IN_PROGRESS => MigrationInProgress,
    NODE_UNREACHABLE => NodeUnreachable,
    CLUSTER => Cluster,

    // Memory.
    MEMORY_EXHAUSTED => MemoryExhausted { engine: String::new() },

    // Encryption.
    ENCRYPTION => Encryption { cipher: "remote".into(), detail: message.to_owned() },

    // Engine: Array.
    ARRAY => Array { array: String::new() },

    // Quota. `DATABASE_QUOTA_EXCEEDED` shares this shape but is not 1-to-1
    // reversible (see `wire_details`'s explicit bucket).
    QUOTA_OVERCOMMIT => QuotaOvercommit { field: String::new() },
    TENANT_QUOTA_EXCEEDED => QuotaExceeded { scope: String::new() },
    SERVER_OVERLOAD => ServerOverload,

    // Clone DDL.
    CLONE_DEPTH_EXCEEDED => CloneDepthExceeded { depth: 0, limit: 0 },
    CANNOT_CLONE_MIRROR => CannotCloneMirror { database: String::new() },
    CLONE_DEPENDENCY => CloneDependency { dependents: Vec::new() },
    CLONE_PREDATES_QUERY_TIME => ClonePredatesQueryTime { as_of_lsn: 0, created_at_lsn: 0 },
    CLONE_WRITE_REQUIRES_MATERIALIZE => CloneWriteRequiresMaterialize {
        collection: String::new(),
        engine: String::new(),
        database: String::new(),
    },

    // Backup / Restore.
    BACKUP_TENANT_MISMATCH => BackupTenantMismatch { expected: 0, actual: 0 },
    BACKUP_KEY_MISMATCH => BackupKeyMismatch,

    // Mirror DDL.
    MIRROR_READ_ONLY => MirrorReadOnly { database: String::new() },
    STALE_READ_NOT_LEADER => StaleReadNotLeader {
        database: String::new(),
        source_cluster: String::new(),
    },
    MIRROR_NOT_PROMOTED => MirrorNotPromoted { database: String::new() },
    CANNOT_DROP_DEFAULT_DATABASE => CannotDropDefaultDatabase,

    // Move Tenant DDL.
    MOVE_TENANT_DRAIN_TIMEOUT => MoveTenantDrainTimeout { tenant: String::new(), source_db: String::new() },
    MOVE_TENANT_PREFLIGHT_FAILED => MoveTenantPreflightFailed {
        tenant: String::new(),
        detail: message.to_owned(),
    },
    MOVE_TENANT_SNAPSHOT_FAILED => MoveTenantSnapshotFailed {
        tenant: String::new(),
        detail: message.to_owned(),
    },
    MOVE_TENANT_CUTOVER_FAILED => MoveTenantCutoverFailed {
        tenant: String::new(),
        detail: message.to_owned(),
    },
    MOVE_TENANT_ALREADY_AT_TARGET => MoveTenantAlreadyAtTarget {
        tenant: String::new(),
        target_db: String::new(),
    },

    // Bridge / Dispatch / Internal.
    BRIDGE => Bridge { plane: "remote".into(), op: String::new(), detail: message.to_owned() },
    DISPATCH => Dispatch { stage: "remote".into(), detail: message.to_owned() },
    INTERNAL => Internal { component: "remote".into(), detail: message.to_owned() },
}

/// Reconstruct `ErrorDetails` for a wire `code`: the primary table, then
/// the documented secondary bucket, then `Internal`. The single function
/// `wire_details` and `remote_typed` both call, so they cannot drift.
pub(crate) fn details_for_code(code: ErrorCode, message: &str) -> ErrorDetails {
    if let Some(details) = primary_details_for_code(code, message) {
        return details;
    }
    match code {
        // Shares `TENANT_QUOTA_EXCEEDED`'s shape; not itself canonical.
        ErrorCode::DATABASE_QUOTA_EXCEEDED => ErrorDetails::QuotaExceeded {
            scope: String::new(),
        },
        // No dedicated variant: a pre-code-field peer sends `0`, and some
        // SQLSTATE-only conditions never reach the wire as a numeric code.
        _ => ErrorDetails::Internal {
            component: "remote".into(),
            detail: message.to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_primary_code_round_trips_through_details_code() {
        for code in [
            ErrorCode::CONSTRAINT_VIOLATION,
            ErrorCode::DATABASE_NOT_FOUND,
            ErrorCode::UNDEFINED_OBJECT,
            ErrorCode::ALREADY_EXISTS,
            ErrorCode::OBJECT_NOT_READY,
            ErrorCode::NOT_FOUND,
            ErrorCode::CANNOT_DROP_DEFAULT_DATABASE,
            ErrorCode::COLLECTION_DEACTIVATED,
            ErrorCode::ARRAY,
            ErrorCode::QUOTA_OVERCOMMIT,
            ErrorCode::CLONE_DEPTH_EXCEEDED,
            ErrorCode::CLONE_WRITE_REQUIRES_MATERIALIZE,
            ErrorCode::MOVE_TENANT_DRAIN_TIMEOUT,
            ErrorCode::MIRROR_READ_ONLY,
            ErrorCode::BACKUP_TENANT_MISMATCH,
            ErrorCode::TENANT_VECTOR_DIM_EXCEEDED,
            ErrorCode::HANDSHAKE_FAILED,
            ErrorCode::SYNC_DELTA_REJECTED,
            ErrorCode::SEGMENT_CORRUPTED,
            ErrorCode::SERIALIZATION,
            ErrorCode::ENCRYPTION,
            ErrorCode::BRIDGE,
            ErrorCode::DISPATCH,
        ] {
            let details = primary_details_for_code(code, "boom").unwrap_or_else(|| {
                panic!("expected a primary details mapping for {code}");
            });
            assert_eq!(details.code(), code);
        }
    }

    #[test]
    fn unmapped_code_has_no_primary_mapping() {
        assert!(primary_details_for_code(ErrorCode::DATABASE_QUOTA_EXCEEDED, "x").is_none());
        assert!(primary_details_for_code(ErrorCode(65000), "x").is_none());
    }
}
