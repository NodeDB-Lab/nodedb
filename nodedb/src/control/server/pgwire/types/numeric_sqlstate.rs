// SPDX-License-Identifier: BUSL-1.1

//! Public numeric `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;

/// Map a numeric `ErrorCode` received from a remote node back to a SQLSTATE.
/// Local errors map by variant identity in `error_to_sqlstate`. A remote
/// error arrives as a bare numeric code, so this recovers the class. Each
/// bucket mirrors the SQLSTATE the local variant arm chooses for the same
/// numeric code, so a constraint violation (say) maps to the same SQLSTATE
/// whether it happened locally or on a remote node. `ErrorCode` is an open
/// numeric newtype, so an unmapped or unknown code renders `INTERNAL_ERROR`.
pub(crate) fn numeric_code_to_sqlstate(code: nodedb_types::error::ErrorCode) -> &'static str {
    use nodedb_types::error::ErrorCode as Ec;
    match code {
        // Mirrors the `RejectedConstraint` arm.
        Ec::CONSTRAINT_VIOLATION => sqlstate::UNIQUE_VIOLATION,
        // Mirrors the `ConflictRetry` / `CalvinSerializationConflict` /
        // `RetryableSchemaChanged` arms, and `OllpExhausted` when it exhausted
        // on drift.
        Ec::WRITE_CONFLICT => sqlstate::SERIALIZATION_FAILURE,
        // Mirrors the `CalvinParticipantError` arm.
        Ec::TRANSACTION_ROLLBACK => sqlstate::TRANSACTION_ROLLBACK,
        // Mirrors the `NotInTransactionBlock` / `CrdtApplyForbiddenInTransaction`
        // / `CrossShardInExplicitTransaction` arms.
        Ec::ACTIVE_SQL_TRANSACTION => sqlstate::ACTIVE_SQL_TRANSACTION,
        // Mirrors the `DependentObjectsExist` arm.
        Ec::DEPENDENT_OBJECTS_EXIST => sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
        // Mirrors the `DeadlineExceeded` arm.
        Ec::DEADLINE_EXCEEDED => sqlstate::QUERY_CANCELED.0,
        // Mirrors the `CollectionNotFound` / `CollectionDeactivated` arms.
        Ec::COLLECTION_NOT_FOUND | Ec::COLLECTION_DEACTIVATED => sqlstate::UNDEFINED_TABLE,
        // Mirrors the `DocumentNotFound` arm.
        Ec::DOCUMENT_NOT_FOUND => sqlstate::NO_DATA,
        // Mirrors the `BadRequest` / `PlanError` arms.
        Ec::BAD_REQUEST | Ec::PLAN_ERROR => sqlstate::SYNTAX_ERROR,
        // Mirrors the `UndefinedFunction` arm.
        Ec::UNDEFINED_FUNCTION => sqlstate::UNDEFINED_FUNCTION,
        // Mirrors the `UndefinedObject` arm.
        Ec::UNDEFINED_OBJECT => sqlstate::UNDEFINED_OBJECT,
        // Mirrors the `ObjectNotInPrerequisiteState` arm.
        Ec::OBJECT_NOT_READY => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        // Mirrors the `UndefinedColumn` arm.
        Ec::UNDEFINED_COLUMN => sqlstate::UNDEFINED_COLUMN,
        // Mirrors the `AmbiguousColumn` arm.
        Ec::AMBIGUOUS_COLUMN => sqlstate::AMBIGUOUS_COLUMN,
        // Mirrors the `DivisionByZero` arm.
        Ec::DIVISION_BY_ZERO => sqlstate::DIVISION_BY_ZERO,
        // Mirrors the `DataException` arm.
        Ec::DATA_EXCEPTION => sqlstate::DATA_EXCEPTION,
        // Mirrors the `InvalidLimitValue` arm.
        Ec::INVALID_LIMIT_VALUE => sqlstate::INVALID_LIMIT_VALUE,
        // Mirrors the `RejectedAuthz` arm.
        Ec::AUTHORIZATION_DENIED => sqlstate::INSUFFICIENT_PRIVILEGE,
        // Mirrors the `SessionTokenExpired` arm.
        Ec::AUTH_EXPIRED => sqlstate::AUTH_TOKEN_EXPIRED.0,
        // Mirrors the credential-failure frames of pgwire and native login.
        Ec::AUTHENTICATION_FAILED => sqlstate::INVALID_AUTHORIZATION,
        // Mirrors the `RateExceeded` arm.
        Ec::RATE_EXCEEDED => sqlstate::TOO_MANY_CONNECTIONS,
        // Mirrors the `MemoryExhausted` / `Backpressure` arms.
        Ec::MEMORY_EXHAUSTED => sqlstate::OUT_OF_MEMORY,
        // Mirrors the `DispatchCapacity` arm.
        Ec::SERVER_OVERLOAD => sqlstate::SERVER_OVERLOAD,
        // Mirrors the `NoLeader` arm.
        Ec::NO_LEADER => sqlstate::LOCK_NOT_AVAILABLE,
        // Mirrors the `NotLeader` arm.
        Ec::NOT_LEADER => sqlstate::DATABASE_DROPPED,
        // Mirrors the `CloneWriteRequiresMaterialize` arm.
        Ec::CLONE_WRITE_REQUIRES_MATERIALIZE => sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
        // Mirrors the `BackupTenantMismatch` arm.
        Ec::BACKUP_TENANT_MISMATCH => sqlstate::BACKUP_TENANT_MISMATCH,
        // Mirrors the `BackupKeyMismatch` arm.
        Ec::BACKUP_KEY_MISMATCH => sqlstate::BACKUP_KEY_MISMATCH.0,
        // Mirrors the `QuotaOvercommit` arm.
        Ec::QUOTA_OVERCOMMIT => sqlstate::QUOTA_OVERCOMMIT,
        // Mirrors the `TenantVectorDimExceeded` / `TenantGraphDepthExceeded`
        // arms.
        Ec::TENANT_VECTOR_DIM_EXCEEDED | Ec::TENANT_GRAPH_DEPTH_EXCEEDED => {
            sqlstate::QUOTA_EXCEEDED
        }
        // Mirrors the `StorageQuotaExceeded` arm.
        Ec::STORAGE_QUOTA_EXCEEDED => sqlstate::DISK_FULL,
        // Mirrors the `MirrorReadOnly` arm.
        Ec::MIRROR_READ_ONLY => sqlstate::READ_ONLY_SQL_TRANSACTION,
        // Mirrors the `MirrorSyncTimeout` arm.
        Ec::MIRROR_SYNC_TIMEOUT => sqlstate::MIRROR_SYNC_TIMEOUT,
        // Mirrors the `StaleReadNotLeader` arm.
        Ec::STALE_READ_NOT_LEADER => sqlstate::STALE_READ_NOT_LEADER,
        // The codes below mirror the Data-Plane code table
        // (`error_code_to_sqlstate`) for the public code each Data-Plane code
        // classifies to, so a verdict that crossed a node as a numeric code
        // renders in the class it has locally.
        Ec::PREVALIDATION_REJECTED | Ec::INSUFFICIENT_BALANCE => sqlstate::CHECK_VIOLATION,
        Ec::APPEND_ONLY_VIOLATION => sqlstate::APPEND_ONLY_VIOLATION,
        Ec::BALANCE_VIOLATION => sqlstate::BALANCE_VIOLATION,
        Ec::PERIOD_LOCKED => sqlstate::PERIOD_LOCKED,
        Ec::PERIOD_LOCK_MISCONFIGURED => sqlstate::PERIOD_LOCK_MISCONFIGURED,
        Ec::STATE_TRANSITION_VIOLATION => sqlstate::STATE_TRANSITION_VIOLATION,
        Ec::TRANSITION_CHECK_VIOLATION => sqlstate::TRANSITION_CHECK_VIOLATION,
        Ec::RETENTION_VIOLATION => sqlstate::RETENTION_VIOLATION,
        Ec::LEGAL_HOLD_ACTIVE => sqlstate::LEGAL_HOLD_ACTIVE,
        Ec::TYPE_GUARD_VIOLATION => sqlstate::TYPE_GUARD_VIOLATION,
        Ec::TYPE_MISMATCH => sqlstate::CANNOT_COERCE,
        Ec::OVERFLOW => sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
        Ec::COLLECTION_DRAINING => sqlstate::CANNOT_CONNECT_NOW,
        Ec::SQL_NOT_ENABLED => sqlstate::FEATURE_NOT_SUPPORTED,
        Ec::PROGRAM_LIMIT_EXCEEDED => sqlstate::PROGRAM_LIMIT_EXCEEDED,
        // The codes below mirror the SQLSTATE the DDL layer sends with each
        // code, and the SQLSTATE `code_for_sqlstate` reads back into it.
        Ec::DATABASE_NOT_FOUND => sqlstate::INVALID_CATALOG_NAME,
        Ec::ALREADY_EXISTS => sqlstate::DUPLICATE_OBJECT,
        Ec::NOT_FOUND | Ec::MOVE_TENANT_ALREADY_AT_TARGET => sqlstate::NO_DATA,
        Ec::TENANT_QUOTA_EXCEEDED | Ec::DATABASE_QUOTA_EXCEEDED => sqlstate::QUOTA_EXCEEDED,
        Ec::CLONE_DEPTH_EXCEEDED => sqlstate::CLONE_DEPTH_EXCEEDED,
        Ec::CANNOT_CLONE_MIRROR => sqlstate::CANNOT_CLONE_MIRROR.0,
        Ec::CANNOT_DROP_DEFAULT_DATABASE => sqlstate::CANNOT_DROP_DEFAULT_DATABASE.0,
        Ec::CLONE_DEPENDENCY => sqlstate::CLONE_DEPENDENCY.0,
        Ec::CLONE_PREDATES_QUERY_TIME => sqlstate::CLONE_PREDATES_QUERY_TIME,
        Ec::MIRROR_NOT_PROMOTED => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        Ec::MOVE_TENANT_DRAIN_TIMEOUT => sqlstate::MOVE_TENANT_DRAIN_TIMEOUT.0,
        Ec::MOVE_TENANT_PREFLIGHT_FAILED => sqlstate::MOVE_TENANT_PREFLIGHT_FAILED,
        // The codes below have no server-side `Error` variant. Each takes the
        // SQLSTATE class of the condition it names.
        Ec::HANDSHAKE_FAILED => sqlstate::PROTOCOL_VIOLATION,
        Ec::SYNC_CONNECTION_FAILED | Ec::NODE_UNREACHABLE => sqlstate::CONNECTION_FAILURE,
        Ec::SHAPE_SUBSCRIPTION_FAILED => sqlstate::SERVER_REJECTED_ESTABLISHMENT,
        // Mirrors the Data-Plane `SyncRejected` code.
        Ec::SYNC_DELTA_REJECTED => sqlstate::CHECK_VIOLATION,
        Ec::MIGRATION_IN_PROGRESS => sqlstate::CANNOT_CONNECT_NOW,
        // Internal faults: pgwire sends `XX000` for every `Error` variant
        // that classifies to one of these. `INTERNAL_CODES` lists them.
        _ => sqlstate::INTERNAL_ERROR,
    }
}

/// The codes whose correct SQLSTATE is `XX000`: each names a server-side
/// fault the client cannot act on.
#[cfg(test)]
const INTERNAL_CODES: &[(nodedb_types::error::ErrorCode, &str)] = {
    use nodedb_types::error::ErrorCode as Ec;
    &[
        (
            Ec::ARRAY,
            "array-catalog registry invariant at bootstrap or open",
        ),
        (
            Ec::MOVE_TENANT_SNAPSHOT_FAILED,
            "server-side phase failure; DDL sends XX000",
        ),
        (
            Ec::MOVE_TENANT_CUTOVER_FAILED,
            "server-side phase failure; DDL sends XX000",
        ),
        (Ec::STORAGE, "storage engine or I/O fault"),
        (Ec::SEGMENT_CORRUPTED, "on-disk segment corruption"),
        (Ec::COLD_STORAGE, "cold-tier backend fault"),
        (Ec::WAL, "WAL append or fsync fault"),
        (Ec::SERIALIZATION, "internal payload encode or decode fault"),
        (Ec::CODEC, "compression codec fault"),
        (Ec::CONFIG, "server configuration fault"),
        (Ec::CLUSTER, "cluster version-compatibility fault"),
        (Ec::ENCRYPTION, "at-rest encryption fault"),
        (Ec::INTERNAL, "generic internal fault"),
        (Ec::BRIDGE, "SPSC bridge fault"),
        (Ec::DISPATCH, "Control-to-Data-Plane dispatch fault"),
    ]
};

/// Codes whose SQLSTATE another code owns: `code_for_sqlstate` reads that
/// SQLSTATE back as the owner. Each pair is `(code, owner)`.
#[cfg(test)]
const SHARED_SQLSTATE: &[(
    nodedb_types::error::ErrorCode,
    nodedb_types::error::ErrorCode,
)] = {
    use nodedb_types::error::ErrorCode as Ec;
    &[
        (Ec::PREVALIDATION_REJECTED, Ec::CONSTRAINT_VIOLATION),
        (Ec::INSUFFICIENT_BALANCE, Ec::CONSTRAINT_VIOLATION),
        (Ec::SYNC_DELTA_REJECTED, Ec::CONSTRAINT_VIOLATION),
        (Ec::DOCUMENT_NOT_FOUND, Ec::NOT_FOUND),
        (Ec::MOVE_TENANT_ALREADY_AT_TARGET, Ec::NOT_FOUND),
        (Ec::COLLECTION_DEACTIVATED, Ec::COLLECTION_NOT_FOUND),
        (Ec::COLLECTION_DRAINING, Ec::SERVER_OVERLOAD),
        (Ec::MIGRATION_IN_PROGRESS, Ec::SERVER_OVERLOAD),
        (Ec::PLAN_ERROR, Ec::BAD_REQUEST),
        (Ec::TENANT_QUOTA_EXCEEDED, Ec::QUOTA_OVERCOMMIT),
        (Ec::DATABASE_QUOTA_EXCEEDED, Ec::QUOTA_OVERCOMMIT),
        (Ec::TENANT_VECTOR_DIM_EXCEEDED, Ec::QUOTA_OVERCOMMIT),
        (Ec::TENANT_GRAPH_DEPTH_EXCEEDED, Ec::QUOTA_OVERCOMMIT),
        (Ec::CANNOT_CLONE_MIRROR, Ec::SQL_NOT_ENABLED),
        (Ec::CANNOT_DROP_DEFAULT_DATABASE, Ec::SQL_NOT_ENABLED),
        (Ec::CLONE_DEPENDENCY, Ec::OBJECT_NOT_READY),
        (Ec::CLONE_WRITE_REQUIRES_MATERIALIZE, Ec::OBJECT_NOT_READY),
        (Ec::MIRROR_NOT_PROMOTED, Ec::OBJECT_NOT_READY),
        (Ec::CLONE_PREDATES_QUERY_TIME, Ec::DATA_EXCEPTION),
        (Ec::BACKUP_TENANT_MISMATCH, Ec::DATA_EXCEPTION),
        (Ec::BACKUP_KEY_MISMATCH, Ec::AUTHENTICATION_FAILED),
        (Ec::AUTH_EXPIRED, Ec::AUTHENTICATION_FAILED),
        (Ec::STALE_READ_NOT_LEADER, Ec::NO_LEADER),
        (Ec::SYNC_CONNECTION_FAILED, Ec::NODE_UNREACHABLE),
    ]
};

/// Codes whose SQLSTATE has no default meaning, so `code_for_sqlstate`
/// reads it back as `INTERNAL` rather than guess. `57014` is sent for a
/// deadline and for a cancellation that is not one.
#[cfg(test)]
const NO_DEFAULT_SQLSTATE: &[nodedb_types::error::ErrorCode] = &[
    nodedb_types::error::ErrorCode::DEADLINE_EXCEEDED,
    nodedb_types::error::ErrorCode::MOVE_TENANT_DRAIN_TIMEOUT,
];

#[cfg(test)]
mod tests {
    use nodedb_types::error::ErrorCode;

    use super::*;
    use crate::control::server::shared::ddl::result::code_for_sqlstate;

    fn class(state: &str) -> &str {
        state.get(..2).unwrap_or(state)
    }

    fn is_internal(code: ErrorCode) -> bool {
        INTERNAL_CODES.iter().any(|(internal, _)| *internal == code)
    }

    /// Every public code renders a class of its own, unless it names a
    /// server-side fault.
    #[test]
    fn every_client_class_code_has_a_sqlstate() {
        for &code in ErrorCode::ALL {
            let state = numeric_code_to_sqlstate(code);
            if is_internal(code) {
                assert_eq!(state, sqlstate::INTERNAL_ERROR, "{code} is internal");
            } else {
                assert_ne!(state, sqlstate::INTERNAL_ERROR, "{code} has no SQLSTATE");
            }
        }
    }

    /// Native code to SQLSTATE and back returns the same code. A code whose
    /// SQLSTATE another code owns returns the owner, which renders the same
    /// class.
    #[test]
    fn every_client_class_code_round_trips_through_its_sqlstate() {
        for &code in ErrorCode::ALL {
            if is_internal(code) {
                continue;
            }
            let state = numeric_code_to_sqlstate(code);
            let back = code_for_sqlstate(state);
            if NO_DEFAULT_SQLSTATE.contains(&code) {
                assert_eq!(
                    back,
                    ErrorCode::INTERNAL,
                    "{code} renders {state}, which has no default"
                );
                continue;
            }
            match SHARED_SQLSTATE.iter().find(|(shared, _)| *shared == code) {
                Some((_, owner)) => {
                    assert_eq!(back, *owner, "{code} renders {state}, owned by {owner}");
                    assert_eq!(
                        class(numeric_code_to_sqlstate(*owner)),
                        class(state),
                        "{code} and its owner {owner} render different classes"
                    );
                }
                None => assert_eq!(back, code, "{code} renders {state}, read back as {back}"),
            }
        }
    }

    /// Both lists name only codes that exist, and no code twice.
    #[test]
    fn the_allowlists_are_well_formed() {
        for (code, reason) in INTERNAL_CODES {
            assert!(ErrorCode::ALL.contains(code), "{code}");
            assert!(!reason.is_empty(), "{code} has no reason");
        }
        for (code, owner) in SHARED_SQLSTATE {
            assert!(!is_internal(*code), "{code} is both internal and shared");
            assert!(
                !is_internal(*owner),
                "{owner} owns a SQLSTATE but is internal"
            );
            assert_ne!(code, owner, "{code} owns its own SQLSTATE");
            assert!(
                !NO_DEFAULT_SQLSTATE.contains(code),
                "{code} is both shared and no-default"
            );
        }
        for code in NO_DEFAULT_SQLSTATE {
            assert!(
                !is_internal(*code),
                "{code} is both internal and no-default"
            );
        }
    }
}
