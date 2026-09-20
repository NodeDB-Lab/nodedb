// SPDX-License-Identifier: BUSL-1.1

//! NodeDB `Error` and Data Plane `ErrorCode` to PostgreSQL SQLSTATE mapping.

use nodedb_types::error::sqlstate;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::OllpExhaustedCause;
use crate::bridge::envelope::{ErrorCode, Status};

/// Create a pgwire ErrorResponse with a SQLSTATE code.
pub fn sqlstate_error(code: &str, message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message.to_owned(),
    )))
}

/// Map an error raised while shaping a response to the pgwire error the
/// client reads, with the SQLSTATE its numeric code maps to. A per-row
/// sequence accessor refusal (`42704`, `55000`) or a division by zero
/// (`22012`) keeps its class instead of collapsing to `XX000`.
///
/// An `XX000`-class failure describes server state, so its detail stays in
/// the server log and the client reads one stable summary
/// ([`super::shaping_error_message`]); every other class keeps its message.
pub fn shape_error_to_pg(e: &nodedb_types::NodeDbError) -> PgWireError {
    sqlstate_error(
        numeric_code_to_sqlstate(e.code()),
        &super::shaping_error_message(e.code(), e.message()),
    )
}

/// Map a NodeDB `Error` to a PostgreSQL SQLSTATE code + message.
pub fn error_to_sqlstate(err: &crate::Error) -> (&'static str, &'static str, String) {
    match err {
        crate::Error::BadRequest { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::BackupTenantMismatch { .. } => {
            ("ERROR", sqlstate::BACKUP_TENANT_MISMATCH, err.to_string())
        }
        crate::Error::BackupKeyMismatch => {
            ("ERROR", sqlstate::BACKUP_KEY_MISMATCH, err.to_string())
        }
        crate::Error::PlanError { detail } => ("ERROR", sqlstate::SYNTAX_ERROR, detail.clone()),
        crate::Error::CollectionNotFound { collection, .. } => (
            "ERROR",
            sqlstate::UNDEFINED_TABLE,
            format!("collection \"{collection}\" does not exist"),
        ),
        crate::Error::CollectionDeactivated { collection, .. } => (
            "ERROR",
            // UNDEFINED_TABLE is the canonical pg code; the distinct message
            // carries the UNDROP hint so client UX can surface a restore
            // button without a custom sqlstate.
            sqlstate::UNDEFINED_TABLE,
            format!(
                "collection \"{collection}\" was dropped and is within its retention \
                 window; restore it with `UNDROP COLLECTION {collection}` before \
                 it is hard-deleted"
            ),
        ),
        crate::Error::FeatureNotSupported { detail } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, detail.clone())
        }
        crate::Error::UndefinedFunction { name } => (
            "ERROR",
            sqlstate::UNDEFINED_FUNCTION,
            format!("function {name}(...) does not exist"),
        ),
        crate::Error::UndefinedObject { .. } => {
            ("ERROR", sqlstate::UNDEFINED_OBJECT, err.to_string())
        }
        crate::Error::ObjectNotInPrerequisiteState { detail, .. } => (
            "ERROR",
            sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
            detail.clone(),
        ),
        crate::Error::UndefinedColumn { column } => (
            "ERROR",
            sqlstate::UNDEFINED_COLUMN,
            format!("column \"{column}\" does not exist"),
        ),
        crate::Error::AmbiguousColumn { column } => (
            "ERROR",
            sqlstate::AMBIGUOUS_COLUMN,
            format!("column reference \"{column}\" is ambiguous"),
        ),
        crate::Error::UnknownStrictField { .. } => {
            ("ERROR", sqlstate::UNDEFINED_COLUMN, err.to_string())
        }
        crate::Error::DivisionByZero => ("ERROR", sqlstate::DIVISION_BY_ZERO, err.to_string()),
        crate::Error::InvalidLimitValue { .. } => {
            ("ERROR", sqlstate::INVALID_LIMIT_VALUE, err.to_string())
        }
        crate::Error::DocumentNotFound {
            collection,
            document_id,
        } => (
            "ERROR",
            sqlstate::NO_DATA,
            format!("document \"{document_id}\" not found in \"{collection}\""),
        ),
        crate::Error::RejectedConstraint {
            constraint, detail, ..
        } => {
            let code = if constraint == "not_null" {
                sqlstate::NOT_NULL_VIOLATION
            } else {
                sqlstate::UNIQUE_VIOLATION
            };
            ("ERROR", code, detail.clone())
        }
        crate::Error::TxnOverlayMemoryExceeded { .. } => {
            ("ERROR", sqlstate::PROGRAM_LIMIT_EXCEEDED, err.to_string())
        }
        crate::Error::DeadlineExceeded { .. } => {
            ("ERROR", sqlstate::QUERY_CANCELED, err.to_string())
        }
        crate::Error::ConflictRetry { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A cross-shard Calvin OCC abort is a serialization failure — the client
        // should retry the whole transaction.
        crate::Error::CalvinSerializationConflict => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        // A participant error aborted the transaction before any read-set was
        // validated, so it is NOT a serialization conflict. TRANSACTION_ROLLBACK
        // (40000) keeps it in the retryable class 40 without claiming 40001.
        crate::Error::CalvinParticipantError => {
            ("ERROR", sqlstate::TRANSACTION_ROLLBACK, err.to_string())
        }
        crate::Error::SourceFrozen { .. } => {
            ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
        }
        crate::Error::CloneWriteRequiresMaterialize { .. } => (
            "ERROR",
            sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
            err.to_string(),
        ),
        crate::Error::RejectedAuthz { .. } => {
            ("ERROR", sqlstate::INSUFFICIENT_PRIVILEGE, err.to_string())
        }
        // A rate-limit rejection is transient/retryable, distinct from a
        // credential or privilege failure. TOO_MANY_CONNECTIONS (53300) is the
        // canonical retryable code clients recognise.
        crate::Error::RateExceeded { .. } => {
            ("ERROR", sqlstate::TOO_MANY_CONNECTIONS, err.to_string())
        }
        crate::Error::MemoryExhausted { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::Backpressure { .. } => ("ERROR", sqlstate::OUT_OF_MEMORY, err.to_string()),
        crate::Error::FanOutExceeded { .. } => {
            ("ERROR", sqlstate::STATEMENT_TOO_COMPLEX, err.to_string())
        }
        // A cross-collection write refused because source and target are not
        // co-resident is a not-yet-supported operation, NOT a transient/internal
        // fault — surface FEATURE_NOT_SUPPORTED (0A000) so clients do not retry.
        crate::Error::CrossCollectionNotColocated { .. } => {
            ("ERROR", sqlstate::FEATURE_NOT_SUPPORTED, err.to_string())
        }
        crate::Error::NoLeader { .. } => ("ERROR", sqlstate::LOCK_NOT_AVAILABLE, err.to_string()),
        // DATABASE_DROPPED (57P04) — the closest Postgres canonical code for
        // "try again later, different node". Client libraries that recognise
        // the 57P* family treat this as retryable transient unavailability,
        // which is exactly the semantics we want. The message carries the
        // hinted leader address so an operator inspecting logs can see the
        // redirect target.
        crate::Error::NotLeader { leader_addr, .. } => (
            "ERROR",
            sqlstate::DATABASE_DROPPED,
            format!("cluster in leader election; leader hint: {leader_addr}"),
        ),
        // OLLP retry exhaustion is retryable only when it exhausted on real
        // drift. A pre-admission cause keeps ITS OWN sqlstate — telling a client
        // to retry a deterministic failure just burns another round trip — and a
        // refused admission gate is transient like any other load rejection.
        crate::Error::OllpExhausted { cause, .. } => match cause {
            OllpExhaustedCause::PredicateDrift => {
                ("ERROR", sqlstate::SERIALIZATION_FAILURE, err.to_string())
            }
            OllpExhaustedCause::PreAdmission(inner) => {
                let (severity, code, _) = error_to_sqlstate(inner);
                (severity, code, err.to_string())
            }
            OllpExhaustedCause::AdmissionRefused { .. } => {
                ("ERROR", sqlstate::TOO_MANY_CONNECTIONS, err.to_string())
            }
        },
        crate::Error::RemoteTyped { code, message } => {
            ("ERROR", numeric_code_to_sqlstate(*code), message.clone())
        }
        // A materialized-sum join key that names no target row breaks the
        // balance invariant, so it carries the same SQLSTATE the Data Plane's
        // `BalanceViolation` does rather than a generic internal error.
        crate::Error::MaterializedSumTargetNotFound { .. } => {
            ("ERROR", sqlstate::BALANCE_VIOLATION, err.to_string())
        }
        // A Data Plane verdict that travelled back as a typed code keeps the
        // SQLSTATE it would have had on the direct dispatch path.
        crate::Error::DataPlane(code) => {
            crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code)
        }
        // A variant with no arm of its own carries its class on the error:
        // `classify` borrows it (the crate's one Error-to-NodeDbError map),
        // then this table answers instead of a blanket `XX000`.
        _ => (
            "ERROR",
            numeric_code_to_sqlstate(crate::error_classify::classify(err).code()),
            err.to_string(),
        ),
    }
}

/// Map a numeric `ErrorCode` received from a remote node back to a SQLSTATE.
/// Local errors map by variant identity above; a remote error arrives as a bare
/// numeric code, so this recovers the classification. Each bucket mirrors the
/// sqlstate the corresponding local variant arm chooses above for the same
/// numeric code, so a constraint violation (say) maps to the same SQLSTATE
/// whether it happened locally or on a remote node. Unmapped/unknown codes
/// fall back to INTERNAL_ERROR — the behaviour before codes were preserved.
pub(crate) fn numeric_code_to_sqlstate(code: nodedb_types::error::ErrorCode) -> &'static str {
    use nodedb_types::error::ErrorCode as Ec;
    match code {
        // Mirrors the `RejectedConstraint` arm.
        Ec::CONSTRAINT_VIOLATION => sqlstate::UNIQUE_VIOLATION,
        // Mirrors the `ConflictRetry` / `CalvinSerializationConflict` /
        // `SourceFrozen` arms, and `OllpExhausted` when it exhausted on drift.
        Ec::WRITE_CONFLICT => sqlstate::SERIALIZATION_FAILURE,
        // Mirrors the `DeadlineExceeded` arm.
        Ec::DEADLINE_EXCEEDED => sqlstate::QUERY_CANCELED,
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
        // Mirrors the `InvalidLimitValue` arm.
        Ec::INVALID_LIMIT_VALUE => sqlstate::INVALID_LIMIT_VALUE,
        // Mirrors the `FanOutExceeded` arm.
        Ec::FAN_OUT_EXCEEDED => sqlstate::STATEMENT_TOO_COMPLEX,
        // Mirrors the `RejectedAuthz` arm.
        Ec::AUTHORIZATION_DENIED => sqlstate::INSUFFICIENT_PRIVILEGE,
        // Mirrors the `RateExceeded` arm.
        Ec::RATE_EXCEEDED => sqlstate::TOO_MANY_CONNECTIONS,
        // Mirrors the `MemoryExhausted` / `Backpressure` arms.
        Ec::MEMORY_EXHAUSTED => sqlstate::OUT_OF_MEMORY,
        // Mirrors the `NoLeader` arm.
        Ec::NO_LEADER => sqlstate::LOCK_NOT_AVAILABLE,
        // Mirrors the `NotLeader` arm.
        Ec::NOT_LEADER => sqlstate::DATABASE_DROPPED,
        // Mirrors the `CloneWriteRequiresMaterialize` arm.
        Ec::CLONE_WRITE_REQUIRES_MATERIALIZE => sqlstate::CLONE_WRITE_REQUIRES_MATERIALIZE.0,
        // ── Completion of the classified set ──────────────────────────────
        //
        // The families below existed in the `crate::Error` table but not
        // here, so a remote or shaper-raised code in one of them collapsed to
        // XX000 at every routed surface.
        //
        // Constraint family — mirrors the Data Plane table.
        Ec::APPEND_ONLY_VIOLATION => sqlstate::APPEND_ONLY_VIOLATION,
        Ec::BALANCE_VIOLATION => sqlstate::BALANCE_VIOLATION,
        Ec::INSUFFICIENT_BALANCE => sqlstate::CHECK_VIOLATION,
        Ec::PERIOD_LOCKED => sqlstate::PERIOD_LOCKED,
        Ec::PERIOD_LOCK_MISCONFIGURED => sqlstate::PERIOD_LOCK_MISCONFIGURED,
        Ec::PREVALIDATION_REJECTED => sqlstate::CHECK_VIOLATION,
        Ec::RETENTION_VIOLATION => sqlstate::RETENTION_VIOLATION,
        Ec::LEGAL_HOLD_ACTIVE => sqlstate::LEGAL_HOLD_ACTIVE,
        Ec::STATE_TRANSITION_VIOLATION => sqlstate::STATE_TRANSITION_VIOLATION,
        Ec::TRANSITION_CHECK_VIOLATION => sqlstate::TRANSITION_CHECK_VIOLATION,
        Ec::TYPE_GUARD_VIOLATION => sqlstate::TYPE_GUARD_VIOLATION,
        Ec::TYPE_MISMATCH => sqlstate::DATATYPE_MISMATCH,
        // The shaper's own code: a payload that cannot be decoded into the
        // shape the projection requires.
        Ec::SERIALIZATION => sqlstate::INVALID_TEXT_REPRESENTATION,
        Ec::CODEC => sqlstate::INVALID_TEXT_REPRESENTATION,
        Ec::ARRAY => sqlstate::DATA_EXCEPTION,
        Ec::OVERFLOW => sqlstate::NUMERIC_VALUE_OUT_OF_RANGE,
        // Read-path absence.
        Ec::NOT_FOUND => sqlstate::NO_DATA,
        Ec::DATABASE_NOT_FOUND => sqlstate::INVALID_CATALOG_NAME,
        // Quota and admission.
        Ec::QUOTA_OVERCOMMIT => sqlstate::QUOTA_OVERCOMMIT,
        Ec::TENANT_QUOTA_EXCEEDED | Ec::DATABASE_QUOTA_EXCEEDED => sqlstate::QUOTA_EXCEEDED,
        Ec::TENANT_VECTOR_DIM_EXCEEDED | Ec::TENANT_GRAPH_DEPTH_EXCEEDED => {
            sqlstate::PROGRAM_LIMIT_EXCEEDED
        }
        Ec::SERVER_OVERLOAD => sqlstate::SERVER_OVERLOAD,
        Ec::MIGRATION_IN_PROGRESS | Ec::COLLECTION_DRAINING => sqlstate::CANNOT_CONNECT_NOW,
        // Clone family.
        Ec::ALREADY_EXISTS => sqlstate::DUPLICATE_TABLE,
        Ec::CANNOT_CLONE_MIRROR => sqlstate::FEATURE_NOT_SUPPORTED,
        Ec::CLONE_DEPENDENCY => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        Ec::CLONE_DEPTH_EXCEEDED => sqlstate::CLONE_DEPTH_EXCEEDED,
        Ec::CLONE_PREDATES_QUERY_TIME => sqlstate::CLONE_PREDATES_QUERY_TIME,
        // Move-tenant family.
        Ec::MOVE_TENANT_DRAIN_TIMEOUT => sqlstate::QUERY_CANCELED,
        Ec::MOVE_TENANT_PREFLIGHT_FAILED => sqlstate::MOVE_TENANT_PREFLIGHT_FAILED,
        Ec::MOVE_TENANT_SNAPSHOT_FAILED
        | Ec::MOVE_TENANT_CUTOVER_FAILED
        | Ec::MOVE_TENANT_ALREADY_AT_TARGET => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        // Mirror / read-only family.
        Ec::MIRROR_READ_ONLY => sqlstate::READ_ONLY_SQL_TRANSACTION,
        Ec::MIRROR_NOT_PROMOTED => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        Ec::STALE_READ_NOT_LEADER => sqlstate::STALE_READ_NOT_LEADER,
        Ec::CANNOT_DROP_DEFAULT_DATABASE => sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
        // Credentials and cluster connectivity.
        Ec::AUTH_EXPIRED | Ec::BACKUP_KEY_MISMATCH => sqlstate::INVALID_AUTHORIZATION,
        Ec::BACKUP_TENANT_MISMATCH => sqlstate::BACKUP_TENANT_MISMATCH,
        Ec::HANDSHAKE_FAILED
        | Ec::SYNC_CONNECTION_FAILED
        | Ec::SHAPE_SUBSCRIPTION_FAILED
        | Ec::NODE_UNREACHABLE
        | Ec::CLUSTER => sqlstate::CONNECTION_FAILURE,
        Ec::SYNC_DELTA_REJECTED => sqlstate::SERIALIZATION_FAILURE,
        // Storage and durability.
        Ec::STORAGE | Ec::COLD_STORAGE | Ec::WAL => sqlstate::IO_ERROR,
        Ec::SEGMENT_CORRUPTED | Ec::ENCRYPTION => sqlstate::DATA_CORRUPTED,
        Ec::CONFIG => sqlstate::OBJECT_NOT_IN_PREREQUISITE_STATE,
        Ec::SQL_NOT_ENABLED => sqlstate::FEATURE_NOT_SUPPORTED,
        // These three stay internal by decision: the explicit arms document
        // that the fallback is intended, not an unmapped code.
        Ec::INTERNAL | Ec::BRIDGE | Ec::DISPATCH => sqlstate::INTERNAL_ERROR,
        _ => sqlstate::INTERNAL_ERROR,
    }
}

/// Create a notice response (WARNING level).
pub fn notice_warning(message: &str) -> pgwire::messages::response::NoticeResponse {
    pgwire::messages::response::NoticeResponse::from(pgwire::error::ErrorInfo::new(
        "WARNING".to_owned(),
        sqlstate::WARNING.to_owned(),
        message.to_owned(),
    ))
}

/// Map a Data Plane response status + error code to a SQLSTATE triple.
pub fn response_status_to_sqlstate(
    status: Status,
    error_code: Option<&ErrorCode>,
) -> Option<(&'static str, &'static str, String)> {
    match status {
        Status::Ok | Status::Partial => None,
        Status::Error => {
            if let Some(code) = error_code {
                Some(crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code))
            } else {
                Some(("ERROR", "XX000", "unknown data plane error".into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::error::ErrorCode as Ec;

    /// The completion table: every code the mapper previously left on
    /// the `XX000` fallback now answers with its decided class.
    #[test]
    fn completion_codes_keep_their_class() {
        let cases = [
            (Ec::APPEND_ONLY_VIOLATION, "23601"),
            (Ec::BALANCE_VIOLATION, "23602"),
            (Ec::PERIOD_LOCKED, "23603"),
            (Ec::STATE_TRANSITION_VIOLATION, "23604"),
            (Ec::TRANSITION_CHECK_VIOLATION, "23605"),
            (Ec::RETENTION_VIOLATION, "23606"),
            (Ec::LEGAL_HOLD_ACTIVE, "23607"),
            (Ec::TYPE_GUARD_VIOLATION, "23608"),
            (Ec::PERIOD_LOCK_MISCONFIGURED, "23609"),
            (Ec::INSUFFICIENT_BALANCE, "23514"),
            (Ec::PREVALIDATION_REJECTED, "23514"),
            (Ec::TYPE_MISMATCH, "42804"),
            (Ec::SERIALIZATION, "22P02"),
            (Ec::CODEC, "22P02"),
            (Ec::ARRAY, "22000"),
            (Ec::OVERFLOW, "22003"),
            (Ec::NOT_FOUND, "02000"),
            (Ec::DATABASE_NOT_FOUND, "3D000"),
            (Ec::QUOTA_OVERCOMMIT, "53400"),
            (Ec::TENANT_QUOTA_EXCEEDED, "53400"),
            (Ec::DATABASE_QUOTA_EXCEEDED, "53400"),
            (Ec::TENANT_VECTOR_DIM_EXCEEDED, "54000"),
            (Ec::TENANT_GRAPH_DEPTH_EXCEEDED, "54000"),
            (Ec::SERVER_OVERLOAD, "57P03"),
            (Ec::MIGRATION_IN_PROGRESS, "57P03"),
            (Ec::COLLECTION_DRAINING, "57P03"),
            (Ec::ALREADY_EXISTS, "42P07"),
            (Ec::CANNOT_CLONE_MIRROR, "0A000"),
            (Ec::SQL_NOT_ENABLED, "0A000"),
            (Ec::CLONE_DEPENDENCY, "55000"),
            (Ec::CLONE_DEPTH_EXCEEDED, "54011"),
            (Ec::CLONE_PREDATES_QUERY_TIME, "22023"),
            (Ec::MOVE_TENANT_DRAIN_TIMEOUT, "57014"),
            (Ec::MOVE_TENANT_PREFLIGHT_FAILED, "55P02"),
            (Ec::MOVE_TENANT_SNAPSHOT_FAILED, "55000"),
            (Ec::MOVE_TENANT_CUTOVER_FAILED, "55000"),
            (Ec::MOVE_TENANT_ALREADY_AT_TARGET, "55000"),
            (Ec::MIRROR_READ_ONLY, "25006"),
            (Ec::MIRROR_NOT_PROMOTED, "55000"),
            (Ec::STALE_READ_NOT_LEADER, "55P03"),
            (Ec::CANNOT_DROP_DEFAULT_DATABASE, "2BP01"),
            (Ec::AUTH_EXPIRED, "28000"),
            (Ec::BACKUP_KEY_MISMATCH, "28000"),
            (Ec::BACKUP_TENANT_MISMATCH, "22023"),
            (Ec::HANDSHAKE_FAILED, "08006"),
            (Ec::SYNC_CONNECTION_FAILED, "08006"),
            (Ec::SHAPE_SUBSCRIPTION_FAILED, "08006"),
            (Ec::NODE_UNREACHABLE, "08006"),
            (Ec::CLUSTER, "08006"),
            (Ec::SYNC_DELTA_REJECTED, "40001"),
            (Ec::STORAGE, "58030"),
            (Ec::COLD_STORAGE, "58030"),
            (Ec::WAL, "58030"),
            (Ec::SEGMENT_CORRUPTED, "XX001"),
            (Ec::ENCRYPTION, "XX001"),
            (Ec::CONFIG, "55000"),
        ];
        for (code, expected) in cases {
            assert_eq!(
                numeric_code_to_sqlstate(code),
                expected,
                "code {} must keep its decided class",
                code.0
            );
        }
    }

    /// The three codes that are internal by decision keep the fallback, as an
    /// explicit arm rather than an implied one.
    #[test]
    fn internal_codes_stay_internal() {
        for code in [Ec::INTERNAL, Ec::BRIDGE, Ec::DISPATCH] {
            assert_eq!(numeric_code_to_sqlstate(code), sqlstate::INTERNAL_ERROR);
        }
    }

    /// An `XX000`-class shaping failure renders the stable summary; the detail
    /// stays in the log (enforced inside the helper `shape_error_to_pg` uses).
    #[test]
    fn shaping_internal_detail_is_not_rendered() {
        let error = nodedb_types::NodeDbError::internal(
            "manifest at /var/lib/nodedb/segment-42 is corrupt",
        );
        let message = super::super::shaping_error_message(error.code(), error.message());

        assert_eq!(message, "internal error while shaping the response");
        assert!(!message.contains("segment-42"));
    }

    /// A client-class shaping failure keeps its actionable message.
    #[test]
    fn shaping_client_message_passes_through() {
        let error = nodedb_types::NodeDbError::serialization(
            "cell",
            "column \"ts\" holds an integer where a timestamp is required",
        );
        let message = super::super::shaping_error_message(error.code(), error.message());

        assert!(message.contains("timestamp"));
    }

    /// A variant with no arm of its own still carries a class: the fallback
    /// borrows it through `classify` and this table answers it.
    #[test]
    fn arm_less_variants_keep_their_class_through_the_fallback() {
        use crate::Error;

        let cases = vec![
            (
                Error::AppendOnlyViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::APPEND_ONLY_VIOLATION,
            ),
            (
                Error::BalanceViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::BALANCE_VIOLATION,
            ),
            (
                Error::InsufficientBalance {
                    collection: "c".into(),
                    key: "k".into(),
                    detail: "d".into(),
                },
                sqlstate::CHECK_VIOLATION,
            ),
            (
                Error::PeriodLocked {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::PERIOD_LOCKED,
            ),
            (
                Error::RetentionViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::RETENTION_VIOLATION,
            ),
            (
                Error::LegalHoldActive {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::LEGAL_HOLD_ACTIVE,
            ),
            (
                Error::StateTransitionViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::STATE_TRANSITION_VIOLATION,
            ),
            (
                Error::TransitionCheckViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::TRANSITION_CHECK_VIOLATION,
            ),
            (
                Error::TypeGuardViolation {
                    collection: "c".into(),
                    detail: "d".into(),
                },
                sqlstate::TYPE_GUARD_VIOLATION,
            ),
            (
                Error::TypeMismatch {
                    collection: "c".into(),
                    key: "k".into(),
                    detail: "d".into(),
                },
                sqlstate::DATATYPE_MISMATCH,
            ),
            (
                Error::MirrorReadOnly {
                    database: "db".into(),
                },
                sqlstate::READ_ONLY_SQL_TRANSACTION,
            ),
            (
                // Retryable in the lease paths, so the fallback must not read
                // as a plan/syntax error now that it answers through `classify`.
                Error::RetryableSchemaChanged {
                    descriptor: "users".into(),
                },
                sqlstate::SERIALIZATION_FAILURE,
            ),
        ];

        for (err, expected) in cases {
            let (_severity, state, _message) = error_to_sqlstate(&err);
            assert_eq!(state, expected, "{err:?}");
        }
    }
}
