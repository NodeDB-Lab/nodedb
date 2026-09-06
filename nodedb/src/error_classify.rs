// SPDX-License-Identifier: BUSL-1.1

//! The one internal [`Error`] to public [`NodeDbError`] mapping table. Takes
//! `&Error` since both `From<Error>` (consuming) and the native protocol's
//! error frames (borrowed) need it — a second table would drift, and a
//! variant missing from it reaches the client as an indistinguishable NDB-9000.

use nodedb_types::error::NodeDbError;

use crate::error::Error;

/// Classify an internal error into the public error a client can match on.
pub(crate) fn classify(e: &Error) -> NodeDbError {
    match e {
        Error::RejectedConstraint {
            collection, detail, ..
        } => NodeDbError::constraint_violation(collection.clone(), detail),
        Error::RejectedAuthz { resource, .. } => {
            NodeDbError::authorization_denied(resource.clone())
        }
        Error::TxnOverlayMemoryExceeded { .. } => NodeDbError::bad_request(e.to_string()),
        Error::BackupTenantMismatch { expected, actual } => {
            NodeDbError::backup_tenant_mismatch(*expected, *actual)
        }
        Error::BackupKeyMismatch => NodeDbError::backup_key_mismatch(),
        Error::OffsetRegression { .. } => NodeDbError::bad_request(e.to_string()),
        Error::DeadlineExceeded { .. } => NodeDbError::deadline_exceeded(),
        // Same contract as a write conflict, which callers already retry.
        Error::RetryableRefusal { reason } => NodeDbError::write_conflict("crdt", reason.clone()),
        Error::ConflictRetry {
            collection,
            document_id,
        } => NodeDbError::write_conflict(collection.clone(), document_id.clone()),
        Error::CalvinSerializationConflict => NodeDbError::write_conflict(
            "cross-shard",
            "global OCC verdict was abort (read-set validation failed)",
        ),
        // Not a write conflict: no read-set was validated, a participant failed.
        Error::CalvinParticipantError => NodeDbError::cluster(
            "cross-shard transaction aborted: a participant vShard returned an error before \
             any read-set was validated",
        ),
        Error::SourceFrozen { database_id } => NodeDbError::write_conflict(
            format!("database:{database_id}"),
            "source database is frozen for clone materialization; retry shortly".to_owned(),
        ),
        Error::RejectedPrevalidation { constraint, reason } => {
            NodeDbError::prevalidation_rejected(constraint.clone(), reason)
        }
        Error::AppendOnlyViolation {
            collection, detail, ..
        } => NodeDbError::append_only_violation(collection.clone(), detail),
        Error::BalanceViolation {
            collection, detail, ..
        } => NodeDbError::balance_violation(collection.clone(), detail),
        // Breaks the balance invariant, so it carries the target as the
        // offending collection.
        Error::MaterializedSumTargetNotFound {
            target_collection,
            join_column,
            join_value,
        } => NodeDbError::balance_violation(
            target_collection.clone(),
            format!(
                "no row with primary key '{join_value}', referenced by join column \
                 '{join_column}'"
            ),
        ),
        // Not a balance violation: the plan and fold disagreed about which
        // rows participate — a system defect, not a missing row.
        Error::MaterializedSumResolutionMissing { .. } => NodeDbError::internal(e.to_string()),
        Error::PeriodLocked {
            collection, detail, ..
        } => NodeDbError::period_locked(collection.clone(), detail),
        Error::RetentionViolation {
            collection, detail, ..
        } => NodeDbError::retention_violation(collection.clone(), detail),
        Error::LegalHoldActive {
            collection, detail, ..
        } => NodeDbError::legal_hold_active(collection.clone(), detail),
        Error::StateTransitionViolation {
            collection, detail, ..
        } => NodeDbError::state_transition_violation(collection.clone(), detail),
        Error::TransitionCheckViolation {
            collection, detail, ..
        } => NodeDbError::transition_check_violation(collection.clone(), detail),
        Error::TypeGuardViolation {
            collection, detail, ..
        } => NodeDbError::type_guard_violation(collection.clone(), detail),
        Error::TypeMismatch {
            collection, detail, ..
        } => NodeDbError::type_mismatch(collection.clone(), detail),
        Error::OverflowError { collection, key } => {
            NodeDbError::overflow(collection.clone(), format!("key {key}"))
        }
        Error::InsufficientBalance {
            collection,
            key,
            detail,
        } => NodeDbError::insufficient_balance(collection.clone(), format!("key {key}: {detail}")),
        Error::RateExceeded { gate, detail, .. } => {
            NodeDbError::rate_exceeded(gate.clone(), detail)
        }

        Error::CollectionNotFound { collection, .. } => {
            NodeDbError::collection_not_found(collection.clone())
        }
        Error::DocumentNotFound {
            collection,
            document_id,
        } => NodeDbError::document_not_found(collection.clone(), document_id.clone()),
        Error::CollectionDeactivated {
            collection,
            retention_expires_at_ns,
            ..
        } => NodeDbError::collection_deactivated(collection.clone(), *retention_expires_at_ns),

        Error::VShardAdmissionCapacityExceeded {
            vshard_id,
            capacity,
        } => NodeDbError::rate_exceeded(
            "vshard_admission",
            format!("vshard {vshard_id} admission queue is full (capacity {capacity})"),
        ),
        Error::CrdtAdmissionRetriesExhausted { .. } => {
            NodeDbError::write_conflict("crdt", "CRDT frontier changed repeatedly; retry the write")
        }
        Error::CrdtAdmissionInvalidPlan { .. }
        | Error::CrdtAdmissionCallerFence
        | Error::CrdtApplyRequiresAdmission
        | Error::CrdtApplyForbiddenInTransaction => {
            NodeDbError::bad_request("invalid CRDT admission request".to_owned())
        }
        Error::CrdtAdmissionTimeout { .. } => NodeDbError::deadline_exceeded(),
        Error::NoLeader { vshard_id } => {
            NodeDbError::no_leader(format!("vshard {vshard_id} has no serving leader"))
        }
        Error::NotLeader { leader_addr, .. } => NodeDbError::not_leader(leader_addr.clone()),
        Error::FanOutExceeded {
            shards_touched,
            limit,
        } => NodeDbError::fan_out_exceeded(*shards_touched, *limit),
        Error::CrossCollectionNotColocated { .. } => NodeDbError::bad_request(e.to_string()),

        Error::BadRequest { detail } => NodeDbError::bad_request(detail),
        Error::QuotaOvercommit { field, detail } => {
            NodeDbError::quota_overcommit(field.clone(), detail)
        }
        Error::PlanError { detail } => NodeDbError::plan_error(detail),
        Error::UndefinedFunction { name } => NodeDbError::undefined_function(name.clone()),
        Error::NotNullViolation { table, column } => {
            NodeDbError::not_null_violation(table.clone(), column.clone())
        }
        Error::DivisionByZero => NodeDbError::division_by_zero(),
        Error::InvalidLimitValue { clause, value } => {
            NodeDbError::invalid_limit_value(*clause, value.clone())
        }
        Error::RetryableSchemaChanged { descriptor } => {
            NodeDbError::plan_error(format!("retryable schema change on {descriptor}"))
        }
        Error::RetryableLeaderChange {
            group_id,
            log_index,
        } => NodeDbError::dispatch(format!(
            "raft leader change overwrote entry at group {group_id} index {log_index}; retry exhausted"
        )),
        Error::MetadataLeaderUnavailable => NodeDbError::dispatch(
            "metadata raft group has no elected leader yet; retry exhausted".to_string(),
        ),
        Error::ExecutionLimitExceeded { detail } => NodeDbError::bad_request(detail),
        Error::LimitExceeded {
            limit_name,
            value,
            max,
        } => NodeDbError::bad_request(format!("{limit_name} = {value} exceeds server cap {max}")),

        Error::Wal(wal_err) => NodeDbError::wal(wal_err),
        Error::Dispatch { detail } => NodeDbError::dispatch(detail),
        Error::Storage { detail, .. } => NodeDbError::storage(detail),
        Error::ColdStorage { detail } => NodeDbError::cold_storage(detail),
        Error::Serialization { format, detail } => {
            NodeDbError::serialization(format.clone(), detail)
        }
        Error::Codec { detail } => NodeDbError::codec(detail),
        Error::SegmentCorrupted { detail } => NodeDbError::segment_corrupted(detail),
        Error::MemoryExhausted { engine } => NodeDbError::memory_exhausted(engine.clone()),
        Error::Backpressure { engine } => NodeDbError::memory_exhausted(engine.to_string()),
        Error::Crdt(crdt_err) => NodeDbError::internal(crdt_err),
        Error::Io(io_err) => NodeDbError::storage(io_err),
        Error::Config { detail } => NodeDbError::config(detail),
        Error::Encryption { detail } => NodeDbError::encryption(detail),
        Error::Bridge { detail } => NodeDbError::bridge(detail),
        Error::VersionCompat { detail } => NodeDbError::cluster(detail),
        Error::Internal { detail } => NodeDbError::internal(detail),
        Error::RemoteTyped { code, message } => NodeDbError::remote_typed(*code, message.clone()),
        Error::DescriptorVersionAnomaly { .. } => NodeDbError::internal(e.to_string()),
        Error::CatalogIntegrityViolation { .. } => NodeDbError::internal(e.to_string()),
        Error::CollectionPurgeRowMissing { .. } => NodeDbError::internal(e.to_string()),
        Error::Promql(promql_err) => NodeDbError::bad_request(promql_err.to_string()),
        Error::DependentObjectsExist {
            tenant_id: _,
            root_kind,
            root_name,
            dependent_count,
            dependents,
        } => {
            let names: Vec<String> = dependents.iter().map(|(k, n)| format!("{k}:{n}")).collect();
            NodeDbError::bad_request(format!(
                "cannot drop {root_kind} '{root_name}': {dependent_count} dependent(s) exist ({})",
                names.join(", ")
            ))
        }
        Error::CascadeCycle {
            tenant_id: _,
            root,
            depth,
        } => NodeDbError::internal(format!(
            "cascade cycle / depth-limit ({depth}) exceeded on '{root}'"
        )),
        Error::CrossShardInExplicitTransaction => NodeDbError::bad_request(
            "cross-shard write inside explicit transaction block is not supported. \
             Calvin cross-shard atomicity requires auto-commit (single-statement). \
             Options: 1) Remove BEGIN/COMMIT to use auto-commit. \
             2) SET cross_shard_txn = 'best_effort_non_atomic' for non-atomic dispatch."
                .to_owned(),
        ),
        Error::SequencerUnavailable => NodeDbError::bad_request(
            "cross-shard transactions require a cluster deployment with the Calvin sequencer; \
             this node is running in embedded/local mode"
                .to_owned(),
        ),
        Error::OllpExhausted { retries, cause } => NodeDbError::bad_request(format!(
            "optimistic retry gave up after {retries} attempts: {cause}"
        )),
        Error::SessionCapExceeded { cap } => NodeDbError::bad_request(format!(
            "session cap ({cap}) exceeded — rejecting new login"
        )),
        Error::TenantVectorDimExceeded { dim, limit } => {
            NodeDbError::tenant_vector_dim_exceeded(*dim, *limit)
        }
        Error::TenantGraphDepthExceeded { depth, limit } => {
            NodeDbError::tenant_graph_depth_exceeded(*depth, *limit)
        }
        Error::RoleInheritanceCycle { child, parent } => NodeDbError::bad_request(format!(
            "role inheritance cycle: granting '{parent}' as parent of '{child}' would create a cycle"
        )),
        Error::RoleInheritanceDepthExceeded { depth, limit } => NodeDbError::bad_request(format!(
            "role inheritance depth {depth} exceeds the maximum allowed depth of {limit}"
        )),
        Error::CloneWriteRequiresMaterialize {
            collection,
            engine,
            database,
            reason,
        } => NodeDbError::clone_write_requires_materialize(
            collection.clone(),
            engine.clone(),
            database.clone(),
            reason,
        ),
        Error::MirrorReadOnly { database } => NodeDbError::mirror_read_only(database.clone()),
        Error::StaleReadNotLeader {
            database,
            source_cluster,
            ..
        } => NodeDbError::stale_read_not_leader(database.clone(), source_cluster.clone()),

        Error::SessionIdleTimeout => {
            NodeDbError::bad_request("session terminated: idle timeout exceeded".to_owned())
        }
        Error::SessionTokenExpired => {
            NodeDbError::bad_request("session terminated: OIDC token expired".to_owned())
        }
        Error::SessionKilledByAdmin => {
            NodeDbError::bad_request("session terminated by administrator".to_owned())
        }
        Error::SessionUserDropped => {
            NodeDbError::bad_request("session terminated: user account dropped".to_owned())
        }
        Error::OidcProviderTenantUnbound => NodeDbError::bad_request(
            "OIDC: authenticated provider has no tenant binding".to_owned(),
        ),
        Error::OidcProviderTenantUnavailable { .. } => NodeDbError::bad_request(
            "OIDC: authenticated provider tenant is unavailable".to_owned(),
        ),
        Error::OidcNoDefaultDatabase { sub } => NodeDbError::bad_request(format!(
            "OIDC: no default database resolved for sub '{sub}'"
        )),
        // Exhaustive by construction: a code falling through to `internal`
        // reaches the client as an indistinguishable NDB-9000.
        Error::DataPlane(code) => {
            crate::error_from_data_plane::data_plane_code_to_public(code.clone())
        }
    }
}

/// True when the error carries neither a client-matchable classification from
/// [`classify`] nor a retry contract a caller matches by variant. Only these
/// may be re-wrapped in a transport error.
pub(crate) fn is_unclassified_failure(e: &Error) -> bool {
    matches!(
        e,
        Error::Wal(_)
            | Error::Dispatch { .. }
            | Error::Storage { .. }
            | Error::ColdStorage { .. }
            | Error::Serialization { .. }
            | Error::Codec { .. }
            | Error::SegmentCorrupted { .. }
            | Error::Crdt(_)
            | Error::Io(_)
            | Error::Config { .. }
            | Error::Encryption { .. }
            | Error::Bridge { .. }
            | Error::VersionCompat { .. }
            | Error::Internal { .. }
            | Error::DescriptorVersionAnomaly { .. }
            | Error::CatalogIntegrityViolation { .. }
            | Error::CollectionPurgeRowMissing { .. }
            | Error::MaterializedSumResolutionMissing { .. }
            | Error::CascadeCycle { .. }
    )
}

#[cfg(test)]
mod tests {
    use nodedb_types::error::ErrorCode;

    use super::*;
    use crate::types::TenantId;

    /// Control-Plane classification must be readable without consuming the
    /// error — the native protocol renders a borrowed `&Error` and would
    /// otherwise have nothing but `XX000` to send.
    #[test]
    fn borrowed_classification_matches_the_consuming_conversion() {
        let err = Error::CollectionNotFound {
            tenant_id: TenantId::new(0),
            collection: "users".to_owned(),
        };
        let borrowed = classify(&err);
        let owned: NodeDbError = err.into();

        assert_eq!(borrowed.code(), ErrorCode::COLLECTION_NOT_FOUND);
        assert_eq!(borrowed.code(), owned.code());
        assert_eq!(borrowed.message(), owned.message());
        assert!(borrowed.is_not_found());
    }

    /// A borrowed classification leaves the error intact for the caller to
    /// keep rendering (the native path also formats its message from it).
    #[test]
    fn classification_does_not_consume_the_error() {
        let err = Error::RejectedAuthz {
            tenant_id: TenantId::new(7),
            resource: "secret_vault".to_owned(),
        };
        assert!(classify(&err).is_auth_denied());
        assert!(classify(&err).is_auth_denied());
        assert!(err.to_string().contains("secret_vault"));
    }

    /// Verdicts the state machine reached must never be re-wrapped as
    /// transport failures; machinery failures may be.
    #[test]
    fn only_machinery_failures_are_unclassified() {
        assert!(!super::is_unclassified_failure(
            &Error::RejectedConstraint {
                collection: "docs".to_owned(),
                constraint: "unique".to_owned(),
                detail: "duplicate key value 'dup'".to_owned(),
            }
        ));
        assert!(!super::is_unclassified_failure(&Error::RejectedAuthz {
            tenant_id: TenantId::new(1),
            resource: "docs".to_owned(),
        }));
        assert!(super::is_unclassified_failure(&Error::Internal {
            detail: "apply error".to_owned(),
        }));
    }
}
