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
            collection,
            constraint,
            detail,
        } => crate::error_from_data_plane::rejected_constraint_to_public(
            collection.clone(),
            constraint.clone(),
            detail.clone(),
        ),
        Error::RejectedAuthz { resource, .. } => {
            NodeDbError::authorization_denied(resource.clone())
        }
        // `54000` on the SQL surfaces, the class `TxnOverlayMemoryExceeded`
        // has as a Data-Plane code.
        Error::TxnOverlayMemoryExceeded { .. } => {
            NodeDbError::program_limit_exceeded(e.to_string())
        }
        Error::BackupTenantMismatch { expected, actual } => {
            NodeDbError::backup_tenant_mismatch(*expected, *actual)
        }
        Error::BackupKeyMismatch => NodeDbError::backup_key_mismatch(),
        // Class `22`, the class of the `22023` SQLSTATE the SQL surfaces
        // render for a regressed offset.
        Error::OffsetRegression { .. } => NodeDbError::data_exception(e.to_string()),
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
        // Not a write conflict: no read-set was validated, a participant
        // failed. `40000` keeps the retryable class 40 across a node hop.
        Error::CalvinParticipantError => NodeDbError::transaction_rollback(
            "cross-shard transaction aborted: a participant vShard returned an error before \
             any read-set was validated",
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
        Error::PeriodLockMisconfigured {
            collection,
            ref_table,
            status_column,
            row_identity,
        } => NodeDbError::period_lock_misconfigured(
            collection.clone(),
            ref_table.clone(),
            status_column.clone(),
            row_identity,
        ),
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
        | Error::CrdtApplyRequiresAdmission => {
            NodeDbError::bad_request("invalid CRDT admission request".to_owned())
        }
        // `25001`: the statement cannot run inside a transaction block.
        Error::CrdtApplyForbiddenInTransaction
        | Error::NotInTransactionBlock { .. }
        | Error::CrossShardInExplicitTransaction => {
            NodeDbError::active_sql_transaction(e.to_string())
        }
        Error::CrdtAdmissionTimeout { .. } => NodeDbError::deadline_exceeded(),
        Error::NoLeader { vshard_id } => {
            NodeDbError::no_leader(format!("vshard {vshard_id} has no serving leader"))
        }
        Error::NotLeader { leader_addr, .. } => NodeDbError::not_leader(leader_addr.clone()),
        // `0A000` on the SQL surfaces, the class `Unsupported` has as a
        // Data-Plane code.
        Error::CrossCollectionNotColocated { .. } => NodeDbError::from_wire(
            nodedb_types::error::ErrorCode::SQL_NOT_ENABLED,
            e.to_string(),
        ),

        Error::BadRequest { detail } => NodeDbError::bad_request(detail),
        Error::QuotaOvercommit { field, detail } => {
            NodeDbError::quota_overcommit(field.clone(), detail)
        }
        Error::PlanError { detail } => NodeDbError::plan_error(detail),
        // `0A000` on every surface, the class `Unsupported` has as a
        // Data-Plane code.
        Error::FeatureNotSupported { detail } => {
            NodeDbError::from_wire(nodedb_types::error::ErrorCode::SQL_NOT_ENABLED, detail)
        }
        Error::UndefinedFunction { name } => NodeDbError::undefined_function(name.clone()),
        Error::UndefinedObject { kind, name } => {
            NodeDbError::undefined_object(format!("{kind} \"{name}\""))
        }
        Error::ObjectNotInPrerequisiteState { object, detail } => {
            NodeDbError::object_not_ready(object.clone(), detail.clone())
        }
        Error::UndefinedColumn { column } => NodeDbError::undefined_column(column.clone()),
        Error::TextColumn {
            collection,
            column,
            fault,
        } => crate::error_from_data_plane::text_column_to_public(collection, column, fault),
        Error::AmbiguousColumn { column } => NodeDbError::ambiguous_column(column.clone()),
        Error::UnknownStrictField { column, .. } => NodeDbError::undefined_column(column.clone()),
        Error::DivisionByZero => NodeDbError::division_by_zero(),
        Error::DataException { detail } => NodeDbError::data_exception(detail.clone()),
        Error::InvalidLimitValue { clause, value } => {
            NodeDbError::invalid_limit_value(*clause, value.clone())
        }
        // Same contract as a write conflict, which callers already retry.
        Error::RetryableSchemaChanged { descriptor } => NodeDbError::write_conflict(
            descriptor.clone(),
            "schema changed during execution; retry the statement".to_owned(),
        ),
        Error::RetryableLeaderChange {
            group_id,
            log_index,
        } => NodeDbError::dispatch(format!(
            "raft leader change overwrote entry at group {group_id} index {log_index}; retry exhausted"
        )),
        // Internal class `XX`: no driver or pool treats it as transient, so
        // nothing re-proposes a write that already committed.
        Error::CommittedResultUnavailable { .. } | Error::ProposalOutcomeUnknown { .. } => {
            NodeDbError::internal(e.to_string())
        }
        Error::MetadataLeaderUnavailable => NodeDbError::dispatch(
            "metadata raft group has no elected leader yet; retry exhausted".to_string(),
        ),
        // The code renders `55P03`, the SQLSTATE pgwire gives the variant, so
        // the class survives a node hop as a bare numeric code.
        Error::AuthorizationStateBehind { .. } | Error::LinearizableReadRefused { .. } => {
            NodeDbError::from_wire(
                nodedb_types::error::ErrorCode::STALE_READ_NOT_LEADER,
                e.to_string(),
            )
        }
        // The code renders `55P03`, the SQLSTATE pgwire gives both variants.
        // Nothing was applied, and a retry succeeds once a majority answers.
        Error::GroupQuorumUnavailable { .. }
        | Error::GroupMarksUnavailable { .. }
        | Error::BackupCaptureMoved { .. } => {
            NodeDbError::from_wire(nodedb_types::error::ErrorCode::NO_LEADER, e.to_string())
        }
        Error::ExecutionLimitExceeded { detail } => NodeDbError::bad_request(detail),
        Error::LimitExceeded {
            limit_name,
            value,
            max,
        } => NodeDbError::bad_request(format!("{limit_name} = {value} exceeds server cap {max}")),

        Error::Wal(wal_err) => NodeDbError::wal(wal_err),
        Error::Dispatch { detail } => NodeDbError::dispatch(detail),
        // A capacity refusal enqueued nothing, so it is the retryable overload class.
        Error::DispatchCapacity { .. } => NodeDbError::server_overload(e),
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
        Error::RestoreTargetNotEmpty { .. } | Error::RestoreVerificationFailed { .. } => {
            NodeDbError::storage(e.to_string())
        }
        Error::Internal { detail } => NodeDbError::internal(detail),
        Error::Shaping(e) => (**e).clone(),
        Error::RemoteTyped { code, message } => NodeDbError::remote_typed(*code, message.clone()),
        Error::DescriptorVersionAnomaly { .. } => NodeDbError::internal(e.to_string()),
        Error::CatalogIntegrityViolation { .. } => NodeDbError::internal(e.to_string()),
        Error::CollectionPurgeRowMissing { .. } => NodeDbError::internal(e.to_string()),
        Error::CollectionUnstamped { .. } => NodeDbError::internal(e.to_string()),
        Error::Promql(promql_err) => NodeDbError::bad_request(promql_err.to_string()),
        Error::DependentObjectsExist {
            tenant_id: _,
            root_kind,
            root_name,
            dependent_count,
            dependents,
        } => {
            let (object, message) =
                dependent_objects_text(root_kind, root_name, *dependent_count, dependents);
            NodeDbError::dependent_objects_exist(object, message)
        }
        Error::Ddl(ddl) => {
            let public = NodeDbError::from_wire_with_details(
                ddl.code,
                ddl.message.clone(),
                ddl.details.as_deref().cloned(),
            );
            match &ddl.cause {
                Some(cause) => public.with_cause((**cause).clone()),
                None => public,
            }
        }
        Error::RoleInUse { role, .. } => {
            NodeDbError::dependent_objects_exist(format!("role \"{role}\""), e.to_string())
        }
        Error::CascadeCycle {
            tenant_id: _,
            root,
            depth,
        } => NodeDbError::internal(format!(
            "cascade cycle / depth-limit ({depth}) exceeded on '{root}'"
        )),
        Error::SequencerUnavailable => NodeDbError::bad_request(
            "the Calvin sequencer is not running on this node; \
             cross-shard transactions are refused until cluster startup completes"
                .to_owned(),
        ),
        // The code follows the cause, the same way pgwire picks the SQLSTATE,
        // so the class survives a node hop as a bare numeric code.
        Error::OllpExhausted { retries, cause } => {
            let message = format!("optimistic retry gave up after {retries} attempts: {cause}");
            let code = match cause {
                crate::OllpExhaustedCause::PredicateDrift => {
                    nodedb_types::error::ErrorCode::WRITE_CONFLICT
                }
                crate::OllpExhaustedCause::PreAdmission(inner) => classify(inner).code(),
                crate::OllpExhaustedCause::AdmissionRefused { .. } => {
                    nodedb_types::error::ErrorCode::RATE_EXCEEDED
                }
            };
            NodeDbError::from_wire(code, message)
        }
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
            NodeDbError::auth_expired("session closed: OIDC token expired")
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
        Error::ExternalRoleUndefined {
            subject,
            role,
            tenant_id,
        } => NodeDbError::bad_request(format!(
            "external identity '{subject}' claims role \"{role}\", which is not defined in \
             tenant {tenant_id}"
        )),
        // Exhaustive by construction: a code falling through to `internal`
        // reaches the client as an indistinguishable NDB-9000.
        Error::DataPlane(code) => {
            crate::error_from_data_plane::data_plane_code_to_public(code.clone())
        }
    }
}

/// The object and the message of a DROP refused for its dependents. The
/// public error and the Data-Plane code both read it, so both name one
/// object with one message.
pub(crate) fn dependent_objects_text(
    root_kind: &str,
    root_name: &str,
    dependent_count: usize,
    dependents: &[(String, String)],
) -> (String, String) {
    let names: Vec<String> = dependents
        .iter()
        .map(|(kind, name)| format!("{kind}:{name}"))
        .collect();
    (
        format!("{root_kind} '{root_name}'"),
        format!(
            "cannot drop {root_kind} '{root_name}': {dependent_count} dependent(s) exist ({})",
            names.join(", ")
        ),
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

    /// Retry exhaustion carries the code of its cause: drift is a write
    /// conflict, a refused gate is a rate refusal, and a pre-admission
    /// failure keeps the class of the error that caused it.
    #[test]
    fn ollp_exhaustion_takes_the_code_of_its_cause() {
        let drift = Error::OllpExhausted {
            retries: 3,
            cause: crate::OllpExhaustedCause::PredicateDrift,
        };
        assert_eq!(classify(&drift).code(), ErrorCode::WRITE_CONFLICT);

        let refused = Error::OllpExhausted {
            retries: 3,
            cause: crate::OllpExhaustedCause::AdmissionRefused {
                detail: "circuit open".to_owned(),
            },
        };
        assert_eq!(classify(&refused).code(), ErrorCode::RATE_EXCEEDED);

        let pre_admission = Error::OllpExhausted {
            retries: 3,
            cause: crate::OllpExhaustedCause::PreAdmission(Box::new(Error::CollectionNotFound {
                tenant_id: TenantId::new(1),
                collection: "orders".to_owned(),
            })),
        };
        let public = classify(&pre_admission);
        assert_eq!(public.code(), ErrorCode::COLLECTION_NOT_FOUND);
        assert!(public.message().contains("orders"));
    }

    /// A retryable cluster refusal carries the code whose SQLSTATE class is
    /// the one pgwire renders, so the class survives a node hop.
    #[test]
    fn retryable_cluster_refusals_carry_the_leader_class_code() {
        let quorum = Error::GroupQuorumUnavailable {
            group_id: 1,
            voters: vec![1, 2, 3],
            unreachable: vec![2, 3],
        };
        assert_eq!(classify(&quorum).code(), ErrorCode::NO_LEADER);
        let marks = Error::GroupMarksUnavailable {
            group_id: 1,
            refused_by: vec![2],
        };
        assert_eq!(classify(&marks).code(), ErrorCode::NO_LEADER);
        let behind = Error::AuthorizationStateBehind {
            detail: "roles".to_owned(),
        };
        assert_eq!(classify(&behind).code(), ErrorCode::STALE_READ_NOT_LEADER);
        let regression = Error::OffsetRegression {
            stream: "s".to_owned(),
            group: "g".to_owned(),
            partition_id: 0,
            offsets: Box::new(crate::error::RegressedOffsets {
                current: crate::event::cdc::CdcOffset {
                    epoch: 1,
                    index: 2,
                    sequence: 2,
                },
                attempted: crate::event::cdc::CdcOffset {
                    epoch: 1,
                    index: 1,
                    sequence: 1,
                },
            }),
        };
        assert_eq!(classify(&regression).code(), ErrorCode::DATA_EXCEPTION);
    }
}
