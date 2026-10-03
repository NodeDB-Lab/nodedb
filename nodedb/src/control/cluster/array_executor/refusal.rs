// SPDX-License-Identifier: BUSL-1.1

//! The cluster error an array shard answers with when its Data Plane refuses.
//!
//! A coded refusal crosses as `ClusterError::DataPlane`, so the coordinator
//! rebuilds `crate::Error::DataPlane(code)` and renders the SQLSTATE a
//! single-node execution renders. Only a refusal with no code is a storage
//! error. A local-execution error crosses in its typed wire form and keeps
//! its class.

use nodedb_cluster::error::ClusterError;
use nodedb_cluster::rpc_codec::DataPlaneErrorCode;

use crate::bridge::envelope::Response;

/// The cluster error for a Data-Plane response with an error status.
pub(super) fn refusal_error(context: &str, response: &Response) -> ClusterError {
    match response.error_code.as_deref() {
        Some(code) => ClusterError::DataPlane {
            code: code.clone().into(),
        },
        None => ClusterError::Storage {
            detail: format!("{context}: data plane returned an error status with no error code"),
        },
    }
}

/// The cluster error for a local-execution error.
///
/// - A Data-Plane verdict keeps its code.
/// - A deadline and a capacity refusal cross as their Data-Plane verdicts.
/// - A missing leader crosses as `WrongOwner`, so the coordinator re-reads
///   its routing and retries.
/// - Every other error crosses as `ShardExecution` with its typed wire form.
///   `context` goes before its message in the log detail.
pub(super) fn execution_error(context: &str, error: crate::Error) -> ClusterError {
    match error {
        crate::Error::DataPlane(code) => ClusterError::DataPlane { code: code.into() },
        crate::Error::DeadlineExceeded { .. } => ClusterError::DataPlane {
            code: DataPlaneErrorCode::DeadlineExceeded,
        },
        capacity @ crate::Error::DispatchCapacity { .. } => ClusterError::DataPlane {
            code: DataPlaneErrorCode::DispatchCapacity {
                reason: capacity.to_string(),
            },
        },
        crate::Error::NotLeader {
            vshard_id,
            leader_node,
            ..
        } => ClusterError::WrongOwner {
            vshard_id: vshard_id.as_u32(),
            expected_owner_node: (leader_node != 0).then_some(leader_node),
        },
        crate::Error::NoLeader { vshard_id } => ClusterError::WrongOwner {
            vshard_id: vshard_id.as_u32(),
            expected_owner_node: None,
        },
        // Every other error crosses in its typed wire form, so the
        // coordinator rebuilds it and renders its own SQLSTATE.
        other @ (crate::Error::RejectedConstraint { .. }
        | crate::Error::TxnOverlayMemoryExceeded { .. }
        | crate::Error::RejectedAuthz { .. }
        | crate::Error::OffsetRegression { .. }
        | crate::Error::ConflictRetry { .. }
        | crate::Error::CalvinSerializationConflict
        | crate::Error::CalvinParticipantError
        | crate::Error::RejectedPrevalidation { .. }
        | crate::Error::RetryableRefusal { .. }
        | crate::Error::AppendOnlyViolation { .. }
        | crate::Error::BalanceViolation { .. }
        | crate::Error::MaterializedSumTargetNotFound { .. }
        | crate::Error::MaterializedSumResolutionMissing { .. }
        | crate::Error::PeriodLocked { .. }
        | crate::Error::PeriodLockMisconfigured { .. }
        | crate::Error::RetentionViolation { .. }
        | crate::Error::LegalHoldActive { .. }
        | crate::Error::StateTransitionViolation { .. }
        | crate::Error::TransitionCheckViolation { .. }
        | crate::Error::TypeGuardViolation { .. }
        | crate::Error::TypeMismatch { .. }
        | crate::Error::InsufficientBalance { .. }
        | crate::Error::RateExceeded { .. }
        | crate::Error::CollectionNotFound { .. }
        | crate::Error::DocumentNotFound { .. }
        | crate::Error::CollectionDeactivated { .. }
        | crate::Error::VShardAdmissionCapacityExceeded { .. }
        | crate::Error::CrdtAdmissionRetriesExhausted { .. }
        | crate::Error::CrdtAdmissionInvalidPlan { .. }
        | crate::Error::CrdtAdmissionCallerFence
        | crate::Error::CrdtApplyRequiresAdmission
        | crate::Error::CrdtApplyForbiddenInTransaction
        | crate::Error::NotInTransactionBlock { .. }
        | crate::Error::CrdtAdmissionTimeout { .. }
        | crate::Error::CrossCollectionNotColocated { .. }
        | crate::Error::CloneWriteRequiresMaterialize { .. }
        | crate::Error::BadRequest { .. }
        | crate::Error::BackupTenantMismatch { .. }
        | crate::Error::BackupKeyMismatch
        | crate::Error::QuotaOvercommit { .. }
        | crate::Error::PlanError { .. }
        | crate::Error::FeatureNotSupported { .. }
        | crate::Error::UndefinedFunction { .. }
        | crate::Error::UndefinedObject { .. }
        | crate::Error::ObjectNotInPrerequisiteState { .. }
        | crate::Error::UndefinedColumn { .. }
        | crate::Error::TextColumn { .. }
        | crate::Error::AmbiguousColumn { .. }
        | crate::Error::UnknownStrictField { .. }
        | crate::Error::DivisionByZero
        | crate::Error::DataException { .. }
        | crate::Error::InvalidLimitValue { .. }
        | crate::Error::RetryableSchemaChanged { .. }
        | crate::Error::RetryableLeaderChange { .. }
        | crate::Error::CommittedResultUnavailable { .. }
        | crate::Error::ProposalOutcomeUnknown { .. }
        | crate::Error::GroupQuorumUnavailable { .. }
        | crate::Error::GroupMarksUnavailable { .. }
        | crate::Error::BackupCaptureMoved { .. }
        | crate::Error::MetadataLeaderUnavailable
        | crate::Error::AuthorizationStateBehind { .. }
        | crate::Error::LinearizableReadRefused { .. }
        | crate::Error::ExecutionLimitExceeded { .. }
        | crate::Error::LimitExceeded { .. }
        | crate::Error::Wal(_)
        | crate::Error::Dispatch { .. }
        | crate::Error::Storage { .. }
        | crate::Error::ColdStorage { .. }
        | crate::Error::Serialization { .. }
        | crate::Error::Codec { .. }
        | crate::Error::SegmentCorrupted { .. }
        | crate::Error::MemoryExhausted { .. }
        | crate::Error::Backpressure { .. }
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
        | crate::Error::Ddl(_)
        | crate::Error::RemoteTyped { .. }
        | crate::Error::DescriptorVersionAnomaly { .. }
        | crate::Error::CollectionPurgeRowMissing { .. }
        | crate::Error::CollectionUnstamped { .. }
        | crate::Error::CatalogIntegrityViolation { .. }
        | crate::Error::Promql(_)
        | crate::Error::DependentObjectsExist { .. }
        | crate::Error::RoleInUse { .. }
        | crate::Error::CascadeCycle { .. }
        | crate::Error::CrossShardInExplicitTransaction
        | crate::Error::SequencerUnavailable
        | crate::Error::SessionCapExceeded { .. }
        | crate::Error::SessionIdleTimeout
        | crate::Error::SessionTokenExpired
        | crate::Error::SessionKilledByAdmin
        | crate::Error::SessionUserDropped
        | crate::Error::OidcProviderTenantUnbound
        | crate::Error::OidcProviderTenantUnavailable { .. }
        | crate::Error::ExternalRoleUndefined { .. }
        | crate::Error::OidcNoDefaultDatabase { .. }
        | crate::Error::TenantVectorDimExceeded { .. }
        | crate::Error::TenantGraphDepthExceeded { .. }
        | crate::Error::RoleInheritanceCycle { .. }
        | crate::Error::RoleInheritanceDepthExceeded { .. }
        | crate::Error::OllpExhausted { .. }
        | crate::Error::MirrorReadOnly { .. }
        | crate::Error::StaleReadNotLeader { .. }) => {
            let detail = format!("{context}: {other}");
            ClusterError::ShardExecution {
                error: Box::new(
                    crate::control::cluster::data_plane_error_wire::execution_error_to_typed(other),
                ),
                detail,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{ErrorCode, Payload, Status};
    use crate::types::{Lsn, RequestId};

    fn refusal(code: Option<ErrorCode>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: code.map(Box::new),
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn unsupported() -> ErrorCode {
        ErrorCode::Unsupported {
            detail: "not on this engine".into(),
        }
    }

    /// A coded refusal keeps its code through the cluster error and back to
    /// the coordinator's typed error.
    #[test]
    fn a_coded_refusal_keeps_its_code() {
        match refusal_error("array slice", &refusal(Some(unsupported()))) {
            ClusterError::DataPlane { code } => {
                assert_eq!(ErrorCode::from(code), unsupported());
            }
            other => panic!("expected the typed refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_with_no_code_is_a_storage_error() {
        match refusal_error("array slice", &refusal(None)) {
            ClusterError::Storage { detail } => assert!(detail.starts_with("array slice: ")),
            other => panic!("expected a storage error, got {other:?}"),
        }
    }

    #[test]
    fn a_local_deadline_crosses_as_the_deadline_verdict() {
        let error = crate::Error::DeadlineExceeded {
            request_id: RequestId::new(1),
        };
        assert!(matches!(
            execution_error("array put", error),
            ClusterError::DataPlane {
                code: DataPlaneErrorCode::DeadlineExceeded
            }
        ));
    }

    #[test]
    fn a_missing_leader_crosses_as_wrong_owner() {
        let error = crate::Error::NotLeader {
            vshard_id: crate::types::VShardId::new(9),
            leader_node: 4,
            leader_addr: "10.0.0.4:9000".into(),
            leader_term: 2,
        };
        assert!(matches!(
            execution_error("array put raft propose", error),
            ClusterError::WrongOwner {
                vshard_id: 9,
                expected_owner_node: Some(4)
            }
        ));
    }

    #[test]
    fn an_execution_verdict_keeps_its_code() {
        let error = crate::Error::DataPlane(unsupported());
        match execution_error("array put", error) {
            ClusterError::DataPlane { code } => {
                assert_eq!(ErrorCode::from(code), unsupported());
            }
            other => panic!("expected the typed refusal, got {other:?}"),
        }
    }

    /// A classified error with no Data-Plane twin crosses in its typed wire
    /// form, and the coordinator renders the SQLSTATE a single-node
    /// execution renders.
    #[test]
    fn a_classified_error_keeps_its_sqlstate_at_the_coordinator() {
        use crate::control::cluster::array_cluster_helpers::cluster_err;
        use crate::control::server::pgwire::types::error_to_sqlstate;
        use nodedb_cluster::rpc_codec::ShardErrorWire;

        let local = || crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(1),
            resource: "grid".into(),
        };
        let shard = execution_error("array put", local());
        match &shard {
            ClusterError::ShardExecution { detail, .. } => {
                assert!(detail.starts_with("array put: "), "{detail}");
            }
            other => panic!("expected a typed shard execution error, got {other:?}"),
        }
        let received = ClusterError::from(ShardErrorWire::from(shard));
        let rebuilt = cluster_err(received);
        assert_eq!(error_to_sqlstate(&rebuilt).1, error_to_sqlstate(&local()).1);
    }

    /// Every variant keeps its SQLSTATE class through the array shard hop,
    /// except those whose class no public numeric code carries.
    #[test]
    fn every_variant_keeps_its_class_through_the_array_hop() {
        use crate::control::cluster::array_cluster_helpers::cluster_err;
        use crate::control::gateway::error_map::class_parity::{
            error_samples, error_variant_index,
        };
        use crate::control::server::pgwire::types::error_to_sqlstate;
        use nodedb_cluster::rpc_codec::ShardErrorWire;

        let gaps = [7, 32, 33, 88, 90];
        for (err, twin) in error_samples().into_iter().zip(error_samples()) {
            if gaps.contains(&error_variant_index(&err)) {
                continue;
            }
            let (_, local, _) = error_to_sqlstate(&err);
            let wire = ShardErrorWire::from(execution_error("array put", twin));
            let rebuilt = cluster_err(ClusterError::from(wire));
            let (_, remote, _) = error_to_sqlstate(&rebuilt);
            assert_eq!(
                remote.get(..2),
                local.get(..2),
                "{err:?} is {local} locally but {remote} at the coordinator"
            );
        }
    }
}
