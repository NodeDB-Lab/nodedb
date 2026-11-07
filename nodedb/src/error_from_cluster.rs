// SPDX-License-Identifier: BUSL-1.1

//! Conversions between the crate error and the cluster wire error
//! `TypedClusterError`.

use crate::Error;

/// Preserve wire-level cluster classifications for local retry handling.
impl From<nodedb_cluster::rpc_codec::TypedClusterError> for Error {
    fn from(e: nodedb_cluster::rpc_codec::TypedClusterError) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            TypedClusterError::NotLeader {
                group_id,
                leader_node_id,
                leader_addr,
                term,
            } => Error::NotLeader {
                // Clamp cluster-managed group IDs for vShard display.
                vshard_id: crate::types::VShardId::new(
                    (group_id as u32).min(crate::types::VShardId::COUNT - 1),
                ),
                leader_node: leader_node_id.unwrap_or(0),
                leader_addr: leader_addr.unwrap_or_default(),
                leader_term: term,
            },
            TypedClusterError::DescriptorMismatch { collection, .. } => {
                Error::RetryableSchemaChanged {
                    descriptor: collection,
                }
            }
            TypedClusterError::DeadlineExceeded { elapsed_ms } => {
                tracing::warn!(
                    elapsed_ms,
                    "a remote node reported the request's deadline exceeded"
                );
                Error::DeadlineExceeded {
                    request_id: crate::types::RequestId::new(0),
                }
            }
            // A remote shard's Data-Plane verdict, verbatim: rebuilding the
            // code keeps the SQLSTATE `error_classify` renders locally.
            TypedClusterError::DataPlane { code } => Error::DataPlane(code.into()),
            // A remote constraint refusal, verbatim: keeps 23502 vs 23505
            // distinct instead of collapsing both into one numeric class.
            TypedClusterError::RejectedConstraint {
                collection,
                constraint,
                detail,
            } => Error::RejectedConstraint {
                collection,
                constraint,
                detail,
            },
            // A routed Calvin abort, rebuilt as the error a local submit
            // returns for the same verdict.
            TypedClusterError::CalvinAborted { reason } => {
                crate::control::planner::calvin::calvin_abort_error(reason.into())
            }
            TypedClusterError::Internal { code, message } => {
                // An unclassified failure (code 0) keeps its message as `Internal`.
                match u16::try_from(code) {
                    Ok(code_u16) if code_u16 != 0 => Error::RemoteTyped {
                        code: nodedb_types::error::ErrorCode(code_u16),
                        message,
                    },
                    _ => Error::Internal { detail: message },
                }
            }
        }
    }
}

/// Build a `TypedClusterError::NotLeader` from an `Error::NotLeader`.
impl From<Error> for nodedb_cluster::rpc_codec::TypedClusterError {
    fn from(e: Error) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            Error::NotLeader {
                vshard_id,
                leader_node,
                leader_addr,
                leader_term,
            } => TypedClusterError::NotLeader {
                group_id: vshard_id.as_u32() as u64,
                leader_node_id: if leader_node == 0 {
                    None
                } else {
                    Some(leader_node)
                },
                leader_addr: if leader_addr.is_empty() {
                    None
                } else {
                    Some(leader_addr)
                },
                term: leader_term,
            },
            Error::DeadlineExceeded { .. } => TypedClusterError::DeadlineExceeded { elapsed_ms: 0 },
            Error::RemoteTyped { code, message } => TypedClusterError::Internal {
                code: u32::from(code.0),
                message,
            },
            // Keep the verdict typed across a further hop instead of
            // degrading it to a numeric class on the second forward.
            Error::DataPlane(code) => TypedClusterError::DataPlane { code: code.into() },
            // A CRDT error crosses as its Data-Plane verdict, so its class
            // survives the hop instead of reading as a server fault.
            Error::Crdt(crdt) => TypedClusterError::DataPlane {
                code: crate::bridge::envelope::ErrorCode::from(&crdt).into(),
            },
            // Keep the constraint kind typed across a further hop, same as
            // a Data-Plane verdict, instead of flattening it to one code.
            Error::RejectedConstraint {
                collection,
                constraint,
                detail,
            } => TypedClusterError::RejectedConstraint {
                collection,
                constraint,
                detail,
            },
            // A Calvin abort keeps its verdict, so the coordinator answers the
            // SQLSTATE and message a local submit answers.
            Error::CalvinSerializationConflict => TypedClusterError::CalvinAborted {
                reason: nodedb_cluster::calvin::AbortReason::SerializationConflict,
            },
            Error::CalvinParticipantError => TypedClusterError::CalvinAborted {
                reason: nodedb_cluster::calvin::AbortReason::ParticipantError,
            },
            // A schema change stays retryable across the hop, with its
            // descriptor text.
            Error::RetryableSchemaChanged { descriptor } => TypedClusterError::DescriptorMismatch {
                collection: descriptor,
                expected_version: 0,
                actual_version: 0,
            },
            // A value refusal crosses as the Data-Plane verdict of the same
            // name, so the coordinator answers its exact SQLSTATE.
            Error::InvalidTextRepresentation { detail } => TypedClusterError::DataPlane {
                code: nodedb_cluster::rpc_codec::DataPlaneErrorCode::InvalidTextRepresentation {
                    detail,
                },
            },
            Error::DatatypeMismatch { detail } => TypedClusterError::DataPlane {
                code: nodedb_cluster::rpc_codec::DataPlaneErrorCode::DatatypeMismatch { detail },
            },
            Error::InvalidDatetimeFormat { detail } => TypedClusterError::DataPlane {
                code: nodedb_cluster::rpc_codec::DataPlaneErrorCode::InvalidDatetimeFormat {
                    detail,
                },
            },
            Error::DatetimeFieldOverflow { detail } => TypedClusterError::DataPlane {
                code: nodedb_cluster::rpc_codec::DataPlaneErrorCode::DatetimeFieldOverflow {
                    detail,
                },
            },
            // Every other error crosses as its public numeric code, so a
            // multi-hop forward keeps its class.
            other @ (Error::TxnOverlayMemoryExceeded { .. }
            | Error::RejectedAuthz { .. }
            | Error::OffsetRegression { .. }
            | Error::ConflictRetry { .. }
            | Error::RejectedPrevalidation { .. }
            | Error::RetryableRefusal { .. }
            | Error::AppendOnlyViolation { .. }
            | Error::BalanceViolation { .. }
            | Error::MaterializedSumTargetNotFound { .. }
            | Error::MaterializedSumResolutionMissing { .. }
            | Error::PeriodLocked { .. }
            | Error::PeriodLockMisconfigured { .. }
            | Error::RetentionViolation { .. }
            | Error::LegalHoldActive { .. }
            | Error::StateTransitionViolation { .. }
            | Error::TransitionCheckViolation { .. }
            | Error::TypeGuardViolation { .. }
            | Error::TypeMismatch { .. }
            | Error::InsufficientBalance { .. }
            | Error::RateExceeded { .. }
            | Error::CollectionNotFound { .. }
            | Error::DocumentNotFound { .. }
            | Error::CollectionDeactivated { .. }
            | Error::VShardAdmissionCapacityExceeded { .. }
            | Error::CrdtAdmissionRetriesExhausted { .. }
            | Error::CrdtAdmissionInvalidPlan { .. }
            | Error::CrdtAdmissionCallerFence
            | Error::CrdtApplyRequiresAdmission
            | Error::CrdtApplyForbiddenInTransaction
            | Error::NotInTransactionBlock { .. }
            | Error::CrdtAdmissionTimeout { .. }
            | Error::NoLeader { .. }
            | Error::CrossCollectionNotColocated { .. }
            | Error::CloneWriteRequiresMaterialize { .. }
            | Error::BadRequest { .. }
            | Error::BackupTenantMismatch { .. }
            | Error::BackupKeyMismatch
            | Error::QuotaOvercommit { .. }
            | Error::PlanError { .. }
            | Error::FeatureNotSupported { .. }
            | Error::UndefinedFunction { .. }
            | Error::UndefinedObject { .. }
            | Error::ObjectNotInPrerequisiteState { .. }
            | Error::WrongObjectType { .. }
            | Error::UndefinedColumn { .. }
            | Error::TextColumn { .. }
            | Error::AmbiguousColumn { .. }
            | Error::UngroupedColumn { .. }
            | Error::UnknownStrictField { .. }
            | Error::DivisionByZero
            | Error::DataException { .. }
            | Error::MixedSrid { .. }
            | Error::UnknownSrid { .. }
            | Error::NumericValueOutOfRange { .. }
            | Error::InvalidLimitValue { .. }
            | Error::RetryableLeaderChange { .. }
            | Error::CommittedResultUnavailable { .. }
            | Error::ProposalOutcomeUnknown { .. }
            | Error::GroupQuorumUnavailable { .. }
            | Error::GroupMarksUnavailable { .. }
            | Error::BackupCaptureMoved { .. }
            | Error::MetadataLeaderUnavailable
            | Error::MetadataGroupGone { .. }
            | Error::MetadataDdlSuperseded { .. }
            | Error::AuthorizationStateBehind { .. }
            | Error::LinearizableReadRefused { .. }
            | Error::ExecutionLimitExceeded { .. }
            | Error::LimitExceeded { .. }
            | Error::Wal(_)
            | Error::Dispatch { .. }
            | Error::DispatchCapacity { .. }
            | Error::Storage { .. }
            | Error::ColdStorage { .. }
            | Error::Serialization { .. }
            | Error::Codec { .. }
            | Error::SegmentCorrupted { .. }
            | Error::MemoryExhausted { .. }
            | Error::Backpressure { .. }
            | Error::Io(_)
            | Error::Config { .. }
            | Error::Encryption { .. }
            | Error::Bridge { .. }
            | Error::VersionCompat { .. }
            | Error::RestoreTargetNotEmpty { .. }
            | Error::RestoreVerificationFailed { .. }
            | Error::Internal { .. }
            | Error::Shaping(_)
            | Error::Ddl(_)
            | Error::DescriptorVersionAnomaly { .. }
            | Error::CollectionPurgeRowMissing { .. }
            | Error::CollectionUnstamped { .. }
            | Error::CatalogIntegrityViolation { .. }
            | Error::Promql(_)
            | Error::DependentObjectsExist { .. }
            | Error::RoleInUse { .. }
            | Error::CascadeCycle { .. }
            | Error::CrossShardInExplicitTransaction
            | Error::SequencerUnavailable
            | Error::SessionCapExceeded { .. }
            | Error::SessionIdleTimeout
            | Error::TxnSnapshotTooOld { .. }
            | Error::SessionTokenExpired
            | Error::SessionKilledByAdmin
            | Error::SessionUserDropped
            | Error::OidcProviderTenantUnavailable { .. }
            | Error::ExternalRoleUndefined { .. }
            | Error::OidcNoDefaultDatabase { .. }
            | Error::TenantVectorDimExceeded { .. }
            | Error::TenantGraphDepthExceeded { .. }
            | Error::StorageQuotaExceeded { .. }
            | Error::RoleInheritanceCycle { .. }
            | Error::RoleInheritanceDepthExceeded { .. }
            | Error::OllpExhausted { .. }
            | Error::MirrorReadOnly { .. }
            | Error::MirrorSyncTimeout { .. }
            | Error::StaleReadNotLeader { .. }) => {
                crate::control::cluster::data_plane_error_wire::numeric_typed(other)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::rpc_codec::TypedClusterError;
    use nodedb_types::error::ErrorCode;

    use super::Error;
    use crate::types::TenantId;

    /// A peer that never classified the failure sends `code == 0`. Decoding
    /// yields the generic `Error::Internal` rather than fabricating a bogus
    /// `ErrorCode(0)` classification.
    #[test]
    fn decode_zero_code_degrades_to_internal() {
        let wire = TypedClusterError::Internal {
            code: 0,
            message: "boom".to_owned(),
        };
        let err: Error = wire.into();
        match err {
            Error::Internal { detail } => assert_eq!(detail, "boom"),
            other => panic!("expected Error::Internal, got {other:?}"),
        }
    }

    /// A Calvin abort and a superseded collection cross a routed submit as the
    /// same errors a local submit returns. A client retries both on 40001.
    #[test]
    fn calvin_aborts_and_schema_changes_survive_the_hop() {
        use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;

        let rebuilt = Error::from(execution_error_to_typed(Error::CalvinSerializationConflict));
        assert!(
            matches!(rebuilt, Error::CalvinSerializationConflict),
            "got {rebuilt:?}"
        );
        let rebuilt = Error::from(execution_error_to_typed(Error::CalvinParticipantError));
        assert!(
            matches!(rebuilt, Error::CalvinParticipantError),
            "got {rebuilt:?}"
        );
        let rebuilt = Error::from(execution_error_to_typed(Error::RetryableSchemaChanged {
            descriptor: "recreated".to_owned(),
        }));
        match rebuilt {
            Error::RetryableSchemaChanged { descriptor } => assert_eq!(descriptor, "recreated"),
            other => panic!("expected Error::RetryableSchemaChanged, got {other:?}"),
        }
    }

    /// A remote peer that populated a real `ErrorCode` must decode to
    /// `Error::RemoteTyped`, carrying that code forward instead of losing it.
    #[test]
    fn decode_real_code_becomes_remote_typed() {
        let wire = TypedClusterError::Internal {
            code: u32::from(ErrorCode::CONSTRAINT_VIOLATION.0),
            message: "duplicate key".to_owned(),
        };
        let err: Error = wire.into();
        match err {
            Error::RemoteTyped { code, message } => {
                assert_eq!(code, ErrorCode::CONSTRAINT_VIOLATION);
                assert_eq!(message, "duplicate key");
            }
            other => panic!("expected Error::RemoteTyped, got {other:?}"),
        }
    }

    /// Encoding a `RejectedConstraint` must keep its kind typed on the wire,
    /// never flatten to `Internal`'s bare numeric code — a NOT NULL refusal
    /// and a UNIQUE refusal both carry `CONSTRAINT_VIOLATION`, so a decoder
    /// reading only the numeric code cannot tell them apart.
    #[test]
    fn encode_rejected_constraint_keeps_its_kind_typed() {
        let err = Error::RejectedConstraint {
            collection: "users".to_owned(),
            constraint: "not_null".to_owned(),
            detail: "column 'email' cannot be null".to_owned(),
        };
        let wire: TypedClusterError = err.into();
        match wire {
            TypedClusterError::RejectedConstraint {
                collection,
                constraint,
                detail,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(constraint, "not_null");
                assert_eq!(detail, "column 'email' cannot be null");
            }
            other => panic!("expected TypedClusterError::RejectedConstraint, got {other:?}"),
        }
    }

    /// The wire round trip reconstructs `Error::RejectedConstraint` with its
    /// kind intact, so the coordinator's SQLSTATE mapper sees the same
    /// `constraint` field a local refusal would carry.
    #[test]
    fn rejected_constraint_round_trips_across_the_wire() {
        let original = Error::RejectedConstraint {
            collection: "orders".to_owned(),
            constraint: "not_null".to_owned(),
            detail: "column 'sku' cannot be null".to_owned(),
        };
        let wire: TypedClusterError = original.into();
        let decoded: Error = wire.into();
        match decoded {
            Error::RejectedConstraint {
                collection,
                constraint,
                detail,
            } => {
                assert_eq!(collection, "orders");
                assert_eq!(constraint, "not_null");
                assert_eq!(detail, "column 'sku' cannot be null");
            }
            other => panic!("expected Error::RejectedConstraint, got {other:?}"),
        }
    }

    /// Encoding then decoding a typed error must preserve the numeric code
    /// end to end, which is the whole point of this fix.
    #[test]
    fn round_trip_preserves_code() {
        let original = Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: "secret_vault".to_owned(),
        };
        let wire: TypedClusterError = original.into();
        let decoded: Error = wire.into();
        match decoded {
            Error::RemoteTyped { code, message } => {
                assert_eq!(code, ErrorCode::AUTHORIZATION_DENIED);
                assert!(message.contains("secret_vault"));
            }
            other => panic!("expected Error::RemoteTyped, got {other:?}"),
        }
    }

    /// A shard's Data-Plane verdict crosses the node hop verbatim: the code
    /// itself survives, so the coordinator renders `22012`, not `XX000`.
    #[test]
    fn data_plane_verdict_round_trips_with_its_code() {
        let original = Error::DataPlane(crate::bridge::envelope::ErrorCode::DivisionByZero);
        let wire: TypedClusterError = original.into();
        assert!(matches!(wire, TypedClusterError::DataPlane { .. }));
        match Error::from(wire) {
            Error::DataPlane(code) => {
                assert_eq!(code, crate::bridge::envelope::ErrorCode::DivisionByZero);
            }
            other => panic!("expected Error::DataPlane, got {other:?}"),
        }
    }
}
