// SPDX-License-Identifier: BUSL-1.1

//! One sample per `crate::Error` variant.

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::ErrorCode;

/// One sample per `crate::Error` variant.
pub(crate) fn error_samples() -> Vec<crate::Error> {
    use crate::Error as E;
    use crate::types::{RequestId, TenantId, VShardId};

    let text = || "detail".to_owned();
    let collection = || "c".to_owned();
    vec![
        E::RejectedConstraint {
            collection: collection(),
            constraint: "unique".into(),
            detail: text(),
        },
        E::TxnOverlayMemoryExceeded { limit: 1 << 20 },
        E::RejectedAuthz {
            tenant_id: TenantId::new(1),
            resource: text(),
        },
        E::OffsetRegression {
            stream: "s".into(),
            group: "g".into(),
            partition_id: 0,
            offsets: Box::new(crate::error::RegressedOffsets {
                current: crate::event::cdc::CdcOffset {
                    epoch: 0,
                    index: 2,
                    sequence: 2,
                },
                attempted: crate::event::cdc::CdcOffset {
                    epoch: 0,
                    index: 1,
                    sequence: 1,
                },
            }),
        },
        E::DeadlineExceeded {
            request_id: RequestId::new(1),
        },
        E::ConflictRetry {
            collection: collection(),
            document_id: "d".into(),
        },
        E::CalvinSerializationConflict,
        E::CalvinParticipantError,
        E::RejectedPrevalidation {
            constraint: "check".into(),
            reason: text(),
        },
        E::RetryableRefusal { reason: text() },
        E::AppendOnlyViolation {
            collection: collection(),
            detail: text(),
        },
        E::BalanceViolation {
            collection: collection(),
            detail: text(),
        },
        E::MaterializedSumTargetNotFound {
            target_collection: "t".into(),
            join_column: "k".into(),
            join_value: "1".into(),
        },
        E::MaterializedSumResolutionMissing {
            target_collection: "t".into(),
            join_column: "k".into(),
            join_value: "1".into(),
        },
        E::PeriodLocked {
            collection: collection(),
            detail: text(),
        },
        E::PeriodLockMisconfigured {
            collection: collection(),
            ref_table: "periods".into(),
            status_column: "status".into(),
            row_identity: "p1".into(),
        },
        E::RetentionViolation {
            collection: collection(),
            detail: text(),
        },
        E::LegalHoldActive {
            collection: collection(),
            detail: text(),
        },
        E::StateTransitionViolation {
            collection: collection(),
            detail: text(),
        },
        E::TransitionCheckViolation {
            collection: collection(),
            detail: text(),
        },
        E::TypeGuardViolation {
            collection: collection(),
            detail: text(),
        },
        E::TypeMismatch {
            collection: collection(),
            key: "k".into(),
            detail: text(),
        },
        E::InsufficientBalance {
            collection: collection(),
            key: "k".into(),
            detail: text(),
        },
        E::RateExceeded {
            gate: "g".into(),
            detail: text(),
            retry_after_ms: 10,
        },
        E::CollectionNotFound {
            tenant_id: TenantId::new(1),
            collection: collection(),
        },
        E::DocumentNotFound {
            collection: collection(),
            document_id: "d".into(),
        },
        E::CollectionDeactivated {
            tenant_id: TenantId::new(1),
            collection: collection(),
            retention_expires_at_ns: 1,
        },
        E::VShardAdmissionCapacityExceeded {
            vshard_id: VShardId::new(1),
            capacity: 4,
        },
        E::CrdtAdmissionRetriesExhausted {
            vshard_id: VShardId::new(1),
            attempts: 3,
        },
        E::CrdtAdmissionInvalidPlan { reason: "empty" },
        E::CrdtAdmissionCallerFence,
        E::CrdtApplyRequiresAdmission,
        E::CrdtApplyForbiddenInTransaction,
        E::NotInTransactionBlock {
            statement: "VACUUM".into(),
        },
        E::CrdtAdmissionTimeout {
            vshard_id: VShardId::new(1),
            timeout_ms: 10,
        },
        E::NoLeader {
            vshard_id: VShardId::new(1),
        },
        E::NotLeader {
            vshard_id: VShardId::new(1),
            leader_node: 2,
            leader_addr: "10.0.0.1:9000".into(),
            leader_term: 3,
        },
        E::CrossCollectionNotColocated {
            op: "insert-select",
            source_collection: "a".into(),
            target_collection: "b".into(),
        },
        E::CloneWriteRequiresMaterialize {
            collection: collection(),
            engine: "kv".into(),
            database: "db".into(),
            reason: "shadowed",
        },
        E::BadRequest { detail: text() },
        E::BackupTenantMismatch {
            expected: 1,
            actual: 2,
        },
        E::BackupKeyMismatch,
        E::QuotaOvercommit {
            field: "max_storage".into(),
            detail: text(),
        },
        E::PlanError { detail: text() },
        E::FeatureNotSupported { detail: text() },
        E::UndefinedFunction { name: "f".into() },
        E::UndefinedObject {
            kind: "sequence",
            name: "s".into(),
        },
        E::ObjectNotInPrerequisiteState {
            object: "s".into(),
            detail: text(),
        },
        E::UndefinedColumn { column: "x".into() },
        E::TextColumn {
            collection: collection(),
            column: "x".into(),
            fault: nodedb_types::text_search::TextColumnFault::NotIndexed,
        },
        E::TextColumn {
            collection: collection(),
            column: "n".into(),
            fault: nodedb_types::text_search::TextColumnFault::NotText {
                data_type: "INT".into(),
            },
        },
        E::AmbiguousColumn {
            column: "id".into(),
        },
        E::UnknownStrictField {
            collection: collection(),
            column: "x".into(),
        },
        E::DivisionByZero,
        E::DataException { detail: text() },
        E::InvalidLimitValue {
            clause: "LIMIT",
            value: "-1".into(),
        },
        E::RetryableSchemaChanged {
            descriptor: "orders".into(),
        },
        E::RetryableLeaderChange {
            group_id: 1,
            log_index: 2,
        },
        E::CommittedResultUnavailable {
            group_id: 1,
            log_index: 2,
        },
        E::ProposalOutcomeUnknown {
            group_id: 1,
            log_index: 2,
        },
        E::GroupQuorumUnavailable {
            group_id: 1,
            voters: vec![1, 2, 3],
            unreachable: vec![2, 3],
        },
        E::GroupMarksUnavailable {
            group_id: 1,
            refused_by: vec![2],
        },
        E::BackupCaptureMoved { group_id: 1 },
        E::MetadataLeaderUnavailable,
        E::AuthorizationStateBehind { detail: text() },
        E::LinearizableReadRefused {
            group_id: 1,
            detail: text(),
        },
        E::ExecutionLimitExceeded { detail: text() },
        E::LimitExceeded {
            limit_name: "max_rows",
            value: 10,
            max: 5,
        },
        E::Wal(nodedb_wal::WalError::Sealed),
        E::Dispatch { detail: text() },
        E::DispatchCapacity {
            scope: crate::DispatchCapacityScope::QueueFull {
                core_id: 0,
                capacity: 4,
            },
        },
        E::Storage {
            engine: "kv".into(),
            detail: text(),
        },
        E::ColdStorage { detail: text() },
        E::Serialization {
            format: "msgpack".into(),
            detail: text(),
        },
        E::Codec { detail: text() },
        E::SegmentCorrupted { detail: text() },
        E::MemoryExhausted {
            engine: "kv".into(),
        },
        E::Backpressure {
            engine: nodedb_mem::EngineId::Vector,
        },
        E::Crdt(nodedb_crdt::CrdtError::ConstraintViolation {
            constraint: "unique".into(),
            collection: collection(),
            detail: text(),
        }),
        E::Io(std::io::Error::other("disk")),
        E::Config { detail: text() },
        E::Encryption { detail: text() },
        E::Bridge { detail: text() },
        E::VersionCompat { detail: text() },
        E::RestoreTargetNotEmpty {
            path: std::path::PathBuf::from("/restore"),
        },
        E::RestoreVerificationFailed {
            phase: nodedb_types::backup_envelope::VerificationPhase::Destination,
            mismatches: vec![nodedb_types::backup_envelope::VerificationMismatch {
                database_id: 0,
                collection: collection(),
                part: nodedb_types::backup_envelope::VerifiedPart::Documents,
                expected: nodedb_types::backup_envelope::VerifiedTally::default(),
                found: nodedb_types::backup_envelope::VerifiedTally::default(),
            }],
        },
        E::Internal { detail: text() },
        E::Shaping(Box::new(nodedb_types::NodeDbError::bad_request(text()))),
        E::RemoteTyped {
            code: nodedb_types::error::ErrorCode::WRITE_CONFLICT,
            message: text(),
        },
        E::DescriptorVersionAnomaly {
            descriptor: "orders".into(),
            carried: 5,
            prior: 2,
        },
        E::CollectionPurgeRowMissing {
            database_id: 1,
            tenant_id: 1,
            name: collection(),
        },
        E::CollectionUnstamped {
            database_id: 1,
            tenant_id: 1,
            name: collection(),
        },
        E::CatalogIntegrityViolation {
            entry_kind: "PutCollection".into(),
            detail: text(),
        },
        E::DataPlane(ErrorCode::NotFound),
        E::Promql(crate::control::promql::PromqlError::UnexpectedEof),
        E::DependentObjectsExist {
            tenant_id: 1,
            root_kind: "collection",
            root_name: collection(),
            dependent_count: 1,
            dependents: vec![("view".into(), "v".into())],
        },
        E::CascadeCycle {
            tenant_id: 1,
            root: collection(),
            depth: 64,
        },
        E::CrossShardInExplicitTransaction,
        E::SequencerUnavailable,
        E::SessionCapExceeded { cap: 8 },
        E::SessionIdleTimeout,
        E::SessionTokenExpired,
        E::SessionKilledByAdmin,
        E::SessionUserDropped,
        E::OidcProviderTenantUnbound,
        E::OidcProviderTenantUnavailable { tenant_id: 1 },
        E::ExternalRoleUndefined {
            subject: "alice".into(),
            role: "auditor".into(),
            tenant_id: 1,
        },
        E::OidcNoDefaultDatabase {
            sub: "alice".into(),
        },
        E::TenantVectorDimExceeded {
            dim: 4096,
            limit: 1024,
        },
        E::TenantGraphDepthExceeded {
            depth: 20,
            limit: 10,
        },
        E::RoleInheritanceCycle {
            child: "a".into(),
            parent: "b".into(),
        },
        E::RoleInheritanceDepthExceeded { depth: 9, limit: 8 },
        E::OllpExhausted {
            retries: 3,
            cause: crate::OllpExhaustedCause::PredicateDrift,
        },
        E::MirrorReadOnly {
            database: "db".into(),
        },
        E::StaleReadNotLeader {
            database: "db".into(),
            source_cluster: "src".into(),
            detail: text(),
        },
        E::RoleInUse {
            role: "analyst".into(),
            dependents: crate::control::security::role_assignment::RoleDependents::Users(vec![
                "bob".into(),
            ]),
        },
        E::Ddl(Box::new(
            crate::control::server::shared::ddl::DdlError::new(
                sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
                text(),
            ),
        )),
    ]
}
