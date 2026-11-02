// SPDX-License-Identifier: Apache-2.0

//! `FromMessagePack` impl for [`ErrorDetails`] — dispatches the variant tag
//! to the matching helper in [`super::readers`] and constructs the variant.

use zerompk::{FromMessagePack, Read};

use super::readers::{
    read_collection_deactivated, read_header, read_no_fields, read_segment_corrupted,
    read_string_vec, read_sync_delta_rejected, read_u8_field, read1_str, read2_str, read2_u32,
    read2_u64, read3_str,
};
use crate::error::details::ErrorDetails;
use crate::error::msgpack::constants::*;

impl<'a> FromMessagePack<'a> for ErrorDetails {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let (tag, field_count) = read_header(reader)?;
        match tag {
            TAG_CONSTRAINT_VIOLATION => {
                let (collection, constraint) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::ConstraintViolation {
                    collection,
                    constraint,
                })
            }
            TAG_WRITE_CONFLICT => {
                let (collection, document_id) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::WriteConflict {
                    collection,
                    document_id,
                })
            }
            TAG_DEADLINE_EXCEEDED => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::DeadlineExceeded)
            }
            TAG_PREVALIDATION_REJECTED => {
                let (constraint,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::PrevalidationRejected { constraint })
            }
            TAG_APPEND_ONLY_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AppendOnlyViolation { collection })
            }
            TAG_BALANCE_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::BalanceViolation { collection })
            }
            TAG_PERIOD_LOCKED => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::PeriodLocked { collection })
            }
            TAG_PERIOD_LOCK_MISCONFIGURED => {
                let (collection, ref_table, status_column) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::PeriodLockMisconfigured {
                    collection,
                    ref_table,
                    status_column,
                })
            }
            TAG_STATE_TRANSITION_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::StateTransitionViolation { collection })
            }
            TAG_TRANSITION_CHECK_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TransitionCheckViolation { collection })
            }
            TAG_TYPE_GUARD_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TypeGuardViolation { collection })
            }
            TAG_RETENTION_VIOLATION => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::RetentionViolation { collection })
            }
            TAG_LEGAL_HOLD_ACTIVE => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::LegalHoldActive { collection })
            }
            TAG_TYPE_MISMATCH => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TypeMismatch { collection })
            }
            TAG_OVERFLOW => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Overflow { collection })
            }
            TAG_INSUFFICIENT_BALANCE => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::InsufficientBalance { collection })
            }
            TAG_RATE_EXCEEDED => {
                let (gate,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::RateExceeded { gate })
            }
            TAG_TRANSACTION_ROLLBACK => {
                let (detail,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::TransactionRollback { detail })
            }
            TAG_ACTIVE_SQL_TRANSACTION => {
                let (detail,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ActiveSqlTransaction { detail })
            }
            TAG_COLLECTION_NOT_FOUND => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::CollectionNotFound { collection })
            }
            TAG_DATABASE_NOT_FOUND => {
                let (database,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::DatabaseNotFound { database })
            }
            TAG_DOCUMENT_NOT_FOUND => {
                let (collection, document_id) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::DocumentNotFound {
                    collection,
                    document_id,
                })
            }
            TAG_COLLECTION_DRAINING => {
                let (collection,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::CollectionDraining { collection })
            }
            TAG_COLLECTION_DEACTIVATED => {
                let (collection, retention_expires_at_ns, undrop_hint) =
                    read_collection_deactivated(reader, field_count)?;
                Ok(ErrorDetails::CollectionDeactivated {
                    collection,
                    retention_expires_at_ns,
                    undrop_hint,
                })
            }
            TAG_PLAN_ERROR => {
                let (phase, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::PlanError { phase, detail })
            }
            TAG_SQL_NOT_ENABLED => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::SqlNotEnabled)
            }
            TAG_UNDEFINED_FUNCTION => {
                let (name,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::UndefinedFunction { name })
            }
            TAG_UNDEFINED_COLUMN => {
                let (column,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::UndefinedColumn { column })
            }
            TAG_AMBIGUOUS_COLUMN => {
                let (column,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AmbiguousColumn { column })
            }
            TAG_DIVISION_BY_ZERO => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::DivisionByZero)
            }
            TAG_DATA_EXCEPTION => {
                let (detail,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::DataException { detail })
            }
            TAG_PROGRAM_LIMIT_EXCEEDED => {
                let (detail,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ProgramLimitExceeded { detail })
            }
            TAG_INVALID_LIMIT_VALUE => {
                let (clause, value) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::InvalidLimitValue { clause, value })
            }
            TAG_AUTHORIZATION_DENIED => {
                let (resource,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AuthorizationDenied { resource })
            }
            TAG_AUTH_EXPIRED => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::AuthExpired)
            }
            TAG_AUTHENTICATION_FAILED => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::AuthenticationFailed)
            }
            TAG_SYNC_CONNECTION_FAILED => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::SyncConnectionFailed)
            }
            TAG_SYNC_DELTA_REJECTED => {
                let compensation = read_sync_delta_rejected(reader, field_count)?;
                Ok(ErrorDetails::SyncDeltaRejected { compensation })
            }
            TAG_SHAPE_SUBSCRIPTION_FAILED => {
                let (shape_id,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ShapeSubscriptionFailed { shape_id })
            }
            TAG_STORAGE => {
                let (component, op, detail) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::Storage {
                    component,
                    op,
                    detail,
                })
            }
            TAG_SEGMENT_CORRUPTED => {
                let (segment_id, corruption, detail) = read_segment_corrupted(reader, field_count)?;
                Ok(ErrorDetails::SegmentCorrupted {
                    segment_id,
                    corruption,
                    detail,
                })
            }
            TAG_COLD_STORAGE => {
                let (backend, op, detail) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::ColdStorage {
                    backend,
                    op,
                    detail,
                })
            }
            TAG_WAL => {
                let (stage, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::Wal { stage, detail })
            }
            TAG_SERIALIZATION => {
                let (format,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Serialization { format })
            }
            TAG_CODEC => {
                let (codec, op, detail) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::Codec { codec, op, detail })
            }
            TAG_CONFIG => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::Config)
            }
            TAG_BAD_REQUEST => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::BadRequest)
            }
            TAG_NO_LEADER => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::NoLeader)
            }
            TAG_NOT_LEADER => {
                let (leader_addr,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::NotLeader { leader_addr })
            }
            TAG_MIGRATION_IN_PROGRESS => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::MigrationInProgress)
            }
            TAG_NODE_UNREACHABLE => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::NodeUnreachable)
            }
            TAG_CLUSTER => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::Cluster)
            }
            TAG_MEMORY_EXHAUSTED => {
                let (engine,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::MemoryExhausted { engine })
            }
            TAG_ENCRYPTION => {
                let (cipher, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::Encryption { cipher, detail })
            }
            TAG_ARRAY => {
                let (array,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::Array { array })
            }
            TAG_BRIDGE => {
                let (plane, op, detail) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::Bridge { plane, op, detail })
            }
            TAG_DISPATCH => {
                let (stage, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::Dispatch { stage, detail })
            }
            TAG_HANDSHAKE_FAILED => {
                let server_code = read_u8_field(reader, field_count)?;
                Ok(ErrorDetails::HandshakeFailed { server_code })
            }
            TAG_INTERNAL => {
                let (component, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::Internal { component, detail })
            }
            TAG_TENANT_VECTOR_DIM_EXCEEDED => {
                let (dim, limit) = read2_u32(reader, field_count)?;
                Ok(ErrorDetails::TenantVectorDimExceeded { dim, limit })
            }
            TAG_TENANT_GRAPH_DEPTH_EXCEEDED => {
                let (depth, limit) = read2_u32(reader, field_count)?;
                Ok(ErrorDetails::TenantGraphDepthExceeded { depth, limit })
            }
            TAG_QUOTA_OVERCOMMIT => {
                let (field,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::QuotaOvercommit { field })
            }
            TAG_QUOTA_EXCEEDED => {
                let (scope,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::QuotaExceeded { scope })
            }
            TAG_SERVER_OVERLOAD => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::ServerOverload)
            }
            TAG_CLONE_DEPTH_EXCEEDED => {
                let (depth, limit) = read2_u32(reader, field_count)?;
                Ok(ErrorDetails::CloneDepthExceeded { depth, limit })
            }
            TAG_CANNOT_CLONE_MIRROR => {
                let (database,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::CannotCloneMirror { database })
            }
            TAG_CLONE_DEPENDENCY => {
                // field_count fields: the array of dependents.
                let dependents = read_string_vec(reader, field_count)?;
                Ok(ErrorDetails::CloneDependency { dependents })
            }
            TAG_BACKUP_TENANT_MISMATCH => {
                let (expected, actual) = read2_u64(reader, field_count)?;
                Ok(ErrorDetails::BackupTenantMismatch { expected, actual })
            }
            TAG_BACKUP_KEY_MISMATCH => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::BackupKeyMismatch)
            }
            TAG_CLONE_PREDATES_QUERY_TIME => {
                let (as_of_lsn, created_at_lsn) = read2_u64(reader, field_count)?;
                Ok(ErrorDetails::ClonePredatesQueryTime {
                    as_of_lsn,
                    created_at_lsn,
                })
            }
            TAG_CLONE_WRITE_REQUIRES_MATERIALIZE => {
                let (collection, engine, database) = read3_str(reader, field_count)?;
                Ok(ErrorDetails::CloneWriteRequiresMaterialize {
                    collection,
                    engine,
                    database,
                })
            }
            TAG_MOVE_TENANT_DRAIN_TIMEOUT => {
                let (tenant, source_db) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::MoveTenantDrainTimeout { tenant, source_db })
            }
            TAG_MOVE_TENANT_PREFLIGHT_FAILED => {
                let (tenant, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::MoveTenantPreflightFailed { tenant, detail })
            }
            TAG_MOVE_TENANT_SNAPSHOT_FAILED => {
                let (tenant, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::MoveTenantSnapshotFailed { tenant, detail })
            }
            TAG_MOVE_TENANT_CUTOVER_FAILED => {
                let (tenant, detail) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::MoveTenantCutoverFailed { tenant, detail })
            }
            TAG_MOVE_TENANT_ALREADY_AT_TARGET => {
                let (tenant, target_db) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::MoveTenantAlreadyAtTarget { tenant, target_db })
            }
            TAG_MIRROR_READ_ONLY => {
                let (database,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::MirrorReadOnly { database })
            }
            TAG_STALE_READ_NOT_LEADER => {
                let (database, source_cluster) = read2_str(reader, field_count)?;
                Ok(ErrorDetails::StaleReadNotLeader {
                    database,
                    source_cluster,
                })
            }
            TAG_MIRROR_NOT_PROMOTED => {
                let (database,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::MirrorNotPromoted { database })
            }
            TAG_UNDEFINED_OBJECT => {
                let (object,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::UndefinedObject { object })
            }
            TAG_ALREADY_EXISTS => {
                let (object,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::AlreadyExists { object })
            }
            TAG_OBJECT_NOT_READY => {
                let (object,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::ObjectNotReady { object })
            }
            TAG_NOT_FOUND => {
                let (detail,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::NotFound { detail })
            }
            TAG_DEPENDENT_OBJECTS_EXIST => {
                let (object,) = read1_str(reader, field_count)?;
                Ok(ErrorDetails::DependentObjectsExist { object })
            }
            TAG_CANNOT_DROP_DEFAULT_DATABASE => {
                read_no_fields(field_count)?;
                Ok(ErrorDetails::CannotDropDefaultDatabase)
            }
            unknown => Err(zerompk::Error::UnknownKey(format!(
                "error details tag {unknown}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::details::ErrorDetails;
    use crate::error::msgpack::constants::*;
    use crate::sync::compensation::CompensationHint;

    fn roundtrip(d: &ErrorDetails) -> ErrorDetails {
        let bytes = zerompk::to_msgpack_vec(d).expect("encode");
        zerompk::from_msgpack(&bytes).expect("decode")
    }

    #[test]
    fn unit_variant_roundtrip() {
        for v in [
            ErrorDetails::DeadlineExceeded,
            ErrorDetails::SqlNotEnabled,
            ErrorDetails::DivisionByZero,
            ErrorDetails::AuthExpired,
            ErrorDetails::AuthenticationFailed,
            ErrorDetails::SyncConnectionFailed,
            ErrorDetails::Config,
            ErrorDetails::BadRequest,
            ErrorDetails::NoLeader,
            ErrorDetails::MigrationInProgress,
            ErrorDetails::NodeUnreachable,
            ErrorDetails::Cluster,
            ErrorDetails::CannotDropDefaultDatabase,
        ] {
            assert_eq!(roundtrip(&v), v, "unit variant roundtrip failed: {v:?}");
        }
    }

    #[test]
    fn storage_enriched_roundtrip() {
        let v = ErrorDetails::Storage {
            component: "redb".into(),
            op: "write".into(),
            detail: "disk full".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn segment_corrupted_enriched_roundtrip() {
        let v = ErrorDetails::SegmentCorrupted {
            segment_id: 42,
            corruption: "crc_mismatch".into(),
            detail: "footer checksum invalid".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn cold_storage_enriched_roundtrip() {
        let v = ErrorDetails::ColdStorage {
            backend: "s3".into(),
            op: "get_object".into(),
            detail: "403 forbidden".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn wal_enriched_roundtrip() {
        let v = ErrorDetails::Wal {
            stage: "fsync".into(),
            detail: "io_uring submission failed".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn codec_enriched_roundtrip() {
        let v = ErrorDetails::Codec {
            codec: "alp".into(),
            op: "encode".into(),
            detail: "unsupported exponent range".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn encryption_enriched_roundtrip() {
        let v = ErrorDetails::Encryption {
            cipher: "aes_gcm".into(),
            detail: "authentication tag mismatch".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn plan_error_enriched_roundtrip() {
        let v = ErrorDetails::PlanError {
            phase: "logical".into(),
            detail: "ambiguous column reference".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn data_exception_roundtrip() {
        let v = ErrorDetails::DataException {
            detail: "vector_distance(): vector dimension mismatch: expected 3, got 2".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn program_limit_exceeded_roundtrip() {
        let v = ErrorDetails::ProgramLimitExceeded {
            detail: "WITH RECURSIVE CTE 'walk' exceeded max recursion depth 100".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn transaction_rollback_roundtrip() {
        let v = ErrorDetails::TransactionRollback {
            detail: "a participant vShard returned an error".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn active_sql_transaction_roundtrip() {
        let v = ErrorDetails::ActiveSqlTransaction {
            detail: "VACUUM cannot run inside a transaction block".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn dependent_objects_exist_roundtrip() {
        let v = ErrorDetails::DependentObjectsExist {
            object: "role \"auditor\"".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    /// The details of each transaction and dependency code decode to the
    /// same variant, which answers the same numeric code.
    #[test]
    fn transaction_and_dependency_details_keep_their_code() {
        use crate::error::NodeDbError;
        for e in [
            NodeDbError::transaction_rollback("participant failed"),
            NodeDbError::active_sql_transaction("VACUUM"),
            NodeDbError::dependent_objects_exist("role \"r\"", "held by users: alice"),
        ] {
            let back = roundtrip(e.details());
            assert_eq!(&back, e.details());
            assert_eq!(back.code(), e.code());
        }
    }

    #[test]
    fn bridge_enriched_roundtrip() {
        let v = ErrorDetails::Bridge {
            plane: "data".into(),
            op: "dispatch".into(),
            detail: "ring buffer full".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn dispatch_enriched_roundtrip() {
        let v = ErrorDetails::Dispatch {
            stage: "route".into(),
            detail: "vshard not found".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn internal_enriched_roundtrip() {
        let v = ErrorDetails::Internal {
            component: "compaction".into(),
            detail: "unreachable state".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    /// Decode `[tag, {}]`: a variant payload with no fields.
    fn decode_zero_fields(tag: u16) -> zerompk::Result<ErrorDetails> {
        // Manual MessagePack: [fixarray(2), uint16(tag), fixmap(0)]
        let buf = [0x92, 0xcd, (tag >> 8) as u8, (tag & 0xff) as u8, 0x80];
        zerompk::from_msgpack(&buf)
    }

    /// A field-bearing variant written without its fields is refused, never
    /// filled with placeholder values.
    #[test]
    fn a_variant_missing_its_fields_is_refused() {
        for tag in [
            TAG_STORAGE,
            TAG_SEGMENT_CORRUPTED,
            TAG_COLD_STORAGE,
            TAG_WAL,
            TAG_CODEC,
            TAG_ENCRYPTION,
            TAG_PLAN_ERROR,
            TAG_BRIDGE,
            TAG_DISPATCH,
            TAG_INTERNAL,
            TAG_TENANT_VECTOR_DIM_EXCEEDED,
            TAG_BACKUP_TENANT_MISMATCH,
        ] {
            assert!(
                matches!(
                    decode_zero_fields(tag),
                    Err(zerompk::Error::MapLengthMismatch { actual: 0, .. })
                ),
                "tag {tag} decoded with no fields"
            );
        }
    }

    /// A unit variant still decodes from its empty payload.
    #[test]
    fn a_unit_variant_decodes_from_no_fields() {
        assert_eq!(
            decode_zero_fields(TAG_DEADLINE_EXCEEDED).expect("unit variant"),
            ErrorDetails::DeadlineExceeded
        );
    }

    /// A payload carrying a field the encoder never writes is refused.
    #[test]
    fn an_extra_field_is_refused() {
        // [fixarray(2), uint16(TAG_INTERNAL), fixmap(3), 1:"a", 2:"b", 3:"c"]
        let buf = [
            0x92,
            0xcd,
            (TAG_INTERNAL >> 8) as u8,
            (TAG_INTERNAL & 0xff) as u8,
            0x83,
            0x01,
            0xa1,
            b'a',
            0x02,
            0xa1,
            b'b',
            0x03,
            0xa1,
            b'c',
        ];
        assert!(matches!(
            zerompk::from_msgpack::<ErrorDetails>(&buf),
            Err(zerompk::Error::MapLengthMismatch {
                expected: 2,
                actual: 3
            })
        ));
    }

    /// A tag no variant owns is refused, never decoded as `Internal`.
    #[test]
    fn an_unknown_tag_is_refused() {
        assert!(matches!(
            decode_zero_fields(48),
            Err(zerompk::Error::UnknownKey(_))
        ));
        assert!(matches!(
            decode_zero_fields(u16::MAX),
            Err(zerompk::Error::UnknownKey(_))
        ));
    }

    /// `CloneDependency` round-trips through its one array field.
    #[test]
    fn clone_dependency_roundtrip() {
        let v = ErrorDetails::CloneDependency {
            dependents: vec!["child_a".into(), "child_b".into()],
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn single_string_field_roundtrip() {
        let variants = vec![
            ErrorDetails::AppendOnlyViolation {
                collection: "ledger".into(),
            },
            ErrorDetails::CollectionNotFound {
                collection: "users".into(),
            },
            ErrorDetails::DatabaseNotFound {
                database: "tenant_db".into(),
            },
            ErrorDetails::AuthorizationDenied {
                resource: "orders.*".into(),
            },
            ErrorDetails::MemoryExhausted {
                engine: "vector".into(),
            },
            ErrorDetails::Array {
                array: "arr1".into(),
            },
            ErrorDetails::NotLeader {
                leader_addr: "10.0.0.1:6432".into(),
            },
            ErrorDetails::UndefinedObject {
                object: "custom_type 'geo_point'".into(),
            },
            ErrorDetails::AlreadyExists {
                object: "tenant 'acme'".into(),
            },
            ErrorDetails::ObjectNotReady {
                object: "collection 'orders'".into(),
            },
            ErrorDetails::NotFound {
                detail: "exchange rate for USD/EUR".into(),
            },
        ];
        for v in variants {
            assert_eq!(roundtrip(&v), v, "single-string roundtrip failed: {v:?}");
        }
    }

    #[test]
    fn two_string_field_roundtrip() {
        let v = ErrorDetails::WriteConflict {
            collection: "orders".into(),
            document_id: "ord-42".into(),
        };
        assert_eq!(roundtrip(&v), v);

        let v2 = ErrorDetails::DocumentNotFound {
            collection: "users".into(),
            document_id: "u-99".into(),
        };
        assert_eq!(roundtrip(&v2), v2);

        let v3 = ErrorDetails::ConstraintViolation {
            collection: "orders".into(),
            constraint: "not_null".into(),
        };
        assert_eq!(roundtrip(&v3), v3);
    }

    #[test]
    fn collection_deactivated_roundtrip() {
        let v = ErrorDetails::CollectionDeactivated {
            collection: "old_logs".into(),
            retention_expires_at_ns: 1_700_000_000_000_u64,
            undrop_hint: "UNDROP COLLECTION old_logs".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn sync_delta_rejected_with_hint_roundtrip() {
        let v = ErrorDetails::SyncDeltaRejected {
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            }),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn sync_delta_rejected_no_hint_roundtrip() {
        let v = ErrorDetails::SyncDeltaRejected { compensation: None };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn serialization_roundtrip() {
        let v = ErrorDetails::Serialization {
            format: "msgpack".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn clone_write_requires_materialize_roundtrip() {
        let v = ErrorDetails::CloneWriteRequiresMaterialize {
            collection: "metrics".into(),
            engine: "columnar".into(),
            database: "clone_db".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn period_lock_misconfigured_roundtrip() {
        let v = ErrorDetails::PeriodLockMisconfigured {
            collection: "journal_entries".into(),
            ref_table: "fiscal_periods".into(),
            status_column: "status".into(),
        };
        assert_eq!(roundtrip(&v), v);
    }
}
