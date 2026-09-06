// SPDX-License-Identifier: Apache-2.0

//! MsgPack encoding for [`ErrorDetails`].
//!
//! Every variant encodes as a 2-element MessagePack array:
//! `[discriminant: u16, fields: map{u8 → value}]`
//!
//! Unit variants write an empty map (`fixmap(0)`).

use zerompk::{ToMessagePack, Write};

use crate::error::details::ErrorDetails;

use super::constants::*;

/// Write a unit variant: `[tag, {}]`.
#[inline]
pub(super) fn write_unit<W: Write>(writer: &mut W, tag: u16) -> zerompk::Result<()> {
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(0)
}

/// Write a 1-field variant: `[tag, {1: field1}]`.
#[inline]
pub(super) fn write1<W, T>(writer: &mut W, tag: u16, v1: &T) -> zerompk::Result<()>
where
    W: Write,
    T: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(1)?;
    writer.write_u8(1)?;
    v1.write(writer)
}

/// Write a 2-field variant: `[tag, {1: field1, 2: field2}]`.
#[inline]
pub(super) fn write2<W, T1, T2>(writer: &mut W, tag: u16, v1: &T1, v2: &T2) -> zerompk::Result<()>
where
    W: Write,
    T1: ToMessagePack,
    T2: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(2)?;
    writer.write_u8(1)?;
    v1.write(writer)?;
    writer.write_u8(2)?;
    v2.write(writer)
}

/// Write a 3-field variant: `[tag, {1: f1, 2: f2, 3: f3}]`.
#[inline]
pub(super) fn write3<W, T1, T2, T3>(
    writer: &mut W,
    tag: u16,
    v1: &T1,
    v2: &T2,
    v3: &T3,
) -> zerompk::Result<()>
where
    W: Write,
    T1: ToMessagePack,
    T2: ToMessagePack,
    T3: ToMessagePack,
{
    writer.write_array_len(2)?;
    writer.write_u16(tag)?;
    writer.write_map_len(3)?;
    writer.write_u8(1)?;
    v1.write(writer)?;
    writer.write_u8(2)?;
    v2.write(writer)?;
    writer.write_u8(3)?;
    v3.write(writer)
}

impl ToMessagePack for ErrorDetails {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            ErrorDetails::ConstraintViolation { collection } => {
                write1(writer, TAG_CONSTRAINT_VIOLATION, collection)
            }
            ErrorDetails::WriteConflict {
                collection,
                document_id,
            } => write2(writer, TAG_WRITE_CONFLICT, collection, document_id),
            ErrorDetails::DeadlineExceeded => write_unit(writer, TAG_DEADLINE_EXCEEDED),
            ErrorDetails::PrevalidationRejected { constraint } => {
                write1(writer, TAG_PREVALIDATION_REJECTED, constraint)
            }
            ErrorDetails::AppendOnlyViolation { collection } => {
                write1(writer, TAG_APPEND_ONLY_VIOLATION, collection)
            }
            ErrorDetails::BalanceViolation { collection } => {
                write1(writer, TAG_BALANCE_VIOLATION, collection)
            }
            ErrorDetails::PeriodLocked { collection } => {
                write1(writer, TAG_PERIOD_LOCKED, collection)
            }
            ErrorDetails::StateTransitionViolation { collection } => {
                write1(writer, TAG_STATE_TRANSITION_VIOLATION, collection)
            }
            ErrorDetails::TransitionCheckViolation { collection } => {
                write1(writer, TAG_TRANSITION_CHECK_VIOLATION, collection)
            }
            ErrorDetails::TypeGuardViolation { collection } => {
                write1(writer, TAG_TYPE_GUARD_VIOLATION, collection)
            }
            ErrorDetails::RetentionViolation { collection } => {
                write1(writer, TAG_RETENTION_VIOLATION, collection)
            }
            ErrorDetails::LegalHoldActive { collection } => {
                write1(writer, TAG_LEGAL_HOLD_ACTIVE, collection)
            }
            ErrorDetails::TypeMismatch { collection } => {
                write1(writer, TAG_TYPE_MISMATCH, collection)
            }
            ErrorDetails::Overflow { collection } => write1(writer, TAG_OVERFLOW, collection),
            ErrorDetails::InsufficientBalance { collection } => {
                write1(writer, TAG_INSUFFICIENT_BALANCE, collection)
            }
            ErrorDetails::RateExceeded { gate } => write1(writer, TAG_RATE_EXCEEDED, gate),
            ErrorDetails::CollectionNotFound { collection } => {
                write1(writer, TAG_COLLECTION_NOT_FOUND, collection)
            }
            ErrorDetails::DatabaseNotFound { database } => {
                write1(writer, TAG_DATABASE_NOT_FOUND, database)
            }
            ErrorDetails::DocumentNotFound {
                collection,
                document_id,
            } => write2(writer, TAG_DOCUMENT_NOT_FOUND, collection, document_id),
            ErrorDetails::CollectionDraining { collection } => {
                write1(writer, TAG_COLLECTION_DRAINING, collection)
            }
            ErrorDetails::CollectionDeactivated {
                collection,
                retention_expires_at_ns,
                undrop_hint,
            } => write3(
                writer,
                TAG_COLLECTION_DEACTIVATED,
                collection,
                retention_expires_at_ns,
                undrop_hint,
            ),
            ErrorDetails::PlanError { phase, detail } => {
                write2(writer, TAG_PLAN_ERROR, phase, detail)
            }
            ErrorDetails::FanOutExceeded {
                shards_touched,
                limit,
            } => write2(writer, TAG_FAN_OUT_EXCEEDED, shards_touched, limit),
            ErrorDetails::SqlNotEnabled => write_unit(writer, TAG_SQL_NOT_ENABLED),
            ErrorDetails::UndefinedFunction { name } => {
                write1(writer, TAG_UNDEFINED_FUNCTION, name)
            }
            ErrorDetails::NotNullViolation { table, column } => {
                write2(writer, TAG_NOT_NULL_VIOLATION, table, column)
            }
            ErrorDetails::DivisionByZero => write_unit(writer, TAG_DIVISION_BY_ZERO),
            ErrorDetails::InvalidLimitValue { clause, value } => {
                write2(writer, TAG_INVALID_LIMIT_VALUE, clause, value)
            }
            ErrorDetails::AuthorizationDenied { resource } => {
                write1(writer, TAG_AUTHORIZATION_DENIED, resource)
            }
            ErrorDetails::AuthExpired => write_unit(writer, TAG_AUTH_EXPIRED),
            ErrorDetails::HandshakeFailed { server_code } => {
                write1(writer, TAG_HANDSHAKE_FAILED, server_code)
            }
            ErrorDetails::SyncConnectionFailed => write_unit(writer, TAG_SYNC_CONNECTION_FAILED),
            ErrorDetails::SyncDeltaRejected { compensation } => {
                writer.write_array_len(2)?;
                writer.write_u16(TAG_SYNC_DELTA_REJECTED)?;
                writer.write_map_len(1)?;
                writer.write_u8(1)?;
                compensation.write(writer)
            }
            ErrorDetails::ShapeSubscriptionFailed { shape_id } => {
                write1(writer, TAG_SHAPE_SUBSCRIPTION_FAILED, shape_id)
            }
            ErrorDetails::Storage {
                component,
                op,
                detail,
            } => write3(writer, TAG_STORAGE, component, op, detail),
            ErrorDetails::SegmentCorrupted {
                segment_id,
                corruption,
                detail,
            } => write3(
                writer,
                TAG_SEGMENT_CORRUPTED,
                segment_id,
                corruption,
                detail,
            ),
            ErrorDetails::ColdStorage {
                backend,
                op,
                detail,
            } => write3(writer, TAG_COLD_STORAGE, backend, op, detail),
            ErrorDetails::Wal { stage, detail } => write2(writer, TAG_WAL, stage, detail),
            ErrorDetails::Serialization { format } => write1(writer, TAG_SERIALIZATION, format),
            ErrorDetails::Codec { codec, op, detail } => {
                write3(writer, TAG_CODEC, codec, op, detail)
            }
            ErrorDetails::Config => write_unit(writer, TAG_CONFIG),
            ErrorDetails::BadRequest => write_unit(writer, TAG_BAD_REQUEST),
            ErrorDetails::NoLeader => write_unit(writer, TAG_NO_LEADER),
            ErrorDetails::NotLeader { leader_addr } => write1(writer, TAG_NOT_LEADER, leader_addr),
            ErrorDetails::MigrationInProgress => write_unit(writer, TAG_MIGRATION_IN_PROGRESS),
            ErrorDetails::NodeUnreachable => write_unit(writer, TAG_NODE_UNREACHABLE),
            ErrorDetails::Cluster => write_unit(writer, TAG_CLUSTER),
            ErrorDetails::MemoryExhausted { engine } => {
                write1(writer, TAG_MEMORY_EXHAUSTED, engine)
            }
            ErrorDetails::Encryption { cipher, detail } => {
                write2(writer, TAG_ENCRYPTION, cipher, detail)
            }
            ErrorDetails::Array { array } => write1(writer, TAG_ARRAY, array),
            ErrorDetails::Bridge { plane, op, detail } => {
                write3(writer, TAG_BRIDGE, plane, op, detail)
            }
            ErrorDetails::Dispatch { stage, detail } => write2(writer, TAG_DISPATCH, stage, detail),
            ErrorDetails::Internal { component, detail } => {
                write2(writer, TAG_INTERNAL, component, detail)
            }
            ErrorDetails::TenantVectorDimExceeded { dim, limit } => {
                write2(writer, TAG_TENANT_VECTOR_DIM_EXCEEDED, dim, limit)
            }
            ErrorDetails::TenantGraphDepthExceeded { depth, limit } => {
                write2(writer, TAG_TENANT_GRAPH_DEPTH_EXCEEDED, depth, limit)
            }
            ErrorDetails::QuotaOvercommit { field } => write1(writer, TAG_QUOTA_OVERCOMMIT, field),
            ErrorDetails::QuotaExceeded { scope } => write1(writer, TAG_QUOTA_EXCEEDED, scope),
            ErrorDetails::ServerOverload => write_unit(writer, TAG_SERVER_OVERLOAD),
            ErrorDetails::CloneDepthExceeded { depth, limit } => {
                write2(writer, TAG_CLONE_DEPTH_EXCEEDED, depth, limit)
            }
            ErrorDetails::CannotCloneMirror { database } => {
                write1(writer, TAG_CANNOT_CLONE_MIRROR, database)
            }
            ErrorDetails::CloneDependency { dependents } => {
                writer.write_array_len(2)?;
                writer.write_u16(TAG_CLONE_DEPENDENCY)?;
                writer.write_array_len(dependents.len())?;
                for dep in dependents {
                    dep.write(writer)?;
                }
                Ok(())
            }
            ErrorDetails::ClonePredatesQueryTime {
                as_of_lsn,
                created_at_lsn,
            } => write2(
                writer,
                TAG_CLONE_PREDATES_QUERY_TIME,
                as_of_lsn,
                created_at_lsn,
            ),
            ErrorDetails::CloneWriteRequiresMaterialize {
                collection,
                engine,
                database,
            } => write3(
                writer,
                TAG_CLONE_WRITE_REQUIRES_MATERIALIZE,
                collection,
                engine,
                database,
            ),
            ErrorDetails::MoveTenantDrainTimeout { tenant, source_db } => {
                write2(writer, TAG_MOVE_TENANT_DRAIN_TIMEOUT, tenant, source_db)
            }
            ErrorDetails::MoveTenantPreflightFailed { tenant, detail } => {
                write2(writer, TAG_MOVE_TENANT_PREFLIGHT_FAILED, tenant, detail)
            }
            ErrorDetails::MoveTenantSnapshotFailed { tenant, detail } => {
                write2(writer, TAG_MOVE_TENANT_SNAPSHOT_FAILED, tenant, detail)
            }
            ErrorDetails::MoveTenantCutoverFailed { tenant, detail } => {
                write2(writer, TAG_MOVE_TENANT_CUTOVER_FAILED, tenant, detail)
            }
            ErrorDetails::MoveTenantAlreadyAtTarget { tenant, target_db } => {
                write2(writer, TAG_MOVE_TENANT_ALREADY_AT_TARGET, tenant, target_db)
            }
            ErrorDetails::MirrorReadOnly { database } => {
                write1(writer, TAG_MIRROR_READ_ONLY, database)
            }
            ErrorDetails::StaleReadNotLeader {
                database,
                source_cluster,
            } => write2(writer, TAG_STALE_READ_NOT_LEADER, database, source_cluster),
            ErrorDetails::MirrorNotPromoted { database } => {
                write1(writer, TAG_MIRROR_NOT_PROMOTED, database)
            }
            ErrorDetails::BackupTenantMismatch { expected, actual } => {
                write2(writer, TAG_BACKUP_TENANT_MISMATCH, expected, actual)
            }
            ErrorDetails::BackupKeyMismatch => write_unit(writer, TAG_BACKUP_KEY_MISMATCH),
            ErrorDetails::UndefinedObject { object } => {
                write1(writer, TAG_UNDEFINED_OBJECT, object)
            }
            ErrorDetails::AlreadyExists { object } => write1(writer, TAG_ALREADY_EXISTS, object),
            ErrorDetails::ObjectNotReady { object } => write1(writer, TAG_OBJECT_NOT_READY, object),
            ErrorDetails::NotFound { detail } => write1(writer, TAG_NOT_FOUND, detail),
            ErrorDetails::CannotDropDefaultDatabase => {
                write_unit(writer, TAG_CANNOT_DROP_DEFAULT_DATABASE)
            }
        }
    }
}
