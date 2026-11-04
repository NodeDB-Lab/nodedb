// SPDX-License-Identifier: Apache-2.0

//! Sync (3000s), storage (4000s), serialization (4200s), config (5000s),
//! cluster (6000s), memory (7000s), encryption (8000s), internal (9000s)
//! constructors.

use std::fmt;

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    // ── Protocol handshake ──

    /// Build a `HandshakeFailed` error from a `HelloErrorCode` and the server's
    /// diagnostic message.
    pub fn handshake_failed(
        code: crate::protocol::HelloErrorCode,
        message: impl fmt::Display,
    ) -> Self {
        let server_code = match code {
            crate::protocol::HelloErrorCode::BadMagic => 0,
            crate::protocol::HelloErrorCode::VersionMismatch => 1,
            crate::protocol::HelloErrorCode::Malformed => 2,
        };
        Self {
            code: ErrorCode::HANDSHAKE_FAILED,
            message: format!("protocol handshake failed: {message}"),
            details: ErrorDetails::HandshakeFailed { server_code },
            cause: None,
        }
    }

    // ── Sync ──

    pub fn sync_connection_failed(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SYNC_CONNECTION_FAILED,
            message: format!("sync connection failed: {detail}"),
            details: ErrorDetails::SyncConnectionFailed,
            cause: None,
        }
    }

    pub fn sync_delta_rejected(
        reason: impl fmt::Display,
        compensation: Option<crate::sync::compensation::CompensationHint>,
    ) -> Self {
        Self {
            code: ErrorCode::SYNC_DELTA_REJECTED,
            message: format!("sync delta rejected: {reason}"),
            details: ErrorDetails::SyncDeltaRejected { compensation },
            cause: None,
        }
    }

    pub fn shape_subscription_failed(
        shape_id: impl Into<String>,
        detail: impl fmt::Display,
    ) -> Self {
        let shape_id = shape_id.into();
        Self {
            code: ErrorCode::SHAPE_SUBSCRIPTION_FAILED,
            message: format!("shape subscription failed for '{shape_id}': {detail}"),
            details: ErrorDetails::ShapeSubscriptionFailed { shape_id },
            cause: None,
        }
    }

    // ── Storage ──

    pub fn storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::STORAGE,
            message: format!("storage error: {detail}"),
            details: ErrorDetails::Storage {
                component: "unspecified".into(),
                op: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn storage_op(
        component: impl Into<String>,
        op: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let component = component.into();
        let op = op.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::STORAGE,
            message: format!("storage error [{component}/{op}]: {detail}"),
            details: ErrorDetails::Storage {
                component,
                op,
                detail,
            },
            cause: None,
        }
    }

    pub fn segment_corrupted(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SEGMENT_CORRUPTED,
            message: format!("segment corrupted: {detail}"),
            details: ErrorDetails::SegmentCorrupted {
                segment_id: 0,
                corruption: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn segment_corrupted_at(
        segment_id: u64,
        corruption: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let corruption = corruption.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::SEGMENT_CORRUPTED,
            message: format!("segment {segment_id} corrupted [{corruption}]: {detail}"),
            details: ErrorDetails::SegmentCorrupted {
                segment_id,
                corruption,
                detail,
            },
            cause: None,
        }
    }

    pub fn cold_storage(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::COLD_STORAGE,
            message: format!("cold storage error: {detail}"),
            details: ErrorDetails::ColdStorage {
                backend: "unspecified".into(),
                op: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn cold_storage_op(
        backend: impl Into<String>,
        op: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let backend = backend.into();
        let op = op.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::COLD_STORAGE,
            message: format!("cold storage error [{backend}/{op}]: {detail}"),
            details: ErrorDetails::ColdStorage {
                backend,
                op,
                detail,
            },
            cause: None,
        }
    }

    pub fn wal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::WAL,
            message: format!("WAL error: {detail}"),
            details: ErrorDetails::Wal {
                stage: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn wal_at(stage: impl Into<String>, detail: impl Into<String>) -> Self {
        let stage = stage.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::WAL,
            message: format!("WAL error [{stage}]: {detail}"),
            details: ErrorDetails::Wal { stage, detail },
            cause: None,
        }
    }

    // ── Serialization ──

    pub fn serialization(format: impl Into<String>, detail: impl fmt::Display) -> Self {
        let format = format.into();
        Self {
            code: ErrorCode::SERIALIZATION,
            message: format!("serialization error ({format}): {detail}"),
            details: ErrorDetails::Serialization { format },
            cause: None,
        }
    }

    pub fn codec(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CODEC,
            message: format!("codec error: {detail}"),
            details: ErrorDetails::Codec {
                codec: "unspecified".into(),
                op: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn codec_at(
        codec: impl Into<String>,
        op: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let codec = codec.into();
        let op = op.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::CODEC,
            message: format!("codec error [{codec}/{op}]: {detail}"),
            details: ErrorDetails::Codec { codec, op, detail },
            cause: None,
        }
    }

    // ── Config ──

    pub fn config(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CONFIG,
            message: format!("configuration error: {detail}"),
            details: ErrorDetails::Config,
            cause: None,
        }
    }

    pub fn bad_request(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BAD_REQUEST,
            message: format!("bad request: {detail}"),
            details: ErrorDetails::BadRequest,
            cause: None,
        }
    }

    // ── Cluster ──

    pub fn no_leader(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NO_LEADER,
            message: format!("no serving leader: {detail}"),
            details: ErrorDetails::NoLeader,
            cause: None,
        }
    }

    pub fn not_leader(leader_addr: impl Into<String>) -> Self {
        let leader_addr = leader_addr.into();
        Self {
            code: ErrorCode::NOT_LEADER,
            message: format!("not leader; redirect to leader at {leader_addr}"),
            details: ErrorDetails::NotLeader { leader_addr },
            cause: None,
        }
    }

    pub fn migration_in_progress(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::MIGRATION_IN_PROGRESS,
            message: format!("migration in progress: {detail}"),
            details: ErrorDetails::MigrationInProgress,
            cause: None,
        }
    }

    pub fn node_unreachable(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::NODE_UNREACHABLE,
            message: format!("node unreachable: {detail}"),
            details: ErrorDetails::NodeUnreachable,
            cause: None,
        }
    }

    pub fn cluster(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::CLUSTER,
            message: format!("cluster error: {detail}"),
            details: ErrorDetails::Cluster,
            cause: None,
        }
    }

    // ── Memory ──

    pub fn memory_exhausted(engine: impl Into<String>) -> Self {
        let engine = engine.into();
        Self {
            code: ErrorCode::MEMORY_EXHAUSTED,
            message: format!("memory budget exhausted for engine {engine}"),
            details: ErrorDetails::MemoryExhausted { engine },
            cause: None,
        }
    }

    // ── Encryption ──

    pub fn encryption(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::ENCRYPTION,
            message: format!("encryption error: {detail}"),
            details: ErrorDetails::Encryption {
                cipher: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn encryption_at(cipher: impl Into<String>, detail: impl Into<String>) -> Self {
        let cipher = cipher.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::ENCRYPTION,
            message: format!("encryption error [{cipher}]: {detail}"),
            details: ErrorDetails::Encryption { cipher, detail },
            cause: None,
        }
    }

    // ── Engine: Array ──

    pub fn array(array: impl Into<String>, detail: impl fmt::Display) -> Self {
        let array = array.into();
        Self {
            code: ErrorCode::ARRAY,
            message: format!("array engine error on '{array}': {detail}"),
            details: ErrorDetails::Array { array },
            cause: None,
        }
    }

    // ── Quota ──

    /// Sum of database or tenant quotas would exceed the configured ceiling.
    pub fn quota_overcommit(field: impl Into<String>, detail: impl fmt::Display) -> Self {
        let field = field.into();
        Self {
            code: ErrorCode::QUOTA_OVERCOMMIT,
            message: format!("quota overcommit on field '{field}': {detail}"),
            details: ErrorDetails::QuotaOvercommit { field },
            cause: None,
        }
    }

    /// Request rejected because a tenant or database quota is exhausted.
    pub fn quota_exceeded(scope: impl Into<String>, detail: impl fmt::Display) -> Self {
        let scope = scope.into();
        Self {
            code: ErrorCode::TENANT_QUOTA_EXCEEDED,
            message: format!("quota exceeded for {scope}: {detail}"),
            details: ErrorDetails::QuotaExceeded { scope },
            cause: None,
        }
    }

    /// Write refused because `scope` (a database or tenant) stores `used`
    /// bytes against a `max_storage_bytes` limit of `limit`.
    pub fn storage_quota_exceeded(scope: impl Into<String>, used: u64, limit: u64) -> Self {
        let scope = scope.into();
        Self {
            code: ErrorCode::STORAGE_QUOTA_EXCEEDED,
            message: format!(
                "storage quota exceeded for {scope}: {used} bytes stored, limit {limit} bytes. \
                 Delete data or raise max_storage_bytes"
            ),
            details: ErrorDetails::QuotaExceeded { scope },
            cause: None,
        }
    }

    /// Global resource pressure; server cannot accept the request.
    pub fn server_overload(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::SERVER_OVERLOAD,
            message: format!("server overload: {detail}"),
            details: ErrorDetails::ServerOverload,
            cause: None,
        }
    }

    // ── Bridge / Dispatch / Internal ──

    pub fn bridge(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::BRIDGE,
            message: format!("bridge error: {detail}"),
            details: ErrorDetails::Bridge {
                plane: "unspecified".into(),
                op: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn bridge_op(
        plane: impl Into<String>,
        op: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let plane = plane.into();
        let op = op.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::BRIDGE,
            message: format!("bridge error [{plane}/{op}]: {detail}"),
            details: ErrorDetails::Bridge { plane, op, detail },
            cause: None,
        }
    }

    pub fn dispatch(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::DISPATCH,
            message: format!("dispatch error: {detail}"),
            details: ErrorDetails::Dispatch {
                stage: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn dispatch_at(stage: impl Into<String>, detail: impl Into<String>) -> Self {
        let stage = stage.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::DISPATCH,
            message: format!("dispatch error [{stage}]: {detail}"),
            details: ErrorDetails::Dispatch { stage, detail },
            cause: None,
        }
    }

    pub fn internal(detail: impl fmt::Display) -> Self {
        Self {
            code: ErrorCode::INTERNAL,
            message: format!("internal error: {detail}"),
            details: ErrorDetails::Internal {
                component: "unspecified".into(),
                detail: detail.to_string(),
            },
            cause: None,
        }
    }

    pub fn internal_at(component: impl Into<String>, detail: impl Into<String>) -> Self {
        let component = component.into();
        let detail = detail.into();
        Self {
            code: ErrorCode::INTERNAL,
            message: format!("internal error [{component}]: {detail}"),
            details: ErrorDetails::Internal { component, detail },
            cause: None,
        }
    }
}
