// SPDX-License-Identifier: Apache-2.0

//! Stable numeric error codes for programmatic error handling.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable numeric error codes for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    // Write path (1000–1099)
    pub const CONSTRAINT_VIOLATION: Self = Self(1000);
    pub const WRITE_CONFLICT: Self = Self(1001);
    pub const DEADLINE_EXCEEDED: Self = Self(1002);
    pub const PREVALIDATION_REJECTED: Self = Self(1003);
    pub const APPEND_ONLY_VIOLATION: Self = Self(1010);
    pub const BALANCE_VIOLATION: Self = Self(1011);
    pub const PERIOD_LOCKED: Self = Self(1012);
    pub const STATE_TRANSITION_VIOLATION: Self = Self(1013);
    pub const TRANSITION_CHECK_VIOLATION: Self = Self(1014);
    pub const RETENTION_VIOLATION: Self = Self(1015);
    pub const LEGAL_HOLD_ACTIVE: Self = Self(1016);
    pub const TYPE_MISMATCH: Self = Self(1020);
    pub const OVERFLOW: Self = Self(1021);
    pub const INSUFFICIENT_BALANCE: Self = Self(1022);
    pub const RATE_EXCEEDED: Self = Self(1023);
    pub const TYPE_GUARD_VIOLATION: Self = Self(1024);

    // Read path (1100–1199)
    pub const COLLECTION_NOT_FOUND: Self = Self(1100);
    pub const DOCUMENT_NOT_FOUND: Self = Self(1101);
    pub const COLLECTION_DRAINING: Self = Self(1102);
    pub const COLLECTION_DEACTIVATED: Self = Self(1103);
    /// The named database does not exist.
    pub const DATABASE_NOT_FOUND: Self = Self(1110);
    /// A named catalog object (type, role, index, alert, …) other than a
    /// collection or database does not exist. Generic: use the object name
    /// in the message for specifics.
    pub const UNDEFINED_OBJECT: Self = Self(1111);
    /// A named catalog object already exists under that name.
    pub const ALREADY_EXISTS: Self = Self(1112);
    /// The target object exists but is not in a state that accepts this
    /// operation (locked, busy, mid-transition).
    pub const OBJECT_NOT_READY: Self = Self(1113);
    /// A requested value/record does not exist. Generic: use for lookups
    /// that don't fit `DOCUMENT_NOT_FOUND`'s collection/id shape.
    pub const NOT_FOUND: Self = Self(1114);

    // Query (1200–1299)
    pub const PLAN_ERROR: Self = Self(1200);
    pub const FAN_OUT_EXCEEDED: Self = Self(1201);
    pub const SQL_NOT_ENABLED: Self = Self(1202);
    /// A function call names no registered scalar/aggregate/window function.
    pub const UNDEFINED_FUNCTION: Self = Self(1203);
    /// Expression evaluation divided or took a modulus by zero.
    pub const DIVISION_BY_ZERO: Self = Self(1204);
    /// A LIMIT/OFFSET/FETCH bound resolved outside `[0, usize::MAX]`.
    pub const INVALID_LIMIT_VALUE: Self = Self(1205);
    /// A NOT NULL column received an explicit NULL or no value at all.
    pub const NOT_NULL_VIOLATION: Self = Self(1207);

    // Engine ops (1300–1399)
    pub const ARRAY: Self = Self(1300);

    // Quota (1400–1499)

    /// The proposed quota allocation would push the sum of all database quotas
    /// past the configured global ceiling, or the sum of all tenant quotas past
    /// the database ceiling.
    pub const QUOTA_OVERCOMMIT: Self = Self(1400);
    /// A request was rejected because the calling tenant has exhausted its quota
    /// (QPS, memory, connections, or storage).
    pub const TENANT_QUOTA_EXCEEDED: Self = Self(1401);
    /// A request was rejected because the target database has exhausted its quota.
    pub const DATABASE_QUOTA_EXCEEDED: Self = Self(1402);
    /// The server is under global resource pressure and cannot accept new requests.
    pub const SERVER_OVERLOAD: Self = Self(1403);

    // Clone (1500–1599)

    /// A `CLONE DATABASE` would exceed the maximum clone chain depth of 8.
    pub const CLONE_DEPTH_EXCEEDED: Self = Self(1500);
    /// A mirror database cannot be cloned; promote the mirror first.
    pub const CANNOT_CLONE_MIRROR: Self = Self(1501);
    /// The source database cannot be dropped while clones depend on it.
    pub const CLONE_DEPENDENCY: Self = Self(1502);
    /// A bitemporal `AS OF` query timestamp predates the clone's creation LSN.
    pub const CLONE_PREDATES_QUERY_TIME: Self = Self(1503);
    /// A write targeted a `Shadowed`/`Materializing` clone collection whose
    /// engine has no copy-on-write support; `MATERIALIZE` the clone first.
    pub const CLONE_WRITE_REQUIRES_MATERIALIZE: Self = Self(1504);

    // Mirror (1700–1799)

    /// Write attempted on a mirror database that has not yet been promoted.
    pub const MIRROR_READ_ONLY: Self = Self(1700);
    /// Strong consistency read requested on a mirror; mirrors cannot serve
    /// strong reads. The client should retry against the source cluster.
    pub const STALE_READ_NOT_LEADER: Self = Self(1701);
    /// Operation requires the mirror to be promoted, but it has not been.
    pub const MIRROR_NOT_PROMOTED: Self = Self(1702);
    /// `DROP DATABASE` targeted the built-in `default` database, which is
    /// immutable. Shares SQLSTATE `0A000` with `SQL_NOT_ENABLED` and
    /// `CANNOT_CLONE_MIRROR`, so it must be constructed explicitly rather
    /// than derived from the bare SQLSTATE string.
    pub const CANNOT_DROP_DEFAULT_DATABASE: Self = Self(1710);

    // Move Tenant (1600–1699)

    /// `MOVE TENANT` drain phase timed out; source left unchanged.
    pub const MOVE_TENANT_DRAIN_TIMEOUT: Self = Self(1600);
    /// `MOVE TENANT` pre-flight failed; collection schema incompatibility.
    pub const MOVE_TENANT_PREFLIGHT_FAILED: Self = Self(1601);
    /// `MOVE TENANT` snapshot phase failed; source left unchanged.
    pub const MOVE_TENANT_SNAPSHOT_FAILED: Self = Self(1602);
    /// `MOVE TENANT` cutover phase failed; source still holds the data.
    pub const MOVE_TENANT_CUTOVER_FAILED: Self = Self(1603);
    /// Tenant is already at the target database; `MOVE TENANT` was a no-op.
    pub const MOVE_TENANT_ALREADY_AT_TARGET: Self = Self(1604);

    // Backup / Restore (1800–1899)
    /// RESTORE targeted a tenant different from the one the envelope belongs to.
    pub const BACKUP_TENANT_MISMATCH: Self = Self(1800);
    /// Backup envelope did not decrypt under this server's configured backup KEK.
    pub const BACKUP_KEY_MISMATCH: Self = Self(1801);

    // Auth / Security (2000–2099)
    pub const AUTHORIZATION_DENIED: Self = Self(2000);
    pub const AUTH_EXPIRED: Self = Self(2001);
    /// Vector insert or index rejected because the vector dimension exceeds the
    /// tenant's `max_vector_dim` quota.
    pub const TENANT_VECTOR_DIM_EXCEEDED: Self = Self(2010);
    /// Graph traversal rejected because the requested depth exceeds the tenant's
    /// `max_graph_depth` quota.
    pub const TENANT_GRAPH_DEPTH_EXCEEDED: Self = Self(2011);

    // Protocol handshake (2100–2199)
    pub const HANDSHAKE_FAILED: Self = Self(2100);

    // Sync (3000–3099)
    pub const SYNC_CONNECTION_FAILED: Self = Self(3000);
    pub const SYNC_DELTA_REJECTED: Self = Self(3001);
    pub const SHAPE_SUBSCRIPTION_FAILED: Self = Self(3002);

    // Storage (4000–4099)
    pub const STORAGE: Self = Self(4000);
    pub const SEGMENT_CORRUPTED: Self = Self(4001);
    pub const COLD_STORAGE: Self = Self(4002);

    // WAL (4100–4199)
    pub const WAL: Self = Self(4100);

    // Serialization (4200–4299)
    pub const SERIALIZATION: Self = Self(4200);
    pub const CODEC: Self = Self(4201);

    // Config (5000–5099)
    pub const CONFIG: Self = Self(5000);
    pub const BAD_REQUEST: Self = Self(5001);

    // Cluster (6000–6099)
    pub const NO_LEADER: Self = Self(6000);
    pub const NOT_LEADER: Self = Self(6001);
    pub const MIGRATION_IN_PROGRESS: Self = Self(6002);
    pub const NODE_UNREACHABLE: Self = Self(6003);
    pub const CLUSTER: Self = Self(6010);

    // Memory (7000–7099)
    pub const MEMORY_EXHAUSTED: Self = Self(7000);

    // Encryption (8000–8099)
    pub const ENCRYPTION: Self = Self(8000);

    // Internal (9000–9099)
    pub const INTERNAL: Self = Self(9000);
    pub const BRIDGE: Self = Self(9001);
    pub const DISPATCH: Self = Self(9002);
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NDB-{:04}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(ErrorCode::CONSTRAINT_VIOLATION.to_string(), "NDB-1000");
        assert_eq!(ErrorCode::INTERNAL.to_string(), "NDB-9000");
        assert_eq!(ErrorCode::WAL.to_string(), "NDB-4100");
    }
}
