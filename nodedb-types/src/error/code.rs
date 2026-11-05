// SPDX-License-Identifier: Apache-2.0

//! Stable numeric error codes for programmatic error handling.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Stable numeric error codes for programmatic error handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ErrorCode(pub u16);

/// Defines each named code once, and [`ErrorCode::ALL`] from the same list,
/// so the list of every code cannot miss a constant.
macro_rules! error_codes {
    ( $( $(#[$meta:meta])* $name:ident = $value:literal; )* ) => {
        impl ErrorCode {
            $(
                $(#[$meta])*
                pub const $name: Self = Self($value);
            )*

            /// Every named code, in declaration order.
            pub const ALL: &'static [ErrorCode] = &[ $( Self::$name ),* ];
        }
    };
}

error_codes! {
    // Write path (1000–1099)
    CONSTRAINT_VIOLATION = 1000;
    WRITE_CONFLICT = 1001;
    DEADLINE_EXCEEDED = 1002;
    PREVALIDATION_REJECTED = 1003;
    APPEND_ONLY_VIOLATION = 1010;
    BALANCE_VIOLATION = 1011;
    PERIOD_LOCKED = 1012;
    STATE_TRANSITION_VIOLATION = 1013;
    TRANSITION_CHECK_VIOLATION = 1014;
    RETENTION_VIOLATION = 1015;
    LEGAL_HOLD_ACTIVE = 1016;
    /// A period-lock reference row exists but does not carry the
    /// configured `status_column` — a misconfigured column name, not a
    /// locked period.
    PERIOD_LOCK_MISCONFIGURED = 1017;
    TYPE_MISMATCH = 1020;
    OVERFLOW = 1021;
    INSUFFICIENT_BALANCE = 1022;
    RATE_EXCEEDED = 1023;
    TYPE_GUARD_VIOLATION = 1024;
    /// A transaction rolled back for a reason other than a serialization
    /// conflict, such as a participant error. The client retries it.
    TRANSACTION_ROLLBACK = 1030;
    /// The statement cannot run inside an explicit transaction block.
    ACTIVE_SQL_TRANSACTION = 1031;

    // Read path (1100–1199)
    COLLECTION_NOT_FOUND = 1100;
    DOCUMENT_NOT_FOUND = 1101;
    COLLECTION_DRAINING = 1102;
    COLLECTION_DEACTIVATED = 1103;
    /// The named database does not exist.
    DATABASE_NOT_FOUND = 1110;
    /// A named catalog object (type, role, index, alert, …) other than a
    /// collection or database does not exist. Generic: use the object name
    /// in the message for specifics.
    UNDEFINED_OBJECT = 1111;
    /// A named catalog object already exists under that name.
    ALREADY_EXISTS = 1112;
    /// The target object exists but is not in a state that accepts this
    /// operation (locked, busy, mid-transition).
    OBJECT_NOT_READY = 1113;
    /// A requested value/record does not exist. Generic: use for lookups
    /// that don't fit `DOCUMENT_NOT_FOUND`'s collection/id shape.
    NOT_FOUND = 1114;
    /// A drop or revoke refused because other objects still depend on the
    /// named object.
    DEPENDENT_OBJECTS_EXIST = 1115;

    // Query (1200–1299)
    PLAN_ERROR = 1200;
    SQL_NOT_ENABLED = 1202;
    /// A function call names no registered scalar/aggregate/window function.
    UNDEFINED_FUNCTION = 1203;
    /// Expression evaluation divided or took a modulus by zero.
    DIVISION_BY_ZERO = 1204;
    /// A LIMIT/OFFSET/FETCH bound resolved outside `[0, usize::MAX]`.
    INVALID_LIMIT_VALUE = 1205;
    /// A column reference names no column of any relation in scope.
    UNDEFINED_COLUMN = 1206;
    /// A bare column name resolves against more than one relation in scope.
    AMBIGUOUS_COLUMN = 1207;
    /// A function received a value it cannot compute on: a vector of the
    /// wrong dimension, an argument of the wrong shape, a malformed path.
    DATA_EXCEPTION = 1208;
    /// A statement exceeded a server limit on its own size or depth: a
    /// recursion depth, a per-transaction staging budget.
    PROGRAM_LIMIT_EXCEEDED = 1209;

    // Engine ops (1300–1399)
    ARRAY = 1300;

    // Quota (1400–1499)

    /// The proposed quota allocation would push the sum of all database quotas
    /// past the configured global ceiling, or the sum of all tenant quotas past
    /// the database ceiling.
    QUOTA_OVERCOMMIT = 1400;
    /// A request was rejected because the calling tenant has exhausted its quota
    /// (QPS, memory, connections, or storage).
    TENANT_QUOTA_EXCEEDED = 1401;
    /// A request was rejected because the target database has exhausted its quota.
    DATABASE_QUOTA_EXCEEDED = 1402;
    /// The server is under global resource pressure and cannot accept new requests.
    SERVER_OVERLOAD = 1403;
    /// A write was refused because the database or tenant has stored bytes at
    /// or over its `max_storage_bytes` quota.
    STORAGE_QUOTA_EXCEEDED = 1404;

    // Clone (1500–1599)

    /// A `CLONE DATABASE` would exceed the maximum clone chain depth of 8.
    CLONE_DEPTH_EXCEEDED = 1500;
    /// A mirror database cannot be cloned; promote the mirror first.
    CANNOT_CLONE_MIRROR = 1501;
    /// The source database cannot be dropped while clones depend on it.
    CLONE_DEPENDENCY = 1502;
    /// A bitemporal `AS OF` query timestamp predates the clone's creation LSN.
    CLONE_PREDATES_QUERY_TIME = 1503;
    /// A write targeted a `Shadowed`/`Materializing` clone collection whose
    /// engine has no copy-on-write support; `MATERIALIZE` the clone first.
    CLONE_WRITE_REQUIRES_MATERIALIZE = 1504;

    // Mirror (1700–1799)

    /// Write attempted on a mirror database that has not yet been promoted.
    MIRROR_READ_ONLY = 1700;
    /// Strong consistency read requested on a mirror; mirrors cannot serve
    /// strong reads. The client should retry against the source cluster.
    STALE_READ_NOT_LEADER = 1701;
    /// Operation requires the mirror to be promoted, but it has not been.
    MIRROR_NOT_PROMOTED = 1702;
    /// `DROP DATABASE` targeted the built-in `default` database, which is
    /// immutable. Shares SQLSTATE `0A000` with `SQL_NOT_ENABLED` and
    /// `CANNOT_CLONE_MIRROR`, so it must be constructed explicitly rather
    /// than derived from the bare SQLSTATE string.
    CANNOT_DROP_DEFAULT_DATABASE = 1710;

    // Move Tenant (1600–1699)

    /// `MOVE TENANT` drain phase timed out; source left unchanged.
    MOVE_TENANT_DRAIN_TIMEOUT = 1600;
    /// `MOVE TENANT` pre-flight failed; collection schema incompatibility.
    MOVE_TENANT_PREFLIGHT_FAILED = 1601;
    /// `MOVE TENANT` snapshot phase failed; source left unchanged.
    MOVE_TENANT_SNAPSHOT_FAILED = 1602;
    /// `MOVE TENANT` cutover phase failed; source still holds the data.
    MOVE_TENANT_CUTOVER_FAILED = 1603;
    /// Tenant is already at the target database; `MOVE TENANT` was a no-op.
    MOVE_TENANT_ALREADY_AT_TARGET = 1604;

    // Backup / Restore (1800–1899)
    /// RESTORE targeted a tenant different from the one the envelope belongs to.
    BACKUP_TENANT_MISMATCH = 1800;
    /// Backup envelope did not decrypt under this server's configured backup KEK.
    BACKUP_KEY_MISMATCH = 1801;

    // Auth / Security (2000–2099)
    AUTHORIZATION_DENIED = 2000;
    AUTH_EXPIRED = 2001;
    /// Authentication failed: a wrong password, an unknown user, or missing
    /// credentials. One code for all of them, so a client cannot tell them
    /// apart.
    AUTHENTICATION_FAILED = 2002;
    /// Vector insert or index rejected because the vector dimension exceeds the
    /// tenant's `max_vector_dim` quota.
    TENANT_VECTOR_DIM_EXCEEDED = 2010;
    /// Graph traversal rejected because the requested depth exceeds the tenant's
    /// `max_graph_depth` quota.
    TENANT_GRAPH_DEPTH_EXCEEDED = 2011;

    // Protocol handshake (2100–2199)
    HANDSHAKE_FAILED = 2100;

    // Sync (3000–3099)
    SYNC_CONNECTION_FAILED = 3000;
    SYNC_DELTA_REJECTED = 3001;
    SHAPE_SUBSCRIPTION_FAILED = 3002;

    // Storage (4000–4099)
    STORAGE = 4000;
    SEGMENT_CORRUPTED = 4001;
    COLD_STORAGE = 4002;

    // WAL (4100–4199)
    WAL = 4100;

    // Serialization (4200–4299)
    SERIALIZATION = 4200;
    CODEC = 4201;

    // Config (5000–5099)
    CONFIG = 5000;
    BAD_REQUEST = 5001;

    // Cluster (6000–6099)
    NO_LEADER = 6000;
    NOT_LEADER = 6001;
    MIGRATION_IN_PROGRESS = 6002;
    NODE_UNREACHABLE = 6003;
    CLUSTER = 6010;

    // Memory (7000–7099)
    MEMORY_EXHAUSTED = 7000;

    // Encryption (8000–8099)
    ENCRYPTION = 8000;

    // Internal (9000–9099)
    INTERNAL = 9000;
    BRIDGE = 9001;
    DISPATCH = 9002;
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

    #[test]
    fn every_code_has_a_distinct_value() {
        let mut seen = std::collections::HashSet::new();
        for code in ErrorCode::ALL {
            assert!(seen.insert(code.0), "{code} is defined twice");
        }
        assert!(ErrorCode::ALL.contains(&ErrorCode::DATABASE_QUOTA_EXCEEDED));
        assert!(ErrorCode::ALL.contains(&ErrorCode::DISPATCH));
    }
}
