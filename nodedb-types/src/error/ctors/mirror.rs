// SPDX-License-Identifier: Apache-2.0

//! Mirror error constructors (1700-range).

use super::super::code::ErrorCode;
use super::super::details::ErrorDetails;
use super::super::types::NodeDbError;

impl NodeDbError {
    /// Write was rejected because the target database is an un-promoted mirror.
    ///
    /// The mirror is read-only until `ALTER DATABASE <name> PROMOTE` is issued.
    pub fn mirror_read_only(database: impl Into<String>) -> Self {
        let database = database.into();
        Self {
            code: ErrorCode::MIRROR_READ_ONLY,
            message: format!(
                "database '{database}' is a read-only mirror; promote it before writing"
            ),
            details: ErrorDetails::MirrorReadOnly { database },
            cause: None,
        }
    }

    /// Strong-consistency read was requested on a mirror database.
    ///
    /// Mirrors cannot serve strong reads; the client should redirect to
    /// `source_cluster`.
    pub fn stale_read_not_leader(
        database: impl Into<String>,
        source_cluster: impl Into<String>,
    ) -> Self {
        let database = database.into();
        let source_cluster = source_cluster.into();
        Self {
            code: ErrorCode::STALE_READ_NOT_LEADER,
            message: format!(
                "database '{database}' is a mirror and cannot serve strong-consistency reads; \
                 redirect to source cluster '{source_cluster}', or read at 'eventual' or \
                 'bounded_staleness:<secs>': set default_read_consistency on pgwire and native, \
                 or the read_consistency field on HTTP and WebSocket"
            ),
            details: ErrorDetails::StaleReadNotLeader {
                database,
                source_cluster,
            },
            cause: None,
        }
    }

    /// Operation requires the database to already be a promoted mirror.
    pub fn mirror_not_promoted(database: impl Into<String>) -> Self {
        let database = database.into();
        Self {
            code: ErrorCode::MIRROR_NOT_PROMOTED,
            message: format!(
                "database '{database}' is not a promoted mirror; \
                 run `ALTER DATABASE {database} PROMOTE` first"
            ),
            details: ErrorDetails::MirrorNotPromoted { database },
            cause: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_read_only_code() {
        let e = NodeDbError::mirror_read_only("replica_eu");
        assert_eq!(e.code(), ErrorCode::MIRROR_READ_ONLY);
        assert!(e.message().contains("replica_eu"));
    }

    #[test]
    fn stale_read_not_leader_code() {
        let e = NodeDbError::stale_read_not_leader("replica_eu", "prod-us-cluster");
        assert_eq!(e.code(), ErrorCode::STALE_READ_NOT_LEADER);
        assert!(e.message().contains("prod-us-cluster"));
        assert!(e.message().contains("replica_eu"));
    }

    #[test]
    fn mirror_not_promoted_code() {
        let e = NodeDbError::mirror_not_promoted("replica_eu");
        assert_eq!(e.code(), ErrorCode::MIRROR_NOT_PROMOTED);
        assert!(e.message().contains("replica_eu"));
    }
}
