// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::status::MirrorStatus;
use crate::DatabaseId;

/// Identifies the source of a mirror database.
///
/// Stored on `DatabaseDescriptor.mirror_origin`; the read planner and write
/// rejector consult it to enforce mirror semantics.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorOrigin {
    /// The cluster that the source database lives in.
    pub source_cluster: String,
    /// The source database being mirrored.
    pub source_database: DatabaseId,
    /// The name `MIRROR DATABASE` gave the source database.
    pub source_database_name: String,
    /// Replication mode: whether the source waits for mirror ack.
    pub mode: MirrorMode,
    /// Current mirror lifecycle status.
    pub status: MirrorStatus,
}

/// Replication mode for a mirror database.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MirrorMode {
    /// Source waits for mirror ack before commit. Strict latency cost;
    /// not recommended cross-region.
    Sync,
    /// Mirror trails source; `MirrorCutState` gives its lag.
    Async,
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{round_trip_msgpack, round_trip_serde};
    use super::*;

    fn sample_origin() -> MirrorOrigin {
        MirrorOrigin {
            source_cluster: "prod-us".to_string(),
            source_database: DatabaseId::DEFAULT,
            source_database_name: "orders".to_string(),
            mode: MirrorMode::Async,
            status: MirrorStatus::Following,
        }
    }

    #[test]
    fn mirror_mode_msgpack_roundtrip() {
        assert_eq!(round_trip_msgpack(&MirrorMode::Sync), MirrorMode::Sync);
        assert_eq!(round_trip_msgpack(&MirrorMode::Async), MirrorMode::Async);
    }

    #[test]
    fn mirror_mode_serde_roundtrip() {
        assert_eq!(round_trip_serde(&MirrorMode::Sync), MirrorMode::Sync);
        assert_eq!(round_trip_serde(&MirrorMode::Async), MirrorMode::Async);
    }

    #[test]
    fn mirror_origin_msgpack_roundtrip() {
        let o = sample_origin();
        assert_eq!(round_trip_msgpack(&o), o);
    }

    #[test]
    fn mirror_origin_serde_roundtrip() {
        let o = sample_origin();
        assert_eq!(round_trip_serde(&o), o);
    }
}
