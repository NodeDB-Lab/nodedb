// SPDX-License-Identifier: Apache-2.0

//! Clone catalog types shared between the catalog layer and the control plane.

use serde::{Deserialize, Serialize};

use crate::{DatabaseId, Lsn};

/// Maximum clone depth (source → clone → clone-of-clone …).
///
/// A clone at depth 9 is rejected with `CLONE_DEPTH_EXCEEDED`.
pub const MAX_CLONE_DEPTH: u32 = 8;

/// Identifies the source of a copy-on-write database clone.
///
/// Stored on each cloned `StoredCollection`; the read and write planners
/// consult it to decide whether source delegation is needed.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
#[msgpack(map)]
pub struct CloneOrigin {
    /// The source database this collection was cloned from.
    pub source_database: DatabaseId,
    /// Collection name in the source database (scoped to `source_database`).
    pub source_collection: String,
    /// WAL LSN up to which source rows are delegated on reads.
    /// Rows in the source with LSN > `as_of_lsn` are invisible through
    /// this clone regardless of query time.
    pub as_of_lsn: Lsn,
    /// WAL LSN at the moment this clone was created. Used to detect
    /// bitemporal queries that pre-date the clone.
    pub clone_created_at: Lsn,
    /// Surrogate high-water captured from the source's `SurrogateAssigner`
    /// at clone-create time.  KV bindings allocated AFTER this value
    /// belong strictly to source-side writes that must not be visible
    /// from the resulting clone — the lazy KV read path uses this
    /// ceiling to filter source-delegated rows. `None` means no ceiling.
    pub kv_surrogate_ceiling: Option<u32>,
}

/// Materialization state of a copy-on-write clone.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    Default,
)]
pub enum CloneStatus {
    /// Reads delegate to source up to `as_of_lsn`; writes go to target.
    /// This is the initial state immediately after a `CLONE DATABASE`.
    #[default]
    Shadowed,
    /// Background materializer is copying source rows into target storage.
    /// Reads still delegate to source for rows not yet copied.
    Materializing {
        /// LSN watermark of the materializer's current position in the source.
        progress_lsn: Lsn,
        /// Bytes materialised so far (best-effort estimate).
        bytes_done: u64,
        /// Total bytes to materialise (best-effort estimate; 0 = unknown).
        bytes_total: u64,
    },
    /// All source rows are physically present in target storage.
    /// Source delegation is no longer needed.
    Materialized,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip_msgpack<T>(val: &T) -> T
    where
        T: zerompk::ToMessagePack + for<'a> zerompk::FromMessagePack<'a>,
    {
        let bytes = zerompk::to_msgpack_vec(val).expect("serialize");
        zerompk::from_msgpack(&bytes).expect("deserialize")
    }

    fn round_trip_serde<T>(val: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let json = sonic_rs::to_string(val).expect("serde serialize");
        sonic_rs::from_str(&json).expect("serde deserialize")
    }

    fn sample_origin() -> CloneOrigin {
        CloneOrigin {
            source_database: DatabaseId::DEFAULT,
            source_collection: "users".to_string(),
            as_of_lsn: Lsn::new(42_000),
            clone_created_at: Lsn::new(42_100),
            kv_surrogate_ceiling: Some(7),
        }
    }

    #[test]
    fn clone_status_shadowed_msgpack() {
        let s = CloneStatus::Shadowed;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn clone_status_materializing_msgpack() {
        let s = CloneStatus::Materializing {
            progress_lsn: Lsn::new(1_000),
            bytes_done: 512,
            bytes_total: 1_024,
        };
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn clone_status_materialized_msgpack() {
        let s = CloneStatus::Materialized;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn clone_status_shadowed_serde() {
        let s = CloneStatus::Shadowed;
        assert_eq!(round_trip_serde(&s), s);
    }

    #[test]
    fn clone_status_materializing_serde() {
        let s = CloneStatus::Materializing {
            progress_lsn: Lsn::new(1_000),
            bytes_done: 512,
            bytes_total: 1_024,
        };
        assert_eq!(round_trip_serde(&s), s);
    }

    #[test]
    fn clone_status_materialized_serde() {
        let s = CloneStatus::Materialized;
        assert_eq!(round_trip_serde(&s), s);
    }

    #[test]
    fn clone_origin_msgpack() {
        let o = sample_origin();
        assert_eq!(round_trip_msgpack(&o), o);
    }

    #[test]
    fn clone_origin_serde() {
        let o = sample_origin();
        assert_eq!(round_trip_serde(&o), o);
    }

    #[test]
    fn max_clone_depth_value() {
        assert_eq!(MAX_CLONE_DEPTH, 8);
    }

    #[test]
    fn clone_status_default_is_shadowed() {
        assert_eq!(CloneStatus::default(), CloneStatus::Shadowed);
    }
}
