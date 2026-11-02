// SPDX-License-Identifier: BUSL-1.1

//! One ended timeseries partition a RESTORE installs: a
//! `RecordType::TimeseriesBatch` sub-record.
//!
//! A backup keeps the partitions a TRUNCATE ended, so a read at a cut
//! before the end sees their rows after a restore as it did before. The
//! sub-record carries the partition's files and its meta, `ended_at` set.
//! Its apply (`CoreLoop::install_ts_ended_partition`) files the partition
//! beside the live ones under a directory its identity names, so applying
//! it twice installs it once, and a replay after a restart is a no-op.
//!
//! The payload is the pair
//! `("timeseries_ended_partition", TimeseriesEndedPartitionRedo)`.

use nodedb_types::timeseries::PartitionMeta;

/// The discriminator an ended-partition payload opens with.
pub const TIMESERIES_ENDED_PARTITION: &str = "timeseries_ended_partition";

/// One ended partition of one timeseries collection.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct TimeseriesEndedPartitionRedo {
    /// The collection, as the destination Data Plane stores it.
    pub collection: String,
    /// The partition's meta. `ended_at` is set.
    pub meta: PartitionMeta,
    /// Every segment file of the partition, plaintext in the segment
    /// format: `(file name, bytes)`. The install stores each under its
    /// core's segment key. Neither
    /// `partition.meta` nor `replay.stamp` is among them: the install writes
    /// the meta, and a source stamp names records of the source's WAL.
    pub files: Vec<(String, Vec<u8>)>,
}

impl TimeseriesEndedPartitionRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(TIMESERIES_ENDED_PARTITION, self)).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal ended partition of '{}': {e}", self.collection),
            }
        })
    }

    /// Decode a sub-record payload. `None` when it is no ended partition.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == TIMESERIES_ENDED_PARTITION).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::timeseries::PartitionState;

    fn meta() -> PartitionMeta {
        PartitionMeta {
            min_ts: 10,
            max_ts: 20,
            row_count: 2,
            size_bytes: 3,
            schema_version: 1,
            state: PartitionState::Sealed,
            interval_ms: 0,
            last_flushed_wal_lsn: 0,
            column_stats: std::collections::HashMap::new(),
            max_system_ts: 7,
            ended_at: Some(9),
        }
    }

    #[test]
    fn an_ended_partition_round_trips() {
        let redo = TimeseriesEndedPartitionRedo {
            collection: "cpu".into(),
            meta: meta(),
            files: vec![("value.col".into(), vec![1, 2, 3])],
        };
        let bytes = redo.encode().expect("encode");
        let back = TimeseriesEndedPartitionRedo::decode(&bytes).expect("decode");
        assert_eq!(back.collection, "cpu");
        assert_eq!(back.meta.ended_at, Some(9));
        assert_eq!(back.files, redo.files);
        assert!(crate::wal::TimeseriesRestoreRedo::decode(&bytes).is_none());
    }
}
