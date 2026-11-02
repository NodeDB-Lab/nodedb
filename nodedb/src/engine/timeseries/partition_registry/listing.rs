// SPDX-License-Identifier: BUSL-1.1

//! One partition as `SHOW PARTITIONS FOR` reports it.
//!
//! The Data Plane core that owns the collection builds the listing from its
//! registry. The Control Plane decodes it into result rows.

use super::entry::PartitionEntry;

/// One queryable partition of a timeseries collection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionListing {
    /// The partition directory name.
    pub name: String,
    /// The lifecycle state: `Active`, `Sealed` or `Merged`.
    pub state: String,
    /// The earliest row timestamp, in milliseconds.
    pub min_ts: i64,
    /// The latest row timestamp, in milliseconds.
    pub max_ts: i64,
    pub row_count: u64,
    /// The bytes the partition's files take on disk.
    pub size_bytes: u64,
    /// The width of the interval the partition was cut at, in milliseconds.
    /// `0` names a partition of an `UNBOUNDED` collection.
    pub interval_ms: u64,
}

impl PartitionListing {
    /// The listing of `entry`.
    pub fn from_entry(entry: &PartitionEntry) -> Self {
        Self {
            name: entry.dir_name.clone(),
            state: format!("{:?}", entry.meta.state),
            min_ts: entry.meta.min_ts,
            max_ts: entry.meta.max_ts,
            row_count: entry.meta.row_count,
            size_bytes: entry.meta.size_bytes,
            interval_ms: entry.meta.interval_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::{PartitionMeta, PartitionState};

    use super::*;

    #[test]
    fn a_listing_carries_every_reported_field() {
        let entry = PartitionEntry {
            meta: PartitionMeta {
                min_ts: 10,
                max_ts: 20,
                row_count: 3,
                size_bytes: 512,
                schema_version: 1,
                state: PartitionState::Sealed,
                interval_ms: 86_400_000,
                last_flushed_wal_lsn: 0,
                column_stats: std::collections::HashMap::new(),
                max_system_ts: 0,
                ended_at: None,
            },
            dir_name: "ts-a".to_string(),
        };
        let listing = PartitionListing::from_entry(&entry);
        assert_eq!(listing.name, "ts-a");
        assert_eq!(listing.state, "Sealed");
        assert_eq!((listing.min_ts, listing.max_ts), (10, 20));
        assert_eq!(listing.row_count, 3);
        assert_eq!(listing.size_bytes, 512);
        assert_eq!(listing.interval_ms, 86_400_000);
    }
}
