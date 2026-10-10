// SPDX-License-Identifier: Apache-2.0

//! The version a committed write gives the rows it writes.
//!
//! A write that applies a data-group Raft entry takes the entry's log
//! position. Every replica applies the entry at the same position, so a
//! version read on one replica compares with the versions every other
//! replica holds.

use serde::{Deserialize, Serialize};

/// The position of the write that produced a row version, within one vShard.
///
/// A write that applies a data-group entry records `epoch`, the epoch at
/// which its vShard moved to the entry's group, and `index`, the entry's log
/// index. `local` is `0`.
///
/// A write that applies no entry records its WAL LSN in `local`, on top of
/// the latest version its vShard holds on the core. Such a write exists on
/// one node only: a single-node write, or a local-only write on a cluster
/// node. It sorts after every version its vShard already holds and before
/// the next entry the vShard applies.
///
/// Versions compare only within one vShard. The writes of one vShard are
/// totally ordered by `(epoch, index, local)`.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct WriteVersion {
    /// The epoch at which the vShard moved to the group that applied the
    /// write. `0` while the vShard stays in its initial group.
    pub epoch: u64,
    /// The log index of the data-group entry that applied the write.
    pub index: u64,
    /// The WAL LSN of a write that applies no entry, `0` otherwise.
    pub local: u64,
}

impl WriteVersion {
    /// The version below every write.
    pub const ZERO: Self = Self {
        epoch: 0,
        index: 0,
        local: 0,
    };

    /// The version of a write that applies the entry at `index`, in `epoch`.
    pub const fn logged(epoch: u64, index: u64) -> Self {
        Self {
            epoch,
            index,
            local: 0,
        }
    }

    /// The version of a write that applies no entry, at WAL LSN `lsn`, on a
    /// vShard whose latest version is `latest`.
    pub const fn local_after(latest: Self, lsn: u64) -> Self {
        Self {
            epoch: latest.epoch,
            index: latest.index,
            local: lsn,
        }
    }

    /// How far `self` sits above `older` in one epoch: the log entries
    /// between them, or the WAL LSNs between two writes of one entry
    /// position. `None` across epochs, or when `older` is not below `self`.
    pub fn distance_above(self, older: Self) -> Option<u64> {
        if self.epoch != older.epoch || self <= older {
            return None;
        }
        if self.index > older.index {
            return Some(self.index - older.index);
        }
        Some(self.local.saturating_sub(older.local))
    }
}

/// One vShard's write version.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ShardVersion {
    /// The vShard whose history positions `version`.
    pub vshard: u32,
    pub version: WriteVersion,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_entry_sorts_after_a_local_write_of_the_earlier_one() {
        let entry = WriteVersion::logged(0, 10);
        let local = WriteVersion::local_after(entry, 900);
        let next = WriteVersion::logged(0, 11);
        assert!(entry < local);
        assert!(local < next);
    }

    #[test]
    fn a_later_epoch_sorts_after_every_index_of_an_earlier_one() {
        assert!(WriteVersion::logged(0, 1_000) < WriteVersion::logged(5, 1));
    }

    #[test]
    fn distance_counts_entries_then_wal_lsns_within_one_epoch() {
        let base = WriteVersion::logged(2, 10);
        assert_eq!(WriteVersion::logged(2, 15).distance_above(base), Some(5));
        let local = WriteVersion::local_after(base, 40);
        assert_eq!(
            WriteVersion::local_after(base, 90).distance_above(local),
            Some(50)
        );
        assert_eq!(WriteVersion::logged(3, 1).distance_above(base), None);
        assert_eq!(base.distance_above(WriteVersion::logged(2, 12)), None);
    }

    #[test]
    fn versions_round_trip_through_msgpack() {
        let version = ShardVersion {
            vshard: 7,
            version: WriteVersion {
                epoch: 3,
                index: 41,
                local: 9,
            },
        };
        let bytes = zerompk::to_msgpack_vec(&version).expect("encode");
        let decoded: ShardVersion = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, version);
    }
}
