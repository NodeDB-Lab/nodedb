// SPDX-License-Identifier: BUSL-1.1

//! The part of one committed record an artifact holds.
//!
//! A [`ReplayStamp`] names whole records. A batched install of a large
//! committed record flushes a memtable between two of its batches, so the
//! artifact holds the record's ops below the batch's end and none above. A
//! [`PartialRecord`] names that part: the record's LSN, and the index of
//! the first op the artifact does not hold. Restart replay applies the
//! record's other ops.
//!
//! A [`PartialRecords`] holds one entry per record, its largest
//! `ops_through`. An entry goes once a stamp names its record whole.

use serde::{Deserialize, Serialize};

use super::replay_stamp::ReplayStamp;

/// The ops `[0, ops_through)` of the record at `lsn`.
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
pub struct PartialRecord {
    pub lsn: u64,
    /// The index of the first op the artifact does not hold.
    pub ops_through: u64,
}

/// The records an artifact holds in part, ascending by LSN, one entry per
/// record.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PartialRecords {
    records: Vec<PartialRecord>,
}

/// Why a decoded [`PartialRecords`] cannot describe an artifact.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPartialRecords {
    /// An entry holds no op.
    #[error("the partial record at lsn {lsn} holds no op")]
    Empty { lsn: u64 },
    /// Two entries name one record, or are out of order.
    #[error("the partial record at lsn {lsn} does not follow the one at lsn {previous}")]
    OutOfOrder { previous: u64, lsn: u64 },
}

impl PartialRecords {
    /// The set naming `record` alone. A record with no op names nothing.
    pub fn naming(record: PartialRecord) -> Self {
        if record.ops_through == 0 {
            return Self::default();
        }
        Self {
            records: vec![record],
        }
    }

    /// Whether the set names no record.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The entries, ascending by LSN.
    pub fn records(&self) -> &[PartialRecord] {
        &self.records
    }

    /// Whether restart replay skips op `op` of the record at `lsn`: the
    /// artifact holds it.
    pub fn skips(&self, lsn: u64, op: u64) -> bool {
        self.records
            .binary_search_by_key(&lsn, |record| record.lsn)
            .is_ok_and(|at| op < self.records[at].ops_through)
    }

    /// The set naming every op `self` or `other` names: the largest
    /// `ops_through` of each record.
    pub fn union(&self, other: &PartialRecords) -> PartialRecords {
        let mut all: Vec<PartialRecord> =
            self.records.iter().chain(&other.records).copied().collect();
        all.sort_unstable_by_key(|record| record.lsn);
        let mut records: Vec<PartialRecord> = Vec::with_capacity(all.len());
        for record in all {
            match records.last_mut() {
                Some(last) if last.lsn == record.lsn => {
                    last.ops_through = last.ops_through.max(record.ops_through);
                }
                _ => records.push(record),
            }
        }
        PartialRecords { records }
    }

    /// The set without the records `whole` names: at or below its prefix,
    /// or in its applied ranges. The artifact holds those whole.
    pub fn pruned(mut self, whole: &ReplayStamp) -> PartialRecords {
        self.records.retain(|record| !whole.skips(record.lsn));
        self
    }

    /// Check the shape `skips` relies on: ascending LSNs, one entry per
    /// record, each holding at least one op.
    pub fn validate(&self) -> Result<(), InvalidPartialRecords> {
        let mut previous: Option<u64> = None;
        for record in &self.records {
            if record.ops_through == 0 {
                return Err(InvalidPartialRecords::Empty { lsn: record.lsn });
            }
            if let Some(previous) = previous
                && record.lsn <= previous
            {
                return Err(InvalidPartialRecords::OutOfOrder {
                    previous,
                    lsn: record.lsn,
                });
            }
            previous = Some(record.lsn);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partial(lsn: u64, ops_through: u64) -> PartialRecords {
        PartialRecords::naming(PartialRecord { lsn, ops_through })
    }

    #[test]
    fn a_partial_stamp_skips_exactly_the_flushed_ops() {
        let stamp = partial(20, 3).union(&partial(9, 1));
        assert!(stamp.skips(20, 0) && stamp.skips(20, 2));
        assert!(!stamp.skips(20, 3), "op 3 is the first op not flushed");
        assert!(stamp.skips(9, 0) && !stamp.skips(9, 1));
        assert!(
            !stamp.skips(10, 0),
            "a record the set does not name replays"
        );
        assert!(stamp.validate().is_ok());
    }

    #[test]
    fn a_union_keeps_the_largest_flushed_prefix_of_each_record() {
        let union = partial(20, 3).union(&partial(20, 7)).union(&partial(30, 2));
        assert_eq!(
            union.records(),
            [
                PartialRecord {
                    lsn: 20,
                    ops_through: 7
                },
                PartialRecord {
                    lsn: 30,
                    ops_through: 2
                }
            ]
        );
        assert_eq!(union, partial(30, 2).union(&partial(20, 7)));
    }

    #[test]
    fn a_record_named_whole_leaves_the_set() {
        let set = partial(5, 2).union(&partial(20, 3)).union(&partial(40, 1));
        let whole = ReplayStamp::through(10).union(&ReplayStamp::naming(40));
        assert_eq!(set.pruned(&whole), partial(20, 3));
    }

    #[test]
    fn a_record_with_no_op_names_nothing() {
        assert!(partial(20, 0).is_empty());
        assert!(!partial(20, 0).skips(20, 0));
    }

    #[test]
    fn a_malformed_set_does_not_validate() {
        let empty = PartialRecords {
            records: vec![PartialRecord {
                lsn: 4,
                ops_through: 0,
            }],
        };
        assert_eq!(
            empty.validate(),
            Err(InvalidPartialRecords::Empty { lsn: 4 })
        );
        let unordered = PartialRecords {
            records: vec![
                PartialRecord {
                    lsn: 9,
                    ops_through: 1,
                },
                PartialRecord {
                    lsn: 9,
                    ops_through: 2,
                },
            ],
        };
        assert!(unordered.validate().is_err());
    }
}
