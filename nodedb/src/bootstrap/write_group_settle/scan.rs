// SPDX-License-Identifier: BUSL-1.1

//! The record groups a recovered WAL shows, and which of them are whole.

use std::collections::{BTreeMap, HashMap, HashSet};

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::WriteGroupRecord;

/// Where a record sits and whose write it is: its tenant, vShard and
/// database, and the apply key, event source and commit HLC its header
/// carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordHome {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub apply_key: u64,
    /// The WAL code of the write's event source.
    pub event_source: u8,
    pub commit_hlc: u64,
}

/// One record group as the WAL shows it.
#[derive(Debug, Default)]
pub struct GroupScan {
    /// The part count the parts announce.
    pub parts: Option<u32>,
    /// LSN and home of every part seen, by part number.
    pub seen: BTreeMap<u32, (u64, RecordHome)>,
    /// Home of the record that starts the group, when the WAL holds it.
    pub start: Option<RecordHome>,
    /// LSN and home of the group's announcements and continuations.
    pub members: Vec<(u64, RecordHome)>,
    /// On a committed record whole at append: the continuation count, and
    /// whether the last continuation is present.
    pub closed: Option<(u32, bool)>,
}

impl GroupScan {
    /// Whether every announced part is present, or the record is whole at
    /// append and its last continuation is present.
    pub fn is_whole(&self) -> bool {
        if let Some((_, last_seen)) = self.closed {
            return last_seen;
        }
        self.parts
            .is_some_and(|parts| (1..=parts).all(|part| self.seen.contains_key(&part)))
    }
}

/// Every record group of a recovered WAL, and the LSNs it holds.
#[derive(Debug, Default)]
pub struct WalGroups {
    pub groups: BTreeMap<u64, GroupScan>,
    /// Home of every record replay applies, by LSN. A record a marker
    /// cancelled is absent.
    pub present: HashMap<u64, RecordHome>,
    /// LSNs of the committed transaction records. Such a record is never
    /// cancelled: replay applies it whether or not its group is whole.
    pub committed: HashSet<u64>,
}

impl WalGroups {
    /// Scan `records`, the stream replay reads.
    pub fn scan(records: &[WalRecord]) -> crate::Result<Self> {
        let mut scanned = Self::default();
        for record in records {
            let home = RecordHome {
                tenant_id: TenantId::new(record.header.tenant_id),
                vshard_id: VShardId::new(record.header.vshard_id),
                database_id: DatabaseId::new(record.header.database_id),
                apply_key: record.header.apply_key,
                event_source: record.header.event_source,
                commit_hlc: record.header.commit_hlc,
            };
            let lsn = record.header.lsn;
            scanned.present.insert(lsn, home);
            match RecordType::from_raw(record.logical_record_type()) {
                Some(RecordType::WriteGroup) => {}
                Some(RecordType::TransactionRedo) => {
                    scanned.committed.insert(lsn);
                    continue;
                }
                _ => continue,
            }
            let group = WriteGroupRecord::from_bytes(&record.payload)?.group;
            let origin = group.origin_at(lsn);
            let scan = scanned.groups.entry(origin).or_default();
            if let Some((index, count)) = group.closed_continuation() {
                let last_seen = scan.closed.is_some_and(|(_, seen)| seen);
                scan.closed = Some((count, last_seen || index == count));
            }
            if group.opens() {
                scan.start = Some(home);
                if lsn != origin {
                    scan.members.push((lsn, home));
                }
            } else {
                scan.parts = Some(scan.parts.map_or(group.parts, |n| n.max(group.parts)));
                scan.seen.insert(group.part, (lsn, home));
            }
        }
        Ok(scanned)
    }

    /// The part numbers of the group at `origin` the WAL holds.
    pub fn parts_present(&self, origin: u64) -> HashSet<u32> {
        self.groups
            .get(&origin)
            .map(|scan| scan.seen.keys().copied().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WriteGroup;
    use crate::wal::manager::{NO_APPLY_KEY, WalManager};

    #[test]
    fn a_scan_sees_announcements_parts_and_wholeness() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        // Row-write records name their write's source, as the funnel's do.
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let forward = appender.append_put(t, v, d, b"f").expect("forward");
        let group = |group: WriteGroup| WriteGroupRecord {
            schemas: Vec::new(),
            group,
            ops: Vec::new(),
            redo: None,
        };
        appender
            .append_write_group(t, v, d, &group(WriteGroup::announcing(forward.as_u64())))
            .expect("announce");
        let opening = appender
            .append_write_group(t, v, d, &group(WriteGroup::OPENING))
            .expect("open");
        appender
            .append_write_group(t, v, d, &group(WriteGroup::part_of(opening.as_u64(), 1, 1)))
            .expect("part");
        wal.sync().expect("sync");
        let scanned = WalGroups::scan(&wal.replay().expect("replay")).expect("scan");

        let announced = &scanned.groups[&forward.as_u64()];
        assert!(announced.start.is_some() && !announced.is_whole());
        let opened = &scanned.groups[&opening.as_u64()];
        assert!(opened.is_whole());
        assert_eq!(scanned.parts_present(opening.as_u64()), HashSet::from([1]));
        assert!(scanned.present.contains_key(&forward.as_u64()));
    }

    /// A committed record whole at append and split over the record limit
    /// is whole once its last continuation is present, with no part. Before
    /// the last one, it is not.
    #[test]
    fn a_record_whole_at_append_is_whole_without_parts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        let appender = wal
            .appender(NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User);
        let (t, v, d) = (TenantId::new(1), VShardId::new(0), DatabaseId::DEFAULT);
        let record = crate::wal::RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        };
        let origin = appender
            .append_whole_transaction_redo(t, v, d, &record)
            .expect("append");
        let continuation = |index: u32| WriteGroupRecord {
            schemas: Vec::new(),
            group: WriteGroup::continuing_closed(origin.as_u64(), index, 2),
            ops: Vec::new(),
            redo: None,
        };
        appender
            .append_write_group(t, v, d, &continuation(1))
            .expect("first continuation");
        wal.sync().expect("sync");
        let cut = WalGroups::scan(&wal.replay().expect("replay")).expect("scan");
        assert!(!cut.groups[&origin.as_u64()].is_whole());

        appender
            .append_write_group(t, v, d, &continuation(2))
            .expect("last continuation");
        wal.sync().expect("sync");
        let scanned = WalGroups::scan(&wal.replay().expect("replay")).expect("scan");
        let group = &scanned.groups[&origin.as_u64()];
        assert!(group.is_whole());
        assert!(group.seen.is_empty(), "no part follows the record");
        assert!(scanned.committed.contains(&origin.as_u64()));
    }
}
