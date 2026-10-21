// SPDX-License-Identifier: BUSL-1.1

//! Position index over the persisted DML audit rows.
//!
//! The Event Plane's dedup ledger needs the DML audit rows of events it can still redeliver.
//! Those are the events above each core's persisted watermark.
//! The index orders rows by event core, then event LSN, so boot reads only that range.

use redb::{ReadableDatabase, ReadableTable};

use super::audit::decode_entry;
use super::types::{AUDIT_DML_POSITIONS, AUDIT_LOG, SystemCatalog, catalog_err};
use crate::control::security::audit::{AuditEntry, AuditEvent};
use crate::event::sink_ledger::audit::dml_event_position;

/// Byte length of one index key: core (4) | event LSN (8) | audit seq (8).
pub(super) const DML_INDEX_KEY_LEN: usize = 20;

fn index_key(core: u32, lsn: u64, seq: u64) -> [u8; DML_INDEX_KEY_LEN] {
    let mut key = [0u8; DML_INDEX_KEY_LEN];
    key[..4].copy_from_slice(&core.to_be_bytes());
    key[4..12].copy_from_slice(&lsn.to_be_bytes());
    key[12..].copy_from_slice(&seq.to_be_bytes());
    key
}

fn lsn_of(key: &[u8]) -> u64 {
    key.get(4..12)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map_or(0, u64::from_be_bytes)
}

fn seq_of(key: &[u8]) -> Option<u64> {
    key.get(12..20)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map(u64::from_be_bytes)
}

/// The index key of `entry`, when it is a DML audit row whose detail names an event.
pub(super) fn dml_index_key(entry: &AuditEntry) -> Option<[u8; DML_INDEX_KEY_LEN]> {
    if entry.event != AuditEvent::DmlAudit {
        return None;
    }
    let (core, lsn) = dml_event_position(&entry.detail)?;
    Some(index_key(core, lsn, entry.seq))
}

impl SystemCatalog {
    /// The persisted DML audit rows whose event LSN lies above its core's floor, in seq order.
    ///
    /// `floors[c]` is core `c`'s floor. A core at or past `floors.len()` has floor 0.
    /// The read walks only the index range above each floor, then fetches those rows by seq.
    pub fn load_dml_audits_above(&self, floors: &[u64]) -> crate::Result<Vec<AuditEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let index = read_txn
            .open_table(AUDIT_DML_POSITIONS)
            .map_err(|e| catalog_err("open audit dml index", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        let mut seqs: Vec<u64> = Vec::new();
        let mut collect = |start: [u8; DML_INDEX_KEY_LEN],
                           end: [u8; DML_INDEX_KEY_LEN],
                           min_lsn: u64|
         -> crate::Result<()> {
            for row in index
                .range(start.as_slice()..=end.as_slice())
                .map_err(|e| catalog_err("range audit dml index", e))?
            {
                let (key, _) = row.map_err(|e| catalog_err("read audit dml index", e))?;
                let key = key.value();
                if lsn_of(key) >= min_lsn
                    && let Some(seq) = seq_of(key)
                {
                    seqs.push(seq);
                }
            }
            Ok(())
        };
        let mut covered = 0u32;
        for (core, floor) in floors.iter().enumerate() {
            let core = u32::try_from(core).unwrap_or(u32::MAX);
            covered = core.saturating_add(1);
            let Some(first) = floor.checked_add(1) else {
                continue;
            };
            collect(
                index_key(core, first, 0),
                index_key(core, u64::MAX, u64::MAX),
                first,
            )?;
        }
        // Cores the floor list does not name keep every row above LSN 0.
        if covered < u32::MAX {
            collect(
                index_key(covered, 0, 0),
                index_key(u32::MAX, u64::MAX, u64::MAX),
                1,
            )?;
        }

        seqs.sort_unstable();
        seqs.dedup();
        let mut entries = Vec::with_capacity(seqs.len());
        for seq in seqs {
            let key = seq.to_be_bytes();
            let Some(value) = table
                .get(key.as_slice())
                .map_err(|e| catalog_err("read audit", e))?
            else {
                continue;
            };
            #[cfg(test)]
            self.faults
                .dml_audit_rows_read
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            entries.push(decode_entry(value.value())?);
        }
        Ok(entries)
    }

    /// DML audit rows the position read decoded since the catalog opened.
    #[cfg(test)]
    pub(crate) fn dml_audit_rows_read_for_test(&self) -> u64 {
        self.faults
            .dml_audit_rows_read
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::sink_ledger::SinkEventKey;
    use crate::event::sink_ledger::audit::detail_suffix;

    fn dml_row(seq: u64, core: u32, lsn: u64) -> AuditEntry {
        let key = SinkEventKey {
            core,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: format!("o-{lsn}"),
        };
        AuditEntry {
            seq,
            timestamp_us: seq,
            event: AuditEvent::DmlAudit,
            tenant_id: None,
            database_id: None,
            auth_user_id: String::new(),
            auth_user_name: String::new(),
            session_id: String::new(),
            source: "alice".into(),
            detail: format!("INSERT orders:o-{lsn} lsn={lsn}{}", detail_suffix(&key)),
            prev_hash: String::new(),
        }
    }

    #[test]
    fn reads_only_rows_above_each_floor() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        // 1000 old rows on core 0 and 1000 on core 1, then 20 new rows on each.
        let mut rows = Vec::new();
        let mut seq = 0u64;
        for lsn in 1..=1020u64 {
            for core in 0..2u32 {
                seq += 1;
                rows.push(dml_row(seq, core, lsn));
            }
        }
        catalog.append_audit_entries(&rows).expect("append");

        let before = catalog.dml_audit_rows_read_for_test();
        let above = catalog.load_dml_audits_above(&[1000, 1010]).expect("read");
        let read = catalog.dml_audit_rows_read_for_test() - before;

        assert_eq!(
            above.len(),
            30,
            "20 rows above core 0's floor, 10 above core 1's"
        );
        assert_eq!(
            read, 30,
            "the read must touch only the rows above the floors"
        );
        for entry in &above {
            let (core, lsn) = dml_event_position(&entry.detail).expect("position");
            let floor = if core == 0 { 1000 } else { 1010 };
            assert!(lsn > floor);
        }
    }

    #[test]
    fn pruned_rows_leave_the_index() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        let rows: Vec<AuditEntry> = (1..=10).map(|i| dml_row(i, 0, i)).collect();
        catalog.append_audit_entries(&rows).expect("append");
        catalog.delete_audit_through(6).expect("prune");
        let above = catalog.load_dml_audits_above(&[0]).expect("read");
        let seqs: Vec<u64> = above.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![7, 8, 9, 10]);
    }

    #[test]
    fn cores_past_the_floor_list_keep_every_row() {
        let catalog = SystemCatalog::open_in_memory().expect("catalog");
        catalog
            .append_audit_entries(&[dml_row(1, 0, 5), dml_row(2, 3, 5)])
            .expect("append");
        let above = catalog.load_dml_audits_above(&[10]).expect("read");
        let seqs: Vec<u64> = above.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![2]);
    }
}
