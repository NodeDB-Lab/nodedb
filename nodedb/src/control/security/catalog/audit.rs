// SPDX-License-Identifier: BUSL-1.1

//! Audit log operations for the system catalog.
//!
//! The `AUDIT_LOG` table stores each full [`AuditEntry`], keyed by big-endian seq.
//! Every field that feeds the hash chain survives persistence, so persisted rows verify.

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata};

use super::audit_dml_index::{DML_INDEX_KEY_LEN, dml_index_key};
use super::types::{AUDIT_DML_POSITIONS, AUDIT_LOG, SystemCatalog, catalog_err};
use crate::control::security::audit::AuditEntry;
use crate::control::security::audit::entry::hash_entry;

/// The oldest-first prefix of the audit table that one retention prune removes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPrunePlan {
    /// Highest seq the prune removes. Every lower seq goes too.
    pub through_seq: u64,
    /// Number of rows the prune removes.
    pub pruned_count: u64,
    /// Hash of the row at `through_seq`, which the first survivor links to.
    pub anchor_hash: String,
}

pub(super) fn decode_entry(bytes: &[u8]) -> crate::Result<AuditEntry> {
    zerompk::from_msgpack(bytes).map_err(|e| catalog_err("deser audit", e))
}

impl SystemCatalog {
    /// Plan a prune of the rows older than `cutoff_us`, oldest first.
    ///
    /// The scan stops at the first row at or after the cutoff. Returns `None` when nothing is older.
    pub fn plan_audit_prune_before(&self, cutoff_us: u64) -> crate::Result<Option<AuditPrunePlan>> {
        self.plan_audit_prune(|_, entry| entry.timestamp_us < cutoff_us)
    }

    /// Plan a prune that leaves at most `max_entries` rows once its checkpoint is added.
    ///
    /// The plan frees one extra row for the checkpoint the prune records.
    /// So the next flush finds the table within the cap and records no new checkpoint.
    /// Returns `None` when the table is already within the cap.
    pub fn plan_audit_prune_to_count(
        &self,
        max_entries: u64,
    ) -> crate::Result<Option<AuditPrunePlan>> {
        let total = self.audit_entry_count()?;
        if total <= max_entries {
            return Ok(None);
        }
        let to_remove = (total - max_entries).saturating_add(1);
        self.plan_audit_prune(|index, _| index < to_remove)
    }

    /// Walk rows oldest first while `remove(index, entry)` holds, and plan their removal.
    fn plan_audit_prune(
        &self,
        remove: impl Fn(u64, &AuditEntry) -> bool,
    ) -> crate::Result<Option<AuditPrunePlan>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit", e))?;
        let mut last_removed: Option<AuditEntry> = None;
        let mut pruned_count = 0u64;
        for row in table.range(..).map_err(|e| catalog_err("range audit", e))? {
            let (_, value) = row.map_err(|e| catalog_err("read audit", e))?;
            let entry = decode_entry(value.value())?;
            if !remove(pruned_count, &entry) {
                break;
            }
            pruned_count += 1;
            last_removed = Some(entry);
        }
        Ok(last_removed.map(|entry| AuditPrunePlan {
            through_seq: entry.seq,
            pruned_count,
            anchor_hash: hash_entry(&entry),
        }))
    }

    /// Remove every row with `seq <= through_seq`. Returns the number removed.
    pub fn delete_audit_through(&self, through_seq: u64) -> crate::Result<u64> {
        let end_key = through_seq.to_be_bytes();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            let mut index = write_txn
                .open_table(AUDIT_DML_POSITIONS)
                .map_err(|e| catalog_err("open audit dml index", e))?;
            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut index_keys: Vec<[u8; DML_INDEX_KEY_LEN]> = Vec::new();
            for row in table
                .range(..=end_key.as_slice())
                .map_err(|e| catalog_err("range audit", e))?
            {
                let (key, value) = row.map_err(|e| catalog_err("read audit", e))?;
                keys.push(key.value().to_vec());
                index_keys.extend(dml_index_key(&decode_entry(value.value())?));
            }
            for key in &keys {
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("prune audit", e))?;
            }
            for key in &index_keys {
                index
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("prune audit dml index", e))?;
            }
            keys.len() as u64
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }

    /// Append a batch of audit entries. Used by the periodic flush and boot recovery.
    ///
    /// A row keyed by an existing seq is overwritten with the same entry.
    /// A DML audit row also gets its position index row in the same transaction.
    pub fn append_audit_entries(&self, entries: &[AuditEntry]) -> crate::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit_log", e))?;
            let mut index = write_txn
                .open_table(AUDIT_DML_POSITIONS)
                .map_err(|e| catalog_err("open audit dml index", e))?;
            for entry in entries {
                let key = entry.seq.to_be_bytes();
                let value = zerompk::to_msgpack_vec(entry)
                    .map_err(|e| catalog_err("serialize audit", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| catalog_err("insert audit", e))?;
                if let Some(index_key) = dml_index_key(entry) {
                    index
                        .insert(index_key.as_slice(), b"".as_slice())
                        .map_err(|e| catalog_err("insert audit dml index", e))?;
                }
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the highest sequence number from the audit log, or 0 when it is empty.
    pub fn load_audit_max_seq(&self) -> crate::Result<u64> {
        Ok(self.load_audit_newest()?.map_or(0, |entry| entry.seq))
    }

    /// Load the persisted entry with the highest seq.
    pub fn load_audit_newest(&self) -> crate::Result<Option<AuditEntry>> {
        Ok(self.load_recent_audit_entries(1, |_| true)?.pop())
    }

    /// Load the newest `limit` persisted entries that pass `filter`.
    ///
    /// The scan walks keys from the highest seq down and stops at `limit` matches.
    /// The result is in ascending seq order.
    pub fn load_recent_audit_entries(
        &self,
        limit: usize,
        filter: impl Fn(&AuditEntry) -> bool,
    ) -> crate::Result<Vec<AuditEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        let mut newest_first = Vec::new();
        for row in table
            .range(..)
            .map_err(|e| catalog_err("range audit", e))?
            .rev()
        {
            if newest_first.len() >= limit {
                break;
            }
            let (_, value) = row.map_err(|e| catalog_err("read audit", e))?;
            let entry = decode_entry(value.value())?;
            if filter(&entry) {
                newest_first.push(entry);
            }
        }
        newest_first.reverse();
        Ok(newest_first)
    }

    /// Visit the persisted entries at or below `through_seq`, in ascending seq order, one at a time.
    ///
    /// One read transaction covers the scan, so a prune during it changes nothing the scan sees.
    pub fn scan_audit_entries(
        &self,
        through_seq: u64,
        mut visit: impl FnMut(AuditEntry) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;
        for row in table.range(..).map_err(|e| catalog_err("range audit", e))? {
            let (_, value) = row.map_err(|e| catalog_err("read audit", e))?;
            let entry = decode_entry(value.value())?;
            if entry.seq > through_seq {
                break;
            }
            visit(entry)?;
        }
        Ok(())
    }

    /// Count total audit entries (for diagnostics).
    pub fn audit_entry_count(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;
        table.len().map_err(|e| catalog_err("count audit", e))
    }
}
