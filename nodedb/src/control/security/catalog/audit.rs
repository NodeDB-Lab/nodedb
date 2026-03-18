//! Audit log operations for the system catalog.

use redb::ReadableTableMetadata;

use super::types::{AUDIT_LOG, StoredAuditEntry, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Delete audit entries older than `cutoff_us` (microseconds since epoch).
    pub fn prune_audit_before(&self, cutoff_us: u64) -> crate::Result<u64> {
        // Phase 1: Read to find keys to delete.
        let to_delete = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("read txn", e))?;
            let table = read_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            let mut keys = Vec::new();
            for entry in table
                .range::<&[u8]>(..)
                .map_err(|e| catalog_err("range audit", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("read audit", e))?;
                let stored: StoredAuditEntry = match rmp_serde::from_slice(value.value()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if stored.timestamp_us < cutoff_us {
                    keys.push(key.value().to_vec());
                } else {
                    break;
                }
            }
            keys
        };

        if to_delete.is_empty() {
            return Ok(0);
        }

        // Phase 2: Write to delete the keys.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(AUDIT_LOG)
                .map_err(|e| catalog_err("open audit", e))?;
            for key in &to_delete {
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("prune audit", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(to_delete.len() as u64)
    }

    /// Append a batch of audit entries. Used by the periodic flush.
    pub fn append_audit_entries(&self, entries: &[StoredAuditEntry]) -> crate::Result<()> {
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
            for entry in entries {
                let key = entry.seq.to_be_bytes();
                let value =
                    rmp_serde::to_vec(entry).map_err(|e| catalog_err("serialize audit", e))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| catalog_err("insert audit", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;

        Ok(())
    }

    /// Load the highest sequence number from the audit log.
    /// Used on startup to resume the sequence counter.
    pub fn load_audit_max_seq(&self) -> crate::Result<u64> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        // Scan all keys to find the maximum sequence number.
        // Audit log keys are u64 big-endian, so the last entry in
        // iteration order is the highest.
        let mut max_seq = 0u64;
        let range = table
            .range::<&[u8]>(..)
            .map_err(|e| catalog_err("range audit", e))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| catalog_err("read audit key", e))?;
            let key_bytes: &[u8] = key.value();
            if key_bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(key_bytes);
                let seq = u64::from_be_bytes(arr);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
        Ok(max_seq)
    }

    /// Load audit entries, most recent first, up to `limit`.
    pub fn load_recent_audit_entries(&self, limit: usize) -> crate::Result<Vec<StoredAuditEntry>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(AUDIT_LOG)
            .map_err(|e| catalog_err("open audit_log", e))?;

        // Read all entries, then take the last `limit` (since redb iterates in key order = seq order).
        let mut all = Vec::new();
        for entry in table
            .range::<&[u8]>(..)
            .map_err(|e| catalog_err("range audit", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read audit", e))?;
            let stored: StoredAuditEntry =
                rmp_serde::from_slice(value.value()).map_err(|e| catalog_err("deser audit", e))?;
            all.push(stored);
        }

        // Return most recent entries (last N).
        if all.len() > limit {
            Ok(all.split_off(all.len() - limit))
        } else {
            Ok(all)
        }
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
