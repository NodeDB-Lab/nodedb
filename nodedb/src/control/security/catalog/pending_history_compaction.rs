// SPDX-License-Identifier: BUSL-1.1

//! Owed CRDT history compactions of this node.
//!
//! Applying a `CompactHistory` entry writes one row here before it deletes the
//! checkpoint rows. Post-apply removes the row once every local core compacted
//! and checkpointed the collection's oplog. A row that survives a crash or a
//! failed fan-out is re-driven by the boot drain and the retry worker.
//!
//! One row per collection. A later compaction of the same collection replaces
//! the row: compacting to its target also discards what the earlier one owed.
//!
//! Table: `{database_id}:{tenant_id}:{collection}` -> MessagePack row.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

pub(super) const PENDING_HISTORY_COMPACTION: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.pending_history_compaction");

/// One owed compaction: "compact this collection's oplog to this version".
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq, Eq)]
#[msgpack(map)]
pub struct StoredPendingHistoryCompaction {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    /// Loro version vector the committed entry carries.
    pub target_version_json: String,
    /// Last error a retry observed. Empty until a retry fails.
    pub last_error: String,
    /// Failed retries of this row.
    pub attempts: u32,
}

fn compaction_key(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}")
}

impl SystemCatalog {
    /// Record an owed compaction, replacing any row for the same collection.
    pub fn enqueue_pending_history_compaction(
        &self,
        entry: &StoredPendingHistoryCompaction,
    ) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry)
            .map_err(|e| catalog_err("encode pending_history_compaction row", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("enqueue_pending_history_compaction txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_HISTORY_COMPACTION)
                .map_err(|e| catalog_err("open pending_history_compaction", e))?;
            let key = compaction_key(entry.database_id, entry.tenant_id, &entry.collection);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert pending_history_compaction row", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_history_compaction enqueue", e))
    }

    /// Every owed compaction on this node.
    pub fn load_pending_history_compactions(
        &self,
    ) -> crate::Result<Vec<StoredPendingHistoryCompaction>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_pending_history_compactions read txn", e))?;
        let table = txn
            .open_table(PENDING_HISTORY_COMPACTION)
            .map_err(|e| catalog_err("open pending_history_compaction", e))?;
        let mut out = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range pending_history_compaction", e))?
        {
            let (_, v) = item.map_err(|e| catalog_err("read pending_history_compaction", e))?;
            out.push(
                zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("decode pending_history_compaction row", e))?,
            );
        }
        Ok(out)
    }

    /// Bump `attempts` and store `last_error` on the row, if it still owes
    /// `target_version_json`.
    pub fn record_pending_history_compaction_attempt(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        target_version_json: &str,
        last_error: &str,
    ) -> crate::Result<()> {
        self.update_owed_compaction(
            database_id,
            tenant_id,
            collection,
            target_version_json,
            |mut row| {
                row.attempts = row.attempts.saturating_add(1);
                row.last_error = last_error.to_string();
                Some(row)
            },
        )
    }

    /// Remove the row once `target_version_json` is durably compacted.
    ///
    /// A row that a later entry replaced with another target stays: that
    /// compaction is still owed.
    pub fn remove_pending_history_compaction(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        target_version_json: &str,
    ) -> crate::Result<()> {
        self.update_owed_compaction(
            database_id,
            tenant_id,
            collection,
            target_version_json,
            |_| None,
        )
    }

    /// Rewrite (`Some`) or remove (`None`) the collection's row, when it owes
    /// `target_version_json`. Any other row, or none, is left as it is.
    fn update_owed_compaction(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        target_version_json: &str,
        update: impl FnOnce(StoredPendingHistoryCompaction) -> Option<StoredPendingHistoryCompaction>,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("pending_history_compaction update txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_HISTORY_COMPACTION)
                .map_err(|e| catalog_err("open pending_history_compaction", e))?;
            let key = compaction_key(database_id, tenant_id, collection);
            let existing = table
                .get(key.as_str())
                .map_err(|e| catalog_err("get pending_history_compaction row", e))?
                .map(|g| g.value().to_vec());
            let Some(raw) = existing else {
                return Ok(());
            };
            let row: StoredPendingHistoryCompaction = zerompk::from_msgpack(&raw)
                .map_err(|e| catalog_err("decode pending_history_compaction row", e))?;
            if row.target_version_json != target_version_json {
                return Ok(());
            }
            match update(row) {
                Some(row) => {
                    let bytes = zerompk::to_msgpack_vec(&row)
                        .map_err(|e| catalog_err("encode pending_history_compaction row", e))?;
                    table
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(|e| catalog_err("update pending_history_compaction row", e))?;
                }
                None => {
                    table
                        .remove(key.as_str())
                        .map_err(|e| catalog_err("remove pending_history_compaction row", e))?;
                }
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_history_compaction update", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cat() -> (SystemCatalog, TempDir) {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        (cat, tmp)
    }

    fn row(collection: &str, target: &str) -> StoredPendingHistoryCompaction {
        StoredPendingHistoryCompaction {
            database_id: 3,
            tenant_id: 7,
            collection: collection.to_string(),
            target_version_json: target.to_string(),
            last_error: String::new(),
            attempts: 0,
        }
    }

    #[test]
    fn enqueue_then_load_roundtrip() {
        let (c, _t) = cat();
        c.enqueue_pending_history_compaction(&row("docs", "v1"))
            .unwrap();
        assert_eq!(
            c.load_pending_history_compactions().unwrap(),
            vec![row("docs", "v1")]
        );
    }

    #[test]
    fn a_later_compaction_replaces_the_row() {
        let (c, _t) = cat();
        c.enqueue_pending_history_compaction(&row("docs", "v1"))
            .unwrap();
        c.enqueue_pending_history_compaction(&row("docs", "v2"))
            .unwrap();
        assert_eq!(
            c.load_pending_history_compactions().unwrap(),
            vec![row("docs", "v2")]
        );
    }

    #[test]
    fn remove_takes_only_the_row_that_owes_the_target() {
        let (c, _t) = cat();
        c.enqueue_pending_history_compaction(&row("docs", "v2"))
            .unwrap();
        c.remove_pending_history_compaction(3, 7, "docs", "v1")
            .unwrap();
        assert_eq!(c.load_pending_history_compactions().unwrap().len(), 1);
        c.remove_pending_history_compaction(3, 7, "docs", "v2")
            .unwrap();
        assert!(c.load_pending_history_compactions().unwrap().is_empty());
        // Idempotent.
        c.remove_pending_history_compaction(3, 7, "docs", "v2")
            .unwrap();
    }

    #[test]
    fn record_attempt_updates_in_place() {
        let (c, _t) = cat();
        c.enqueue_pending_history_compaction(&row("docs", "v1"))
            .unwrap();
        c.record_pending_history_compaction_attempt(3, 7, "docs", "v1", "core 0 refused")
            .unwrap();
        let rows = c.load_pending_history_compactions().unwrap();
        assert_eq!(rows[0].attempts, 1);
        assert_eq!(rows[0].last_error, "core 0 refused");
    }
}
