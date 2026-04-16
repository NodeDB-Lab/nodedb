//! Checkpoint metadata operations for the system catalog.

use super::types::{CHECKPOINTS, CheckpointRecord, SystemCatalog, catalog_err};

impl SystemCatalog {
    /// Store a checkpoint record.
    pub fn put_checkpoint(&self, record: &CheckpointRecord) -> crate::Result<()> {
        let key = record.catalog_key();
        let bytes =
            zerompk::to_msgpack_vec(record).map_err(|e| catalog_err("serialize checkpoint", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert checkpoint", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Delete a checkpoint. Returns true if it existed.
    pub fn delete_checkpoint(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
        checkpoint_name: &str,
    ) -> crate::Result<bool> {
        let key = format!("{tenant_id}:{collection}:{doc_id}:{checkpoint_name}");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete checkpoint", e))?
                .is_some();
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Get a single checkpoint by name.
    pub fn get_checkpoint(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
        checkpoint_name: &str,
    ) -> crate::Result<Option<CheckpointRecord>> {
        let key = format!("{tenant_id}:{collection}:{doc_id}:{checkpoint_name}");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get checkpoint", e))?
        {
            Some(guard) => {
                let record: CheckpointRecord = zerompk::from_msgpack(guard.value())
                    .map_err(|e| catalog_err("deserialize checkpoint", e))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// List all checkpoints for a document, ordered by created_at descending.
    pub fn list_checkpoints(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
        limit: usize,
    ) -> crate::Result<Vec<CheckpointRecord>> {
        let prefix = CheckpointRecord::doc_prefix(tenant_id, collection, doc_id);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;

        let mut records = Vec::new();
        // Scan by range prefix.
        let prefix_end = format!("{prefix}\x7f"); // '\x7f' > any printable char.
        let range = table
            .range(prefix.as_str()..prefix_end.as_str())
            .map_err(|e| catalog_err("range scan checkpoints", e))?;
        for entry in range {
            let (_key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
            let record: CheckpointRecord = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize checkpoint", e))?;
            records.push(record);
        }

        // Sort by created_at descending (most recent first).
        records.sort_by_key(|r| std::cmp::Reverse(r.created_at));

        if records.len() > limit && limit > 0 {
            records.truncate(limit);
        }
        Ok(records)
    }

    /// Delete all checkpoints for a document created before a given timestamp.
    ///
    /// Used by COMPACT HISTORY to clean up checkpoints that reference
    /// oplog entries that have been discarded.
    pub fn delete_checkpoints_before(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
        before_timestamp: u64,
    ) -> crate::Result<usize> {
        let prefix = CheckpointRecord::doc_prefix(tenant_id, collection, doc_id);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;

        let prefix_end = format!("{prefix}\x7f");
        let range = table
            .range(prefix.as_str()..prefix_end.as_str())
            .map_err(|e| catalog_err("range scan checkpoints", e))?;

        let mut keys_to_delete = Vec::new();
        for entry in range {
            let (key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
            let record: CheckpointRecord = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize checkpoint", e))?;
            if record.created_at < before_timestamp {
                keys_to_delete.push(key.value().to_owned());
            }
        }
        drop(table);
        drop(read_txn);

        if keys_to_delete.is_empty() {
            return Ok(0);
        }

        let count = keys_to_delete.len();
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            for key in &keys_to_delete {
                table
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("delete checkpoint", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(count)
    }
}
