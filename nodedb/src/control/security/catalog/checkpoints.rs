// SPDX-License-Identifier: BUSL-1.1

//! Checkpoint metadata operations for the system catalog.

use super::checkpoint::key_of;
use super::types::{CHECKPOINTS, CheckpointRecord, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

/// Identifies the document a checkpoint call operates on.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointDoc<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
}

impl<'a> CheckpointDoc<'a> {
    pub fn new(database_id: u64, tenant_id: u64, collection: &'a str, doc_id: &'a str) -> Self {
        Self {
            database_id,
            tenant_id,
            collection,
            doc_id,
        }
    }

    fn prefix(&self) -> String {
        CheckpointRecord::doc_prefix(
            self.database_id,
            self.tenant_id,
            self.collection,
            self.doc_id,
        )
    }

    fn upper_bound(&self) -> String {
        CheckpointRecord::doc_upper_bound(
            self.database_id,
            self.tenant_id,
            self.collection,
            self.doc_id,
        )
    }

    fn key(&self, checkpoint_name: &str) -> String {
        key_of(
            self.database_id,
            self.tenant_id,
            self.collection,
            self.doc_id,
            checkpoint_name,
        )
    }
}

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
        doc: CheckpointDoc<'_>,
        checkpoint_name: &str,
    ) -> crate::Result<bool> {
        let key = doc.key(checkpoint_name);
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
        doc: CheckpointDoc<'_>,
        checkpoint_name: &str,
    ) -> crate::Result<Option<CheckpointRecord>> {
        let key = doc.key(checkpoint_name);
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

    /// Every checkpoint of every document, in key order.
    pub fn load_all_checkpoints(&self) -> crate::Result<Vec<CheckpointRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;
        let mut records = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| catalog_err("scan checkpoints", e))?
        {
            let (_key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
            records.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize checkpoint", e))?,
            );
        }
        Ok(records)
    }

    /// List all checkpoints for a document, ordered by created_at descending.
    ///
    /// The scan is bounded to the document's key range, so a catalog holding
    /// many databases reads only the rows it returns.
    pub fn list_checkpoints(
        &self,
        doc: CheckpointDoc<'_>,
        limit: usize,
    ) -> crate::Result<Vec<CheckpointRecord>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;

        let prefix = doc.prefix();
        let upper = doc.upper_bound();
        let mut records = Vec::new();
        let range = table
            .range(prefix.as_str()..upper.as_str())
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

    /// Keys of every checkpoint for a document created before `before_timestamp`.
    ///
    /// The boundary is exclusive: `created_at == before_timestamp` is kept.
    fn checkpoint_keys_before(
        &self,
        doc: CheckpointDoc<'_>,
        before_timestamp: u64,
    ) -> crate::Result<Vec<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(CHECKPOINTS)
            .map_err(|e| catalog_err("open checkpoints", e))?;

        let prefix = doc.prefix();
        let upper = doc.upper_bound();
        let range = table
            .range(prefix.as_str()..upper.as_str())
            .map_err(|e| catalog_err("range scan checkpoints", e))?;

        let mut keys = Vec::new();
        for entry in range {
            let (key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
            let record: CheckpointRecord = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize checkpoint", e))?;
            if record.created_at < before_timestamp {
                keys.push(key.value().to_owned());
            }
        }
        Ok(keys)
    }

    /// Count the checkpoints a `delete_checkpoints_before` call removes.
    ///
    /// The leader reports this to the client before proposing the range delete.
    pub fn count_checkpoints_before(
        &self,
        doc: CheckpointDoc<'_>,
        before_timestamp: u64,
    ) -> crate::Result<usize> {
        self.checkpoint_keys_before(doc, before_timestamp)
            .map(|keys| keys.len())
    }

    /// Delete all checkpoints for a document created before a given timestamp.
    ///
    /// Used by COMPACT HISTORY to clean up checkpoints that reference
    /// oplog entries that have been discarded. The boundary is exclusive.
    pub fn delete_checkpoints_before(
        &self,
        doc: CheckpointDoc<'_>,
        before_timestamp: u64,
    ) -> crate::Result<usize> {
        let keys_to_delete = self.checkpoint_keys_before(doc, before_timestamp)?;
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

    /// Copy every checkpoint of one collection from `source` into `target`.
    ///
    /// Returns the number of rows written. `CLONE DATABASE` calls this so a
    /// clone answers `SHOW VERSIONS` and `AT VERSION` the way its source does.
    pub fn copy_checkpoints_for_collection(
        &self,
        source: u64,
        target: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        let lower = format!("{source}:{tenant_id}:{collection}:");
        let upper = format!("{source}:{tenant_id}:{collection};");
        let mut copied = Vec::new();
        {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| catalog_err("read txn", e))?;
            let table = read_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            for entry in table
                .range(lower.as_str()..upper.as_str())
                .map_err(|e| catalog_err("range scan checkpoints", e))?
            {
                let (_key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
                let mut record: CheckpointRecord = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize checkpoint", e))?;
                record.database_id = target;
                copied.push(record);
            }
        }
        if copied.is_empty() {
            return Ok(0);
        }

        // One transaction: a clone reads either every checkpoint of the
        // collection or none of them.
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            for record in &copied {
                let bytes = zerompk::to_msgpack_vec(record)
                    .map_err(|e| catalog_err("serialize checkpoint", e))?;
                table
                    .insert(record.catalog_key().as_str(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert checkpoint", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(copied.len())
    }

    /// Remove every checkpoint of one collection, across its documents.
    /// Returns how many rows went.
    pub fn delete_checkpoints_for_collection(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<usize> {
        let lower = format!("{database_id}:{tenant_id}:{collection}:");
        let upper = format!("{database_id}:{tenant_id}:{collection};");
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let removed = {
            let mut table = write_txn
                .open_table(CHECKPOINTS)
                .map_err(|e| catalog_err("open checkpoints", e))?;
            // A collection name can hold ':', so the range only yields
            // candidates: the stored collection must match exactly.
            let mut keys = Vec::new();
            for entry in table
                .range(lower.as_str()..upper.as_str())
                .map_err(|e| catalog_err("range scan checkpoints", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("iterate checkpoints", e))?;
                let record: CheckpointRecord = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize checkpoint", e))?;
                if record.collection == collection {
                    keys.push(key.value().to_string());
                }
            }
            for key in &keys {
                table
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("remove checkpoint", e))?;
            }
            keys.len()
        };
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TENANT: u64 = 7;
    const COLLECTION: &str = "documents";
    const DOC: &str = "doc-1";

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn record(database_id: u64, name: &str, created_at: u64) -> CheckpointRecord {
        CheckpointRecord {
            database_id,
            tenant_id: TENANT,
            collection: COLLECTION.to_string(),
            doc_id: DOC.to_string(),
            checkpoint_name: name.to_string(),
            version_vector_json: "{\"n1\":4}".to_string(),
            created_by: "admin".to_string(),
            created_at,
        }
    }

    fn doc(database_id: u64) -> CheckpointDoc<'static> {
        CheckpointDoc::new(database_id, TENANT, COLLECTION, DOC)
    }

    /// Two databases of one tenant hold a same-named checkpoint on a same-named
    /// collection. Each keeps its own row.
    #[test]
    fn checkpoints_of_one_database_survive_a_delete_in_another() {
        let (_dir, catalog) = make_catalog();
        catalog.put_checkpoint(&record(2, "launch", 100)).unwrap();
        catalog.put_checkpoint(&record(3, "launch", 200)).unwrap();

        assert!(catalog.delete_checkpoint(doc(3), "launch").unwrap());

        let kept = catalog
            .get_checkpoint(doc(2), "launch")
            .unwrap()
            .expect("the key is scoped by database");
        assert_eq!(kept.created_at, 100);
        assert!(catalog.get_checkpoint(doc(3), "launch").unwrap().is_none());
    }

    #[test]
    fn listing_a_document_excludes_another_database() {
        let (_dir, catalog) = make_catalog();
        catalog.put_checkpoint(&record(2, "a", 1)).unwrap();
        catalog.put_checkpoint(&record(2, "b", 2)).unwrap();
        catalog.put_checkpoint(&record(3, "c", 3)).unwrap();

        let listed: Vec<String> = catalog
            .list_checkpoints(doc(2), 0)
            .unwrap()
            .into_iter()
            .map(|r| r.checkpoint_name)
            .collect();
        assert_eq!(listed, vec!["b".to_string(), "a".to_string()]);
        assert_eq!(catalog.list_checkpoints(doc(3), 0).unwrap().len(), 1);
    }

    #[test]
    fn the_range_delete_stops_at_the_database_boundary() {
        let (_dir, catalog) = make_catalog();
        catalog.put_checkpoint(&record(2, "old", 1)).unwrap();
        catalog.put_checkpoint(&record(3, "old", 1)).unwrap();

        assert_eq!(catalog.delete_checkpoints_before(doc(2), 100).unwrap(), 1);
        assert_eq!(catalog.list_checkpoints(doc(3), 0).unwrap().len(), 1);
    }

    #[test]
    fn the_range_excludes_a_collection_sharing_a_name_prefix() {
        let (_dir, catalog) = make_catalog();
        let mut sibling = record(2, "a", 1);
        sibling.collection = "documents_archive".into();
        catalog.put_checkpoint(&record(2, "a", 1)).unwrap();
        catalog.put_checkpoint(&sibling).unwrap();

        let listed = catalog.list_checkpoints(doc(2), 0).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].collection, COLLECTION);
    }

    #[test]
    fn a_copy_lands_every_row_under_the_target_database() {
        let (_dir, catalog) = make_catalog();
        catalog.put_checkpoint(&record(2, "a", 1)).unwrap();
        catalog.put_checkpoint(&record(2, "b", 2)).unwrap();

        assert_eq!(
            catalog
                .copy_checkpoints_for_collection(2, 3, TENANT, COLLECTION)
                .unwrap(),
            2
        );
        assert_eq!(catalog.list_checkpoints(doc(3), 0).unwrap().len(), 2);
        assert_eq!(catalog.list_checkpoints(doc(2), 0).unwrap().len(), 2);
        assert_eq!(
            catalog
                .get_checkpoint(doc(3), "a")
                .unwrap()
                .expect("copied row")
                .database_id,
            3
        );
    }

    #[test]
    fn delete_for_collection_removes_only_that_collection() {
        let (_dir, catalog) = make_catalog();
        catalog.put_checkpoint(&record(2, "a", 1)).unwrap();
        catalog.put_checkpoint(&record(2, "b", 2)).unwrap();
        catalog.put_checkpoint(&record(3, "a", 1)).unwrap();

        assert_eq!(
            catalog
                .delete_checkpoints_for_collection(2, TENANT, COLLECTION)
                .unwrap(),
            2
        );
        assert!(catalog.list_checkpoints(doc(2), 0).unwrap().is_empty());
        assert_eq!(catalog.list_checkpoints(doc(3), 0).unwrap().len(), 1);
    }
}
