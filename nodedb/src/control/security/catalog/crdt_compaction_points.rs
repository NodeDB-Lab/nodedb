// SPDX-License-Identifier: BUSL-1.1

//! `_system.crdt_compaction_points`: the last CRDT history compaction each
//! collection committed.
//!
//! Applying `CompactHistory` writes the collection's row. The metadata group
//! replicates it, so a metadata image carries every collection's point. A
//! node that installs an image skips the `CompactHistory` entries it covers.
//! The install compares each point with the one it held before and owes a
//! compaction for every point that moved. Purging the collection removes its
//! row, so a later collection of the same name never inherits a point.
//!
//! Table: `{database_id}:{tenant_id}:{collection}` -> MessagePack row.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

pub(super) const CRDT_COMPACTION_POINTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_system.crdt_compaction_points");

/// The version one collection's CRDT history was last compacted to.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq, Eq)]
#[msgpack(map)]
pub struct StoredCompactionPoint {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    /// Loro version vector of the last committed `CompactHistory`.
    pub target_version_json: String,
}

fn point_key(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!("{database_id}:{tenant_id}:{collection}")
}

impl SystemCatalog {
    /// Record `point` as its collection's compaction point, replacing any
    /// earlier one.
    pub fn put_compaction_point(&self, point: &StoredCompactionPoint) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(point)
            .map_err(|e| catalog_err("encode crdt_compaction_points row", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("crdt_compaction_points write txn", e))?;
        {
            let mut table = txn
                .open_table(CRDT_COMPACTION_POINTS)
                .map_err(|e| catalog_err("open crdt_compaction_points", e))?;
            let key = point_key(point.database_id, point.tenant_id, &point.collection);
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert crdt_compaction_points row", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit crdt_compaction_points put", e))
    }

    /// Remove one collection's compaction point. Removing an absent row
    /// succeeds.
    pub fn delete_compaction_point(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("crdt_compaction_points write txn", e))?;
        {
            let mut table = txn
                .open_table(CRDT_COMPACTION_POINTS)
                .map_err(|e| catalog_err("open crdt_compaction_points", e))?;
            let key = point_key(database_id, tenant_id, collection);
            table
                .remove(key.as_str())
                .map_err(|e| catalog_err("remove crdt_compaction_points row", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit crdt_compaction_points delete", e))
    }

    /// Every collection's compaction point.
    pub fn load_compaction_points(&self) -> crate::Result<Vec<StoredCompactionPoint>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("crdt_compaction_points read txn", e))?;
        let table = txn
            .open_table(CRDT_COMPACTION_POINTS)
            .map_err(|e| catalog_err("open crdt_compaction_points", e))?;
        let mut out = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range crdt_compaction_points", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read crdt_compaction_points", e))?;
            out.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode crdt_compaction_points row", e))?,
            );
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn point(collection: &str, target: &str) -> StoredCompactionPoint {
        StoredCompactionPoint {
            database_id: 3,
            tenant_id: 1,
            collection: collection.to_string(),
            target_version_json: target.to_string(),
        }
    }

    #[test]
    fn a_later_point_replaces_the_earlier_and_delete_removes_it() {
        let tmp = TempDir::new().unwrap();
        let cat = SystemCatalog::open(&tmp.path().join("system.redb")).unwrap();
        cat.put_compaction_point(&point("docs", "{\"1\":4}"))
            .unwrap();
        cat.put_compaction_point(&point("docs", "{\"1\":9}"))
            .unwrap();
        cat.put_compaction_point(&point("notes", "{\"1\":2}"))
            .unwrap();
        assert_eq!(
            cat.load_compaction_points().unwrap(),
            vec![point("docs", "{\"1\":9}"), point("notes", "{\"1\":2}")]
        );
        cat.delete_compaction_point(3, 1, "docs").unwrap();
        cat.delete_compaction_point(3, 1, "docs").unwrap();
        assert_eq!(
            cat.load_compaction_points().unwrap(),
            vec![point("notes", "{\"1\":2}")]
        );
    }
}
