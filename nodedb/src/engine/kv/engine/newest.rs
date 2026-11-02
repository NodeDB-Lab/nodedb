// SPDX-License-Identifier: BUSL-1.1

//! The newest system-time ordinal a KV collection holds: the start of a
//! live row or the end of an ended version, deletes included.

use nodedb_types::DatabaseId;

use super::KvEngine;
use crate::engine::kv::engine_helpers::table_key;

/// The newest ordinal one KV collection holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvCollectionNewest {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    /// The collection name as the table was created with it.
    pub collection: String,
    pub newest: i64,
}

impl KvEngine {
    /// The newest ordinal any version or delete of `collection` names.
    /// `None` when the collection holds neither.
    pub fn collection_newest_ordinal(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> Option<i64> {
        self.tables
            .get(&table_key(database_id, tenant_id, collection))?
            .max_ordinal()
    }

    /// The newest ordinal of every collection that holds a version.
    pub fn newest_ordinal_by_collection(&self) -> Vec<KvCollectionNewest> {
        self.live_collections()
            .filter_map(|coll| {
                Some(KvCollectionNewest {
                    database_id: coll.database_id,
                    tenant_id: coll.tenant_id,
                    collection: coll.collection.to_string(),
                    newest: coll.table?.max_ordinal()?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::engine::kv::KvEngine;
    use crate::engine::kv::KvPutParams;

    fn put(engine: &mut KvEngine, key: &[u8], ordinal: i64) {
        let scope = engine.open_write_scope(Some(ordinal));
        engine
            .put(KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "kv",
                key,
                value: b"v",
                ttl_ms: 0,
                now_ms: 0,
                surrogate: Surrogate::new(u32::from(key[0])),
            })
            .expect("put");
        engine.close_write_scope(scope);
    }

    #[test]
    fn a_collection_newest_counts_deletes() {
        let mut engine = KvEngine::new(0, 16, 0.75, 4, 64, 1000, 1024);
        assert_eq!(engine.collection_newest_ordinal(0, 1, "kv"), None);
        put(&mut engine, b"a", 100);
        put(&mut engine, b"b", 200);
        assert_eq!(engine.collection_newest_ordinal(0, 1, "kv"), Some(200));
        let scope = engine.open_write_scope(Some(300));
        engine.delete(0, 1, "kv", &[b"a".to_vec()], 0);
        engine.close_write_scope(scope);
        assert_eq!(engine.collection_newest_ordinal(0, 1, "kv"), Some(300));
        assert_eq!(engine.key_newest_ordinal(0, 1, "kv", b"a"), Some(300));
        assert_eq!(engine.key_newest_ordinal(0, 1, "kv", b"b"), Some(200));
        let all = engine.newest_ordinal_by_collection();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].newest, 300);
    }
}
