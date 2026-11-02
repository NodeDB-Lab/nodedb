// SPDX-License-Identifier: BUSL-1.1

//! The bytes an edge collection's stored versions hold.

use nodedb_types::TenantId;
use redb::{ReadableDatabase, ReadableTable};

use super::store::{EDGES, EdgeStore, redb_err};

impl EdgeStore {
    /// Bytes every stored version of `collection`'s edges holds, live and
    /// ended: each forward key and its value. The reverse index mirrors the
    /// keys, so it counts once more.
    pub fn collection_version_bytes(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
    ) -> crate::Result<u64> {
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read edge bytes", e))?;
        let edges = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges bytes", e))?;
        let prefix = format!("{collection}\x00");
        let mut bytes = 0u64;
        for entry in edges
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("range edge bytes", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("entry edge bytes", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let row = 2 * (composite.len() + 16) + value.value().len();
            bytes = bytes.saturating_add(row as u64);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::DatabaseId;

    use super::*;
    use crate::engine::graph::edge_store::EdgeRef;

    #[test]
    fn the_bytes_grow_with_versions_and_stay_in_their_collection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open");
        let tid = TenantId::new(1);
        let edge = EdgeRef::new(DatabaseId::DEFAULT, tid, "c", "a", "L", "b");
        store
            .put_edge_versioned(edge, b"v", 10, 0, i64::MAX)
            .expect("put");
        let one = store.collection_version_bytes(0, tid, "c").expect("bytes");
        store
            .put_edge_versioned(edge, b"v", 20, 0, i64::MAX)
            .expect("put");
        let two = store.collection_version_bytes(0, tid, "c").expect("bytes");
        assert!(one > 0 && two > one);
        assert_eq!(
            store.collection_version_bytes(0, tid, "cc").expect("bytes"),
            0
        );
    }
}
