// SPDX-License-Identifier: BUSL-1.1

//! The newest system-time ordinal the edge versions of a collection hold,
//! per endpoint home vShard.
//!
//! An edge lives on the home vShard of each endpoint, so its versions count
//! toward both homes. The answer reads the stored keys only, so every core
//! that holds the edges of a vShard answers alike for it, whichever other
//! vShards share the core.

use std::collections::BTreeMap;

use redb::{ReadableDatabase, ReadableTable};

use super::keys::parse_versioned_edge_key;
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, redb_err};
use crate::types::VShardId;

/// The newest edge version of one collection on one endpoint home vShard.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EdgeHomeNewest {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
    pub vshard: VShardId,
    pub newest: i64,
}

/// Fold one edge version into `out` under both endpoint homes.
fn fold_version(
    out: &mut BTreeMap<(u64, u64, String, u32), i64>,
    database_id: u64,
    tenant_id: u64,
    composite: &str,
) {
    let Some((collection, src, _label, dst, sys_from)) = parse_versioned_edge_key(composite) else {
        return;
    };
    for node in [src, dst] {
        let home = VShardId::from_key(node.as_bytes()).as_u32();
        let slot = out
            .entry((database_id, tenant_id, collection.to_string(), home))
            .or_insert(sys_from);
        *slot = (*slot).max(sys_from);
    }
}

fn into_rows(out: BTreeMap<(u64, u64, String, u32), i64>) -> Vec<EdgeHomeNewest> {
    out.into_iter()
        .map(
            |((database_id, tenant_id, collection, vshard), newest)| EdgeHomeNewest {
                database_id,
                tenant_id,
                collection,
                vshard: VShardId::new(vshard),
                newest,
            },
        )
        .collect()
}

impl EdgeStore {
    /// The newest version, tombstones included, of the edges of
    /// `collection` whose source or destination homes to `vshard`. `None`
    /// when no such edge holds a version.
    pub fn collection_newest_on_home(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        vshard: VShardId,
    ) -> crate::Result<Option<i64>> {
        let lo = format!("{collection}\x00");
        let hi = format!("{collection}\x01");
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut out = BTreeMap::new();
        for entry in table
            .range((database_id, tenant_id, lo.as_str())..(database_id, tenant_id, hi.as_str()))
            .map_err(|e| redb_err("collection newest range", e))?
        {
            let (key, _) = entry.map_err(|e| redb_err("collection newest iter", e))?;
            fold_version(&mut out, database_id, tenant_id, key.value().2);
        }
        Ok(into_rows(out)
            .into_iter()
            .filter(|row| row.vshard == vshard)
            .map(|row| row.newest)
            .max())
    }

    /// The newest edge version of every collection on every endpoint home
    /// vShard the store holds edges of. Reads every edge key once.
    pub fn newest_by_home(&self) -> crate::Result<Vec<EdgeHomeNewest>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;
        let mut out = BTreeMap::new();
        for entry in table.iter().map_err(|e| redb_err("newest iter", e))? {
            let (key, _) = entry.map_err(|e| redb_err("newest iter", e))?;
            let (database_id, tenant_id, composite) = key.value();
            fold_version(&mut out, database_id, tenant_id, composite);
        }
        Ok(into_rows(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::graph::edge_store::temporal::EdgeRef;
    use crate::types::{DatabaseId, TenantId};

    #[test]
    fn an_edge_version_counts_toward_both_endpoint_homes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("g.redb")).expect("open");
        let edge = EdgeRef::new(DatabaseId::DEFAULT, TenantId::new(1), "g", "a", "L", "b");
        store
            .put_edge_versioned(edge, b"", 40, i64::MIN, i64::MAX)
            .expect("put edge");
        store.soft_delete_edge(edge, 70).expect("delete edge");
        let src_home = VShardId::from_key(b"a");
        let dst_home = VShardId::from_key(b"b");
        assert_eq!(
            store
                .collection_newest_on_home(0, 1, "g", src_home)
                .expect("newest"),
            Some(70)
        );
        assert_eq!(
            store
                .collection_newest_on_home(0, 1, "g", dst_home)
                .expect("newest"),
            Some(70)
        );
        let rows = store.newest_by_home().expect("newest by home");
        assert!(rows.iter().all(|row| row.newest == 70));
        assert!(rows.iter().any(|row| row.vshard == src_home));
    }
}
