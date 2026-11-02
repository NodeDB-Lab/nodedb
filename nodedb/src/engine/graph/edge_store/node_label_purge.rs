// SPDX-License-Identifier: BUSL-1.1

//! History purge of node-label versions below a system-time horizon.
//!
//! A read at or above horizon `H` takes each node's newest label version at
//! or below its cut. The purge removes every version older than the node's
//! newest version at or below `H`. That newest version goes too when its
//! label set is empty and no version follows it: the node then reads with no
//! labels at every such cut. Versions above `H` stay.

use std::collections::BTreeMap;

use redb::{ReadableDatabase, ReadableTable};

use super::node_labels::{NODE_LABEL_VERSIONS, decode_labels, parse_version_key};
use super::store::{EdgeStore, redb_err};

/// One stored label version: its key and whether its label set is empty.
struct LabelVersion {
    key: String,
    ordinal: i64,
    empty: bool,
}

impl EdgeStore {
    /// Remove every node-label version no read at or above its tenant's
    /// horizon reaches. `horizon_of(db, tid)` names the horizon of a
    /// `(database, tenant)`, `None` to keep its whole history. Returns the
    /// number of versions removed.
    pub fn purge_node_label_versions_below(
        &self,
        horizon_of: &dyn Fn(u64, u64) -> Option<i64>,
    ) -> crate::Result<usize> {
        let victims = self.collect_label_victims(horizon_of)?;
        if victims.is_empty() {
            return Ok(0);
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write label purge", e))?;
        {
            let mut versions = write_txn
                .open_table(NODE_LABEL_VERSIONS)
                .map_err(|e| redb_err("open node_label_versions purge", e))?;
            for (d, t, key) in &victims {
                versions
                    .remove((*d, *t, key.as_str()))
                    .map_err(|e| redb_err("remove node label version", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit label purge", e))?;
        Ok(victims.len())
    }

    fn collect_label_victims(
        &self,
        horizon_of: &dyn Fn(u64, u64) -> Option<i64>,
    ) -> crate::Result<Vec<(u64, u64, String)>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read label purge", e))?;
        let table = read_txn
            .open_table(NODE_LABEL_VERSIONS)
            .map_err(|e| redb_err("open node_label_versions read purge", e))?;
        // Grouped by node, so a node id that extends another past a NUL byte
        // never interleaves with it.
        let mut nodes: BTreeMap<(u64, u64, String), Vec<LabelVersion>> = BTreeMap::new();
        for entry in table
            .iter()
            .map_err(|e| redb_err("scan node_label_versions", e))?
        {
            let (key, value) = entry.map_err(|e| redb_err("node label version entry", e))?;
            let (d, t, composite) = key.value();
            let Some((node, ordinal)) = parse_version_key(composite) else {
                continue;
            };
            nodes
                .entry((d, t, node.to_string()))
                .or_default()
                .push(LabelVersion {
                    key: composite.to_string(),
                    ordinal,
                    empty: decode_labels(value.value())?.is_empty(),
                });
        }
        let mut victims = Vec::new();
        for ((d, t, _), mut versions) in nodes {
            let Some(horizon) = horizon_of(d, t) else {
                continue;
            };
            versions.sort_by_key(|version| version.ordinal);
            let at_or_below = versions.partition_point(|version| version.ordinal <= horizon);
            let Some(newest) = at_or_below.checked_sub(1) else {
                continue;
            };
            let terminal = versions[newest].empty && at_or_below == versions.len();
            let removed = if terminal { at_or_below } else { newest };
            victims.extend(versions.drain(..removed).map(|version| (d, t, version.key)));
        }
        Ok(victims)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open");
        (store, dir)
    }

    fn put(store: &EdgeStore, node: &str, ordinal: i64, labels: &[&str]) {
        let labels: Vec<String> = labels.iter().map(|l| l.to_string()).collect();
        store
            .put_node_label_version(
                DatabaseId::DEFAULT,
                TenantId::new(1),
                node,
                ordinal,
                None,
                &labels,
            )
            .expect("put");
    }

    fn labels_at(store: &EdgeStore, cut: i64) -> Vec<(String, Vec<String>)> {
        store
            .node_labels_at(DatabaseId::DEFAULT, TenantId::new(1), cut)
            .expect("read")
    }

    /// The version visible at the horizon stays, and every read at or above
    /// the horizon answers as before.
    #[test]
    fn the_purge_keeps_the_label_set_visible_at_the_horizon() {
        let (store, _dir) = make_store();
        put(&store, "n", 10, &["A"]);
        put(&store, "n", 20, &["B"]);
        put(&store, "n", 40, &["C"]);
        let before: Vec<_> = [25, 40, i64::MAX].map(|cut| labels_at(&store, cut)).into();

        let removed = store
            .purge_node_label_versions_below(&|_, _| Some(25))
            .expect("purge");

        assert_eq!(removed, 1, "only the set at 10 is superseded below 25");
        let after: Vec<_> = [25, 40, i64::MAX].map(|cut| labels_at(&store, cut)).into();
        assert_eq!(before, after);
    }

    /// An empty label set at or below the horizon with nothing after it
    /// leaves the node's history entirely. A kept tenant keeps everything.
    #[test]
    fn an_emptied_node_leaves_and_a_kept_tenant_stays() {
        let (store, _dir) = make_store();
        put(&store, "n", 10, &["A"]);
        put(&store, "n", 20, &[]);
        assert_eq!(
            store
                .purge_node_label_versions_below(&|_, _| None)
                .expect("purge"),
            0
        );
        assert_eq!(
            store
                .purge_node_label_versions_below(&|_, _| Some(30))
                .expect("purge"),
            2
        );
        assert!(labels_at(&store, i64::MAX).is_empty());
    }
}
