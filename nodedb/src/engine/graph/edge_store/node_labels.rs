// SPDX-License-Identifier: BUSL-1.1

//! System-time versions of node labels.
//!
//! The CSR holds a node's current labels only. Every label set or removal
//! this core applies also writes one version here: the node's whole label
//! set after the op, at the op's system-time ordinal. A graph read `AS OF` a
//! system time reads each node's labels at its cut from these versions.
//!
//! A version records the LSN of the WAL record it applies. Boot replay
//! re-applies label records to the CSR. It writes a version only for a
//! record above the newest LSN this store held at open: a record at or below
//! it already wrote its version before the restart. Every version of one
//! record and the record's LSN mark commit in one storage transaction, so a
//! record is versioned whole or not at all.

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::store::{EdgeStore, redb_err};
use super::temporal::SYSTEM_TIME_WIDTH;

/// Label version table: `(db, tid, "{node}\x00{ordinal:020}")` → msgpack
/// label set.
pub(super) const NODE_LABEL_VERSIONS: TableDefinition<(u64, u64, &str), &[u8]> =
    TableDefinition::new("node_label_versions");

/// The newest WAL LSN a label version records, under [`LABEL_LSN_KEY`].
pub(super) const NODE_LABEL_LSN: TableDefinition<&str, u64> =
    TableDefinition::new("node_label_lsn");

const LABEL_LSN_KEY: &str = "applied";

/// One node's label set at a cut.
pub type NodeLabelsAtCut = (String, Vec<String>);

/// One label version: `node`'s whole label set at `ordinal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLabelVersion {
    pub db: DatabaseId,
    pub tid: TenantId,
    pub node: String,
    pub ordinal: i64,
    pub labels: Vec<String>,
}

pub(super) fn version_key(node: &str, ordinal: i64) -> crate::Result<String> {
    if ordinal < 0 {
        return Err(crate::Error::BadRequest {
            detail: format!("node label version of '{node}' at negative ordinal {ordinal}"),
        });
    }
    Ok(format!(
        "{node}\x00{ordinal:0width$}",
        width = SYSTEM_TIME_WIDTH
    ))
}

pub(super) fn parse_version_key(key: &str) -> Option<(&str, i64)> {
    let (node, ordinal) = key.rsplit_once('\x00')?;
    if ordinal.len() != SYSTEM_TIME_WIDTH {
        return None;
    }
    Some((node, ordinal.parse().ok()?))
}

fn encode_labels(labels: &[String]) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&labels.to_vec()).map_err(|e| crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("encode node label version: {e}"),
    })
}

pub(super) fn decode_labels(bytes: &[u8]) -> crate::Result<Vec<String>> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("decode node label version: {e}"),
    })
}

impl EdgeStore {
    /// The newest WAL LSN a label version recorded, read at open.
    pub(super) fn read_label_lsn(db: &redb::Database) -> crate::Result<u64> {
        let txn = db.begin_read().map_err(|e| redb_err("begin_read", e))?;
        let table = txn
            .open_table(NODE_LABEL_LSN)
            .map_err(|e| redb_err("open node_label_lsn", e))?;
        Ok(table
            .get(LABEL_LSN_KEY)
            .map_err(|e| redb_err("read node label lsn", e))?
            .map_or(0, |value| value.value()))
    }

    /// Whether a label op of the WAL record at `lsn` writes a version. A
    /// record at or below the newest LSN this store held at open already
    /// wrote it. An op without a WAL record always writes one.
    pub fn label_version_due(&self, lsn: Option<u64>) -> bool {
        lsn.is_none_or(|lsn| lsn > self.label_lsn_floor)
    }

    /// Write `node`'s label set `labels` at `ordinal`, applying the WAL
    /// record at `lsn`. Returns the label set the version's key held before.
    pub fn put_node_label_version(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
        ordinal: i64,
        lsn: Option<u64>,
        labels: &[String],
    ) -> crate::Result<Option<Vec<String>>> {
        let version = NodeLabelVersion {
            db,
            tid,
            node: node.to_string(),
            ordinal,
            labels: labels.to_vec(),
        };
        let mut replaced = self.put_node_label_versions(std::slice::from_ref(&version), lsn)?;
        Ok(replaced.pop().flatten())
    }

    /// Write every version of `versions` and raise the applied-LSN mark to
    /// `lsn`, all in one storage transaction: a crash leaves all of them or
    /// none. Returns, per version and in order, the label set its key held
    /// before. A later version of the same key reports the earlier one.
    pub fn put_node_label_versions(
        &self,
        versions: &[NodeLabelVersion],
        lsn: Option<u64>,
    ) -> crate::Result<Vec<Option<Vec<String>>>> {
        let mut encoded = Vec::with_capacity(versions.len());
        for version in versions {
            let key = version_key(&version.node, version.ordinal)?;
            let value = encode_labels(&version.labels)?;
            encoded.push((version.db.as_u64(), version.tid.as_u64(), key, value));
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let mut replaced = Vec::with_capacity(encoded.len());
        {
            let mut table = write_txn
                .open_table(NODE_LABEL_VERSIONS)
                .map_err(|e| redb_err("open node_label_versions", e))?;
            for (d, t, key, value) in &encoded {
                let prior = table
                    .insert((*d, *t, key.as_str()), value.as_slice())
                    .map_err(|e| redb_err("insert node label version", e))?;
                replaced.push(match prior {
                    Some(prior) => Some(decode_labels(prior.value())?),
                    None => None,
                });
            }
            if let Some(lsn) = lsn {
                let mut marks = write_txn
                    .open_table(NODE_LABEL_LSN)
                    .map_err(|e| redb_err("open node_label_lsn", e))?;
                let newest = marks
                    .get(LABEL_LSN_KEY)
                    .map_err(|e| redb_err("read node label lsn", e))?
                    .map_or(0, |value| value.value());
                if lsn > newest {
                    marks
                        .insert(LABEL_LSN_KEY, lsn)
                        .map_err(|e| redb_err("write node label lsn", e))?;
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit node label versions", e))?;
        Ok(replaced)
    }

    /// The ordinal of `node`'s newest label version, `None` when it has none.
    pub fn newest_node_label_ordinal(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
    ) -> crate::Result<Option<i64>> {
        Ok(self
            .newest_node_label_version(db, tid, node, i64::MIN)?
            .map(|(ordinal, _)| ordinal))
    }

    /// `node`'s newest label version above `ordinal`, as its label set, or
    /// `None` when no version is above it.
    pub fn node_labels_above(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
        ordinal: i64,
    ) -> crate::Result<Option<Vec<String>>> {
        Ok(self
            .newest_node_label_version(db, tid, node, ordinal)?
            .map(|(_, labels)| labels))
    }

    /// `node`'s newest label version strictly above `floor`.
    fn newest_node_label_version(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
        floor: i64,
    ) -> crate::Result<Option<(i64, Vec<String>)>> {
        let d = db.as_u64();
        let t = tid.as_u64();
        let start = format!("{node}\x00");
        let end = format!("{node}\x01");
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(NODE_LABEL_VERSIONS)
            .map_err(|e| redb_err("open node_label_versions", e))?;
        let range = table
            .range((d, t, start.as_str())..(d, t, end.as_str()))
            .map_err(|e| redb_err("node label version range", e))?;
        // The range also holds the versions of a node whose id extends
        // `node` past a NUL byte. The newest version of `node` itself is the
        // last key that parses back to it.
        for entry in range.rev() {
            let (key, value) = entry.map_err(|e| redb_err("node label version entry", e))?;
            let Some((stored, ordinal)) = parse_version_key(key.value().2) else {
                continue;
            };
            if stored != node {
                continue;
            }
            if ordinal <= floor {
                return Ok(None);
            }
            return Ok(Some((ordinal, decode_labels(value.value())?)));
        }
        Ok(None)
    }

    /// Remove the label version of `node` at `ordinal`: the rollback of the
    /// op that wrote it.
    pub fn remove_node_label_version(
        &self,
        db: DatabaseId,
        tid: TenantId,
        node: &str,
        ordinal: i64,
    ) -> crate::Result<()> {
        let key = version_key(node, ordinal)?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut versions = write_txn
                .open_table(NODE_LABEL_VERSIONS)
                .map_err(|e| redb_err("open node_label_versions", e))?;
            versions
                .remove((db.as_u64(), tid.as_u64(), key.as_str()))
                .map_err(|e| redb_err("remove node label version", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit node label version removal", e))
    }

    /// Every node of `(db, tid)` whose newest label version at or below
    /// `cutoff_ordinal` holds a label, with that label set, in node order.
    pub fn node_labels_at(
        &self,
        db: DatabaseId,
        tid: TenantId,
        cutoff_ordinal: i64,
    ) -> crate::Result<Vec<NodeLabelsAtCut>> {
        let d = db.as_u64();
        let t = tid.as_u64();
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(NODE_LABEL_VERSIONS)
            .map_err(|e| redb_err("open node_label_versions", e))?;
        let mut out: Vec<NodeLabelsAtCut> = Vec::new();
        // Versions sort by node, then ascending ordinal: the last one at or
        // below the cutoff is the node's label set there.
        let mut current: Option<NodeLabelsAtCut> = None;
        let range = table
            .range((d, t, "")..(d, t + 1, ""))
            .map_err(|e| redb_err("node label version range", e))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| redb_err("node label version entry", e))?;
            let Some((node, ordinal)) = parse_version_key(key.value().2) else {
                continue;
            };
            if current.as_ref().is_none_or(|(seen, _)| seen != node) {
                if let Some(done) = current.take()
                    && !done.1.is_empty()
                {
                    out.push(done);
                }
                current = Some((node.to_string(), Vec::new()));
            }
            if ordinal > cutoff_ordinal {
                continue;
            }
            if let Some(slot) = current.as_mut() {
                slot.1 = decode_labels(value.value())?;
            }
        }
        if let Some(done) = current
            && !done.1.is_empty()
        {
            out.push(done);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: DatabaseId = DatabaseId::DEFAULT;
    const T: TenantId = TenantId::new(1);

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = EdgeStore::open(&dir.path().join("graph.redb")).expect("open store");
        (store, dir)
    }

    fn labels(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn a_cut_reads_each_nodes_newest_label_set_at_or_below_it() {
        let (store, _dir) = make_store();
        store
            .put_node_label_version(DB, T, "a", 100, None, &labels(&["Person"]))
            .expect("put");
        store
            .put_node_label_version(DB, T, "a", 300, None, &[])
            .expect("put");
        store
            .put_node_label_version(DB, T, "b", 200, None, &labels(&["City"]))
            .expect("put");
        assert_eq!(
            store.node_labels_at(DB, T, 150).expect("read"),
            vec![("a".to_string(), labels(&["Person"]))]
        );
        assert_eq!(
            store.node_labels_at(DB, T, 250).expect("read"),
            vec![
                ("a".to_string(), labels(&["Person"])),
                ("b".to_string(), labels(&["City"]))
            ]
        );
        assert_eq!(
            store.node_labels_at(DB, T, 350).expect("read"),
            vec![("b".to_string(), labels(&["City"]))]
        );
        assert!(
            store
                .node_labels_at(DB, TenantId::new(2), i64::MAX)
                .expect("read")
                .is_empty()
        );
    }

    #[test]
    fn a_removed_version_leaves_the_older_one() {
        let (store, _dir) = make_store();
        store
            .put_node_label_version(DB, T, "a", 100, None, &labels(&["Person"]))
            .expect("put");
        store
            .put_node_label_version(DB, T, "a", 200, None, &labels(&["Person", "Admin"]))
            .expect("put");
        store
            .remove_node_label_version(DB, T, "a", 200)
            .expect("remove");
        assert_eq!(
            store.node_labels_at(DB, T, i64::MAX).expect("read"),
            vec![("a".to_string(), labels(&["Person"]))]
        );
    }

    #[test]
    fn a_reopened_store_skips_records_it_already_versioned() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("graph.redb");
        {
            let store = EdgeStore::open(&path).expect("open");
            assert!(store.label_version_due(Some(5)));
            store
                .put_node_label_version(DB, T, "a", 100, Some(5), &labels(&["Person"]))
                .expect("put");
        }
        let store = EdgeStore::open(&path).expect("reopen");
        assert!(!store.label_version_due(Some(5)));
        assert!(!store.label_version_due(Some(4)));
        assert!(store.label_version_due(Some(6)));
        assert!(store.label_version_due(None));
    }

    #[test]
    fn a_record_writes_all_its_versions_with_its_mark_in_one_commit() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("graph.redb");
        let version = |node: &str, ordinal: i64, names: &[&str]| NodeLabelVersion {
            db: DB,
            tid: T,
            node: node.to_string(),
            ordinal,
            labels: labels(names),
        };
        {
            let store = EdgeStore::open(&path).expect("open");
            let replaced = store
                .put_node_label_versions(
                    &[
                        version("a", 100, &["Person"][..]),
                        version("b", 100, &["City"][..]),
                        version("a", 100, &["Person", "Admin"][..]),
                    ],
                    Some(9),
                )
                .expect("put");
            assert_eq!(
                replaced,
                vec![None, None, Some(labels(&["Person"]))],
                "a later version of one key reports the earlier one"
            );
        }
        let store = EdgeStore::open(&path).expect("reopen");
        assert!(!store.label_version_due(Some(9)));
        assert_eq!(
            store.node_labels_at(DB, T, i64::MAX).expect("read"),
            vec![
                ("a".to_string(), labels(&["Person", "Admin"])),
                ("b".to_string(), labels(&["City"]))
            ]
        );
    }

    #[test]
    fn the_newest_version_above_an_ordinal_names_the_current_set() {
        let (store, _dir) = make_store();
        store
            .put_node_label_version(DB, T, "a", 100, None, &labels(&["Person"]))
            .expect("put");
        store
            .put_node_label_version(DB, T, "a", 300, None, &labels(&["Admin"]))
            .expect("put");
        store
            .put_node_label_version(DB, T, "a\x00b", 900, None, &labels(&["Other"]))
            .expect("put");
        assert_eq!(
            store.newest_node_label_ordinal(DB, T, "a").expect("read"),
            Some(300)
        );
        assert_eq!(
            store.node_labels_above(DB, T, "a", 200).expect("read"),
            Some(labels(&["Admin"]))
        );
        assert_eq!(
            store.node_labels_above(DB, T, "a", 300).expect("read"),
            None
        );
        assert_eq!(
            store.newest_node_label_ordinal(DB, T, "z").expect("read"),
            None
        );
    }
}
