use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};

/// Edge table: composite key `"src_id\x00edge_label\x00dst_id"` → edge properties (MessagePack).
///
/// Separator \x00 ensures lexicographic ordering groups all edges from the same
/// source together, then by label, then by destination — enabling efficient
/// prefix scans for outbound traversals.
pub(super) const EDGES: TableDefinition<&str, &[u8]> = TableDefinition::new("edges");

/// Reverse edge index: `"dst_id\x00edge_label\x00src_id"` → empty.
///
/// Enables efficient inbound traversals (`GRAPH_NEIGHBORS(node, label, IN)`).
/// Maintained synchronously with the forward edge table.
pub(super) const REVERSE_EDGES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("reverse_edges");

pub(super) fn redb_err<E: std::fmt::Display>(ctx: &str, e: E) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Composite edge key using \x00 separator.
pub(super) fn edge_key(src: &str, label: &str, dst: &str) -> String {
    format!("{src}\x00{label}\x00{dst}")
}

/// Parse a composite edge key back into (src, label, dst).
pub(super) fn parse_edge_key(key: &str) -> Option<(&str, &str, &str)> {
    let mut parts = key.splitn(3, '\x00');
    let src = parts.next()?;
    let label = parts.next()?;
    let dst = parts.next()?;
    Some((src, label, dst))
}

/// Traversal direction for graph queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

/// A single edge with its properties.
#[derive(Debug, Clone)]
pub struct Edge {
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
}

/// redb-backed edge storage for the Knowledge Graph engine.
///
/// Stores directed labeled edges as composite keys in redb B-Trees.
/// Forward edges keyed by `(src, label, dst)` for outbound traversal.
/// Reverse index keyed by `(dst, label, src)` for inbound traversal.
///
/// Each Data Plane core owns its own `EdgeStore` instance — no cross-core sharing.
pub struct EdgeStore {
    pub(super) db: Arc<Database>,
}

impl EdgeStore {
    /// Open or create the edge store database at the given path.
    pub fn open(path: &Path) -> crate::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path).map_err(|e| redb_err("open", e))?;

        // Ensure tables exist.
        let write_txn = db.begin_write().map_err(|e| redb_err("begin_write", e))?;
        {
            let _ = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let _ = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update an edge with properties.
    ///
    /// Maintains both forward and reverse indexes atomically in a single transaction.
    /// Properties are MessagePack-encoded bytes (empty `&[]` for edges without properties).
    pub fn put_edge(
        &self,
        src: &str,
        label: &str,
        dst: &str,
        properties: &[u8],
    ) -> crate::Result<()> {
        let fwd_key = edge_key(src, label, dst);
        let rev_key = edge_key(dst, label, src);

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert(fwd_key.as_str(), properties)
                .map_err(|e| redb_err("insert edge", e))?;

            let mut rev = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev.insert(rev_key.as_str(), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Delete an edge. Removes both forward and reverse entries atomically.
    pub fn delete_edge(&self, src: &str, label: &str, dst: &str) -> crate::Result<bool> {
        let fwd_key = edge_key(src, label, dst);
        let rev_key = edge_key(dst, label, src);

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        let existed = {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let existed = edges
                .remove(fwd_key.as_str())
                .map_err(|e| redb_err("remove edge", e))?
                .is_some();

            let mut rev = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev.remove(rev_key.as_str())
                .map_err(|e| redb_err("remove reverse", e))?;

            existed
        };
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(existed)
    }

    /// Delete ALL edges where the given node is source or destination.
    ///
    /// Used during document deletion cascade. Scans both forward and
    /// reverse edge tables for entries containing the node.
    pub fn delete_edges_for_node(&self, node: &str) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let mut rev = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;

            // Remove outgoing edges: keys starting with "node\x00".
            let out_prefix = format!("{node}\x00");
            let out_end = format!("{node}\x01");
            let out_keys: Vec<String> = edges
                .range(out_prefix.as_str()..out_end.as_str())
                .map_err(|e| redb_err("out range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for key in &out_keys {
                edges
                    .remove(key.as_str())
                    .map_err(|e| redb_err("remove out edge", e))?;
                // Build reverse key: "dst\x00label\x00src"
                let parts: Vec<&str> = key.splitn(3, '\x00').collect();
                if parts.len() == 3 {
                    let rev_key = format!("{}\x00{}\x00{}", parts[2], parts[1], parts[0]);
                    let _ = rev.remove(rev_key.as_str());
                }
            }

            // Remove incoming edges: keys starting with "node\x00" in reverse table.
            let in_prefix = format!("{node}\x00");
            let in_end = format!("{node}\x01");
            let in_keys: Vec<String> = rev
                .range(in_prefix.as_str()..in_end.as_str())
                .map_err(|e| redb_err("in range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for key in &in_keys {
                rev.remove(key.as_str())
                    .map_err(|e| redb_err("remove in edge", e))?;
                // Build forward key: "src\x00label\x00dst"
                let parts: Vec<&str> = key.splitn(3, '\x00').collect();
                if parts.len() == 3 {
                    let fwd_key = format!("{}\x00{}\x00{}", parts[2], parts[1], parts[0]);
                    let _ = edges.remove(fwd_key.as_str());
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit edge cascade", e))?;
        Ok(())
    }

    /// Get a single edge's properties. Returns None if the edge doesn't exist.
    pub fn get_edge(&self, src: &str, label: &str, dst: &str) -> crate::Result<Option<Vec<u8>>> {
        let key = edge_key(src, label, dst);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        match table.get(key.as_str()).map_err(|e| redb_err("get", e))? {
            Some(val) => Ok(Some(val.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Scan forward edges with a key prefix, parsing composite keys.
    pub(super) fn scan_edges_with_prefix<F>(
        &self,
        prefix: &str,
        mut make_edge: F,
    ) -> crate::Result<Vec<Edge>>
    where
        F: FnMut(&str, &str, &str) -> Edge,
    {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        let mut edges = Vec::new();
        let range = table.range(prefix..).map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let key_str = key.value();
            if !key_str.starts_with(prefix) {
                break;
            }
            if let Some((src, label, dst)) = parse_edge_key(key_str) {
                let mut edge = make_edge(src, label, dst);
                edge.properties = val.value().to_vec();
                edges.push(edge);
            }
        }

        Ok(edges)
    }

    /// Insert by raw pre-formed key into the forward edge table (snapshot restore).
    pub fn put_raw(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("raw write txn", e))?;
        {
            let mut table = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("raw insert", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Insert by raw pre-formed key into the reverse edge table (snapshot restore).
    pub fn put_raw_reverse(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("raw write txn", e))?;
        {
            let mut table = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse_edges", e))?;
            table
                .insert(key, value)
                .map_err(|e| redb_err("raw insert reverse", e))?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    #[test]
    fn put_and_get_edge() {
        let (store, _dir) = make_store();
        let props = b"msgpack-props";
        store.put_edge("alice", "KNOWS", "bob", props).unwrap();

        let result = store.get_edge("alice", "KNOWS", "bob").unwrap();
        assert_eq!(result, Some(props.to_vec()));
    }

    #[test]
    fn get_nonexistent_edge() {
        let (store, _dir) = make_store();
        let result = store.get_edge("alice", "KNOWS", "bob").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_edge() {
        let (store, _dir) = make_store();
        store.put_edge("alice", "KNOWS", "bob", b"").unwrap();
        assert!(store.delete_edge("alice", "KNOWS", "bob").unwrap());
        assert!(!store.delete_edge("alice", "KNOWS", "bob").unwrap());
        assert!(store.get_edge("alice", "KNOWS", "bob").unwrap().is_none());
    }

    #[test]
    fn neighbors_out_all_labels() {
        let (store, _dir) = make_store();
        store.put_edge("alice", "KNOWS", "bob", b"").unwrap();
        store.put_edge("alice", "KNOWS", "carol", b"").unwrap();
        store.put_edge("alice", "WORKS_WITH", "dave", b"").unwrap();

        let edges = store.neighbors_out("alice", None).unwrap();
        assert_eq!(edges.len(), 3);

        let dst_ids: Vec<&str> = edges.iter().map(|e| e.dst_id.as_str()).collect();
        assert!(dst_ids.contains(&"bob"));
        assert!(dst_ids.contains(&"carol"));
        assert!(dst_ids.contains(&"dave"));
    }

    #[test]
    fn neighbors_out_filtered_by_label() {
        let (store, _dir) = make_store();
        store.put_edge("alice", "KNOWS", "bob", b"").unwrap();
        store.put_edge("alice", "WORKS_WITH", "carol", b"").unwrap();

        let edges = store.neighbors_out("alice", Some("KNOWS")).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst_id, "bob");
    }

    #[test]
    fn neighbors_in() {
        let (store, _dir) = make_store();
        store.put_edge("alice", "KNOWS", "bob", b"").unwrap();
        store.put_edge("carol", "KNOWS", "bob", b"").unwrap();

        let edges = store.neighbors_in("bob", Some("KNOWS")).unwrap();
        assert_eq!(edges.len(), 2);
        let src_ids: Vec<&str> = edges.iter().map(|e| e.src_id.as_str()).collect();
        assert!(src_ids.contains(&"alice"));
        assert!(src_ids.contains(&"carol"));
    }

    #[test]
    fn neighbors_both() {
        let (store, _dir) = make_store();
        store.put_edge("alice", "KNOWS", "bob", b"").unwrap();
        store.put_edge("carol", "KNOWS", "alice", b"").unwrap();

        let edges = store
            .neighbors("alice", Some("KNOWS"), Direction::Both)
            .unwrap();
        assert_eq!(edges.len(), 2);
    }

    #[test]
    fn edge_properties_preserved() {
        let (store, _dir) = make_store();
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::F64(0.95),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();

        store.put_edge("a", "CITES", "b", &buf).unwrap();

        let loaded = store.get_edge("a", "CITES", "b").unwrap().unwrap();
        let decoded: rmpv::Value = rmpv::decode::read_value(&mut loaded.as_slice()).unwrap();
        assert_eq!(decoded, props);
    }

    #[test]
    fn put_overwrites_properties() {
        let (store, _dir) = make_store();
        store.put_edge("a", "L", "b", b"v1").unwrap();
        store.put_edge("a", "L", "b", b"v2").unwrap();

        let result = store.get_edge("a", "L", "b").unwrap().unwrap();
        assert_eq!(result, b"v2");
    }

    #[test]
    fn out_degree_and_in_degree() {
        let (store, _dir) = make_store();
        store.put_edge("a", "X", "b", b"").unwrap();
        store.put_edge("a", "X", "c", b"").unwrap();
        store.put_edge("d", "X", "b", b"").unwrap();

        assert_eq!(store.out_degree("a", None).unwrap(), 2);
        assert_eq!(store.in_degree("b", None).unwrap(), 2);
        assert_eq!(store.in_degree("c", None).unwrap(), 1);
    }

    #[test]
    fn traverse_bfs_simple() {
        let (store, _dir) = make_store();
        // a -> b -> c -> d
        store.put_edge("a", "NEXT", "b", b"").unwrap();
        store.put_edge("b", "NEXT", "c", b"").unwrap();
        store.put_edge("c", "NEXT", "d", b"").unwrap();

        // 1 hop from a: {a, b}
        let mut result = store
            .traverse_bfs(&["a"], Some("NEXT"), Direction::Out, 1)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b"]);

        // 2 hops from a: {a, b, c}
        let mut result = store
            .traverse_bfs(&["a"], Some("NEXT"), Direction::Out, 2)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);

        // 3 hops: all nodes
        let mut result = store
            .traverse_bfs(&["a"], Some("NEXT"), Direction::Out, 3)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn traverse_bfs_with_cycle() {
        let (store, _dir) = make_store();
        store.put_edge("a", "L", "b", b"").unwrap();
        store.put_edge("b", "L", "c", b"").unwrap();
        store.put_edge("c", "L", "a", b"").unwrap(); // cycle

        let mut result = store
            .traverse_bfs(&["a"], Some("L"), Direction::Out, 5)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]); // no infinite loop
    }

    #[test]
    fn traverse_bfs_bidirectional() {
        let (store, _dir) = make_store();
        store.put_edge("a", "L", "b", b"").unwrap();
        store.put_edge("c", "L", "a", b"").unwrap();

        // Both directions from a: reaches b (outbound) and c (inbound)
        let mut result = store
            .traverse_bfs(&["a"], Some("L"), Direction::Both, 1)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn traverse_bfs_multiple_start_nodes() {
        let (store, _dir) = make_store();
        store.put_edge("a", "L", "b", b"").unwrap();
        store.put_edge("c", "L", "d", b"").unwrap();

        let mut result = store
            .traverse_bfs(&["a", "c"], Some("L"), Direction::Out, 1)
            .unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn isolated_node_traversal() {
        let (store, _dir) = make_store();
        let result = store
            .traverse_bfs(&["lonely"], None, Direction::Out, 5)
            .unwrap();
        assert_eq!(result, vec!["lonely"]);
    }

    #[test]
    fn inbound_neighbors_carry_properties() {
        let (store, _dir) = make_store();
        store
            .put_edge("alice", "CITED", "paper1", b"props-data")
            .unwrap();

        let edges = store.neighbors_in("paper1", Some("CITED")).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].src_id, "alice");
        assert_eq!(edges[0].properties, b"props-data");
    }
}
