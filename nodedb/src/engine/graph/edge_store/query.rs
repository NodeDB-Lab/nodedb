// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::{DatabaseId, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::store::{Direction, Edge, EdgeStore, REVERSE_EDGES, redb_err};
use super::temporal::{EdgeRef, parse_versioned_edge_key};

impl EdgeStore {
    /// Outbound neighbors of a node within the caller's database, tenant and
    /// collection, current-state only.
    pub fn neighbors_out(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        src: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<Vec<Edge>> {
        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{src}\x00{label}\x00"),
            None => format!("{collection}\x00{src}\x00"),
        };

        self.scan_edges_with_prefix(
            db,
            tid,
            &prefix,
            |fwd_collection, fwd_src, fwd_label, fwd_dst| Edge {
                collection: fwd_collection.to_string(),
                src_id: fwd_src.to_string(),
                label: fwd_label.to_string(),
                dst_id: fwd_dst.to_string(),
                properties: Vec::new(),
            },
        )
    }

    /// Inbound neighbors of a node within the caller's tenant and
    /// collection, current-state only.
    ///
    /// Scans the reverse index (dst-first versioned keys) for the bases that
    /// have a version, and resolves each through the forward index, which
    /// skips the versions a TRUNCATE hides.
    pub fn neighbors_in(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<Vec<Edge>> {
        use std::collections::BTreeSet;

        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{dst}\x00{label}\x00"),
            None => format!("{collection}\x00{dst}\x00"),
        };
        let t = tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        // In the reverse index the key is `{coll}\x00{dst}\x00{label}\x00{src}\x00{sys}`.
        // So the parsed tuple's "src" position IS the original dst, and the
        // "dst" position IS the original src. We track bases by the logical
        // `(label, src, dst)`, i.e. after the swap.
        let mut bases: BTreeSet<(String, String, String)> = BTreeSet::new();
        let range = table
            .range((db, t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, _) = entry.map_err(|e| redb_err("iter", e))?;
            let (kd, kt, composite) = key.value();
            if kd != db || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((_coll, _rev_dst, rev_label, rev_src, _sys)) =
                parse_versioned_edge_key(composite)
            else {
                continue;
            };
            bases.insert((rev_label.to_string(), rev_src.to_string(), dst.to_string()));
        }
        drop(table);
        drop(read_txn);

        // The forward Ceiling decides each base: live, ended or hidden.
        let mut edges = Vec::with_capacity(bases.len());
        for (label, src_id, dst_id) in bases {
            let Some(props) = self.ceiling_resolve_edge(
                EdgeRef::new(
                    DatabaseId::new(db),
                    tid,
                    collection,
                    &src_id,
                    &label,
                    &dst_id,
                ),
                i64::MAX,
                None,
            )?
            else {
                continue;
            };
            edges.push(Edge {
                collection: collection.to_string(),
                src_id,
                label,
                dst_id,
                properties: props,
            });
        }
        Ok(edges)
    }

    /// All neighbors (both directions) within the caller's tenant and collection.
    /// `Both` lists each physical edge once, a self-loop included.
    pub fn neighbors(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> crate::Result<Vec<Edge>> {
        match direction {
            Direction::Out => self.neighbors_out(db, tid, collection, node, label_filter),
            Direction::In => self.neighbors_in(db, tid, collection, node, label_filter),
            Direction::Both => {
                let mut out = self.neighbors_out(db, tid, collection, node, label_filter)?;
                let inbound = self.neighbors_in(db, tid, collection, node, label_filter)?;
                out.extend(
                    inbound
                        .into_iter()
                        .filter(|edge| direction.in_pass_keeps(edge.src_id.as_str(), node)),
                );
                Ok(out)
            }
        }
    }

    /// Count outbound edges from a source node (current-state).
    pub fn out_degree(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        src: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self
            .neighbors_out(db, tid, collection, src, label_filter)?
            .len())
    }

    /// Count inbound edges to a destination node (current-state).
    pub fn in_degree(
        &self,
        db: u64,
        tid: TenantId,
        collection: &str,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self
            .neighbors_in(db, tid, collection, dst, label_filter)?
            .len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::OrdinalClock;

    const T: TenantId = TenantId::new(1);
    const COLL: &str = "people";

    /// A self-loop is one edge. `Both` lists it once, beside the node's
    /// other edges.
    #[test]
    fn both_lists_a_self_loop_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        let clock = OrdinalClock::new();
        for (src, dst) in [("a", "b"), ("a", "a")] {
            let ord = clock.next_ordinal();
            store
                .put_edge_versioned(
                    EdgeRef::new(DatabaseId::DEFAULT, T, COLL, src, "L", dst),
                    b"",
                    ord,
                    ord,
                    i64::MAX,
                )
                .unwrap();
        }
        let db = DatabaseId::DEFAULT.as_u64();
        let mut both: Vec<(String, String)> = store
            .neighbors(db, T, COLL, "a", None, Direction::Both)
            .unwrap()
            .into_iter()
            .map(|edge| (edge.src_id, edge.dst_id))
            .collect();
        both.sort();
        let pair = |s: &str, d: &str| (s.to_string(), d.to_string());
        assert_eq!(both, vec![pair("a", "a"), pair("a", "b")]);
        let inbound = store
            .neighbors(db, T, COLL, "a", None, Direction::In)
            .unwrap();
        assert_eq!(inbound.len(), 1);
    }
}
