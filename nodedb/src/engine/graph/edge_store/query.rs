use nodedb_types::TenantId;

use super::store::{Direction, Edge, EdgeStore, REVERSE_EDGES, redb_err};
use super::temporal::{
    EdgeValuePayload, is_sentinel, parse_versioned_edge_key, versioned_edge_key,
};

impl EdgeStore {
    /// Outbound neighbors of a node within the caller's tenant and
    /// collection, current-state only.
    pub fn neighbors_out(
        &self,
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
    /// Scans the reverse index (dst-first versioned keys), dedupes by base,
    /// and drops bases whose latest reverse entry is a sentinel.
    pub fn neighbors_in(
        &self,
        tid: TenantId,
        collection: &str,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<Vec<Edge>> {
        use std::collections::HashMap;

        let prefix = match label_filter {
            Some(label) => format!("{collection}\x00{dst}\x00{label}\x00"),
            None => format!("{collection}\x00{dst}\x00"),
        };
        let t = tid.as_u32();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        // In the reverse index the key is `{coll}\x00{dst}\x00{label}\x00{src}\x00{sys}`.
        // So the parsed tuple's "src" position IS the original dst, and the
        // "dst" position IS the original src. We track versions by the
        // logical base `(coll, src, label, dst)` i.e. after the swap.
        let mut latest: HashMap<(String, String, String), (i64, bool)> = HashMap::new();
        let range = table
            .range((t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;
        for entry in range {
            let (key, val) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let Some((_coll, _rev_dst, rev_label, rev_src, sys)) =
                parse_versioned_edge_key(composite)
            else {
                continue;
            };
            // Logical src (the neighbor we want) and label form the base.
            let base = (rev_label.to_string(), rev_src.to_string(), dst.to_string());
            let is_sent = is_sentinel(val.value());
            latest
                .entry(base)
                .and_modify(|(cur, cur_sent)| {
                    if sys > *cur {
                        *cur = sys;
                        *cur_sent = is_sent;
                    }
                })
                .or_insert((sys, is_sent));
        }

        // Live bases only — load properties from forward table's Ceiling.
        let mut edges = Vec::with_capacity(latest.len());
        for ((label, src_id, dst_id), (_sys, is_sent)) in latest {
            if is_sent {
                continue;
            }
            let Some(props) = self.ceiling_resolve_edge(
                tid,
                collection,
                &src_id,
                &label,
                &dst_id,
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
    pub fn neighbors(
        &self,
        tid: TenantId,
        collection: &str,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> crate::Result<Vec<Edge>> {
        match direction {
            Direction::Out => self.neighbors_out(tid, collection, node, label_filter),
            Direction::In => self.neighbors_in(tid, collection, node, label_filter),
            Direction::Both => {
                let mut out = self.neighbors_out(tid, collection, node, label_filter)?;
                let inbound = self.neighbors_in(tid, collection, node, label_filter)?;
                out.extend(inbound);
                Ok(out)
            }
        }
    }

    /// Count outbound edges from a source node (current-state).
    pub fn out_degree(
        &self,
        tid: TenantId,
        collection: &str,
        src: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self
            .neighbors_out(tid, collection, src, label_filter)?
            .len())
    }

    /// Count inbound edges to a destination node (current-state).
    pub fn in_degree(
        &self,
        tid: TenantId,
        collection: &str,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self.neighbors_in(tid, collection, dst, label_filter)?.len())
    }
}

// Keep `versioned_edge_key` / `EdgeValuePayload` referenced in case a future
// tier inlines them here instead of routing through `scan_edges_with_prefix`.
#[allow(dead_code)]
fn _keep_temporal_helpers_referenced() {
    let _ = versioned_edge_key;
    let _: fn(_, _, _) -> _ = EdgeValuePayload::new;
}
