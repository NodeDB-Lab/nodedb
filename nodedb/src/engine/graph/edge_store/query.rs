use nodedb_types::TenantId;

use super::store::{
    Direction, EDGES, Edge, EdgeStore, REVERSE_EDGES, edge_key, parse_edge_key, redb_err,
};

impl EdgeStore {
    /// Outbound neighbors of a node within the caller's tenant,
    /// optionally filtered by edge label.
    pub fn neighbors_out(
        &self,
        tid: TenantId,
        src: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<Vec<Edge>> {
        let prefix = match label_filter {
            Some(label) => format!("{src}\x00{label}\x00"),
            None => format!("{src}\x00"),
        };

        self.scan_edges_with_prefix(tid, &prefix, |fwd_src, fwd_label, fwd_dst| Edge {
            src_id: fwd_src.to_string(),
            label: fwd_label.to_string(),
            dst_id: fwd_dst.to_string(),
            properties: Vec::new(),
        })
    }

    /// Inbound neighbors of a node within the caller's tenant.
    pub fn neighbors_in(
        &self,
        tid: TenantId,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<Vec<Edge>> {
        let prefix = match label_filter {
            Some(label) => format!("{dst}\x00{label}\x00"),
            None => format!("{dst}\x00"),
        };
        let t = tid.as_u32();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(REVERSE_EDGES)
            .map_err(|e| redb_err("open reverse", e))?;

        let mut edges = Vec::new();
        let range = table
            .range((t, prefix.as_str())..)
            .map_err(|e| redb_err("range", e))?;

        for entry in range {
            let (key, _) = entry.map_err(|e| redb_err("iter", e))?;
            let (kt, composite) = key.value();
            if kt != t || !composite.starts_with(&prefix) {
                break;
            }
            if let Some((_rev_dst, rev_label, rev_src)) = parse_edge_key(composite) {
                edges.push(Edge {
                    src_id: rev_src.to_string(),
                    label: rev_label.to_string(),
                    dst_id: dst.to_string(),
                    properties: Vec::new(),
                });
            }
        }

        if !edges.is_empty() {
            let fwd_table = read_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            for edge in &mut edges {
                let fwd_key = edge_key(&edge.src_id, &edge.label, &edge.dst_id);
                if let Some(val) = fwd_table
                    .get((t, fwd_key.as_str()))
                    .map_err(|e| redb_err("get props", e))?
                {
                    edge.properties = val.value().to_vec();
                }
            }
        }

        Ok(edges)
    }

    /// All neighbors (both directions) within the caller's tenant.
    pub fn neighbors(
        &self,
        tid: TenantId,
        node: &str,
        label_filter: Option<&str>,
        direction: Direction,
    ) -> crate::Result<Vec<Edge>> {
        match direction {
            Direction::Out => self.neighbors_out(tid, node, label_filter),
            Direction::In => self.neighbors_in(tid, node, label_filter),
            Direction::Both => {
                let mut out = self.neighbors_out(tid, node, label_filter)?;
                let inbound = self.neighbors_in(tid, node, label_filter)?;
                out.extend(inbound);
                Ok(out)
            }
        }
    }

    /// Count outbound edges from a source node within the caller's tenant.
    pub fn out_degree(
        &self,
        tid: TenantId,
        src: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self.neighbors_out(tid, src, label_filter)?.len())
    }

    /// Count inbound edges to a destination node within the caller's tenant.
    pub fn in_degree(
        &self,
        tid: TenantId,
        dst: &str,
        label_filter: Option<&str>,
    ) -> crate::Result<usize> {
        Ok(self.neighbors_in(tid, dst, label_filter)?.len())
    }
}
