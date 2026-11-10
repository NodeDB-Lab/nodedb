// SPDX-License-Identifier: Apache-2.0

//! Edge weight management for the CSR index.
//!
//! Optional `f64` weight per edge stored in parallel arrays. `None` when the
//! graph is entirely unweighted (zero memory overhead). Populated from the
//! `"weight"` edge property at insertion time. Unweighted edges default to 1.0.

use super::index::CsrIndex;
use crate::csr::LocalNodeId;

impl CsrIndex {
    /// Enable weight tracking. Backfills existing buffer entries with 1.0.
    pub(crate) fn enable_weights(&mut self) {
        self.has_weights = true;

        // Backfill existing dense arrays with 1.0.
        if !self.out_targets.is_empty() {
            self.out_weights = Some(vec![1.0; self.out_targets.len()].into());
        }
        if !self.in_targets.is_empty() {
            self.in_weights = Some(vec![1.0; self.in_targets.len()].into());
        }

        // Backfill existing buffer entries with 1.0.
        for (buf, wbuf) in self
            .buffer_out
            .iter()
            .zip(self.buffer_out_weights.iter_mut())
        {
            if wbuf.len() < buf.len() {
                wbuf.resize(buf.len(), 1.0);
            }
        }
        for (buf, wbuf) in self.buffer_in.iter().zip(self.buffer_in_weights.iter_mut()) {
            if wbuf.len() < buf.len() {
                wbuf.resize(buf.len(), 1.0);
            }
        }
    }

    /// Whether this CSR has any weighted edges.
    pub fn has_weights(&self) -> bool {
        self.has_weights
    }

    /// Get the weight of the i-th outbound edge from a node (dense CSR only).
    ///
    /// `edge_idx` is the absolute index into `out_targets`/`out_weights`.
    /// Returns 1.0 for unweighted graphs.
    pub fn out_edge_weight(&self, edge_idx: usize) -> f64 {
        self.out_weights
            .as_ref()
            .and_then(|ws| ws.get(edge_idx).copied())
            .unwrap_or(1.0)
    }

    /// Get the weight of the i-th inbound edge to a node (dense CSR only).
    pub fn in_edge_weight(&self, edge_idx: usize) -> f64 {
        self.in_weights
            .as_ref()
            .and_then(|ws| ws.get(edge_idx).copied())
            .unwrap_or(1.0)
    }

    /// Get the weight of a specific outbound edge from `src` to `dst` via `label`.
    ///
    /// Checks both dense and buffer. Returns 1.0 if the edge exists but has
    /// no weight, or `None` if the edge doesn't exist.
    pub fn edge_weight(&self, src: &str, label: &str, dst: &str) -> Option<f64> {
        let src_id = *self.node_to_id.get(src)?;
        let dst_id = *self.node_to_id.get(dst)?;
        let label_id = *self.label_to_id.get(label)?;

        // Check dense CSR.
        let idx = src_id as usize;
        if idx + 1 < self.out_offsets.len() {
            let start = self.out_offsets[idx] as usize;
            let end = self.out_offsets[idx + 1] as usize;
            for i in start..end {
                let coll = self.out_collections.get(i).copied().unwrap_or(0);
                if self.out_labels[i] == label_id
                    && self.out_targets[i] == dst_id
                    && !self
                        .deleted_edges
                        .contains(&(src_id, label_id, dst_id, coll))
                {
                    return Some(self.out_edge_weight(i));
                }
            }
        }

        // Check buffer.
        if idx < self.buffer_out.len() {
            for (buf_idx, &(l, d)) in self.buffer_out[idx].iter().enumerate() {
                if l == label_id && d == dst_id {
                    if self.has_weights {
                        return Some(
                            self.buffer_out_weights[idx]
                                .get(buf_idx)
                                .copied()
                                .unwrap_or(1.0),
                        );
                    }
                    return Some(1.0);
                }
            }
        }

        None
    }

    /// Iterate outbound edges of a node with weights: `(label_id, dst_id, weight)`.
    ///
    /// Yields from both dense CSR and buffer, excluding deleted edges.
    /// Weights are 1.0 for unweighted graphs.
    pub fn iter_out_edges_weighted(
        &self,
        node: LocalNodeId,
    ) -> impl Iterator<Item = (u32, LocalNodeId, f64)> + '_ {
        let node = node.raw(self.partition_tag);
        let tag = self.partition_tag;
        let idx = node as usize;

        // Dense edges with weights.
        let dense_start = if idx + 1 < self.out_offsets.len() {
            self.out_offsets[idx] as usize
        } else {
            0
        };
        let dense_end = if idx + 1 < self.out_offsets.len() {
            self.out_offsets[idx + 1] as usize
        } else {
            0
        };

        let dense = (dense_start..dense_end)
            .filter_map(move |i| {
                let lid = self.out_labels[i];
                let dst = self.out_targets[i];
                let coll = self.out_collections.get(i).copied().unwrap_or(0);
                if self.deleted_edges.contains(&(node, lid, dst, coll)) {
                    None
                } else {
                    Some((lid, dst, self.out_edge_weight(i)))
                }
            })
            .collect::<Vec<_>>();

        // Buffer edges with weights.
        let buffer = if idx < self.buffer_out.len() {
            self.buffer_out[idx]
                .iter()
                .enumerate()
                .map(|(buf_idx, &(lid, dst))| {
                    let w = if self.has_weights {
                        self.buffer_out_weights[idx]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0)
                    } else {
                        1.0
                    };
                    (lid, dst, w)
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        dense
            .into_iter()
            .chain(buffer)
            .map(move |(lid, dst, w)| (lid, LocalNodeId::new(dst, tag), w))
    }

    /// Raw u32 variant of `iter_out_edges_weighted`. In-partition
    /// algorithm use only — see [`Self::iter_out_edges_raw`] for the
    /// safety rationale.
    pub fn iter_out_edges_weighted_raw(
        &self,
        node: u32,
    ) -> impl Iterator<Item = (u32, u32, f64)> + '_ {
        self.iter_out_edges_weighted(self.local(node))
            .map(move |(lid, dst, w)| (lid, dst.raw(self.partition_tag), w))
    }
}

/// Extract the `"weight"` property from an edge's stored property image.
///
/// Reads the native tagged object and a plain MessagePack map alike. An
/// Integer, Float or Decimal weight is the weight, a Decimal rounded to the
/// nearest `f64`. Returns 1.0 when the properties are empty or malformed,
/// carry no `"weight"`, or carry a non-numeric one.
pub fn extract_weight_from_properties(properties: &[u8]) -> f64 {
    if properties.is_empty() {
        return 1.0;
    }
    let Ok(view) = nodedb_types::ObjectView::new(properties) else {
        return 1.0;
    };
    match view.get("weight") {
        Ok(Some(value)) => crate::properties::property_weight(&value).unwrap_or(1.0),
        Ok(None) | Err(_) => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::test_memory;

    #[test]
    fn unweighted_graph_has_no_weight_arrays() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        assert!(!csr.has_weights());
        assert!(csr.out_weights.is_none());
        assert!(csr.in_weights.is_none());
    }

    #[test]
    fn weighted_edge_basic() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_weighted("a", "ROAD", "b", 5.0).unwrap();
        csr.add_edge_weighted("b", "ROAD", "c", 3.0).unwrap();
        csr.add_edge("c", "ROAD", "d").unwrap(); // unweighted → 1.0

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "ROAD", "b"), Some(5.0));
        assert_eq!(csr.edge_weight("b", "ROAD", "c"), Some(3.0));
        assert_eq!(csr.edge_weight("c", "ROAD", "d"), Some(1.0));
        assert_eq!(csr.edge_weight("a", "ROAD", "c"), None);
    }

    #[test]
    fn weighted_edges_survive_compaction() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_weighted("a", "R", "b", 2.5).unwrap();
        csr.add_edge_weighted("b", "R", "c", 7.0).unwrap();
        csr.add_edge("c", "R", "d").unwrap();

        csr.compact()
            .expect("test governor ceiling covers this reservation");

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(csr.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(csr.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn weighted_edge_remove_keeps_weights_consistent() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_weighted("a", "R", "b", 2.0).unwrap();
        csr.add_edge_weighted("a", "R", "c", 3.0).unwrap();
        csr.add_edge_weighted("a", "R", "d", 4.0).unwrap();

        csr.remove_edge("a", "R", "c");

        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.0));
        assert_eq!(csr.edge_weight("a", "R", "c"), None);
        assert_eq!(csr.edge_weight("a", "R", "d"), Some(4.0));
    }

    #[test]
    fn iter_out_edges_weighted_returns_weights() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge_weighted("a", "R", "b", 2.5).unwrap();
        csr.add_edge_weighted("a", "R", "c", 7.0).unwrap();
        csr.compact()
            .expect("test governor ceiling covers this reservation");

        let edges: Vec<(u32, LocalNodeId, f64)> =
            csr.iter_out_edges_weighted(csr.local(0)).collect();
        assert_eq!(edges.len(), 2);

        let weights: Vec<f64> = edges.iter().map(|e| e.2).collect();
        assert!(weights.contains(&2.5));
        assert!(weights.contains(&7.0));
    }

    #[test]
    fn mixed_weighted_unweighted_backfill() {
        let mut csr = CsrIndex::new(test_memory());
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        assert!(!csr.has_weights());

        csr.add_edge_weighted("c", "L", "d", 5.0).unwrap();
        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "L", "b"), Some(1.0));
        assert_eq!(csr.edge_weight("c", "L", "d"), Some(5.0));
    }

    #[test]
    fn extract_weight_from_empty_properties() {
        assert_eq!(extract_weight_from_properties(b""), 1.0);
    }

    fn stored(fields: Vec<(&str, nodedb_types::Value)>) -> Vec<u8> {
        crate::properties::encode_edge_properties(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
        .expect("encode properties")
    }

    #[test]
    fn extract_weight_f64() {
        let buf = stored(vec![("weight", nodedb_types::Value::Float(0.75))]);
        assert_eq!(extract_weight_from_properties(&buf), 0.75);
    }

    #[test]
    fn extract_weight_integer() {
        let buf = stored(vec![("weight", nodedb_types::Value::Integer(42))]);
        assert_eq!(extract_weight_from_properties(&buf), 42.0);
    }

    #[test]
    fn extract_weight_decimal() {
        let buf = stored(vec![(
            "weight",
            nodedb_types::Value::Decimal("12.50".parse().expect("decimal")),
        )]);
        assert_eq!(extract_weight_from_properties(&buf), 12.5);
    }

    #[test]
    fn extract_weight_from_a_plain_map() {
        let buf = nodedb_types::json_msgpack::value_to_msgpack(&nodedb_types::Value::Object(
            [("weight".to_string(), nodedb_types::Value::Float(2.5))].into(),
        ))
        .expect("encode plain");
        assert_eq!(extract_weight_from_properties(&buf), 2.5);
    }

    #[test]
    fn extract_weight_missing_key() {
        let buf = stored(vec![("color", nodedb_types::Value::String("red".into()))]);
        assert_eq!(extract_weight_from_properties(&buf), 1.0);
    }

    #[test]
    fn a_non_numeric_or_malformed_weight_defaults_to_one() {
        let buf = stored(vec![("weight", nodedb_types::Value::String("5".into()))]);
        assert_eq!(extract_weight_from_properties(&buf), 1.0);
        assert_eq!(extract_weight_from_properties(&[0xc1]), 1.0);
    }
}
