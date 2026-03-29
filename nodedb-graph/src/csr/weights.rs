//! Edge weight management for the CSR index.
//!
//! Optional `f64` weight per edge stored in parallel arrays. `None` when the
//! graph is entirely unweighted (zero memory overhead). Populated from the
//! `"weight"` edge property at insertion time. Unweighted edges default to 1.0.

use super::index::CsrIndex;

impl CsrIndex {
    /// Enable weight tracking. Backfills existing buffer entries with 1.0.
    pub(crate) fn enable_weights(&mut self) {
        self.has_weights = true;

        // Backfill existing dense arrays with 1.0.
        if !self.out_targets.is_empty() {
            self.out_weights = Some(vec![1.0; self.out_targets.len()]);
        }
        if !self.in_targets.is_empty() {
            self.in_weights = Some(vec![1.0; self.in_targets.len()]);
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
                if self.out_labels[i] == label_id
                    && self.out_targets[i] == dst_id
                    && !self.deleted_edges.contains(&(src_id, label_id, dst_id))
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
    pub fn iter_out_edges_weighted(&self, node: u32) -> impl Iterator<Item = (u16, u32, f64)> + '_ {
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
            .map(move |i| {
                let w = self.out_edge_weight(i);
                (self.out_labels[i], self.out_targets[i], w)
            })
            .filter(move |&(lid, dst, _)| !self.deleted_edges.contains(&(node, lid, dst)))
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

        dense.into_iter().chain(buffer)
    }
}

/// Extract the `"weight"` property from MessagePack-encoded edge properties.
///
/// Returns 1.0 if properties are empty, malformed, or don't contain a
/// `"weight"` key. Handles F64, F32, and integer weight values; other
/// numeric types default to 1.0.
pub fn extract_weight_from_properties(properties: &[u8]) -> f64 {
    if properties.is_empty() {
        return 1.0;
    }
    let Ok(val) = rmpv::decode::read_value(&mut &properties[..]) else {
        return 1.0;
    };
    match val {
        rmpv::Value::Map(entries) => {
            for (k, v) in entries {
                if let rmpv::Value::String(ref s) = k
                    && s.as_str() == Some("weight")
                {
                    return match v {
                        rmpv::Value::F64(f) => f,
                        rmpv::Value::F32(f) => f as f64,
                        rmpv::Value::Integer(i) => i.as_f64().unwrap_or(1.0),
                        _ => 1.0,
                    };
                }
            }
            1.0
        }
        _ => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unweighted_graph_has_no_weight_arrays() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        assert!(!csr.has_weights());
        assert!(csr.out_weights.is_none());
        assert!(csr.in_weights.is_none());
    }

    #[test]
    fn weighted_edge_basic() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "ROAD", "b", 5.0);
        csr.add_edge_weighted("b", "ROAD", "c", 3.0);
        csr.add_edge("c", "ROAD", "d"); // unweighted → 1.0

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "ROAD", "b"), Some(5.0));
        assert_eq!(csr.edge_weight("b", "ROAD", "c"), Some(3.0));
        assert_eq!(csr.edge_weight("c", "ROAD", "d"), Some(1.0));
        assert_eq!(csr.edge_weight("a", "ROAD", "c"), None);
    }

    #[test]
    fn weighted_edges_survive_compaction() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("b", "R", "c", 7.0);
        csr.add_edge("c", "R", "d");

        csr.compact();

        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.5));
        assert_eq!(csr.edge_weight("b", "R", "c"), Some(7.0));
        assert_eq!(csr.edge_weight("c", "R", "d"), Some(1.0));
    }

    #[test]
    fn weighted_edge_remove_keeps_weights_consistent() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.0);
        csr.add_edge_weighted("a", "R", "c", 3.0);
        csr.add_edge_weighted("a", "R", "d", 4.0);

        csr.remove_edge("a", "R", "c");

        assert_eq!(csr.edge_weight("a", "R", "b"), Some(2.0));
        assert_eq!(csr.edge_weight("a", "R", "c"), None);
        assert_eq!(csr.edge_weight("a", "R", "d"), Some(4.0));
    }

    #[test]
    fn iter_out_edges_weighted_returns_weights() {
        let mut csr = CsrIndex::new();
        csr.add_edge_weighted("a", "R", "b", 2.5);
        csr.add_edge_weighted("a", "R", "c", 7.0);
        csr.compact();

        let edges: Vec<(u16, u32, f64)> = csr.iter_out_edges_weighted(0).collect();
        assert_eq!(edges.len(), 2);

        let weights: Vec<f64> = edges.iter().map(|e| e.2).collect();
        assert!(weights.contains(&2.5));
        assert!(weights.contains(&7.0));
    }

    #[test]
    fn mixed_weighted_unweighted_backfill() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b");
        csr.add_edge("b", "L", "c");
        assert!(!csr.has_weights());

        csr.add_edge_weighted("c", "L", "d", 5.0);
        assert!(csr.has_weights());
        assert_eq!(csr.edge_weight("a", "L", "b"), Some(1.0));
        assert_eq!(csr.edge_weight("c", "L", "d"), Some(5.0));
    }

    #[test]
    fn extract_weight_from_empty_properties() {
        assert_eq!(extract_weight_from_properties(b""), 1.0);
    }

    #[test]
    fn extract_weight_f64() {
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::F64(0.75),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();
        assert_eq!(extract_weight_from_properties(&buf), 0.75);
    }

    #[test]
    fn extract_weight_integer() {
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("weight".into()),
            rmpv::Value::Integer(rmpv::Integer::from(42)),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();
        assert_eq!(extract_weight_from_properties(&buf), 42.0);
    }

    #[test]
    fn extract_weight_missing_key() {
        let props = rmpv::Value::Map(vec![(
            rmpv::Value::String("color".into()),
            rmpv::Value::String("red".into()),
        )]);
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &props).unwrap();
        assert_eq!(extract_weight_from_properties(&buf), 1.0);
    }
}
