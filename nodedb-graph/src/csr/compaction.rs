//! CSR compaction: merges the mutable write buffer into dense arrays.

use super::index::CsrIndex;

impl CsrIndex {
    /// Merge the mutable buffer into the dense CSR arrays.
    ///
    /// Called during idle periods. Rebuilds the contiguous offset/target/label
    /// (and weight) arrays from scratch (buffer + surviving dense edges).
    /// The old arrays are dropped, freeing memory. O(E) where E = total edges.
    pub fn compact(&mut self) {
        let n = self.id_to_node.len();
        let mut new_out_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut new_in_edges: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n];
        let mut new_out_weights: Vec<Vec<f64>> = if self.has_weights {
            vec![Vec::new(); n]
        } else {
            Vec::new()
        };
        let mut new_in_weights: Vec<Vec<f64>> = if self.has_weights {
            vec![Vec::new(); n]
        } else {
            Vec::new()
        };

        // Collect surviving dense edges.
        for node in 0..n {
            let node_id = node as u32;
            let idx = node_id as usize;

            // Outbound dense edges.
            if idx + 1 < self.out_offsets.len() {
                let start = self.out_offsets[idx] as usize;
                let end = self.out_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.out_labels[i];
                    let dst = self.out_targets[i];
                    if !self.deleted_edges.contains(&(node_id, lid, dst)) {
                        new_out_edges[node].push((lid, dst));
                        if self.has_weights {
                            let w = self
                                .out_weights
                                .as_ref()
                                .map_or(1.0, |ws| ws.get(i).copied().unwrap_or(1.0));
                            new_out_weights[node].push(w);
                        }
                    }
                }
            }

            // Inbound dense edges.
            if idx + 1 < self.in_offsets.len() {
                let start = self.in_offsets[idx] as usize;
                let end = self.in_offsets[idx + 1] as usize;
                for i in start..end {
                    let lid = self.in_labels[i];
                    let src = self.in_targets[i];
                    if !self.deleted_edges.contains(&(src, lid, node_id)) {
                        new_in_edges[node].push((lid, src));
                        if self.has_weights {
                            let w = self
                                .in_weights
                                .as_ref()
                                .map_or(1.0, |ws| ws.get(i).copied().unwrap_or(1.0));
                            new_in_weights[node].push(w);
                        }
                    }
                }
            }
        }

        // Merge buffer edges.
        for node in 0..n {
            for (buf_idx, &(lid, dst)) in self.buffer_out[node].iter().enumerate() {
                if !new_out_edges[node]
                    .iter()
                    .any(|&(l, d)| l == lid && d == dst)
                {
                    new_out_edges[node].push((lid, dst));
                    if self.has_weights {
                        let w = self.buffer_out_weights[node]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0);
                        new_out_weights[node].push(w);
                    }
                }
            }
            for (buf_idx, &(lid, src)) in self.buffer_in[node].iter().enumerate() {
                if !new_in_edges[node]
                    .iter()
                    .any(|&(l, s)| l == lid && s == src)
                {
                    new_in_edges[node].push((lid, src));
                    if self.has_weights {
                        let w = self.buffer_in_weights[node]
                            .get(buf_idx)
                            .copied()
                            .unwrap_or(1.0);
                        new_in_weights[node].push(w);
                    }
                }
            }
        }

        // Build new dense arrays.
        let (out_offsets, out_targets, out_labels) = Self::build_dense(&new_out_edges);
        let (in_offsets, in_targets, in_labels) = Self::build_dense(&new_in_edges);

        self.out_offsets = out_offsets;
        self.out_targets = out_targets;
        self.out_labels = out_labels;
        self.in_offsets = in_offsets;
        self.in_targets = in_targets;
        self.in_labels = in_labels;

        // Build weight arrays (flatten per-node vecs into contiguous array).
        if self.has_weights {
            self.out_weights = Some(new_out_weights.into_iter().flatten().collect());
            self.in_weights = Some(new_in_weights.into_iter().flatten().collect());
        }

        // Clear buffer and deleted set.
        for buf in &mut self.buffer_out {
            buf.clear();
        }
        for buf in &mut self.buffer_in {
            buf.clear();
        }
        if self.has_weights {
            for buf in &mut self.buffer_out_weights {
                buf.clear();
            }
            for buf in &mut self.buffer_in_weights {
                buf.clear();
            }
        }
        self.deleted_edges.clear();
    }
}
