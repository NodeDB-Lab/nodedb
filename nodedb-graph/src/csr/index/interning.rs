// SPDX-License-Identifier: Apache-2.0

//! Node / label string↔id interning and the node-label bitset.

use std::collections::hash_map::Entry;

use super::types::CsrIndex;

impl CsrIndex {
    /// Get or create a dense ID for a node.
    ///
    /// Returns `Err(GraphError::NodeOverflow)` when the partition already holds
    /// `MAX_NODES_PER_CSR` nodes and a new name is introduced. The check uses a
    /// typed `Result` rather than `debug_assert!` so the failure mode is loud
    /// and deterministic — a silent `u32` wrap would reproduce the same class
    /// of bug as the label-overflow issue this crate fixed previously.
    pub(crate) fn ensure_node(&mut self, node: &str) -> Result<u32, crate::GraphError> {
        match self.node_to_id.entry(node.to_string()) {
            Entry::Occupied(e) => Ok(*e.get()),
            Entry::Vacant(e) => {
                let len = self.id_to_node.len();
                if len >= crate::MAX_NODES_PER_CSR {
                    return Err(crate::GraphError::NodeOverflow { used: len });
                }
                let id = len as u32;
                e.insert(id);
                self.id_to_node.push(node.to_string());
                // Extend dense offsets (new node has 0 edges in dense part).
                self.out_offsets
                    .push(*self.out_offsets.last().unwrap_or(&0));
                self.in_offsets.push(*self.in_offsets.last().unwrap_or(&0));
                // Extend buffer and access tracking.
                self.buffer_out.push(Vec::new());
                self.buffer_in.push(Vec::new());
                self.buffer_out_weights.push(Vec::new());
                self.buffer_in_weights.push(Vec::new());
                self.buffer_out_collections.push(Vec::new());
                self.buffer_in_collections.push(Vec::new());
                self.node_label_bits.push(0);
                // Surrogate is populated later by the EdgePut handler; start
                // with the ZERO sentinel so unset nodes are never in a bitmap.
                self.node_surrogates.push(0);
                self.access_counts.push(std::cell::Cell::new(0));
                Ok(id)
            }
        }
    }

    /// Get or create a dense ID for a label.
    ///
    /// Returns `Err(GraphError::LabelOverflow)` if the id space is
    /// exhausted (`MAX_EDGE_LABELS` distinct labels already interned).
    /// The DSL ingress layer enforces a far tighter user-facing cap,
    /// so this branch is effectively unreachable in practice — but the
    /// Result is propagated so the ceiling failure mode is typed and
    /// loud rather than a silent cast.
    pub(crate) fn ensure_label(&mut self, label: &str) -> Result<u32, crate::GraphError> {
        match self.label_to_id.entry(label.to_string()) {
            Entry::Occupied(e) => Ok(*e.get()),
            Entry::Vacant(e) => {
                let len = self.id_to_label.len();
                if len >= crate::MAX_EDGE_LABELS {
                    return Err(crate::GraphError::LabelOverflow { used: len });
                }
                let id = len as u32;
                e.insert(id);
                self.id_to_label.push(label.to_string());
                Ok(id)
            }
        }
    }

    /// Get or create a dense id for a collection name.
    ///
    /// Unlike labels/nodes there is no hard cap: the number of distinct
    /// collections in a partition is bounded by DDL and far below any `u32`
    /// concern. `""` (the unscoped sentinel) interns like any other name.
    pub(crate) fn ensure_collection(&mut self, collection: &str) -> u32 {
        match self.collection_to_id.entry(collection.to_string()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let id = self.id_to_collection.len() as u32;
                e.insert(id);
                self.id_to_collection.push(collection.to_string());
                id
            }
        }
    }

    /// Look up the dense collection id for a collection name, if interned.
    ///
    /// Returns `None` when no edge in this partition was inserted under
    /// `collection` — the collection-scoped read paths (MATCH `IN '<c>'`,
    /// RAG) treat that as "no edges in this collection" (empty result).
    pub fn collection_id(&self, collection: &str) -> Option<u32> {
        self.collection_to_id.get(collection).copied()
    }

    /// Collection name for a dense collection id.
    pub fn collection_name(&self, collection_id: u32) -> Option<&str> {
        self.id_to_collection
            .get(collection_id as usize)
            .map(String::as_str)
    }

    // ── Surrogate methods ──

    /// Set the surrogate for a node, identified by its user-visible name.
    ///
    /// Call once per node after `add_edge` / `add_edge_weighted` has allocated
    /// a CSR-local id for it. If the node is not yet interned this is a no-op
    /// (the edge insertion that follows will intern it and a subsequent call
    /// with the correct surrogate will set it). Calling with `Surrogate::ZERO`
    /// is a no-op — the zero sentinel is the initial state and has no meaning.
    pub fn set_node_surrogate(&mut self, node: &str, surrogate: nodedb_types::Surrogate) {
        let raw = surrogate.as_u32();
        if raw == 0 {
            return;
        }
        if let Some(&id) = self.node_to_id.get(node)
            && let Some(slot) = self.node_surrogates.get_mut(id as usize)
        {
            *slot = raw;
            self.surrogate_to_local.insert(raw, id);
        }
    }

    /// Forward lookup: the `Surrogate` bound to a node by name, or `None` if the
    /// node is unknown to this partition or has no surrogate set (ZERO sentinel).
    ///
    /// A graph node and its same-pk document share one global surrogate, so this
    /// is the bridge from a MATCH binding's node name to the document storage key
    /// (`StorageKey::for_surrogate`) used to fetch the node's properties.
    pub fn node_surrogate(&self, node: &str) -> Option<nodedb_types::Surrogate> {
        let &local_id = self.node_to_id.get(node)?;
        let raw = self.node_surrogate_raw(local_id);
        if raw == 0 {
            None
        } else {
            Some(nodedb_types::Surrogate::new(raw))
        }
    }

    /// Return the raw surrogate `u32` for a CSR-local node id.
    ///
    /// Returns `0` (the ZERO sentinel) when the node has no surrogate set.
    pub fn node_surrogate_raw(&self, local_id: u32) -> u32 {
        self.node_surrogates
            .get(local_id as usize)
            .copied()
            .unwrap_or(0)
    }

    /// Resolve a node name to its CSR-local id.
    ///
    /// The name table is rebuilt from durable storage on every open, so this
    /// resolution always works; the surrogate table is not, which is why a
    /// name-seeded read must not be routed through
    /// [`Self::local_id_for_surrogate`].
    pub fn local_id_for_node(&self, node: &str) -> Option<u32> {
        self.node_to_id.get(node).copied()
    }

    /// Resolve a `Surrogate` to its CSR-local node id without touching the
    /// node-name table.
    ///
    /// This is the entry point for reads that stay in the surrogate domain:
    /// seeding a traversal through [`Self::node_id_for_surrogate`] would
    /// materialize a name only to hash it straight back to this same id.
    /// Returns `None` for `Surrogate::ZERO` and for surrogates never bound
    /// in this partition.
    pub fn local_id_for_surrogate(&self, surrogate: nodedb_types::Surrogate) -> Option<u32> {
        let raw = surrogate.as_u32();
        if raw == 0 {
            return None;
        }
        self.surrogate_to_local.get(&raw).copied()
    }

    /// Look up the user-visible node name bound to a `Surrogate`.
    ///
    /// Returns `None` for `Surrogate::ZERO` and for surrogates that have
    /// never been bound to a node in this partition. Used by cross-engine
    /// fusion (graph RAG) to translate a vector-side surrogate into a
    /// graph BFS seed name.
    pub fn node_id_for_surrogate(&self, surrogate: nodedb_types::Surrogate) -> Option<&str> {
        let raw = surrogate.as_u32();
        if raw == 0 {
            return None;
        }
        let local_id = *self.surrogate_to_local.get(&raw)?;
        self.id_to_node.get(local_id as usize).map(String::as_str)
    }

    // ── Node label methods ──

    /// Get or create a node label ID (max 64 labels).
    fn ensure_node_label(&mut self, label: &str) -> Option<u8> {
        if let Some(&id) = self.node_label_to_id.get(label) {
            return Some(id);
        }
        let id = self.node_label_names.len();
        if id >= 64 {
            return None; // bitset limit
        }
        let id = id as u8;
        self.node_label_to_id.insert(label.to_string(), id);
        self.node_label_names.push(label.to_string());
        Some(id)
    }

    /// Add a label to a node.
    ///
    /// Returns `Ok(false)` if the 64 distinct node-label limit is hit (the
    /// label is silently ignored). Returns `Err(GraphError::NodeOverflow)` if
    /// the node is new and the partition's node-id space is exhausted.
    pub fn add_node_label(&mut self, node: &str, label: &str) -> Result<bool, crate::GraphError> {
        let node_id = self.ensure_node(node)?;
        let Some(label_id) = self.ensure_node_label(label) else {
            return Ok(false);
        };
        self.node_label_bits[node_id as usize] |= 1u64 << label_id;
        Ok(true)
    }

    /// Remove a label from a node.
    pub fn remove_node_label(&mut self, node: &str, label: &str) {
        let Some(&node_id) = self.node_to_id.get(node) else {
            return;
        };
        let Some(&label_id) = self.node_label_to_id.get(label) else {
            return;
        };
        self.node_label_bits[node_id as usize] &= !(1u64 << label_id);
    }

    /// Check if a node has a specific label.
    pub fn node_has_label(&self, node_id: u32, label: &str) -> bool {
        let Some(&label_id) = self.node_label_to_id.get(label) else {
            return false;
        };
        let bits = self
            .node_label_bits
            .get(node_id as usize)
            .copied()
            .unwrap_or(0);
        bits & (1u64 << label_id) != 0
    }

    /// Every node carrying at least one label, as `(node_name, label_names)`.
    ///
    /// Node NAMES, not local ids: the local id of a node is assigned by
    /// whatever order the CSR was built in (the `EdgeStore` scan on a rebuild,
    /// or `ensure_node` vivification order on a replay), so it is not stable
    /// across restarts. Any consumer that persists this state and installs it
    /// back through `add_node_label` therefore reattaches each label to the
    /// node it actually belonged to, instead of to whichever node happens to
    /// hold that id in the next process.
    ///
    /// Unlabeled nodes are skipped — their bitset is zero and carries no
    /// information.
    pub fn labeled_nodes(&self) -> Vec<(&str, Vec<&str>)> {
        let mut out = Vec::new();
        for (local_id, &bits) in self.node_label_bits.iter().enumerate() {
            if bits == 0 {
                continue;
            }
            let Some(name) = self.id_to_node.get(local_id) else {
                continue;
            };
            let labels = self.node_labels(local_id as u32);
            if labels.is_empty() {
                continue;
            }
            out.push((name.as_str(), labels));
        }
        out
    }

    /// Get all label names for a node.
    pub fn node_labels(&self, node_id: u32) -> Vec<&str> {
        let bits = self
            .node_label_bits
            .get(node_id as usize)
            .copied()
            .unwrap_or(0);
        if bits == 0 {
            return Vec::new();
        }
        let mut labels = Vec::new();
        for (i, name) in self.node_label_names.iter().enumerate() {
            if bits & (1u64 << i) != 0 {
                labels.push(name.as_str());
            }
        }
        labels
    }
}
