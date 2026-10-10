// SPDX-License-Identifier: BUSL-1.1

//! CSR graph node-label checkpoint write + load operations for `CoreLoop`.
//!
//! ## Why the labels need this and the edges do not
//!
//! Graph edges are durable without the WAL: `apply_edge_put` commits them to
//! the redb-backed `EdgeStore` synchronously, and `CoreLoop::open` rebuilds the
//! whole CSR from that store
//! (`engine::graph::csr::rebuild::rebuild_sharded_from_store`) before any WAL
//! replay runs. Truncating the WAL cannot lose an edge, and checkpointing them
//! here would persist a second copy of state redb already owns.
//!
//! The node-label bitset (`CsrIndex::node_label_bits`) has no such store. The
//! rebuild reconstructs nodes and edges and leaves every node's label bitset at
//! zero — a `GraphNodeLabelSet` / `GraphNodeLabelRemove` WAL record is the only
//! durable trace a label ever existed, which is exactly why
//! `wal_replay_graph_labels.rs` is a standalone replay pass. Yet a label write
//! advances the core watermark (`GraphOp::SetNodeLabels` goes through
//! `note_write` like any other write), so the periodic checkpoint reported
//! it as durable and the manager truncated the segments holding the only copy.
//! The label silently vanished on the next restart, while the edges around it
//! came back intact — the node was still there, `MATCH (a:Person)` just stopped
//! matching it.
//!
//! ## What is persisted
//!
//! Per `(database, tenant)` partition, one `(node_name, [label_names])` entry
//! for every node whose bitset is non-zero. Nothing else: no nodes, no edges,
//! no surrogates, no interning tables.
//!
//! Node NAMES, not the raw `node_label_bits` vector, because a local node id is
//! assigned by the order the CSR was built in — the `EdgeStore` scan order on a
//! rebuild, or `ensure_node` vivification order on a replay — and neither is
//! stable across restarts. Restoring a bitset by index would attach each label
//! to whichever node happened to land on that id in the new process. Label
//! names rather than label ids for the same reason: `ensure_node_label` interns
//! in first-seen order.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/graph-label-ckpt/core-{core_id}/STATE
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core owns only the partitions routed to its vShards.
//!
//! ## Why one file and no generation
//!
//! The KV checkpoint publishes a `gen-{n}/` directory named by a separate
//! manifest because its state is one file per collection, and a multi-file
//! write cannot be made atomic by rename alone. That is the only thing a
//! generation buys, so the question here is whether the labels must be split
//! across files at all.
//!
//! They must not. The LSN this checkpoint reports is core-wide, so every
//! partition has to become durable at one LSN together or none may: a published
//! state where tenant A's labels advanced and tenant B's did not is not a state
//! this checkpoint is allowed to claim. Splitting per partition would create
//! exactly that possibility and then need a manifest to rule it out again.
//! Holding every partition in ONE file makes the split unrepresentable, and a
//! single `atomic_write_fsync` is already all-or-nothing. Nor does splitting
//! save any work: the export is a full rewrite of the live state every cycle,
//! identical in total bytes either way.
//!
//! ## Why no replay floor
//!
//! Both label records are absolute, not deltas: `GraphNodeLabelSet` ORs a bit
//! on, `GraphNodeLabelRemove` ANDs it off, both keyed by `(node, label)` name.
//! Re-applying one over a restored state that already contains it reproduces
//! that state, so replaying the retained WAL in LSN order on top of the restore
//! converges on the same bitset a from-zero replay would reach. A floor would
//! gate records for no reason — see `replay_floors.rs`.

mod format;
mod load;
mod paths;
mod write;

pub(crate) use paths::{graph_label_ckpt_dir, graph_label_ckpt_state_path};
