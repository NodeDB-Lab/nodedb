// SPDX-License-Identifier: BUSL-1.1

//! The CSR graph node-label checkpoint write path: export every partition's
//! labeled nodes and publish them with one atomic write.

use tracing::info;

use super::format::{
    GRAPH_LABEL_CKPT_FORMAT_VERSION, GraphLabelCheckpointFile, GraphLabelPartition,
};
use super::paths::{GRAPH_LABEL_CKPT_STATE, graph_label_ckpt_dir, graph_label_ckpt_state_path};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Flush this core's CSR node labels to disk and return the LSN they are now
    /// durable through.
    ///
    /// Returns `Ok(watermark)` only once the state file has landed and been
    /// fsynced. Any failure returns `Err` — the caller must then clamp the
    /// reported checkpoint LSN to the last LSN the labels were known durable
    /// through, so a failed flush costs WAL growth instead of the
    /// `GraphNodeLabelSet` records that are the labels' only other copy.
    ///
    /// The single write is the commit point: before it the previous state file
    /// is intact and live, after it the new one is. There is no window in which
    /// half a core's partitions are published.
    ///
    /// Stamping with the core watermark rests on this: the checkpoint runs
    /// on the core's own thread between tasks, and a label write reaches
    /// `note_write_lsn` (which raises the watermark) only after
    /// `add_node_label` / `remove_node_label` has already mutated the bitset. So
    /// every label change with `lsn <= watermark` is in the export below.
    ///
    /// Edges are deliberately absent from the export. They are committed to the
    /// redb `EdgeStore` at apply time and the whole CSR is rebuilt from it in
    /// `CoreLoop::open` before replay, so persisting them here would write a
    /// second copy of state that cannot be lost — and a stale one, since the
    /// rebuild would overwrite it on the next boot regardless.
    pub(in crate::data::executor) fn checkpoint_graph_labels(&self) -> crate::Result<Lsn> {
        let durable_through = self.watermark;

        // Sorted at both levels so identical label state always encodes to
        // identical bytes.
        let mut partitions: Vec<GraphLabelPartition> = self
            .csr
            .iter()
            .filter_map(|(&(db, tid), partition)| {
                let mut nodes: Vec<(String, Vec<String>)> = partition
                    .labeled_nodes()
                    .into_iter()
                    .map(|(node, labels)| {
                        (
                            node.to_string(),
                            labels.into_iter().map(str::to_string).collect(),
                        )
                    })
                    .collect();
                // A partition with no labeled nodes carries nothing the rebuild
                // cannot reproduce, so it is omitted rather than written empty.
                if nodes.is_empty() {
                    return None;
                }
                nodes.sort_by(|a, b| a.0.cmp(&b.0));
                Some(GraphLabelPartition {
                    database_id: db.as_u64(),
                    tenant_id: tid.as_u64(),
                    nodes,
                })
            })
            .collect();
        partitions.sort_by_key(|p| (p.database_id, p.tenant_id));

        let labeled_nodes: usize = partitions.iter().map(|p| p.nodes.len()).sum();
        let file = GraphLabelCheckpointFile {
            format_version: GRAPH_LABEL_CKPT_FORMAT_VERSION,
            durable_through_lsn: durable_through.as_u64(),
            partitions,
        };
        let bytes = zerompk::to_msgpack_vec(&file).map_err(|e| crate::Error::Serialization {
            format: "msgpack".to_string(),
            detail: format!("graph node-label checkpoint encode failed: {e}"),
        })?;

        let ckpt_dir = graph_label_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;
        let path = graph_label_ckpt_state_path(&ckpt_dir);
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, GRAPH_LABEL_CKPT_STATE, &bytes)
            .map_err(|e| storage_err(&path, "publish state", &e))?;

        info!(
            core = self.core_id,
            partitions = file.partitions.len(),
            labeled_nodes,
            durable_through_lsn = durable_through.as_u64(),
            "graph node-label checkpoint published"
        );
        Ok(durable_through)
    }
}

/// Wrap a filesystem failure as the graph engine's typed storage error.
fn storage_err(path: &std::path::Path, action: &str, e: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "graph".to_string(),
        detail: format!(
            "graph node-label checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}
