// SPDX-License-Identifier: BUSL-1.1

//! Apply node-label ops to the CSR and stage the label versions they write.
//!
//! Each op applies to the CSR on its own. Its version is the node's label set
//! after the op, at the op's ordinal: the ordinal its record carries, or a
//! fresh one from this core's clock. An exact set is a restored version: it
//! holds exactly the op's labels, and the CSR takes them unless a newer
//! version of the node is already stored.
//!
//! The caller writes the versions of every op of one WAL record together
//! (`EdgeStore::put_node_label_versions`), in one storage transaction with
//! the record's LSN mark.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::graph::edge_store::NodeLabelVersion;
use crate::types::{DatabaseId, TenantId};
use crate::wal::NodeLabelRedo;

/// Whether a label op sets or removes its labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum LabelOpKind {
    Set,
    Remove,
}

/// One label op applied to the CSR.
pub(in crate::data::executor) struct AppliedLabelOp {
    /// Reverses the op's CSR change. It names no version: the version is not
    /// written yet.
    pub(in crate::data::executor) undo: UndoEntry,
    /// The version the op writes. `None` when boot replay finds its record
    /// already versioned.
    pub(in crate::data::executor) version: Option<NodeLabelVersion>,
}

impl CoreLoop {
    /// Apply `op` to the CSR for a record at `lsn`. A refused op leaves the
    /// CSR as it found it and answers the refusal.
    pub(in crate::data::executor) fn apply_label_op(
        &mut self,
        database_id: u64,
        tid: u64,
        op: &NodeLabelRedo,
        kind: LabelOpKind,
        lsn: Option<u64>,
    ) -> Result<AppliedLabelOp, ErrorCode> {
        let db = DatabaseId::new(database_id);
        let tenant = TenantId::new(tid);
        // Node labels are tenant-wide: every historical CSR of the tenant
        // drops.
        self.historical_csr.invalidate_tenant(db, tenant);
        let due = self.edge_store.label_version_due(lsn);
        let exact = op.exact && kind == LabelOpKind::Set;
        let ordinal = (due || exact).then(|| self.label_op_ordinal(op.system_from));
        let undo = match kind {
            LabelOpKind::Set if exact => {
                self.set_exact_node_labels(database_id, tid, op, ordinal)?
            }
            LabelOpKind::Set => {
                let undo = self.capture_node_labels_undo(database_id, tid, &op.node_id, &op.labels);
                self.set_node_labels(database_id, tid, &op.node_id, &op.labels)?;
                undo
            }
            LabelOpKind::Remove => {
                let undo = self.capture_node_labels_undo(database_id, tid, &op.node_id, &op.labels);
                let partition = self.csr_partition_mut(database_id, tid);
                // `remove_node_label` never vivifies a node.
                for label in &op.labels {
                    partition.remove_node_label(&op.node_id, label);
                }
                undo
            }
        };
        let version = match ordinal {
            Some(ordinal) if due => {
                let labels = if exact {
                    let mut labels = op.labels.clone();
                    labels.sort();
                    labels.dedup();
                    labels
                } else {
                    self.current_node_labels(database_id, tid, &op.node_id)
                };
                Some(NodeLabelVersion {
                    db,
                    tid: tenant,
                    node: op.node_id.clone(),
                    ordinal,
                    labels,
                })
            }
            _ => None,
        };
        Ok(AppliedLabelOp { undo, version })
    }

    /// The undo of a written label version: it writes back `replaced`, or
    /// removes the version when `replaced` is `None`.
    pub(in crate::data::executor) fn label_version_undo(
        version: &NodeLabelVersion,
        replaced: Option<Vec<String>>,
    ) -> UndoEntry {
        UndoEntry::NodeLabels {
            database_id: version.db.as_u64(),
            tid: version.tid.as_u64(),
            node_id: version.node.clone(),
            prior: Vec::new(),
            interned_labels: Vec::new(),
            created_node: false,
            version: Some(version.ordinal),
            replaced,
        }
    }

    /// `node_id`'s current label set, sorted.
    pub(in crate::data::executor) fn current_node_labels(
        &self,
        database_id: u64,
        tid: u64,
        node_id: &str,
    ) -> Vec<String> {
        let mut labels: Vec<String> = self
            .csr_partition(database_id, tid)
            .and_then(|p| {
                p.node_id_raw(node_id)
                    .map(|id| p.node_labels(id).into_iter().map(str::to_string).collect())
            })
            .unwrap_or_default();
        labels.sort();
        labels
    }

    /// The ordinal of a label op: the one its record carries, folded into
    /// the local clock, or a fresh one.
    fn label_op_ordinal(&self, system_from: Option<i64>) -> i64 {
        match system_from {
            Some(ordinal) => {
                self.hlc.update_from_remote(ordinal);
                ordinal
            }
            None => self.graph_write_ordinal(),
        }
    }

    /// Bring the node's labels to the exact set of a restored version at
    /// `ordinal`: the op's labels, or a newer stored version's labels.
    fn set_exact_node_labels(
        &mut self,
        database_id: u64,
        tid: u64,
        op: &NodeLabelRedo,
        ordinal: Option<i64>,
    ) -> Result<UndoEntry, ErrorCode> {
        let newer = match ordinal {
            Some(ordinal) => self
                .edge_store
                .node_labels_above(
                    DatabaseId::new(database_id),
                    TenantId::new(tid),
                    &op.node_id,
                    ordinal,
                )
                .map_err(ErrorCode::from)?,
            None => None,
        };
        let target = newer.unwrap_or_else(|| op.labels.clone());
        let current = self.current_node_labels(database_id, tid, &op.node_id);
        let added: Vec<String> = target
            .iter()
            .filter(|label| !current.contains(label))
            .cloned()
            .collect();
        let removed: Vec<String> = current
            .iter()
            .filter(|label| !target.contains(label))
            .cloned()
            .collect();
        let touched: Vec<String> = added.iter().chain(&removed).cloned().collect();
        let undo = self.capture_node_labels_undo(database_id, tid, &op.node_id, &touched);
        self.set_node_labels(database_id, tid, &op.node_id, &added)?;
        let partition = self.csr_partition_mut(database_id, tid);
        for label in &removed {
            partition.remove_node_label(&op.node_id, label);
        }
        Ok(undo)
    }
}
