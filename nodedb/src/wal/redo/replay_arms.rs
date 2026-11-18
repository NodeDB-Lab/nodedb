// SPDX-License-Identifier: BUSL-1.1

//! The body of each replay arm `CoreLoop::replay_engines_in_lsn_order`
//! dispatches, one arm per call.
//!
//! Each `replay_*_wal` filters the slice it takes by `RecordType`, so every
//! arm takes the whole merged stream and selects its own records. Two arms
//! take another slice:
//! - `GraphNodeLabel` takes the standalone records. Its redo counterpart,
//!   `GraphNodeLabelRedo`, takes the reconstituted sub-records. Handing
//!   both the merged stream applies every label delta twice.
//! - `GraphNodeLabelRedo` is the only decoder that routes node-label deltas
//!   from a reconstituted `TransactionRedo` record.

use nodedb_wal::{TombstoneSet, WalRecord};

use super::op_index::RedoOpIndex;
use super::replay_arm::RedoArm;
use super::replay_origin::GroupedReplayOrigins;
use crate::data::executor::core_loop::CoreLoop;

/// The slices and gates one replay arm reads.
#[derive(Clone, Copy)]
pub(crate) struct ArmReplayInput<'a> {
    /// The records as read, before reconstitution.
    pub standalone: &'a [WalRecord],
    /// The standalone records merged with the reconstituted sub-records,
    /// in LSN order.
    pub ordered: &'a [WalRecord],
    /// The op index of each record of `ordered`.
    pub ops: &'a RedoOpIndex,
    /// The reconstituted sub-records only.
    pub redo_ops: &'a [WalRecord],
    pub grouped_origins: &'a GroupedReplayOrigins,
    pub num_cores: usize,
    pub tombstones: &'a TombstoneSet,
}

impl CoreLoop {
    /// Run the replay arm `arm` over `input`. An open committed-redo scope
    /// tags each error the arm records with `arm`.
    ///
    /// `Reconstitute` replays no record: reconstitution runs before every
    /// arm, in `replay_engines_in_lsn_order`.
    pub(crate) fn replay_one_arm(&mut self, arm: RedoArm, input: &ArmReplayInput<'_>) {
        self.enter_redo_arm(arm);
        let ArmReplayInput {
            standalone,
            ordered,
            ops,
            redo_ops,
            grouped_origins,
            num_cores,
            tombstones,
        } = *input;
        match arm {
            RedoArm::Reconstitute => {}
            // `VectorParams` registers here before the extended arm and the
            // document arm index any vector.
            RedoArm::Vector => {
                self.replay_vector_wal(ordered, num_cores, tombstones, grouped_origins)
            }
            // The only decoder of the `VectorDirectUpsert`, `SparseVectorPut`,
            // `SparseVectorDelete`, `MultiVectorPut` and `MultiVectorDelete`
            // sub-records the vector resolver emits.
            RedoArm::VectorExtended => {
                self.replay_vector_extended_wal(ordered, num_cores, tombstones)
            }
            // Every document row the WAL names, standalone and redo alike.
            RedoArm::Document => self.replay_document_redo(ordered, ops, num_cores, tombstones),
            RedoArm::Kv => self.replay_kv_wal(ordered, num_cores, tombstones),
            RedoArm::Timeseries => self.replay_timeseries_wal_with_origins(
                ordered,
                ops,
                num_cores,
                tombstones,
                grouped_origins,
            ),
            RedoArm::Array => self.replay_array_wal_with_ops(ordered, ops, num_cores, tombstones),
            // CRDT deltas and document/list intents share Loro state, so they
            // replay together in global LSN order: the standalone records and
            // the intents a committed transaction journalled as sub-records.
            RedoArm::Crdt => self.replay_crdt_wal_ordered(ordered, num_cores, tombstones),
            RedoArm::Fts => self.replay_fts_wal(ordered, num_cores, tombstones, grouped_origins),
            RedoArm::Spatial => {
                self.replay_spatial_wal(ordered, num_cores, tombstones, grouped_origins)
            }
            // Graph node labels have no redb-backed durability. A WAL record
            // is their only durable backing.
            RedoArm::GraphNodeLabel => self.replay_graph_node_label_wal(standalone, num_cores),
            // Every graph edge version the WAL names, standalone and redo
            // alike. The CSR rebuilds from the `EdgeStore` at open, so a
            // restart re-applies versions it holds at their exact keys, and a
            // point-in-time restore applies the versions its base lacks.
            RedoArm::Graph => self.replay_graph_redo(ordered, num_cores, tombstones),
            // Node-label deltas staged in a transaction resolve to the
            // `GraphNodeLabelSet` / `GraphNodeLabelRemove` shape the
            // autocommit path produces.
            RedoArm::GraphNodeLabelRedo => self.replay_graph_node_labels_redo(redo_ops, num_cores),
        }
    }
}
