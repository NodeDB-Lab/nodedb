// SPDX-License-Identifier: BUSL-1.1

//! The replay arms `CoreLoop::replay_engines_in_lsn_order` drives, in its
//! dispatch order.
//!
//! A committed-redo apply tags each error an arm records with the arm that
//! ran. The variants order as the arms run, so the smallest tag among a
//! record's errors names the error a whole-record pass records first.

/// One step of the replay dispatch, in the order the steps run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RedoArm {
    /// The redo records reconstitute into sub-records before any arm runs.
    Reconstitute,
    Vector,
    VectorExtended,
    Document,
    Kv,
    Timeseries,
    Array,
    Crdt,
    Fts,
    Spatial,
    /// Standalone graph node-label records.
    GraphNodeLabel,
    Graph,
    /// Node-label deltas inside redo records.
    GraphNodeLabelRedo,
}
