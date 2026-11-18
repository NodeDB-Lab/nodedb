// SPDX-License-Identifier: BUSL-1.1

//! The replay arms `CoreLoop::replay_engines_in_lsn_order` drives, in its
//! dispatch order.
//!
//! A committed-redo apply tags each error an arm records with the arm that
//! ran. The variants order as the arms run, so the smallest tag among a
//! record's errors names the error a whole-record pass records first.

/// One step of the replay dispatch, in the order the steps run.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
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

impl RedoArm {
    /// The replay arms in the order `replay_engines_in_lsn_order` runs
    /// them. `Reconstitute` runs before them and is no arm of the list.
    pub(crate) const DISPATCH: [RedoArm; 12] = [
        RedoArm::Vector,
        RedoArm::VectorExtended,
        RedoArm::Document,
        RedoArm::Kv,
        RedoArm::Timeseries,
        RedoArm::Array,
        RedoArm::Crdt,
        RedoArm::Fts,
        RedoArm::Spatial,
        RedoArm::GraphNodeLabel,
        RedoArm::Graph,
        RedoArm::GraphNodeLabelRedo,
    ];
}

#[cfg(test)]
mod tests {
    use super::RedoArm;

    /// Whether replay dispatches `arm` through [`RedoArm::DISPATCH`]. The
    /// match is exhaustive, so a new variant fails to compile here until
    /// its place is decided.
    fn dispatched(arm: RedoArm) -> bool {
        match arm {
            RedoArm::Reconstitute => false,
            RedoArm::Vector
            | RedoArm::VectorExtended
            | RedoArm::Document
            | RedoArm::Kv
            | RedoArm::Timeseries
            | RedoArm::Array
            | RedoArm::Crdt
            | RedoArm::Fts
            | RedoArm::Spatial
            | RedoArm::GraphNodeLabel
            | RedoArm::Graph
            | RedoArm::GraphNodeLabelRedo => true,
        }
    }

    /// Errors rank by the variant order, so the dispatch order must be the
    /// variant order: strictly ascending, every dispatched arm once, and
    /// every arm after `Reconstitute`.
    #[test]
    fn dispatch_order_matches_replay_order() {
        for pair in RedoArm::DISPATCH.windows(2) {
            assert!(pair[0] < pair[1], "{:?} runs before {:?}", pair[0], pair[1]);
        }
        for arm in RedoArm::DISPATCH {
            assert!(dispatched(arm), "{arm:?} is no dispatched arm");
            assert!(
                RedoArm::Reconstitute < arm,
                "{arm:?} ranks before reconstitution"
            );
        }
        let all = [
            RedoArm::Reconstitute,
            RedoArm::Vector,
            RedoArm::VectorExtended,
            RedoArm::Document,
            RedoArm::Kv,
            RedoArm::Timeseries,
            RedoArm::Array,
            RedoArm::Crdt,
            RedoArm::Fts,
            RedoArm::Spatial,
            RedoArm::GraphNodeLabel,
            RedoArm::Graph,
            RedoArm::GraphNodeLabelRedo,
        ];
        for arm in all {
            assert_eq!(
                dispatched(arm),
                RedoArm::DISPATCH.contains(&arm),
                "{arm:?} dispatch membership"
            );
        }
    }
}
