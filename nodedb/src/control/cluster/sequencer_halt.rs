// SPDX-License-Identifier: BUSL-1.1

//! Node-wide markers for a halted Calvin sequencer state machine and a halted
//! Calvin scheduler.
//!
//! The sequencer stops applying epoch batches when a NEW committed entry
//! re-mints an epoch this replica already consumed — the one divergence that
//! can neither be applied (its transaction identities alias live lock-table and
//! completion state) nor dropped (that is silent loss of committed writes).
//!
//! The consequence is scoped to sequencing, and the escalation is scoped to
//! match. A halted sequencer does not stop this node from reading, from serving
//! metadata, or from applying any engine's non-Calvin writes; taking the whole
//! process down would convert a subsystem fault into a full outage — on a
//! single-node deployment, into total unavailability — which is a worse outcome
//! than the fault itself. So the node keeps serving, Calvin submissions fail
//! fast instead of hanging, and this marker makes the degradation visible on the
//! same surfaces a wedged metadata applier uses, so it can never be mistaken for
//! a healthy node.
//!
//! A Calvin scheduler halts one vShard when a replica-local error leaves a
//! sequenced txn neither applied nor identically aborted on this replica. The
//! same scoping holds: the node keeps serving, and [`CalvinApplyHaltMarker`]
//! makes the lost vShard visible on the same surfaces.

use std::sync::OnceLock;

use nodedb_cluster::calvin::SequencerHalt;

/// First-writer-wins record of why this node stopped sequencing.
///
/// The halt is latched in the state machine itself and never clears without
/// operator intervention, so there is nothing to overwrite: a later report would
/// only restate the same cause.
#[derive(Debug, Default)]
pub struct SequencerHaltMarker {
    halt: OnceLock<SequencerHalt>,
    apply: CalvinApplyHaltMarker,
}

impl SequencerHaltMarker {
    /// Record the first halt. Later calls are ignored.
    pub fn record(&self, halt: SequencerHalt) {
        let _ = self.halt.set(halt);
    }

    /// The recorded halt, if this node's sequencer has stopped.
    pub fn report(&self) -> Option<&SequencerHalt> {
        self.halt.get()
    }

    pub fn is_halted(&self) -> bool {
        self.halt.get().is_some()
    }

    /// The node-wide marker for halted Calvin schedulers.
    pub fn apply_halt(&self) -> &CalvinApplyHaltMarker {
        &self.apply
    }
}

/// Why one vShard's Calvin scheduler stopped applying sequenced txns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalvinApplyHalt {
    pub vshard_id: u32,
    /// Epoch of the txn held unapplied.
    pub epoch: u64,
    /// Position of the txn held unapplied.
    pub position: u32,
    /// Halt reason label, as on `nodedb_calvin_apply_halted`.
    pub reason: &'static str,
    /// Sub-operation of the txn that failed.
    pub step: &'static str,
    pub error: String,
}

/// First-writer-wins record of the first Calvin scheduler that halted on this
/// node. A halted scheduler stays halted until restart, so nothing clears it.
#[derive(Debug, Default)]
pub struct CalvinApplyHaltMarker {
    halt: OnceLock<CalvinApplyHalt>,
}

impl CalvinApplyHaltMarker {
    /// Record the first halt. Later calls are ignored.
    pub fn record(&self, halt: CalvinApplyHalt) {
        let _ = self.halt.set(halt);
    }

    /// The recorded halt, if a scheduler on this node halted.
    pub fn report(&self) -> Option<&CalvinApplyHalt> {
        self.halt.get()
    }

    pub fn is_halted(&self) -> bool {
        self.halt.get().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn halt(found_epoch: u64) -> SequencerHalt {
        SequencerHalt {
            expected_epoch: 7,
            found_epoch,
            txns_in_batch: 2,
            raft_index: 41,
        }
    }

    #[test]
    fn marker_starts_clear() {
        let marker = SequencerHaltMarker::default();
        assert!(!marker.is_halted());
        assert!(marker.report().is_none());
    }

    fn apply_halt(vshard_id: u32) -> CalvinApplyHalt {
        CalvinApplyHalt {
            vshard_id,
            epoch: 9,
            position: 1,
            reason: "flush_failed",
            step: "flush",
            error: "flush returned Error".to_string(),
        }
    }

    #[test]
    fn apply_marker_keeps_the_first_recorded_halt() {
        let marker = SequencerHaltMarker::default();
        assert!(!marker.apply_halt().is_halted());
        marker.apply_halt().record(apply_halt(3));
        marker.apply_halt().record(apply_halt(4));
        assert!(marker.apply_halt().is_halted());
        assert_eq!(marker.apply_halt().report().map(|h| h.vshard_id), Some(3));
        assert!(
            !marker.is_halted(),
            "a scheduler halt leaves the sequencer halt clear"
        );
    }

    #[test]
    fn marker_keeps_the_first_recorded_halt() {
        let marker = SequencerHaltMarker::default();
        marker.record(halt(3));
        marker.record(halt(4));
        assert!(marker.is_halted());
        assert_eq!(marker.report().map(|h| h.found_epoch), Some(3));
    }
}
