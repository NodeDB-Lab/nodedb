// SPDX-License-Identifier: BUSL-1.1

//! Data-Plane idempotency gate for inbound sync frames.
//!
//! Each Data Plane core maintains two per-core maps:
//!
//! - `sync_hwm`: `(producer_id, stream_id) → last_applied_seq`
//! - `producer_epoch_floor`: `producer_id → highest_epoch_seen`
//!
//! Before applying a sync frame, the ingest handler calls [`CoreLoop::sync_admit`]
//! with the frame's [`SyncProvenance`]. Only [`SyncAdmit::Apply`] frames should be
//! written to the WAL and applied to engine state. After WAL durability the handler
//! calls [`CoreLoop::sync_commit`] to advance the HWM.
//!
//! Callers wired in Stage 3; HWM advance via `sync_commit` post-WAL-commit.

use nodedb_types::sync::wire::{AckStatus, SyncAckResult, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Decision returned by [`CoreLoop::sync_admit`].
///
/// The caller must match exhaustively — no `_ =>` default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAdmit {
    /// Frame is new and in-order: apply it to engine state, then call `sync_commit`.
    Apply,
    /// Frame has already been applied (`seq <= hwm`): ACK without reapplying.
    Duplicate,
    /// Frame's epoch is below the known floor for this producer: discard silently.
    Fenced,
    /// Frame skipped one or more sequence numbers: gap detected.
    Gap {
        /// The sequence number the receiver expected next.
        expected: u64,
    },
}

/// Map a [`SyncAdmit`] decision to the wire [`AckStatus`] for non-Apply arms.
///
/// Apply is intentionally excluded — callers construct `AckStatus::Applied`
/// themselves after a successful engine write and HWM advance.
pub(in crate::data::executor) fn ack_status_from_admit(d: &SyncAdmit) -> AckStatus {
    match d {
        SyncAdmit::Apply => AckStatus::Applied,
        SyncAdmit::Duplicate => AckStatus::Duplicate,
        SyncAdmit::Fenced => AckStatus::Fenced,
        SyncAdmit::Gap { expected } => AckStatus::Gap {
            expected: *expected,
        },
    }
}

impl CoreLoop {
    /// Classify a sync frame without changing epoch fencing or the HWM.
    ///
    /// This is the side-effect-free half of [`Self::sync_admit`]. Callers that
    /// must validate another precondition before consuming a newer producer
    /// epoch use it first; normal ingest must use `sync_admit` to retain the
    /// monotonic epoch-floor update.
    pub(in crate::data::executor) fn sync_classify(&self, prov: &SyncProvenance) -> SyncAdmit {
        // Producer zero is the local/unidentified sentinel and is deliberately
        // outside the producer sequencing protocol.
        if prov.producer_id == 0 {
            return SyncAdmit::Apply;
        }

        let floor = self
            .producer_epoch_floor
            .get(&prov.producer_id)
            .copied()
            .unwrap_or(0);
        if prov.epoch < floor {
            return SyncAdmit::Fenced;
        }

        let hwm = self
            .sync_hwm
            .get(&(prov.producer_id, prov.stream_id))
            .copied()
            .unwrap_or(0);
        if prov.seq <= hwm {
            return SyncAdmit::Duplicate;
        }
        if prov.seq > hwm + 1 {
            return SyncAdmit::Gap { expected: hwm + 1 };
        }
        SyncAdmit::Apply
    }

    /// Admit a sync frame and fence-forward a newer producer epoch.
    ///
    /// This preserves the historical admission semantics: every non-fenced
    /// frame from a newer epoch advances the epoch floor immediately, while
    /// HWM advancement remains the caller's post-durability responsibility.
    pub(in crate::data::executor) fn sync_admit(&mut self, prov: &SyncProvenance) -> SyncAdmit {
        let admit = self.sync_classify(prov);
        if prov.producer_id != 0
            && !matches!(admit, SyncAdmit::Fenced)
            && self
                .producer_epoch_floor
                .get(&prov.producer_id)
                .copied()
                .unwrap_or(0)
                < prov.epoch
        {
            self.producer_epoch_floor
                .insert(prov.producer_id, prov.epoch);
        }
        admit
    }

    /// Epoch-only fence check for engines that use their own dedup/ordering mechanism
    /// (e.g. the Array engine's HLC `already_seen` dedup).
    ///
    /// Unlike [`CoreLoop::sync_admit`], this does **not** check `seq` or the HWM.
    /// It is additive: the engine's native dedup continues to operate unchanged.
    ///
    /// Logic:
    /// 1. Look up `producer_epoch_floor[producer_id]` (default 0).
    /// 2. If `prov.epoch < floor` → return `false` (FENCED, no state change).
    /// 3. If `prov.epoch > floor` → advance the floor and return `true`.
    /// 4. If equal → return `true` (same generation, not fenced).
    pub(in crate::data::executor) fn sync_fence(&mut self, prov: &SyncProvenance) -> bool {
        // Unidentified producer (sentinel 0) is never fenced — see `sync_admit`.
        if prov.producer_id == 0 {
            return true;
        }

        let floor = self
            .producer_epoch_floor
            .get(&prov.producer_id)
            .copied()
            .unwrap_or(0);

        if prov.epoch < floor {
            return false;
        }

        if prov.epoch > floor {
            self.producer_epoch_floor
                .insert(prov.producer_id, prov.epoch);
        }

        true
    }

    /// Advance the sync HWM for `(producer_id, stream_id)` to `max(current, prov.seq)`.
    ///
    /// Must be called by Stage-3 ingest handlers **only after** the corresponding
    /// `SyncSeqAdvance` WAL record has been durably committed (fsync'd and Raft-quorum'd
    /// where applicable). Calling before durability leaves the HWM ahead of the WAL and
    /// breaks post-crash deduplication.
    ///
    /// Callers wired in Stage 3; HWM advance via `sync_commit` post-WAL-commit.
    pub(in crate::data::executor) fn sync_commit(&mut self, prov: &SyncProvenance) {
        // Never track the unidentified-producer sentinel — see `sync_admit`.
        if prov.producer_id == 0 {
            return;
        }
        let entry = self
            .sync_hwm
            .entry((prov.producer_id, prov.stream_id))
            .or_insert(0);
        if prov.seq > *entry {
            *entry = prov.seq;
        }
    }

    /// Build the `Response` carrying a msgpack-encoded [`SyncAckResult`] for a
    /// processed sync frame.
    ///
    /// Centralises the gate-ack reply that every engine ingest handler returns
    /// (vector / spatial / fts / timeseries / columnar), so the encoding and the
    /// failure behaviour stay identical across engines. A serialisation failure
    /// is surfaced as a deterministic `Internal` error — never a silent
    /// `response_ok` with an empty payload, which would leave the Control Plane
    /// unable to decode the ack and force it into a default-`Applied` fallback.
    pub(in crate::data::executor) fn sync_ack_response(
        &self,
        task: &ExecutionTask,
        status: AckStatus,
        applied_seq: u64,
    ) -> Response {
        self.sync_outcome_response(task, SyncAckResult::acked(status, applied_seq))
    }

    /// Refuse a frame **permanently**, before anything of it installed.
    ///
    /// The high-water-mark advances: the same bytes will fail identically on a
    /// re-push, so holding the stream for them buys nothing. The refusal is an
    /// error response, so the Control Plane cancels the frame's record and
    /// journals the mark in a `SyncSeqAdvance` record of its own. Restart
    /// replay then restores the mark and never applies the frame.
    ///
    /// A refusal the sender *should* retry is not this: it reports an
    /// [`AckStatus::Gap`] through [`Self::sync_ack_response`] and holds the
    /// mark, which is what keeps the re-push admissible.
    pub(in crate::data::executor) fn sync_reject_response(
        &mut self,
        task: &ExecutionTask,
        violation: nodedb_types::sync::violation::ViolationType,
        prov: &SyncProvenance,
    ) -> Response {
        self.sync_commit(prov);
        let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
        self.response_error(
            task,
            ErrorCode::SyncRejected {
                violation,
                applied_seq,
                provenance: prov.clone(),
            },
        )
    }

    fn sync_outcome_response(&self, task: &ExecutionTask, gate_result: SyncAckResult) -> Response {
        match zerompk::to_msgpack_vec(&gate_result) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("sync gate: serialize ack: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_types::OrdinalClock;
    use nodedb_types::sync::wire::SyncProvenance;
    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

    fn make_prov(producer_id: u64, epoch: u64, stream_id: u64, seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id,
            epoch,
            stream_id,
            seq,
        }
    }

    fn open_core() -> (CoreLoop, TempDir) {
        let dir = TempDir::new().expect("tempdir");
        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // not needed in gate tests
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    #[test]
    fn fresh_seq1_is_apply() {
        let (mut core, _dir) = open_core();
        let prov = make_prov(1, 1, 1, 1);
        assert_eq!(core.sync_admit(&prov), SyncAdmit::Apply);
    }

    #[test]
    fn producer_zero_sentinel_always_applies_and_is_not_tracked() {
        let (mut core, _dir) = open_core();
        // producer_id 0 = unidentified: never gated, never fenced, never tracked.
        let p0 = make_prov(0, 0, 5, 0);
        assert_eq!(core.sync_admit(&p0), SyncAdmit::Apply);
        assert!(core.sync_fence(&p0));
        core.sync_commit(&p0);
        // A repeat with the same zero provenance still applies (no dedup).
        assert_eq!(core.sync_admit(&p0), SyncAdmit::Apply);
        // The HWM map is not polluted by the sentinel.
        assert_eq!(core.sync_hwm_value(0, 5), 0);
        assert!(core.sync_hwm.is_empty());
    }

    #[test]
    fn after_commit_same_seq_is_duplicate() {
        let (mut core, _dir) = open_core();
        let prov = make_prov(1, 1, 1, 1);
        assert_eq!(core.sync_admit(&prov), SyncAdmit::Apply);
        core.sync_commit(&prov);
        assert_eq!(core.sync_admit(&prov), SyncAdmit::Duplicate);
    }

    #[test]
    fn gap_detected_when_seq_skips() {
        let (mut core, _dir) = open_core();
        // Advance HWM to seq=1 first.
        let prov1 = make_prov(1, 1, 1, 1);
        assert_eq!(core.sync_admit(&prov1), SyncAdmit::Apply);
        core.sync_commit(&prov1);
        // Now submit seq=3 (gap at seq=2).
        let prov3 = make_prov(1, 1, 1, 3);
        assert_eq!(core.sync_admit(&prov3), SyncAdmit::Gap { expected: 2 });
    }

    #[test]
    fn older_epoch_is_fenced() {
        let (mut core, _dir) = open_core();
        // Establish epoch floor at 5.
        let prov_new = make_prov(42, 5, 1, 1);
        assert_eq!(core.sync_admit(&prov_new), SyncAdmit::Apply);
        // Frame with epoch=3 < floor=5 → Fenced.
        let prov_old = make_prov(42, 3, 1, 2);
        assert_eq!(core.sync_admit(&prov_old), SyncAdmit::Fenced);
    }

    #[test]
    fn newer_epoch_advances_floor_and_is_accepted() {
        let (mut core, _dir) = open_core();
        // Start at epoch=2.
        let prov_e2 = make_prov(7, 2, 1, 1);
        assert_eq!(core.sync_admit(&prov_e2), SyncAdmit::Apply);
        core.sync_commit(&prov_e2);
        // Epoch=4 > floor=2 → fence-forward, accepted. seq continues in-order
        // (durable seq is preserved across epoch bumps), so the next frame is seq=2.
        let prov_e4 = make_prov(7, 4, 1, 2);
        assert_eq!(core.sync_admit(&prov_e4), SyncAdmit::Apply);
        assert_eq!(core.producer_epoch_floor.get(&7).copied().unwrap_or(0), 4);
    }

    #[test]
    fn sync_commit_advances_hwm_and_re_admit_is_duplicate() {
        let (mut core, _dir) = open_core();
        // Pre-seed the stream HWM to 9 so seq=10 is the in-order next frame.
        core.sync_hwm.insert((99, 5), 9);
        let prov = make_prov(99, 1, 5, 10);
        assert_eq!(core.sync_admit(&prov), SyncAdmit::Apply);
        core.sync_commit(&prov);
        assert_eq!(core.sync_hwm.get(&(99, 5)).copied().unwrap_or(0), 10);
        assert_eq!(core.sync_admit(&prov), SyncAdmit::Duplicate);
    }

    #[test]
    fn commit_is_idempotent_at_same_seq() {
        let (mut core, _dir) = open_core();
        let prov = make_prov(1, 1, 1, 5);
        // Set hwm manually higher, commit with lower seq → no regression.
        core.sync_hwm.insert((1, 1), 7);
        core.sync_commit(&prov); // seq=5 < hwm=7 → no change
        assert_eq!(core.sync_hwm.get(&(1, 1)).copied().unwrap_or(0), 7);
    }

    // ── sync_fence tests ─────────────────────────────────────────────────────

    #[test]
    fn fence_lower_epoch_returns_false_no_state_change() {
        let (mut core, _dir) = open_core();
        // Establish epoch floor at 5.
        let prov5 = make_prov(10, 5, 1, 1);
        assert!(core.sync_fence(&prov5));
        assert_eq!(core.producer_epoch_floor.get(&10).copied().unwrap_or(0), 5);
        // Older epoch → fenced, floor stays at 5.
        let prov3 = make_prov(10, 3, 1, 2);
        assert!(!core.sync_fence(&prov3));
        assert_eq!(core.producer_epoch_floor.get(&10).copied().unwrap_or(0), 5);
    }

    #[test]
    fn fence_equal_epoch_returns_true() {
        let (mut core, _dir) = open_core();
        let prov = make_prov(20, 7, 1, 1);
        assert!(core.sync_fence(&prov));
        // Same epoch again → still true (not fenced).
        assert!(core.sync_fence(&prov));
    }

    #[test]
    fn fence_higher_epoch_advances_floor_and_returns_true() {
        let (mut core, _dir) = open_core();
        let prov2 = make_prov(30, 2, 1, 1);
        assert!(core.sync_fence(&prov2));
        assert_eq!(core.producer_epoch_floor.get(&30).copied().unwrap_or(0), 2);
        // Higher epoch advances the floor.
        let prov9 = make_prov(30, 9, 1, 2);
        assert!(core.sync_fence(&prov9));
        assert_eq!(core.producer_epoch_floor.get(&30).copied().unwrap_or(0), 9);
        // Now epoch=2 < floor=9 → fenced.
        assert!(!core.sync_fence(&prov2));
    }
}
