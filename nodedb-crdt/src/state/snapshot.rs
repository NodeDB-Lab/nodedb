// SPDX-License-Identifier: Apache-2.0

//! Snapshot export/import, history compaction, memory estimation.

use crate::error::{CrdtError, Result};

use super::core::CrdtState;
use super::import_admission::{
    CrdtImportLimits, ImportAdmission, admit_import, admit_local_import,
};

impl CrdtState {
    /// Export the current state as bytes for sync.
    pub fn export_snapshot(&self) -> Result<Vec<u8>> {
        self.doc
            .export(loro::ExportMode::Snapshot)
            .map_err(|e| CrdtError::Loro(format!("snapshot export failed: {e}")))
    }

    /// Import remote updates under the finite default byte and operation caps.
    ///
    /// Returns the same [`ImportAdmission`] as [`Self::import_with_limits`].
    pub fn import(&self, data: &[u8]) -> Result<ImportAdmission> {
        self.import_with_limits(data, CrdtImportLimits::default())
    }

    /// Import bytes this process produced itself — a snapshot read back from
    /// durable storage, or a shallow snapshot taken during compaction.
    ///
    /// Keeps every structural check [`Self::import`] performs: metadata is
    /// decoded with Loro's authenticated decoder, and per-peer ranges that
    /// regress are rejected before any state mutation. What it drops is the
    /// size ceilings, which bound how much work an untrusted peer may cause
    /// and have no meaning for bytes this library just wrote.
    ///
    /// Without this split the ceilings apply to reloading as well as to
    /// receiving, and since `export_snapshot` has no bound at all, a document a
    /// healthy process wrote can exceed what the same binary will re-import.
    /// Past that point the document neither opens nor compacts — compaction
    /// being the operation that would bring it back under the limit.
    ///
    /// This is not a way to raise the peer limits: `import_with_limits` is the
    /// knob for that, and bytes off the wire must go through it.
    pub fn import_local(&self, data: &[u8]) -> Result<ImportAdmission> {
        let admission = admit_local_import(data, &self.doc.oplog_vv())?;
        let status = self
            .doc
            .import(data)
            .map_err(|e| CrdtError::DeltaApplyFailed(e.to_string()))?;
        if status.pending.is_some() {
            return Err(CrdtError::ImportPendingDependencies);
        }
        Ok(admission)
    }

    /// Import remote updates after bounded authenticated metadata admission.
    ///
    /// The limits are checked before Loro can import the blob, so rejected
    /// bytes never mutate this document or allocate an import graph.
    ///
    /// A change whose causal predecessors are absent from this document is
    /// buffered by Loro as *pending* and does not reach the applied state.
    /// A blob that carries any such change returns
    /// [`CrdtError::ImportPendingDependencies`], never success: a caller that
    /// took `Ok` here would acknowledge a write that was never applied. The
    /// ready changes in the same blob still apply, so the state can advance
    /// under that error. [`Self::import_tracked`] reports the rows they
    /// changed. The buffered changes stay queued inside Loro, so a later
    /// import carrying the missing predecessors still converges.
    ///
    /// The returned [`ImportAdmission`] reports how much of the blob was new
    /// and how much Loro trimmed as already-known. An `Ok` whose
    /// `new_operations` is zero means the document did not move: correct for an
    /// idempotent replay, and the exact shape a peer-id collision takes. A
    /// caller that treats every `Ok` as "the write landed" cannot tell the two
    /// apart, which is how a collision discards writes behind a green ack.
    pub fn import_with_limits(
        &self,
        data: &[u8],
        limits: CrdtImportLimits,
    ) -> Result<ImportAdmission> {
        let admission = admit_import(data, &self.doc.oplog_vv(), limits)?;
        let status = self
            .doc
            .import(data)
            .map_err(|e| CrdtError::DeltaApplyFailed(e.to_string()))?;
        if status.pending.is_some() {
            return Err(CrdtError::ImportPendingDependencies);
        }
        Ok(admission)
    }

    /// Compact the CRDT history by replacing the internal LoroDoc with a
    /// shallow snapshot.
    ///
    /// A shallow snapshot contains the current state but discards the
    /// full operation history. This is the CRDT equivalent of WAL
    /// truncation after checkpoint.
    ///
    /// After compaction:
    /// - All current state is preserved (reads return same values).
    /// - New deltas can still be applied and merged.
    /// - Historical operations before the snapshot point are gone.
    /// - Peers that sync after compaction receive a full snapshot
    ///   instead of incremental deltas (acceptable for long-offline peers).
    ///
    /// Call this periodically (e.g., every 30 minutes or when memory
    /// pressure exceeds threshold) to prevent unbounded history growth.
    pub fn compact_history(&mut self) -> Result<()> {
        // `oplog_frontiers` excludes an open auto-commit transaction. Commit
        // it first so the shallow root covers every write made so far.
        self.doc.commit();
        self.compact_to_frontiers(&self.doc.oplog_frontiers())
    }

    /// Replace this document with a shallow snapshot taken at `frontiers`.
    ///
    /// The whole of compaction, shared by `compact_history` (current
    /// frontiers) and `compact_at_version` (a chosen version), so the two
    /// cannot drift apart on how the snapshot is admitted or how the document
    /// is swapped in.
    ///
    /// The snapshot is admitted through [`admit_local_import`]: this process
    /// exported it one line earlier. Under the peer ceilings, compaction would
    /// be refused for precisely the documents large enough to need it — the
    /// operation that would bring a document back under the limit gated by
    /// that limit.
    pub(in crate::state) fn compact_to_frontiers(
        &mut self,
        frontiers: &loro::Frontiers,
    ) -> Result<()> {
        let snapshot = self
            .doc
            .export(loro::ExportMode::shallow_snapshot(frontiers))
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot export: {e}")))?;

        let compacted = Self::new_doc(self.peer_id)?;
        admit_local_import(&snapshot, &compacted.oplog_vv())?;
        compacted
            .import(&snapshot)
            .map_err(|e| CrdtError::Loro(format!("shallow snapshot import: {e}")))?;

        // `replace` also drops everything cached from the outgoing document. A
        // shallow snapshot keeps the version vector, so a derived value keyed
        // on the version alone would look current while describing bytes that
        // no longer exist.
        self.doc.replace(compacted);
        Ok(())
    }

    /// Estimated memory usage of the CRDT state (bytes).
    ///
    /// Includes operation history, current state, and internal caches.
    /// Use this to decide when to trigger `compact_history()`.
    ///
    /// Cheap enough to call on the write path. The underlying proxy is a
    /// snapshot export, which costs O(document), so it is used to calibrate a
    /// bytes-per-operation ratio and the answer comes from the oplog's
    /// operation counter — a real export runs only when the document has
    /// halved or doubled since it was last measured. Exact for a document that
    /// has not changed since then, an interpolation otherwise.
    ///
    /// Compaction discards the measurement along with the document it
    /// described.
    pub fn estimated_memory_bytes(&self) -> usize {
        self.doc.estimated_bytes()
    }

    /// Real snapshot exports performed to answer `estimated_memory_bytes`.
    #[cfg(test)]
    pub(crate) fn export_count_for_test(&self) -> usize {
        self.doc.export_count()
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::*;

    fn source_delta() -> Vec<u8> {
        let source = CrdtState::new(7).expect("source state");
        source
            .upsert(
                "docs",
                "row",
                &[("title", LoroValue::String("value".into()))],
            )
            .expect("source write");
        let empty = loro::VersionVector::default();
        source.export_updates_since(&empty).expect("source delta")
    }

    #[test]
    fn import_rejects_oversize_before_state_mutation() {
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        let result = state.import_with_limits(
            &[0u8; 2],
            CrdtImportLimits {
                max_bytes: 1,
                max_encoded_operations: 1,
                max_new_operations: 1,
            },
        );
        assert!(matches!(result, Err(CrdtError::ImportTooLarge { .. })));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn local_import_admits_what_the_peer_ceilings_would_reject() {
        let delta = source_delta();
        let capped = CrdtState::new(1).expect("capped state");
        let tight = CrdtImportLimits {
            max_bytes: delta.len(),
            max_encoded_operations: 0,
            max_new_operations: 0,
        };
        assert!(
            matches!(
                capped.import_with_limits(&delta, tight),
                Err(CrdtError::ImportOperationLimitExceeded { .. })
            ),
            "the peer path must stay capped"
        );

        let local = CrdtState::new(1).expect("local state");
        local.import_local(&delta).expect("local import");
        assert!(
            local.row_exists("docs", "row"),
            "bytes this process wrote must be reloadable whatever the peer ceilings are; a \
             document large enough to trip them can otherwise neither be opened nor compacted"
        );
    }

    #[test]
    fn from_local_snapshot_loads_without_the_peer_ceilings() {
        let source = CrdtState::new(3).expect("source state");
        source
            .upsert("docs", "row", &[("title", LoroValue::String("v".into()))])
            .expect("source write");
        let snapshot = source.export_snapshot().expect("snapshot");

        let reloaded = CrdtState::from_local_snapshot(3, &snapshot).expect("reload");
        assert!(
            reloaded.row_exists("docs", "row"),
            "a state loaded from our own snapshot must carry its rows"
        );
    }

    #[test]
    fn from_local_snapshot_rejects_malformed_bytes() {
        assert!(
            matches!(
                CrdtState::from_local_snapshot(1, &[0xff]),
                Err(CrdtError::ImportMalformed { .. })
            ),
            "the convenience constructor keeps every structural check"
        );
    }

    #[test]
    fn local_import_still_rejects_malformed_metadata() {
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        assert!(
            matches!(
                state.import_local(&[0xff]),
                Err(CrdtError::ImportMalformed { .. })
            ),
            "dropping the size ceilings must not drop the structural checks"
        );
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn import_rejects_malformed_metadata_before_state_mutation() {
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        assert!(matches!(
            state.import(&[0xff]),
            Err(CrdtError::ImportMalformed { .. })
        ));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn import_rejects_operation_limit_before_state_mutation() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        let before = state.frontier();
        let result = state.import_with_limits(
            &delta,
            CrdtImportLimits {
                max_bytes: delta.len(),
                max_encoded_operations: 0,
                max_new_operations: 0,
            },
        );
        assert!(matches!(
            result,
            Err(CrdtError::ImportOperationLimitExceeded { .. })
        ));
        assert_eq!(state.frontier(), before);
        assert!(!state.row_exists("docs", "row"));
    }

    #[test]
    fn already_known_operations_still_count_toward_decode_budget() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        state.import(&delta).expect("initial import");
        let before = state.frontier();
        let result = state.import_with_limits(
            &delta,
            CrdtImportLimits {
                max_bytes: delta.len(),
                max_encoded_operations: 0,
                max_new_operations: 0,
            },
        );
        assert!(matches!(
            result,
            Err(CrdtError::ImportOperationLimitExceeded { .. })
        ));
        assert_eq!(state.frontier(), before);
    }

    #[test]
    fn stale_replay_after_receiver_advances_is_idempotent() {
        let source = CrdtState::new(7).expect("source");
        let empty = loro::VersionVector::default();
        source
            .upsert("docs", "row", &[("value", LoroValue::I64(1))])
            .expect("first write");
        let stale = source.export_updates_since(&empty).expect("stale delta");
        source
            .set_fields("docs", "row", &[("value", LoroValue::I64(2))])
            .expect("second write");
        let current = source.export_updates_since(&empty).expect("current delta");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&current).expect("advance receiver");
        let before = receiver.frontier();
        receiver.import(&stale).expect("stale replay is idempotent");
        assert_eq!(receiver.frontier(), before);
    }

    #[test]
    fn a_first_import_trims_nothing_and_counts_every_operation_as_new() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        let admission = state.import(&delta).expect("first import");
        assert!(admission.new_operations > 0);
        assert_eq!(admission.trimmed_operations(), 0);
        assert_eq!(admission.encoded_operations, admission.new_operations);
    }

    #[test]
    fn a_replayed_delta_reports_every_operation_trimmed() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        let first = state.import(&delta).expect("first import");
        let replay = state.import(&delta).expect("replay is idempotent");

        // The replay reports `Ok`, exactly as it must — but nothing moved, and
        // the counts are the only thing that says so.
        assert_eq!(replay.new_operations, 0);
        assert_eq!(replay.trimmed_operations(), first.encoded_operations);
    }

    /// Two replicas that claim the same Loro peer id allocate overlapping
    /// `(peer, counter)` ranges for *different* writes. Loro trims the second
    /// replica's operations as already-known and reports a successful import,
    /// so the row it carried is silently discarded.
    ///
    /// The import cannot refuse this — trimming is how idempotent resync works,
    /// and at the `(peer, counter)` level the two cases are identical. What it
    /// must do is report the difference: an import that contributed nothing is
    /// distinguishable from one that advanced the document.
    #[test]
    fn a_colliding_peer_id_import_reports_no_new_operations() {
        let first_replica = CrdtState::new(1).expect("first replica");
        first_replica
            .upsert("docs", "from-a", &[("value", LoroValue::I64(1))])
            .expect("replica A write");
        let delta_a = first_replica.export_snapshot().expect("replica A delta");

        // A fresh replica claiming the same peer id restarts its counters at 0.
        let second_replica = CrdtState::new(1).expect("second replica");
        second_replica
            .upsert("docs", "from-b", &[("value", LoroValue::I64(2))])
            .expect("replica B write");
        let delta_b = second_replica.export_snapshot().expect("replica B delta");

        let origin = CrdtState::new(99).expect("origin");
        origin.import(&delta_a).expect("replica A applies");
        let collided = origin.import(&delta_b).expect("replica B is not refused");

        assert_eq!(
            collided.new_operations, 0,
            "a colliding peer id contributes no new operations"
        );
        assert!(collided.trimmed_operations() > 0);
        assert!(
            !origin.row_exists("docs", "from-b"),
            "this test only means something while the row is genuinely lost"
        );
    }

    /// Loro accepts a delta whose causal predecessors are missing and buffers
    /// its operations as *pending*: the import call succeeds, but the applied
    /// state never advances and the row is never written.
    ///
    /// Reporting that as a plain success is what makes the loss silent — the
    /// caller advances its high-water-mark and acknowledges a write that does
    /// not exist. An import must never report success while leaving the row it
    /// carried unwritten.
    #[test]
    fn import_does_not_report_success_while_operations_stay_pending() {
        // One source document, three writes, each exported as an incremental
        // delta. The middle delta is withheld, so the third depends on
        // operations the receiver never sees.
        let source = CrdtState::new(9).expect("source");

        let v0 = source.oplog_version_vector();
        source
            .upsert("docs", "first", &[("value", LoroValue::I64(1))])
            .expect("first write");
        let delta_first = source.export_updates_since(&v0).expect("first delta");

        source
            .upsert("docs", "withheld", &[("value", LoroValue::I64(2))])
            .expect("withheld write");

        let v2 = source.oplog_version_vector();
        source
            .upsert("docs", "third", &[("value", LoroValue::I64(3))])
            .expect("third write");
        let delta_third = source.export_updates_since(&v2).expect("third delta");

        let receiver = CrdtState::new(1).expect("receiver");
        receiver.import(&delta_first).expect("first delta applies");
        assert!(receiver.row_exists("docs", "first"));

        let result = receiver.import(&delta_third);

        // Fix-shape agnostic: the import may refuse the delta outright, or it
        // may report success — but it MUST NOT do the latter while the row it
        // carried is absent.
        assert!(
            result.is_err() || receiver.row_exists("docs", "third"),
            "import reported success while its operations stayed causally \
             pending and the row was never written"
        );
    }

    /// The pending-operation buffer must not be observable as a silent
    /// frontier stall either: if an import reports success, the applied state
    /// must have advanced past the pre-import frontier.
    #[test]
    fn successful_import_advances_the_applied_frontier() {
        let source = CrdtState::new(21).expect("source");

        let v0 = source.oplog_version_vector();
        source
            .upsert("docs", "a", &[("value", LoroValue::I64(1))])
            .expect("first write");
        let delta_a = source.export_updates_since(&v0).expect("delta a");

        source
            .upsert("docs", "gap", &[("value", LoroValue::I64(2))])
            .expect("withheld write");

        let v2 = source.oplog_version_vector();
        source
            .upsert("docs", "b", &[("value", LoroValue::I64(3))])
            .expect("second write");
        let delta_b = source.export_updates_since(&v2).expect("delta b");

        let receiver = CrdtState::new(2).expect("receiver");
        receiver.import(&delta_a).expect("delta a applies");
        let before = receiver.frontier();

        if receiver.import(&delta_b).is_ok() {
            assert_ne!(
                receiver.frontier(),
                before,
                "a successful import left the applied frontier unchanged — its \
                 operations were buffered as pending, not applied"
            );
        }
    }

    #[test]
    fn bounded_import_accepts_valid_delta_and_snapshot() {
        let delta = source_delta();
        let state = CrdtState::new(1).expect("state");
        state.import(&delta).expect("bounded delta import");
        assert!(state.row_exists("docs", "row"));

        let snapshot = state.export_snapshot().expect("snapshot");
        let restored = CrdtState::new(2).expect("restored state");
        restored.import(&snapshot).expect("bounded snapshot import");
        assert!(restored.row_exists("docs", "row"));
    }
}
