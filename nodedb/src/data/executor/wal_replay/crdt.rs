// SPDX-License-Identifier: BUSL-1.1

//! CRDT WAL replay: rebuilds Loro tenant state after crash.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Replay WAL CRDT delta records to rebuild Loro tenant state after crash.
    ///
    /// CRDT records use `RecordType::CrdtDelta`; the payload is a
    /// `CrdtDeltaWalPayload` as written by `append_crdt_delta` for both
    /// `CrdtOp::Apply` and `CrdtOp::ImportSnapshot`. Loro `import` is
    /// idempotent and commutative, so there is no LSN gate: re-importing a
    /// delta already folded into a loaded checkpoint is a safe no-op.
    ///
    /// Collection lifecycle tombstones are external to Loro and must suppress
    /// older deltas so a hard-purged collection cannot be resurrected.
    pub fn replay_crdt_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;
        use tracing::warn;

        let mut replayed = 0usize;

        for record in records {
            if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtDelta) {
                continue;
            }

            // Route to the correct core by vShard.
            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                continue;
            }

            let tid = crate::types::TenantId::new(record.header.tenant_id);

            // Single self-describing decode. The delta is routed to its
            // per-collection LoroDoc by `payload.collection`.
            let Ok(payload) =
                zerompk::from_msgpack::<crate::wal::CrdtDeltaWalPayload>(&record.payload)
            else {
                continue;
            };

            // Every CRDT delta / snapshot-import record written by the current
            // binary carries its collection. A record with no collection cannot
            // be routed to a per-collection doc; skip it (a pre-per-collection
            // record from an earlier dev binary — there is no released data to
            // preserve).
            let Some(collection) = payload.collection.as_deref() else {
                warn!(
                    core = self.core_id,
                    tenant = tid.as_u64(),
                    "CRDT WAL record without collection; skipping (cannot route per-collection)"
                );
                continue;
            };
            if tombstones.is_tombstoned(
                record.header.database_id,
                tid.as_u64(),
                collection,
                record.header.lsn,
            ) {
                continue;
            }

            match self.get_crdt_engine(
                crate::types::DatabaseId::new(record.header.database_id),
                tid,
            ) {
                Ok(engine) => {
                    // NOTE: replays committed CRDT deltas via a bare import, with NO
                    // constraint validation. If deterministic apply-time validation is
                    // ever added to the live apply path, it MUST also gate this replay
                    // path (and the batch apply path) — otherwise a delta rejected live
                    // could be re-imported here on restart and diverge from peers.
                    if let Err(e) = engine.apply_committed_delta(collection, &payload.bytes) {
                        warn!(
                            core = self.core_id,
                            tenant = tid.as_u64(),
                            error = %e,
                            "CRDT WAL delta import failed during replay"
                        );
                    } else {
                        replayed += 1;
                    }
                }
                Err(e) => warn!(
                    core = self.core_id,
                    tenant = tid.as_u64(),
                    error = %e,
                    "failed to create CRDT engine during WAL replay"
                ),
            }
        }

        if replayed > 0 {
            tracing::info!(core = self.core_id, replayed, "WAL CRDT replay complete");
        }
    }
}

#[cfg(test)]
mod crdt_replay_tests {
    use super::CoreLoop;
    use crate::types::TenantId;
    use loro::LoroValue;
    use nodedb_wal::record::RecordType;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The tests drive replay directly and never tick the event loop, so the
    /// far ends are unused — they just must not be dropped.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core(core_id: usize) -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            core_id,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Build a CRDT snapshot for `tid` containing one row, then wrap it in a
    /// `CrdtDelta` WAL record exactly as `append_crdt_delta` does
    /// (`CrdtDeltaWalPayload` msgpack payload). Snapshot import and delta
    /// apply share the same idempotent Loro `state.import`, so a snapshot rides
    /// the delta record identically.
    fn make_crdt_record(
        database_id: u64,
        tid: TenantId,
        vshard_id: u32,
        collection: &str,
        row_id: &str,
    ) -> nodedb_wal::WalRecord {
        // Build one collection's CRDT doc directly; the WAL record carries the
        // collection so replay routes the import to the matching per-collection
        // LoroDoc.
        let state = nodedb_crdt::state::CrdtState::new(0).expect("state");
        state
            .upsert(
                collection,
                row_id,
                &[("name", LoroValue::String("alice".into()))],
            )
            .expect("upsert");
        let snapshot = state.export_snapshot().expect("export");
        assert!(!snapshot.is_empty(), "snapshot must be non-empty");

        let wal_payload = crate::wal::CrdtDeltaWalPayload {
            bytes: snapshot,
            collection: Some(collection.to_string()),
            provenance: None,
        };
        let payload = zerompk::to_msgpack_vec(&wal_payload).expect("encode payload");
        nodedb_wal::WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: RecordType::CrdtDelta as u32,
            lsn: 1,
            tenant_id: tid.as_u64(),
            vshard_id,
            database_id,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    #[test]
    fn replay_crdt_wal_restores_state() {
        let tid = TenantId::new(7);
        let record = make_crdt_record(0, tid, 0, "notes", "row1");

        // Fresh core with empty CRDT state, mimicking a restart with no
        // checkpoint — only the WAL is available.
        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();

        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 1, &tombstones);

        let engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(
            engine.row_exists("notes", "row1"),
            "CRDT row must be restored from WAL replay"
        );
    }

    #[test]
    fn replay_crdt_wal_honors_database_scoped_collection_tombstones() {
        let tid = TenantId::new(7);
        let dropped = make_crdt_record(1, tid, 0, "notes", "dropped-row");
        let retained = make_crdt_record(2, tid, 0, "notes", "retained-row");
        let mut tombstones = nodedb_wal::TombstoneSet::new();
        tombstones.insert(1, tid.as_u64(), "notes".to_string(), 2);

        let mut h = make_core(0);
        h.core.replay_crdt_wal(&[dropped, retained], 1, &tombstones);

        let dropped_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(1), tid)
            .expect("dropped database engine");
        assert!(!dropped_engine.row_exists("notes", "dropped-row"));
        let retained_engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::new(2), tid)
            .expect("retained database engine");
        assert!(retained_engine.row_exists("notes", "retained-row"));
    }

    #[test]
    fn replay_crdt_wal_skips_other_cores() {
        // vshard 1 with num_cores 2 routes to core 1, so core 0 must skip it.
        let tid = TenantId::new(9);
        let record = make_crdt_record(0, tid, 1, "notes", "row1");

        let mut h = make_core(0);
        let tombstones = nodedb_wal::TombstoneSet::new();
        h.core
            .replay_crdt_wal(std::slice::from_ref(&record), 2, &tombstones);

        let engine = h
            .core
            .get_crdt_engine(crate::types::DatabaseId::DEFAULT, tid)
            .expect("engine");
        assert!(
            !engine.row_exists("notes", "row1"),
            "core 0 must not replay a record routed to core 1"
        );
    }
}
