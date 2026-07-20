// SPDX-License-Identifier: BUSL-1.1

//! CRDT document-op WAL replay: re-executes logged document-row mutation intent
//! (`CrdtOp::DocUpsert` / `DocDelete`) after a crash.
//!
//! `RecordType::CrdtDocOp` carries the **intent** — collection, document,
//! surrogate, fields, and (for upsert) the partial flag — rather than a Loro
//! delta, because the Data Plane never appends to the WAL and the Control Plane
//! has no `LoroDoc` to compute a delta from. Replay re-executes the exact same
//! live handler (`execute_crdt_doc_upsert` / `_delete`) that ran when the write
//! was first accepted, so replay and live semantics cannot diverge. See
//! `wal::CrdtDocOpWalRecord`'s doc comment for the full rationale and why this
//! is deliberately NOT `RecordType::CrdtDelta`.

use tracing::warn;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::CrdtDocOpWalRecord;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::Surrogate;

impl CoreLoop {
    /// Try to decode `record` as a `RecordType::CrdtDocOp` record and, if it is
    /// one, replay it.
    ///
    /// Returns `None` when `record` is not a `CrdtDocOp` record (caller falls
    /// through to the next replay pass), `Some(0)` when it decoded but was
    /// tombstoned, routed to another core, or the live re-execution reported a
    /// typed error (logged, never panics), `Some(1)` on successful replay.
    pub(in crate::data::executor) fn try_replay_crdt_doc(
        &mut self,
        record: &nodedb_wal::WalRecord,
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        use nodedb_wal::record::RecordType;

        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtDocOp) {
            return None;
        }

        // Route to the correct core by vShard — same scheme every other
        // per-core replay pass uses (see `replay_crdt_wal`).
        let vshard_id = record.header.vshard_id as usize;
        let target_core = if num_cores > 0 {
            vshard_id % num_cores
        } else {
            0
        };
        if target_core != self.core_id {
            return Some(0);
        }

        let Ok(payload) = zerompk::from_msgpack::<CrdtDocOpWalRecord>(&record.payload) else {
            warn!(
                core = self.core_id,
                lsn = record.header.lsn,
                "malformed CrdtDocOp WAL record; skipping"
            );
            return Some(0);
        };

        let tenant_id = record.header.tenant_id;
        let record_lsn = record.header.lsn;
        let collection = match &payload {
            CrdtDocOpWalRecord::Upsert { collection, .. }
            | CrdtDocOpWalRecord::Delete { collection, .. } => collection,
        };
        if tombstones.is_tombstoned(record.header.database_id, tenant_id, collection, record_lsn) {
            return Some(0);
        }

        let tid = TenantId::new(tenant_id);
        let database_id = DatabaseId::new(record.header.database_id);
        let vshard = VShardId::new(record.header.vshard_id);

        // The task carries the real intent so a handler that starts reading the
        // plan cannot silently degrade to a no-op (mirrors `try_replay_crdt_list`).
        let (collection, document_id, response) = match &payload {
            CrdtDocOpWalRecord::Upsert {
                collection,
                document_id,
                surrogate,
                fields_json,
                partial,
            } => {
                let plan = PhysicalPlan::Crdt(CrdtOp::DocUpsert {
                    collection: collection.clone(),
                    document_id: document_id.clone(),
                    fields_json: fields_json.clone(),
                    surrogate: Surrogate::new(*surrogate),
                    partial: *partial,
                    returning: None,
                });
                let task =
                    Self::replay_task(tid, database_id, vshard, plan, Some(Lsn::new(record_lsn)));
                let response = self.execute_crdt_doc_upsert(
                    &task,
                    crate::data::executor::handlers::control::crdt_doc::CrdtDocUpsert {
                        collection,
                        document_id,
                        fields_json,
                        surrogate: Surrogate::new(*surrogate),
                        partial: *partial,
                        returning: None,
                    },
                );
                (collection, document_id, response)
            }
            CrdtDocOpWalRecord::Delete {
                collection,
                document_id,
                surrogate,
            } => {
                let plan = PhysicalPlan::Crdt(CrdtOp::DocDelete {
                    collection: collection.clone(),
                    document_id: document_id.clone(),
                    surrogate: Surrogate::new(*surrogate),
                    returning: None,
                });
                let task =
                    Self::replay_task(tid, database_id, vshard, plan, Some(Lsn::new(record_lsn)));
                let response = self.execute_crdt_doc_delete(
                    &task,
                    collection,
                    document_id,
                    Surrogate::new(*surrogate),
                    None,
                );
                (collection, document_id, response)
            }
        };

        if response.status != Status::Ok {
            warn!(
                core = self.core_id,
                collection = %collection,
                document_id = %document_id,
                lsn = record_lsn,
                error = ?response.error_code,
                "CRDT doc-op WAL replay failed; skipping record"
            );
            return Some(0);
        }
        Some(1)
    }

    /// Replay every `RecordType::CrdtDocOp` record in `records` to reconstruct
    /// document-row content after a crash.
    ///
    /// Called once during startup, after `open()` but before the event loop, as
    /// part of the per-engine replay chain (see `wal_replay_all`).
    pub fn replay_crdt_doc_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut replayed = 0usize;
        for record in records {
            if let Some(applied) = self.try_replay_crdt_doc(record, num_cores, tombstones) {
                replayed += applied;
            }
        }
        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL CRDT doc-op replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_physical::physical_plan::CrdtOp;
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const COLLECTION: &str = "users";
    const DOCUMENT_ID: &str = "u1";
    const SURROGATE: u32 = 7;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
        use nodedb_bridge::buffer::RingBuffer;

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    fn upsert_plan(fields_json: &str, partial: bool) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection: COLLECTION.to_string(),
            document_id: DOCUMENT_ID.to_string(),
            fields_json: fields_json.to_string(),
            surrogate: Surrogate::new(SURROGATE),
            partial,
            returning: None,
        })
    }

    fn delete_plan() -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection: COLLECTION.to_string(),
            document_id: DOCUMENT_ID.to_string(),
            surrogate: Surrogate::new(SURROGATE),
            returning: None,
        })
    }

    fn i64_field(map: &loro::LoroValue, key: &str) -> Option<i64> {
        let loro::LoroValue::Map(m) = map else {
            return None;
        };
        match m.get(key) {
            Some(loro::LoroValue::I64(n)) => Some(*n),
            _ => None,
        }
    }

    #[test]
    fn doc_upsert_replace_then_partial_set_then_delete_reconstructs_after_replay_from_empty() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let tid = TenantId::new(TID);
        let vs = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        // {a:1,b:2} full replace, then partial-set {b:9}. Untouched key `a`
        // must survive the partial set.
        let plans = [
            upsert_plan(r#"{"a":1,"b":2}"#, false),
            upsert_plan(r#"{"b":9}"#, true),
        ];
        for plan in &plans {
            let outcome = wal_append_if_write(&wal, tid, vs, db, plan).expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "every doc op must be durably WAL-appended"
            );
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        let tombstones = TombstoneSet::new();
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_doc_wal(&records, 1, &tombstones);

        let engine = h.core.get_crdt_engine(db, tid).expect("engine");
        let row = engine
            .read_row(COLLECTION, DOCUMENT_ID)
            .expect("row present");
        assert_eq!(
            i64_field(&row, "a"),
            Some(1),
            "untouched key `a` must survive partial set"
        );
        assert_eq!(
            i64_field(&row, "b"),
            Some(9),
            "partial set must overwrite `b` to 9"
        );
    }

    #[test]
    fn doc_upsert_full_replace_prunes_absent_key_after_replay() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let tid = TenantId::new(TID);
        let vs = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        // {a:1,b:2} then full replace {a:5}: `b` must be pruned.
        let plans = [
            upsert_plan(r#"{"a":1,"b":2}"#, false),
            upsert_plan(r#"{"a":5}"#, false),
        ];
        for plan in &plans {
            wal_append_if_write(&wal, tid, vs, db, plan).expect("wal append");
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        let tombstones = TombstoneSet::new();
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_doc_wal(&records, 1, &tombstones);

        let engine = h.core.get_crdt_engine(db, tid).expect("engine");
        let row = engine
            .read_row(COLLECTION, DOCUMENT_ID)
            .expect("row present");
        assert_eq!(
            i64_field(&row, "a"),
            Some(5),
            "full replace must set `a` to 5"
        );
        assert_eq!(
            i64_field(&row, "b"),
            None,
            "full replace must prune key `b` absent from the projection"
        );
    }

    #[test]
    fn doc_delete_tombstones_after_replay() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let tid = TenantId::new(TID);
        let vs = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        let plans = [upsert_plan(r#"{"a":1}"#, false), delete_plan()];
        for plan in &plans {
            wal_append_if_write(&wal, tid, vs, db, plan).expect("wal append");
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        let tombstones = TombstoneSet::new();
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_doc_wal(&records, 1, &tombstones);

        let engine = h.core.get_crdt_engine(db, tid).expect("engine");
        assert!(
            engine.read_row(COLLECTION, DOCUMENT_ID).is_none(),
            "row must be gone after the delete is replayed"
        );
    }
}
