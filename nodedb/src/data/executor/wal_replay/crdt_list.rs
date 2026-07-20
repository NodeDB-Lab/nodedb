// SPDX-License-Identifier: BUSL-1.1

//! CRDT list-op WAL replay: re-executes logged block-list mutation intent
//! (`CrdtOp::ListInsert` / `ListDelete` / `ListMove`) after a crash.
//!
//! `RecordType::CrdtListOp` carries the **intent** — collection, document,
//! list path, operation kind, and position(s) — rather than a Loro delta,
//! because the Data Plane never appends to the WAL and the Control Plane has
//! no `LoroDoc` to compute a delta from. Replay re-executes the exact same
//! live handler (`execute_crdt_list_insert` / `_delete` / `_move`) that ran
//! when the write was first accepted, so replay and live semantics cannot
//! diverge. See `wal::CrdtListOpWalRecord`'s doc comment for the full
//! rationale and why this is deliberately NOT `RecordType::CrdtDelta`.

use tracing::warn;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::CrdtListOpWalRecord;
use nodedb_physical::physical_plan::CrdtOp;
use nodedb_types::Surrogate;

/// Narrow a WAL-logged `u64` list position to the `usize` the live
/// `execute_crdt_list_*` handlers take. Returns `None` (with the record
/// skipped by the caller) on a platform where `usize` is narrower than
/// `u64` and the logged position doesn't fit — never truncates via `as`,
/// which would silently replay at the wrong position.
fn wal_list_index(core_id: usize, lsn: u64, field: &str, value: u64) -> Option<usize> {
    match usize::try_from(value) {
        Ok(v) => Some(v),
        Err(_) => {
            warn!(
                core = core_id,
                lsn,
                field,
                value,
                "CrdtListOp WAL record position does not fit usize; skipping record"
            );
            None
        }
    }
}

impl CoreLoop {
    /// Try to decode `record` as a `RecordType::CrdtListOp` record and, if it
    /// is one, replay it.
    ///
    /// Returns `None` when `record` is not a `CrdtListOp` record (caller
    /// falls through to the next replay pass), `Some(0)` when it decoded but
    /// was tombstoned, routed to another core, or the live re-execution
    /// reported a typed error (logged, never panics), `Some(1)` on
    /// successful replay.
    pub(in crate::data::executor) fn try_replay_crdt_list(
        &mut self,
        record: &nodedb_wal::WalRecord,
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) -> Option<usize> {
        use nodedb_wal::record::RecordType;

        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::CrdtListOp) {
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

        let Ok(payload) = zerompk::from_msgpack::<CrdtListOpWalRecord>(&record.payload) else {
            warn!(
                core = self.core_id,
                lsn = record.header.lsn,
                "malformed CrdtListOp WAL record; skipping"
            );
            return Some(0);
        };

        let tenant_id = record.header.tenant_id;
        let record_lsn = record.header.lsn;
        let collection = match &payload {
            CrdtListOpWalRecord::Insert { collection, .. }
            | CrdtListOpWalRecord::Delete { collection, .. }
            | CrdtListOpWalRecord::Move { collection, .. } => collection,
        };
        if tombstones.is_tombstoned(record.header.database_id, tenant_id, collection, record_lsn) {
            return Some(0);
        }

        let tid = TenantId::new(tenant_id);
        let database_id = DatabaseId::new(record.header.database_id);
        let vshard = VShardId::new(record.header.vshard_id);
        let core_id = self.core_id;

        // The task carries the real intent even though today's handlers read
        // only the explicit args passed alongside it, not the plan itself —
        // mirrors `try_replay_columnar_predicate_dml`'s rationale: a
        // placeholder plan would silently degrade to a no-op the day a
        // handler starts reading it, and on a once-per-record startup path
        // the clone costs nothing worth that risk. `surrogate` is unused by
        // every live `CrdtOp::List*` dispatch arm (`surrogate: _`), so
        // `Surrogate::ZERO` here carries no live meaning; it exists only to
        // fill the plan shape.
        //
        // Every position field below comes straight off the decoded enum
        // variant — no `Option<u64>` + `unwrap_or(0)` fallback. A record
        // whose position doesn't fit `usize` is refused (`Some(0)`, logged),
        // never silently replayed at position 0.
        let (collection, document_id, list_path, response) = match &payload {
            CrdtListOpWalRecord::Insert {
                collection,
                document_id,
                list_path,
                index,
                fields_json,
            } => {
                let Some(index) = wal_list_index(core_id, record_lsn, "index", *index) else {
                    return Some(0);
                };
                let plan = PhysicalPlan::Crdt(CrdtOp::ListInsert {
                    collection: collection.clone(),
                    document_id: document_id.clone(),
                    list_path: list_path.clone(),
                    index,
                    fields_json: fields_json.clone(),
                    surrogate: Surrogate::ZERO,
                });
                let task =
                    Self::replay_task(tid, database_id, vshard, plan, Some(Lsn::new(record_lsn)));
                let response = self.execute_crdt_list_insert(
                    &task,
                    collection,
                    document_id,
                    list_path,
                    index,
                    fields_json,
                );
                (collection, document_id, list_path, response)
            }
            CrdtListOpWalRecord::Delete {
                collection,
                document_id,
                list_path,
                index,
            } => {
                let Some(index) = wal_list_index(core_id, record_lsn, "index", *index) else {
                    return Some(0);
                };
                let plan = PhysicalPlan::Crdt(CrdtOp::ListDelete {
                    collection: collection.clone(),
                    document_id: document_id.clone(),
                    list_path: list_path.clone(),
                    index,
                    surrogate: Surrogate::ZERO,
                });
                let task =
                    Self::replay_task(tid, database_id, vshard, plan, Some(Lsn::new(record_lsn)));
                let response =
                    self.execute_crdt_list_delete(&task, collection, document_id, list_path, index);
                (collection, document_id, list_path, response)
            }
            CrdtListOpWalRecord::Move {
                collection,
                document_id,
                list_path,
                from_index,
                to_index,
            } => {
                let Some(from_index) =
                    wal_list_index(core_id, record_lsn, "from_index", *from_index)
                else {
                    return Some(0);
                };
                let Some(to_index) = wal_list_index(core_id, record_lsn, "to_index", *to_index)
                else {
                    return Some(0);
                };
                let plan = PhysicalPlan::Crdt(CrdtOp::ListMove {
                    collection: collection.clone(),
                    document_id: document_id.clone(),
                    list_path: list_path.clone(),
                    from_index,
                    to_index,
                    surrogate: Surrogate::ZERO,
                });
                let task =
                    Self::replay_task(tid, database_id, vshard, plan, Some(Lsn::new(record_lsn)));
                let response = self.execute_crdt_list_move(
                    &task,
                    collection,
                    document_id,
                    list_path,
                    from_index,
                    to_index,
                );
                (collection, document_id, list_path, response)
            }
        };

        if response.status != Status::Ok {
            warn!(
                core = self.core_id,
                collection = %collection,
                document_id = %document_id,
                list_path = %list_path,
                lsn = record_lsn,
                error = ?response.error_code,
                "CRDT list-op WAL replay failed; skipping record"
            );
            return Some(0);
        }
        Some(1)
    }

    /// Replay every `RecordType::CrdtListOp` record in `records` to
    /// reconstruct block-list order/content after a crash.
    ///
    /// Called once during startup, after `open()` but before the event
    /// loop, as the last step of the per-engine replay chain (see
    /// `data::runtime`).
    pub fn replay_crdt_list_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        let mut replayed = 0usize;
        for record in records {
            if let Some(applied) = self.try_replay_crdt_list(record, num_cores, tombstones) {
                replayed += applied;
            }
        }
        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                "WAL CRDT list-op replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use loro::{LoroMap, LoroMovableList};
    use nodedb_physical::physical_plan::CrdtOp;
    use nodedb_wal::TombstoneSet;

    use super::CoreLoop;
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::wal_dispatch::wal_append_if_write;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use crate::wal::manager::WalManager;
    use nodedb_types::Surrogate;

    const TID: u64 = 1;
    const COLLECTION: &str = "pages";
    const DOCUMENT_ID: &str = "doc-1";

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

    /// Build a `pages` collection snapshot containing one document with an
    /// empty `blocks` `LoroMovableList`, wrapped as a `CrdtDelta` WAL payload
    /// exactly as `append_crdt_delta` writes it. This seeds the container the
    /// list ops mutate; it is durable via the same `CrdtDelta` path
    /// `CrdtOp::Apply`/`ImportSnapshot` already use, deliberately not part of
    /// what this unit is adding.
    fn seed_empty_blocks_list_bytes() -> Vec<u8> {
        let state = nodedb_crdt::state::CrdtState::new(0).expect("state");
        let coll = state.doc().get_map(COLLECTION);
        let row = coll
            .insert_container(DOCUMENT_ID, LoroMap::new())
            .expect("insert row");
        row.insert_container("blocks", LoroMovableList::new())
            .expect("insert blocks list");
        state.export_snapshot().expect("export snapshot")
    }

    fn list_insert_plan(index: usize, fields_json: &str) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection: COLLECTION.to_string(),
            document_id: DOCUMENT_ID.to_string(),
            list_path: "blocks".to_string(),
            index,
            fields_json: fields_json.to_string(),
            surrogate: Surrogate::ZERO,
        })
    }

    fn list_move_plan(from_index: usize, to_index: usize) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection: COLLECTION.to_string(),
            document_id: DOCUMENT_ID.to_string(),
            list_path: "blocks".to_string(),
            from_index,
            to_index,
            surrogate: Surrogate::ZERO,
        })
    }

    fn list_delete_plan(index: usize) -> PhysicalPlan {
        PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection: COLLECTION.to_string(),
            document_id: DOCUMENT_ID.to_string(),
            list_path: "blocks".to_string(),
            index,
            surrogate: Surrogate::ZERO,
        })
    }

    #[test]
    fn autocommit_list_insert_produces_durable_lsn() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let plan = list_insert_plan(0, r#"{"id":"blk-0"}"#);
        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(TID),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("wal append");
        assert!(
            outcome.lsn.is_some(),
            "autocommit CrdtOp::ListInsert must be durably WAL-appended \
             (pre-fix: it fell through the catch-all and was never logged)"
        );
    }

    #[test]
    fn list_insert_move_delete_reconstruct_identically_after_replay_from_empty() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let tid = TenantId::new(TID);
        let vs = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        // Seed the empty `blocks` list via the existing `CrdtDelta` path
        // (durable, but out of scope for this unit).
        let seed_payload = crate::wal::CrdtDeltaWalPayload {
            bytes: seed_empty_blocks_list_bytes(),
            collection: Some(COLLECTION.to_string()),
            provenance: None,
        };
        let seed_bytes = zerompk::to_msgpack_vec(&seed_payload).expect("encode seed");
        wal.append_crdt_delta(tid, vs, db, &seed_bytes)
            .expect("append seed");

        // blocks: [] -> [blk-0] -> [blk-0, blk-1] -> [blk-1, blk-0] -> [blk-0]
        let plans = [
            list_insert_plan(0, r#"{"id":"blk-0"}"#),
            list_insert_plan(1, r#"{"id":"blk-1"}"#),
            list_move_plan(0, 1),
            list_delete_plan(0),
        ];
        for plan in &plans {
            let outcome = wal_append_if_write(&wal, tid, vs, db, plan).expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "every list op must be durably WAL-appended"
            );
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        let tombstones = TombstoneSet::new();
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_list_wal(&records, 1, &tombstones);

        let engine = h.core.get_crdt_engine(db, tid).expect("engine");
        let doc = engine.collection_doc(COLLECTION).expect("doc");
        let len = nodedb_crdt::list_ops::list_length(doc, COLLECTION, DOCUMENT_ID, "blocks")
            .expect("list length");
        assert_eq!(
            len, 1,
            "list must have exactly one block after replay from empty \
             (pre-fix: ListInsert/ListMove/ListDelete were never WAL-logged, \
             so replay only restored the empty seed list)"
        );
        let remaining = nodedb_crdt::list_ops::list_get(doc, COLLECTION, DOCUMENT_ID, "blocks", 0)
            .expect("list get")
            .expect("block present");
        if let loro::LoroValue::Map(map) = remaining {
            assert_eq!(
                map.get("id"),
                Some(&loro::LoroValue::String("blk-0".into())),
                "surviving block must be blk-0: insert blk-0, insert blk-1, \
                 move(0,1) -> [blk-1, blk-0], delete(0) removes blk-1"
            );
        } else {
            panic!("expected a map block, got {remaining:?}");
        }
    }

    /// Proves the fix at the replay layer: a `Move` with distinct
    /// `from_index`/`to_index` (3 and 1, neither `0`) reconstructs the exact
    /// same list order after WAL replay from empty as it did live. Before the
    /// fix, `CrdtListOpWalRecord` carried `from_index`/`to_index` as
    /// `Option<u64>` and replay read them via `unwrap_or(0)` — a decoded
    /// record missing either field would silently replay as `move(0, 0)`
    /// instead of the logged `move(3, 1)`, silently corrupting the
    /// reconstructed order. The non-optional enum variant makes that
    /// collapse unrepresentable: this test pins the correct order through
    /// replay.
    #[test]
    fn move_with_distinct_nonzero_indices_replays_to_same_order_as_live() {
        let dir = tempfile::tempdir().expect("wal tempdir");
        let wal = WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal");
        let tid = TenantId::new(TID);
        let vs = VShardId::new(0);
        let db = DatabaseId::DEFAULT;

        let seed_payload = crate::wal::CrdtDeltaWalPayload {
            bytes: seed_empty_blocks_list_bytes(),
            collection: Some(COLLECTION.to_string()),
            provenance: None,
        };
        let seed_bytes = zerompk::to_msgpack_vec(&seed_payload).expect("encode seed");
        wal.append_crdt_delta(tid, vs, db, &seed_bytes)
            .expect("append seed");

        // blocks: [] -> [blk-0, blk-1, blk-2, blk-3] -> move(3, 1)
        // -> [blk-0, blk-3, blk-1, blk-2]
        let plans = [
            list_insert_plan(0, r#"{"id":"blk-0"}"#),
            list_insert_plan(1, r#"{"id":"blk-1"}"#),
            list_insert_plan(2, r#"{"id":"blk-2"}"#),
            list_insert_plan(3, r#"{"id":"blk-3"}"#),
            list_move_plan(3, 1),
        ];
        for plan in &plans {
            let outcome = wal_append_if_write(&wal, tid, vs, db, plan).expect("wal append");
            assert!(
                outcome.lsn.is_some(),
                "every list op must be durably WAL-appended"
            );
        }
        wal.sync().expect("wal sync");
        let records = wal.replay().expect("wal replay read");

        let mut h = make_core();
        let tombstones = TombstoneSet::new();
        h.core.replay_crdt_wal(&records, 1, &tombstones);
        h.core.replay_crdt_list_wal(&records, 1, &tombstones);

        let engine = h.core.get_crdt_engine(db, tid).expect("engine");
        let doc = engine.collection_doc(COLLECTION).expect("doc");
        let len = nodedb_crdt::list_ops::list_length(doc, COLLECTION, DOCUMENT_ID, "blocks")
            .expect("list length");
        assert_eq!(len, 4, "all four inserted blocks must survive the move");

        let expected_order = ["blk-0", "blk-3", "blk-1", "blk-2"];
        for (i, expected_id) in expected_order.iter().enumerate() {
            let cell = nodedb_crdt::list_ops::list_get(doc, COLLECTION, DOCUMENT_ID, "blocks", i)
                .expect("list get")
                .unwrap_or_else(|| panic!("block present at index {i}"));
            let loro::LoroValue::Map(map) = cell else {
                panic!("expected a map block at index {i}, got {cell:?}");
            };
            assert_eq!(
                map.get("id"),
                Some(&loro::LoroValue::String((*expected_id).into())),
                "index {i} must be {expected_id} after replaying move(3, 1); \
                 a from_index/to_index collapse to 0 would instead leave the \
                 list in insertion order [blk-0, blk-1, blk-2, blk-3]"
            );
        }
    }
}
