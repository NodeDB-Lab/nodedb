// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for FTS engine startup recovery.
//!
//! Called once during startup, after `open()` but before the event loop.
//! Processes `FtsIndex` and `FtsDelete` records, routing each through the
//! same apply handler (`execute_fts_index_doc` / `execute_fts_delete_doc`)
//! that the live sync path uses so the idempotency gate fires on replay.
//!
//! ## Surrogate re-derivation on replay
//!
//! The WAL payload stores the document key as the hex-encoded surrogate
//! string produced by `StorageKey`'s `Display` impl (format `{:08x}`).
//! On replay we parse it back via `u32::from_str_radix(&doc_id, 16)` —
//! the same conversion used by the scan / prefilter paths.  This does not
//! require a catalog or surrogate-assigner round-trip: the 8-hex-char key
//! is already the stable `u32` surrogate identity.
//!
//! ## Why there is no replay floor or watermark here
//!
//! Every retained `FtsIndex` / `FtsDelete` record is fed back through the apply
//! handlers on every boot, including records a restored FTS state already
//! contains. That is safe because BOTH handlers are total overwrites keyed by
//! the surrogate, in all three tables the Origin index reads:
//!
//! * POSTINGS — `write_index_data` does `retain(doc_id != surrogate)` then
//!   pushes exactly one posting back, so a term's list cannot grow a duplicate
//!   however many times the record is applied.
//! * DOC_LENGTHS — the surrogate's entry is overwritten with the same length.
//! * STATS — the counters are the part that a `retain`-then-insert does NOT
//!   make idempotent on its own, so `write_index_data` derives the deltas from
//!   the PRIOR DOC_LENGTHS entry read in the same write transaction: an
//!   already-counted surrogate re-indexes with `count_delta = 0` and a
//!   `total_delta` of (new length − prior length), which is zero for an
//!   unchanged replay. `remove_document` symmetrically decrements only when a
//!   prior length existed, so a repeated delete subtracts nothing.
//!
//! The Origin search path reads posting lists from POSTINGS alone
//! (`RedbFtsBackend::read_postings`) and these writes bypass the FTS memtable
//! entirely, so there is no second copy of a posting that a re-apply could
//! diverge from. The tests at the bottom of this file pin all of that.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::replay_abort::abort_replay;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};
use nodedb_physical::physical_plan::TextOp;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

impl CoreLoop {
    /// Build a synthetic `ExecutionTask` for FTS WAL replay.
    fn replay_fts_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now()
                    + crate::data::executor::deadline::REPLAY_DEADLINE,
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: None,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn: None,
            resolved_now_ms: None,
        }
    }

    /// Replay WAL FTS records to rebuild in-memory inverted indexes after crash.
    ///
    /// Processes `FtsIndex` and `FtsDelete` records in LSN order. Each record
    /// is decoded and routed through the apply handler so the idempotency gate
    /// runs on replay exactly as it does on the live ingest path.
    pub fn replay_fts_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload};

        let mut indexed = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_fts_index = record_type == Some(RecordType::FtsIndex);
            let is_fts_delete = record_type == Some(RecordType::FtsDelete);
            if !is_fts_index && !is_fts_delete {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let record_lsn = record.header.lsn;
            // Replayed writes land under the database recorded in the WAL header.
            // Pre-scoping records carry `database_id == 0`, which maps to
            // `DatabaseId::DEFAULT` — exactly where the migration placed legacy
            // rows, so old and replayed data co-locate correctly.
            let database_id = DatabaseId::new(record.header.database_id);

            if is_fts_index {
                let payload = match FtsIndexPayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => abort_replay(
                        "fts",
                        "decode_index",
                        self.core_id,
                        record_lsn,
                        &format!("FtsIndexPayload could not be decoded: {e}"),
                    ),
                };

                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &payload.collection,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }

                // Re-derive surrogate from the hex doc_id stored in the WAL.
                let surrogate = match u32::from_str_radix(&payload.doc_id, 16) {
                    Ok(raw) => Surrogate::new(raw),
                    Err(e) => abort_replay(
                        "fts",
                        "doc_id",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "doc_id '{}' is not the hex surrogate the index path writes: {e}",
                            payload.doc_id
                        ),
                    ),
                };

                let prov = payload.provenance.clone();

                let vshard = crate::types::VShardId::from_collection_in_database(
                    database_id,
                    &payload.collection,
                );
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsIndexDoc {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            payload.collection.clone(),
                        ),
                        surrogate,
                        text: payload.text.clone(),
                        provenance: Some(prov.clone()),
                    }),
                );

                let response = self.execute_fts_index_doc(
                    &task,
                    tenant_id,
                    &payload.collection,
                    surrogate,
                    &payload.text,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    abort_replay(
                        "fts",
                        "index_handler",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "the FtsIndexDoc handler rejected a committed write into '{}'",
                            payload.collection
                        ),
                    );
                }
                indexed += 1;
            } else {
                // FtsDelete
                let payload = match FtsDeletePayload::from_bytes(&record.payload) {
                    Ok(p) => p,
                    Err(e) => abort_replay(
                        "fts",
                        "decode_delete",
                        self.core_id,
                        record_lsn,
                        &format!("FtsDeletePayload could not be decoded: {e}"),
                    ),
                };

                if tombstones.is_tombstoned(
                    database_id.as_u64(),
                    tenant_id,
                    &payload.collection,
                    record_lsn,
                ) {
                    skipped += 1;
                    continue;
                }

                let surrogate = match u32::from_str_radix(&payload.doc_id, 16) {
                    Ok(raw) => Surrogate::new(raw),
                    Err(e) => abort_replay(
                        "fts",
                        "doc_id",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "doc_id '{}' is not the hex surrogate the index path writes: {e}",
                            payload.doc_id
                        ),
                    ),
                };

                let prov = payload.provenance.clone();

                let vshard = crate::types::VShardId::from_collection_in_database(
                    database_id,
                    &payload.collection,
                );
                let task = Self::replay_fts_task(
                    nodedb_types::TenantId::new(tenant_id),
                    database_id,
                    vshard,
                    PhysicalPlan::Text(TextOp::FtsDeleteDoc {
                        collection: nodedb_types::QualifiedCollection::from_stored(
                            payload.collection.clone(),
                        ),
                        surrogate,
                        provenance: Some(prov.clone()),
                    }),
                );

                let response = self.execute_fts_delete_doc(
                    &task,
                    tenant_id,
                    &payload.collection,
                    surrogate,
                    Some(&prov),
                );

                if response.status != crate::bridge::envelope::Status::Ok {
                    abort_replay(
                        "fts",
                        "delete_handler",
                        self.core_id,
                        record_lsn,
                        &format!(
                            "the FtsDeleteDoc handler rejected a committed delete in '{}'",
                            payload.collection
                        ),
                    );
                }
                deleted += 1;
            }
        }

        if indexed > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                indexed,
                deleted,
                skipped,
                "WAL FTS replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::TenantId;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload, WalRecordArgs};
    use std::sync::Arc;

    const DB: u64 = 0;
    const TENANT: u64 = 7;
    const COLLECTION: &str = "articles";
    const SURROGATE: u32 = 0x2a;
    const TEXT: &str = "alpha bravo charlie bravo";

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime. The
    /// tests drive `replay_fts_wal` directly and never tick the event loop, so
    /// the far ends are unused — they just must not be dropped.
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
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// Provenance with the local/unidentified producer sentinel, which is what
    /// a non-sync write records. It deliberately bypasses the sync idempotency
    /// gate, so these tests exercise the engine's OWN idempotency rather than
    /// the gate's — the property replay actually depends on.
    fn local_provenance() -> SyncProvenance {
        SyncProvenance::default()
    }

    fn wal_record(record_type: RecordType, lsn: u64, payload: Vec<u8>) -> nodedb_wal::WalRecord {
        nodedb_wal::WalRecord::new(WalRecordArgs {
            record_type: record_type as u32,
            lsn,
            tenant_id: TENANT,
            vshard_id: 0,
            database_id: DB,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    fn index_record(lsn: u64, text: &str) -> nodedb_wal::WalRecord {
        let payload = FtsIndexPayload::new(
            local_provenance(),
            COLLECTION,
            format!("{SURROGATE:08x}"),
            text,
        )
        .to_bytes()
        .expect("encode FtsIndexPayload");
        wal_record(RecordType::FtsIndex, lsn, payload)
    }

    fn delete_record(lsn: u64) -> nodedb_wal::WalRecord {
        let payload =
            FtsDeletePayload::new(local_provenance(), COLLECTION, format!("{SURROGATE:08x}"))
                .to_bytes()
                .expect("encode FtsDeletePayload");
        wal_record(RecordType::FtsDelete, lsn, payload)
    }

    fn replay(core: &mut CoreLoop, record: &nodedb_wal::WalRecord) {
        core.replay_fts_wal(
            std::slice::from_ref(record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );
    }

    /// Everything the Origin FTS reads: the corpus counters and the length of
    /// every posting list the indexed text produces. Postings catch a duplicate
    /// entry; the counters catch a double-counted document or token sum, which
    /// a `retain`-then-insert on the posting alone would NOT prevent.
    fn fts_state(core: &CoreLoop) -> ((u32, f32), Vec<(String, u32)>) {
        let tid = TenantId::new(TENANT);
        let stats = core
            .inverted
            .corpus_stats(DB, tid, COLLECTION)
            .expect("corpus stats");
        // Ask the analyzer for the canonical terms rather than hardcoding
        // stems, so the assertion follows the collection's analyzer config.
        let mut terms = core
            .inverted
            .analyze_for_collection(DB, tid, COLLECTION, TEXT)
            .expect("analyze");
        terms.sort();
        terms.dedup();
        let dfs = terms
            .into_iter()
            .map(|term| {
                let df = core
                    .inverted
                    .term_df(DB, tid, COLLECTION, &term)
                    .expect("term df");
                (term, df)
            })
            .collect();
        (stats, dfs)
    }

    /// The property FTS replay relies on instead of a floor: applying the SAME
    /// `FtsIndex` record a second time must leave the index bit-for-bit
    /// equivalent — no duplicated posting, no second document counted, no
    /// inflated token sum (which would skew avgdl and therefore every BM25
    /// score in the collection).
    #[test]
    fn replaying_an_index_record_twice_leaves_the_index_unchanged() {
        let mut h = make_core();
        let record = index_record(10, TEXT);

        replay(&mut h.core, &record);
        let after_first = fts_state(&h.core);
        assert_eq!(after_first.0.0, 1, "one document indexed");
        assert!(
            after_first.1.iter().all(|(_, df)| *df == 1),
            "each term must list the document exactly once: {:?}",
            after_first.1
        );

        replay(&mut h.core, &record);
        assert_eq!(
            fts_state(&h.core),
            after_first,
            "re-applying a durable FtsIndex record must be a no-op"
        );
    }

    /// A record whose text genuinely changed is not a replay, and must still
    /// take effect — the idempotency above must come from comparing state, not
    /// from ignoring repeat writes to a surrogate.
    #[test]
    fn a_changed_index_record_still_takes_effect() {
        let mut h = make_core();
        replay(&mut h.core, &index_record(10, TEXT));
        let ((count, avg_len), _) = fts_state(&h.core);
        assert_eq!(count, 1);

        replay(&mut h.core, &index_record(11, "alpha"));
        let ((count_after, avg_after), _) = fts_state(&h.core);
        assert_eq!(count_after, 1, "still one document, not two");
        assert!(
            avg_after < avg_len,
            "the shorter text must shrink the average document length"
        );
    }

    /// The delete arm carries the same obligation: a repeated `FtsDelete`
    /// must not decrement the corpus counters a second time (which would
    /// underflow the document count toward zero while other documents remain).
    #[test]
    fn replaying_a_delete_record_twice_leaves_the_index_unchanged() {
        let mut h = make_core();
        replay(&mut h.core, &index_record(10, TEXT));

        let record = delete_record(11);
        replay(&mut h.core, &record);
        let after_first = fts_state(&h.core);
        assert_eq!(after_first.0.0, 0, "the only document is gone");
        assert!(
            after_first.1.iter().all(|(_, df)| *df == 0),
            "no term may still list the deleted document: {:?}",
            after_first.1
        );

        replay(&mut h.core, &record);
        assert_eq!(
            fts_state(&h.core),
            after_first,
            "re-applying a durable FtsDelete record must be a no-op"
        );
    }
}
