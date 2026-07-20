// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for timeseries records.
//!
//! On startup, replays `TimeseriesBatch` records into the per-core
//! columnar memtable. Only replays records with LSN > `last_flushed_wal_lsn`
//! per partition (not max_ts — safe with out-of-order data).

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::TimeseriesIngestExec;
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::engine::timeseries::columnar_memtable::{
    ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::types::DatabaseId;
use crate::types::ReadConsistency;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp, TimeseriesOp};
use nodedb_types::timeseries::MetricSample;

/// Decoded fields of a `TimeseriesBatch` WAL record.
///
/// `kind` is `Some("columnar")` / `Some("timeseries")` for tagged records and
/// `None` for the legacy 2-tuple shape. `surrogates` is only non-empty for
/// map-shaped columnar records that carried per-row cross-engine identity.
type DecodedBatchRecord = (
    Option<String>,
    String,
    Vec<u8>,
    Option<nodedb_types::sync::wire::SyncProvenance>,
    Vec<nodedb_types::Surrogate>,
);

/// Decode a `TimeseriesBatch` WAL payload into its logical fields.
///
/// Tries the shapes in newest-first order:
/// 1. Map-shaped [`nodedb_types::columnar::ColumnarWalRecord`] — the current
///    columnar encoding, which carries per-row surrogates.
/// 2. Legacy 4-tuple `(kind, collection, payload, provenance)` — current
///    timeseries encoding and pre-surrogate columnar records.
/// 3. Legacy 3-tuple `(kind, collection, payload)`.
/// 4. Legacy 2-tuple `(collection, payload)` (untagged).
///
/// The map form (1) is a msgpack map while the tuple forms (2-4) are msgpack
/// arrays, so they are unambiguous: a timeseries 4-tuple never matches (1) and
/// a legacy columnar 4-tuple falls through to (2) with empty surrogates.
/// Returns `Err(())` only when none of the shapes decode.
fn decode_batch_record(payload: &[u8]) -> Result<DecodedBatchRecord, ()> {
    if let Ok(rec) = zerompk::from_msgpack::<nodedb_types::columnar::ColumnarWalRecord>(payload) {
        return Ok((
            Some(rec.kind),
            rec.collection,
            rec.payload,
            rec.provenance,
            rec.surrogates,
        ));
    }
    zerompk::from_msgpack::<(
        String,
        String,
        Vec<u8>,
        Option<nodedb_types::sync::wire::SyncProvenance>,
    )>(payload)
    .map(|(kind, collection, payload, prov)| (Some(kind), collection, payload, prov, Vec::new()))
    .or_else(|_| {
        zerompk::from_msgpack::<(String, String, Vec<u8>)>(payload)
            .map(|(kind, collection, payload)| (Some(kind), collection, payload, None, Vec::new()))
    })
    .or_else(|_| {
        zerompk::from_msgpack::<(String, Vec<u8>)>(payload)
            .map(|(collection, payload)| (None, collection, payload, None, Vec::new()))
    })
    .map_err(|_| ())
}

/// Record-level fields for replaying a single columnar WAL batch.
///
/// Groups the collection identity, raw payload, LSN, sync provenance, and
/// cross-engine surrogates that together describe one columnar replay
/// operation, reducing the argument count on
/// [`CoreLoop::replay_columnar_payload`].
struct ColumnarReplayArgs<'a> {
    collection: &'a str,
    payload: &'a [u8],
    record_lsn: u64,
    provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    /// Per-row surrogates index-aligned with `payload` rows. An empty `Vec`
    /// falls back to fresh surrogate allocation (legacy records / sync path).
    surrogates: Vec<nodedb_types::Surrogate>,
}

impl CoreLoop {
    /// Build a synthetic replay `ExecutionTask` embedding `plan`.
    ///
    /// Shared with `wal_replay_columnar_dml` — every replay handler that
    /// re-invokes a live execute_* method needs the same minimal task shape.
    pub(in crate::data::executor) fn replay_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
        wal_lsn: Option<crate::types::Lsn>,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn,
                resolved_now_ms: None,
                admission: crate::bridge::envelope::Admission::Exempt(
                    crate::bridge::envelope::ExemptReason::AlreadyOrdered,
                ),
            },
            state: TaskState::Running,
            wal_lsn,
            resolved_now_ms: None,
        }
    }

    /// Ensure a timeseries memtable exists for the given collection, creating if needed.
    ///
    /// Uses the same operator tuning the live ingest path does. A memtable keeps
    /// the limits it was built with for its whole life, so seeding replay with
    /// hardcoded defaults would leave a restarted node running budgets the
    /// operator did not configure until every collection happened to flush.
    fn ensure_columnar_memtable(
        &mut self,
        key: (DatabaseId, crate::types::TenantId, String),
        schema: ColumnarSchema,
    ) {
        let config = ColumnarMemtableConfig::from_tuning(&self.ts_tuning);
        self.columnar_memtables
            .entry(key)
            .or_insert_with(|| ColumnarMemtable::new(schema, config));
    }

    fn replay_timeseries_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        collection: &str,
        payload: &[u8],
        record_lsn: u64,
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
    ) -> usize {
        if let Ok(batch) =
            zerompk::from_msgpack::<nodedb_types::timeseries::TimeseriesWalBatch>(payload)
        {
            let key = (db_id, tid, collection.to_string());
            self.ensure_columnar_memtable(key.clone(), ColumnarSchema::metric_default());

            let Some(mt) = self.columnar_memtables.get_mut(&key) else {
                return 0;
            };
            for (series_id, timestamp_ms, value) in &batch.samples {
                mt.ingest_metric(
                    *series_id,
                    MetricSample {
                        timestamp_ms: *timestamp_ms,
                        value: *value,
                    },
                );
            }
            let sample_count = batch.samples.len();
            // Re-charge the engine memory budget to the memtable's resident
            // footprint after replaying these samples. The reservation is
            // held until the memtable is drained on flush, so a replay-driven
            // flush balances its release instead of over-releasing.
            self.recharge_ts_memtable_budget(tid, db_id, collection);
            return sample_count;
        }

        let format = if std::str::from_utf8(payload).is_ok() {
            "ilp"
        } else {
            "msgpack"
        };
        let task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: collection.to_string(),
                payload: payload.to_vec(),
                format: format.to_string(),
                wal_lsn: Some(record_lsn),
                surrogates: Vec::new(),
                provenance: provenance.clone(),
            }),
            Some(crate::types::Lsn::new(record_lsn)),
        );
        let response = self.execute_timeseries_ingest(TimeseriesIngestExec {
            task: &task,
            tid,
            collection,
            payload,
            format,
            wal_lsn: Some(record_lsn),
            provenance: provenance.as_ref(),
        });
        if response.status != crate::bridge::envelope::Status::Ok {
            tracing::warn!(
                "timeseries WAL replay failed for collection={collection} lsn={record_lsn}: {:?}",
                response.error_code
            );
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }

    fn replay_columnar_payload(
        &mut self,
        tid: crate::types::TenantId,
        db_id: DatabaseId,
        args: ColumnarReplayArgs<'_>,
    ) -> usize {
        let ColumnarReplayArgs {
            collection,
            payload,
            record_lsn,
            provenance,
            surrogates,
        } = args;
        // `execute_columnar_insert` reads only `task.request.{database_id,
        // tenant_id, request_id}` — it never inspects the embedded plan.
        // Embed empty vecs for the plan-level surrogates/provenance to avoid
        // cloning the owned values we need to pass as explicit args below.
        let task = Self::replay_task(
            tid,
            db_id,
            crate::types::VShardId::from_collection_in_database(db_id, collection),
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: collection.to_string(),
                payload: payload.to_vec(),
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: Vec::new(),
                surrogates: Vec::new(),
                schema_bytes: Vec::new(),
                provenance: None,
                wal_lsn: Some(record_lsn),
            }),
            Some(crate::types::Lsn::new(record_lsn)),
        );
        // Restore the persisted per-row surrogates so `execute_columnar_insert`
        // rebinds the exact same cross-engine identity via
        // `insert_with_surrogate`. An empty slice (legacy records / sync path)
        // falls back to fresh allocation as before.
        let response = self.execute_columnar_insert(
            &task,
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection,
                payload,
                format: "msgpack",
                intent: ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates: &surrogates,
                schema_bytes: &[],
                provenance: provenance.as_ref(),
            },
        );
        if response.status != crate::bridge::envelope::Status::Ok {
            tracing::warn!(
                "columnar WAL replay failed for collection={collection} lsn={record_lsn}: {:?}",
                response.error_code
            );
            return 0;
        }
        match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows.len(),
            Ok(nodedb_types::Value::Object(_)) => 1,
            _ => 0,
        }
    }

    /// Replay WAL timeseries records to rebuild in-memory memtable state after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `TimeseriesBatch` records, ignoring records for other vShards.
    /// Uses LSN-based skip: only replays records with LSN > last flushed LSN.
    pub fn replay_timeseries_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use nodedb_wal::record::RecordType;

        let mut replayed = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();
            let record_type = RecordType::from_raw(logical_type);

            let is_ts_batch = record_type == Some(RecordType::TimeseriesBatch);
            if !is_ts_batch {
                continue;
            }

            // Route by vShard to the correct core.
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

            // Predicate DML (`columnar_dml`) rides the same `TimeseriesBatch`
            // record type but a disjoint map shape from both `ColumnarWalRecord`
            // and the legacy tuples (see `ColumnarDmlWalRecord`'s doc comment),
            // so it must be tried BEFORE `decode_batch_record` below — that
            // decoder's tuple fallbacks would otherwise mis-classify it as a
            // malformed row-payload record and drop it.
            if let Some(applied) = self.try_replay_columnar_predicate_dml(
                &record.payload,
                record.header.tenant_id,
                DatabaseId::new(record.header.database_id),
                record.header.lsn,
                tombstones,
            ) {
                replayed += applied;
                continue;
            }

            // Decode the record. The columnar path now uses a map-shaped
            // `ColumnarWalRecord` carrying per-row surrogates; legacy records
            // (timeseries 4-tuple, and pre-surrogate columnar 4-tuple / older
            // 3-/2-tuples) fall back through the tuple shapes with empty
            // surrogates. Records iterate in LSN order (guaranteed by the WAL
            // segment layout), so provenance-aware replay processes seq in
            // order.
            let Ok((kind, raw_collection, payload, record_provenance, record_surrogates)) =
                decode_batch_record(&record.payload)
            else {
                tracing::warn!(
                    core = self.core_id,
                    lsn = record.header.lsn,
                    "skipping malformed TimeseriesBatch WAL record"
                );
                continue;
            };

            let tenant_id = record.header.tenant_id;
            let tid_id = crate::types::TenantId::new(tenant_id);
            let db_id = DatabaseId::new(record.header.database_id);
            let collection = raw_collection.as_str();
            let key = (db_id, tid_id, raw_collection.clone());

            let record_lsn = record.header.lsn;

            // Skip records for collections that were hard-deleted after
            // this write. Otherwise the purged memtable would resurrect.
            if tombstones.is_tombstoned(db_id.as_u64(), tenant_id, collection, record_lsn) {
                skipped += 1;
                continue;
            }

            // Check if this record was already flushed (LSN-based skip).
            if let Some(registry) = self.ts_registries.get(&key) {
                // Find the max flushed LSN across all partitions.
                let max_flushed_lsn = registry
                    .iter()
                    .map(|(_, e)| e.meta.last_flushed_wal_lsn)
                    .max()
                    .unwrap_or(0);
                if record_lsn <= max_flushed_lsn {
                    skipped += 1;
                    continue;
                }
            }

            // Track the max WAL LSN ingested per collection for flush metadata.
            if let Some(entry) = self.ts_max_ingested_lsn.get_mut(&key) {
                *entry = (*entry).max(record_lsn);
            } else {
                self.ts_max_ingested_lsn.insert(key.clone(), record_lsn);
            }

            let accepted = match kind.as_deref() {
                // The columnar floor is consulted HERE and not above the `kind`
                // match, because it is the columnar engines' floor and this
                // record type is shared: a `timeseries` record routes to
                // `columnar_memtables` / `ts_registries`, which this checkpoint
                // does not cover and whose replay it must therefore not gate.
                // Gating one engine's records on another engine's durability
                // would drop the writes outright.
                Some("columnar") if self.floors.replay_floors.columnar.covers(record_lsn) => {
                    // Already folded into the restored generation. Replaying it
                    // would re-insert every row: an upsert masks the duplicate
                    // on a plain collection, but a `bitemporal=true` collection
                    // deliberately retains every version, so the duplicate
                    // becomes a second version visible to `AS OF` queries.
                    skipped += 1;
                    continue;
                }
                Some("columnar") => self.replay_columnar_payload(
                    tid_id,
                    db_id,
                    ColumnarReplayArgs {
                        collection,
                        payload: &payload,
                        record_lsn,
                        provenance: record_provenance,
                        surrogates: record_surrogates,
                    },
                ),
                Some("timeseries") | None => self.replay_timeseries_payload(
                    tid_id,
                    db_id,
                    collection,
                    &payload,
                    record_lsn,
                    record_provenance,
                ),
                Some(other) => {
                    tracing::warn!(
                        core = self.core_id,
                        lsn = record_lsn,
                        kind = other,
                        "skipping unknown TimeseriesBatch WAL kind"
                    );
                    0
                }
            };
            if accepted == 0 {
                continue;
            }
            replayed += accepted;
        }

        if replayed > 0 {
            tracing::info!(
                core = self.core_id,
                replayed,
                skipped,
                collections = self.columnar_memtables.len(),
                "WAL timeseries replay complete"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::decode_batch_record;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::write_index::CollKey;
    use crate::types::{DatabaseId, Lsn, TenantId};
    use nodedb_types::Surrogate;
    use nodedb_types::columnar::ColumnarWalRecord;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::WalRecord;
    use nodedb_wal::record::{RecordType, WalRecordArgs};
    use std::sync::Arc;

    /// Holds the bridge endpoints + tempdir alive for the core's lifetime.
    /// The test drives replay directly and never ticks the event loop, so
    /// the far ends of the bridge are unused — they just must not be
    /// dropped mid-test.
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

    /// A one-row msgpack `Value::Object` columnar payload: `{col: "v"}`.
    fn row_payload(col: &str, value: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(
            col.to_string(),
            nodedb_types::Value::String(value.to_string()),
        );
        // Mirror the production columnar-insert write path, which encodes rows
        // with the PLAIN msgpack writer (`value_to_msgpack`) and reads them back
        // with `value_from_msgpack`. `zerompk::to_msgpack_vec(&Value)` would emit
        // a tagged `[variant, payload]` array that the plain reader mis-parses.
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(obj)).expect("encode row")
    }

    /// A `TimeseriesBatch`-typed WAL record carrying a map-shaped
    /// `ColumnarWalRecord` with `kind = "columnar"`, at `lsn`.
    fn columnar_wal_record(collection: &str, lsn: u64, tenant_id: u64) -> WalRecord {
        let rec = ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: collection.to_string(),
            payload: row_payload("name", "alice"),
            provenance: None,
            surrogates: Vec::new(),
        };
        let payload = zerompk::to_msgpack_vec(&rec).expect("encode columnar wal record");
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn,
            tenant_id,
            vshard_id: 0,
            database_id: 0,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record")
    }

    /// WAL replay threads the record LSN into `replay_task` so
    /// `execute_columnar_insert`'s `note_collection_write_lsn(task, ..)` call
    /// (gated on `task.wal_lsn().is_some()`) fires during WAL replay too, not
    /// just on live writes. This proves the collection floor in
    /// `WriteVersionIndex` is populated end-to-end through
    /// `replay_timeseries_wal` -> `replay_columnar_payload` ->
    /// `execute_columnar_insert`.
    #[test]
    fn columnar_insert_replay_populates_collection_write_lsn_floor() {
        let mut h = make_core();
        let record = columnar_wal_record("events_wv", 123, 7);

        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let coll_key = CollKey {
            db: DatabaseId::new(0),
            tenant: TenantId::new(7),
            collection: Box::from("events_wv"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(123)),
            "columnar insert replay must record the record LSN as the \
             collection write-version floor"
        );
    }

    /// A columnar record already folded into a restored checkpoint must NOT be
    /// replayed. Columnar replay is not idempotent, so re-applying an insert
    /// re-runs the whole upsert; on a `bitemporal=true` collection it appends a
    /// second version outright. The floor is what restores the "from state that
    /// does not contain this record" precondition the replay depends on.
    #[test]
    fn columnar_records_at_or_below_the_floor_are_not_replayed() {
        let mut h = make_core();
        h.core.floors.replay_floors.columnar.set(Lsn::new(200));
        let record = columnar_wal_record("events_gated", 150, 7);

        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        assert!(
            h.core.columnar_engines.is_empty(),
            "a record at or below the floor is already in the restored \
             checkpoint and must not be applied a second time"
        );
    }

    /// The floor gates only what the checkpoint covers. A record ABOVE it is
    /// absent from the restored state, so gating it would not prevent a
    /// duplicate — it would drop the write.
    #[test]
    fn columnar_records_above_the_floor_still_replay() {
        let mut h = make_core();
        h.core.floors.replay_floors.columnar.set(Lsn::new(100));
        let record = columnar_wal_record("events_ungated", 150, 7);

        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let coll_key = CollKey {
            db: DatabaseId::new(0),
            tenant: TenantId::new(7),
            collection: Box::from("events_ungated"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(150)),
            "a record above the floor must be applied"
        );
    }

    /// `TimeseriesBatch` is a shared record type: a `timeseries`-kind record
    /// routes to `columnar_memtables` / `ts_registries`, which the columnar
    /// checkpoint does not cover. Gating it on the COLUMNAR engines' durability
    /// would not deduplicate anything — it would silently drop timeseries
    /// writes whose only durable copy is the record being skipped.
    #[test]
    fn the_columnar_floor_does_not_gate_timeseries_records() {
        let mut h = make_core();
        // A floor far above the record's LSN: if the gate were applied by
        // record type instead of by kind, this would suppress it.
        h.core.floors.replay_floors.columnar.set(Lsn::new(10_000));

        let batch = nodedb_types::timeseries::TimeseriesWalBatch {
            collection: "metrics_ungated".to_string(),
            samples: vec![(1u64, 1_000i64, 42.0f64)],
            provenance: None,
        };
        let payload = zerompk::to_msgpack_vec(&batch).expect("encode ts batch");
        let rec_bytes = zerompk::to_msgpack_vec(&(
            "timeseries".to_string(),
            "metrics_ungated".to_string(),
            payload,
            Option::<SyncProvenance>::None,
        ))
        .expect("encode timeseries tuple");
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn: 150,
            tenant_id: 7,
            vshard_id: 0,
            database_id: 0,
            payload: rec_bytes,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        assert!(
            h.core
                .columnar_memtables
                .keys()
                .any(|(_, t, c)| { *t == TenantId::new(7) && c == "metrics_ungated" }),
            "a timeseries record must replay regardless of the columnar floor"
        );
    }

    /// Replaying a `TimeseriesWalBatch` larger than the memtable's hard limit
    /// must retain EVERY sample. The batch is one already-committed WAL record;
    /// dropping samples that push past the ceiling is silent loss of durable
    /// data on restart. `ingest_metric` therefore never rejects, and the
    /// resident footprint overshoots the limit rather than truncating.
    #[test]
    fn oversized_timeseries_batch_replays_every_sample() {
        let mut h = make_core();
        // Size the replay memtable's hard limit far below the batch: 100
        // samples charge 16 B each (1600 B) against a 64 B ceiling, so the old
        // reject would have kept only the handful that fit.
        h.core.ts_tuning.memtable_hard_limit_bytes = 64;
        h.core.ts_tuning.memtable_budget_bytes = 32;

        const N: usize = 100;
        let samples: Vec<(u64, i64, f64)> =
            (0..N).map(|i| (1u64, 1_000 + i as i64, i as f64)).collect();
        let batch = nodedb_types::timeseries::TimeseriesWalBatch {
            collection: "metrics_big".to_string(),
            samples,
            provenance: None,
        };
        let payload = zerompk::to_msgpack_vec(&batch).expect("encode ts batch");
        let rec_bytes = zerompk::to_msgpack_vec(&(
            "timeseries".to_string(),
            "metrics_big".to_string(),
            payload,
            Option::<SyncProvenance>::None,
        ))
        .expect("encode timeseries tuple");
        let record = WalRecord::new(WalRecordArgs {
            record_type: RecordType::TimeseriesBatch as u32,
            lsn: 200,
            tenant_id: 7,
            vshard_id: 0,
            database_id: 0,
            payload: rec_bytes,
            encryption_key: None,
            preamble_bytes: None,
        })
        .expect("wal record");

        h.core.replay_timeseries_wal(
            std::slice::from_ref(&record),
            1,
            &nodedb_wal::TombstoneSet::new(),
        );

        let key = (
            DatabaseId::new(0),
            TenantId::new(7),
            "metrics_big".to_string(),
        );
        let mt = h
            .core
            .columnar_memtables
            .get(&key)
            .expect("memtable created by replay");
        assert_eq!(
            mt.row_count(),
            N as u64,
            "every sample of an over-limit replayed batch must be retained"
        );
    }

    #[test]
    fn decodes_map_columnar_record_with_surrogates() {
        let prov = SyncProvenance {
            producer_id: 1,
            epoch: 0,
            stream_id: 5,
            seq: 42,
        };
        let rec = ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: "events".to_string(),
            payload: vec![7, 8, 9],
            provenance: Some(prov.clone()),
            surrogates: vec![Surrogate::new(100), Surrogate::new(101)],
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode map record");

        let (kind, collection, payload, provenance, surrogates) =
            decode_batch_record(&bytes).expect("decode map record");
        assert_eq!(kind.as_deref(), Some("columnar"));
        assert_eq!(collection, "events");
        assert_eq!(payload, vec![7, 8, 9]);
        assert_eq!(provenance, Some(prov));
        assert_eq!(surrogates, vec![Surrogate::new(100), Surrogate::new(101)]);
    }

    #[test]
    fn legacy_columnar_tuple_decodes_with_empty_surrogates() {
        // Pre-surrogate columnar records were a 4-tuple array. They must still
        // replay, with surrogates defaulting to empty.
        let prov: Option<SyncProvenance> = None;
        let bytes = zerompk::to_msgpack_vec(&(
            "columnar".to_string(),
            "events".to_string(),
            vec![1u8, 2, 3],
            prov,
        ))
        .expect("encode legacy columnar tuple");

        let (kind, collection, payload, provenance, surrogates) =
            decode_batch_record(&bytes).expect("decode legacy tuple");
        assert_eq!(kind.as_deref(), Some("columnar"));
        assert_eq!(collection, "events");
        assert_eq!(payload, vec![1, 2, 3]);
        assert_eq!(provenance, None);
        assert!(surrogates.is_empty());
    }

    #[test]
    fn legacy_timeseries_tuple_unaffected() {
        // Timeseries records share the same WAL record type but use the
        // "timeseries" kind tag and never carried surrogates. They must
        // continue decoding via the tuple fallback with empty surrogates.
        let prov: Option<SyncProvenance> = None;
        let bytes = zerompk::to_msgpack_vec(&(
            "timeseries".to_string(),
            "metrics".to_string(),
            vec![4u8, 5, 6],
            prov,
        ))
        .expect("encode timeseries tuple");

        let (kind, collection, payload, _provenance, surrogates) =
            decode_batch_record(&bytes).expect("decode timeseries tuple");
        assert_eq!(kind.as_deref(), Some("timeseries"));
        assert_eq!(collection, "metrics");
        assert_eq!(payload, vec![4, 5, 6]);
        assert!(surrogates.is_empty());
    }

    #[test]
    fn legacy_untagged_two_tuple_decodes() {
        let bytes = zerompk::to_msgpack_vec(&("metrics".to_string(), vec![1u8, 2]))
            .expect("encode 2-tuple");
        let (kind, collection, payload, _, surrogates) =
            decode_batch_record(&bytes).expect("decode 2-tuple");
        assert_eq!(kind, None);
        assert_eq!(collection, "metrics");
        assert_eq!(payload, vec![1, 2]);
        assert!(surrogates.is_empty());
    }
}
