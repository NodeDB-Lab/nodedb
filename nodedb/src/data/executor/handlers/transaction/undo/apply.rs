// SPDX-License-Identifier: BUSL-1.1

//! Per-engine undo entry application logic.
//!
//! Each `apply_undo_*` method handles one engine family's undo entries.
//! All methods return `Err((entry_index, detail))` on fatal failure so the
//! caller can escalate to a typed `RollbackFailed` response.

use nodedb_types::Surrogate;
use tracing::error;

use crate::data::executor::core_loop::CoreLoop;

use super::{TimeseriesIngestUndo, UndoEntry};

impl CoreLoop {
    // ── Vector ───────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_vector(
        &mut self,
        _tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::InsertVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            } => match self.vector_collections.get_mut(&index_key) {
                Some(index) => {
                    index.delete(vector_id);
                    // Reverse the forward insert's `vector_doc_map` write —
                    // without this a rolled-back insert leaves a stale
                    // doc→vector_id mapping behind (unbounded leak), mirroring
                    // `apply_undo_spatial`'s `spatial_doc_map.remove`. `None`
                    // `doc_id` marks the direct primary-vector write path
                    // (`PhysicalPlan::Vector`), which never populates
                    // `vector_doc_map` — skip the mutation for that path.
                    if let Some(doc_id) = doc_id {
                        self.vector_doc_map.remove(&(
                            index_key.0,
                            index_key.1,
                            collection,
                            field,
                            doc_id,
                        ));
                    }
                    Ok(())
                }
                None => {
                    let detail = format!(
                        "vector index {:?} not found during undo of vector insert {}",
                        index_key, vector_id
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: vector index missing; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            },
            UndoEntry::DeleteVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            } => match self.vector_collections.get_mut(&index_key) {
                Some(index) => {
                    index.undelete(vector_id);
                    // Restore the `vector_doc_map` entry the forward delete
                    // removed — without this a rolled-back delete leaves the
                    // doc→vector reverse lookup missing, so a later delete of
                    // the same document can never find (and soft-delete) its
                    // vector: a permanent orphan. Mirrors
                    // `apply_undo_spatial`'s `spatial_doc_map.insert`. `None`
                    // `doc_id` marks the direct primary-vector write path,
                    // which never populates `vector_doc_map` — skip it there.
                    if let Some(doc_id) = doc_id {
                        self.vector_doc_map.insert(
                            (index_key.0, index_key.1, collection, field, doc_id),
                            vector_id,
                        );
                    }
                    Ok(())
                }
                None => {
                    let detail = format!(
                        "vector index {:?} not found during undo of vector delete {}",
                        index_key, vector_id
                    );
                    error!(
                        core = self.core_id,
                        entry_index,
                        error = %detail,
                        "transaction undo: vector index missing; shard state unknown"
                    );
                    Err((entry_index, detail))
                }
            },
            _ => Err((
                entry_index,
                "apply_undo_vector called with non-vector entry".to_string(),
            )),
        }
    }

    // ── Graph ────────────────────────────────────────────────────────────────

    #[cfg(test)]
    pub(super) fn apply_undo_edge(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        self.apply_undo_edge_with_stats(did, tid, entry_index, entry, true)
    }

    pub(super) fn apply_undo_edge_with_stats(
        &mut self,
        did: u64,
        tid: u64,
        entry_index: usize,
        entry: UndoEntry,
        account_stats: bool,
    ) -> Result<(), (usize, String)> {
        use crate::engine::graph::edge_store::EdgeRef;
        let database = nodedb_types::DatabaseId::new(did);
        match entry {
            UndoEntry::PutEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            } => {
                let tenant = nodedb_types::TenantId::new(tid);
                let ord = self.hlc.next_ordinal();
                let edge_ref =
                    EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id);
                if let Some(old_props) = old_properties {
                    let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
                    self.edge_store
                        .put_edge_versioned_with_stats(
                            edge_ref,
                            &old_props,
                            ord,
                            valid_from_ms,
                            i64::MAX,
                            account_stats,
                        )
                        .map_err(|e| {
                            let detail = format!(
                                "edge restore {collection} {src_id}-[{label}]->{dst_id}: {e}"
                            );
                            error!(
                                core = self.core_id, entry_index,
                                error = %detail,
                                "transaction undo: edge restore failed; shard state unknown"
                            );
                            (entry_index, detail)
                        })?;
                    let weight =
                        crate::engine::graph::csr::extract_weight_from_properties(&old_props);
                    let partition = self.csr_partition_mut(did, tid);
                    partition.remove_edge_in_collection(&src_id, &label, &dst_id, &collection);
                    let csr_res = if weight != 1.0 {
                        partition.add_edge_weighted_in_collection(
                            &src_id,
                            &label,
                            &dst_id,
                            &collection,
                            weight,
                        )
                    } else {
                        partition.add_edge_in_collection(&src_id, &label, &dst_id, &collection)
                    };
                    csr_res.map_err(|e| {
                        let detail =
                            format!("CSR restore {collection} {src_id}-[{label}]->{dst_id}: {e}");
                        error!(
                            core = self.core_id, entry_index,
                            error = %detail,
                            "transaction undo: CSR restore failed after edge_store restore; \
                             shard state unknown"
                        );
                        (entry_index, detail)
                    })?;
                } else {
                    self.edge_store
                        .soft_delete_edge_with_stats(edge_ref, ord, account_stats)
                        .map_err(|e| {
                            let detail = format!(
                                "edge tombstone {collection} {src_id}-[{label}]->{dst_id}: {e}"
                            );
                            error!(
                                core = self.core_id, entry_index,
                                error = %detail,
                                "transaction undo: edge tombstone failed; shard state unknown"
                            );
                            (entry_index, detail)
                        })?;
                    self.csr_partition_mut(did, tid).remove_edge_in_collection(
                        &src_id,
                        &label,
                        &dst_id,
                        &collection,
                    );
                }
                Ok(())
            }
            UndoEntry::DeleteEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            } => {
                let tenant = nodedb_types::TenantId::new(tid);
                let ord = self.hlc.next_ordinal();
                let valid_from_ms = nodedb_types::ordinal_to_ms(ord);
                // The cascade that produced this entry dropped the endpoints'
                // durable identity bindings. The in-memory CSR still holds
                // them, so restoring the edge restores the binding with it —
                // otherwise a rolled-back delete would leave the graph intact
                // but invisible to every cross-engine read after a restart.
                let (src_surrogate, dst_surrogate) = self
                    .csr_partition(did, tid)
                    .map(|p| {
                        (
                            p.node_surrogate(&src_id).unwrap_or(Surrogate::ZERO),
                            p.node_surrogate(&dst_id).unwrap_or(Surrogate::ZERO),
                        )
                    })
                    .unwrap_or((Surrogate::ZERO, Surrogate::ZERO));
                self.edge_store
                    .put_edge_versioned_with_stats(
                        EdgeRef::new(database, tenant, &collection, &src_id, &label, &dst_id)
                            .with_surrogates(src_surrogate, dst_surrogate),
                        &old_properties,
                        ord,
                        valid_from_ms,
                        i64::MAX,
                        account_stats,
                    )
                    .map_err(|e| {
                        let detail = format!(
                            "edge re-insert {collection} {src_id}-[{label}]->{dst_id}: {e}"
                        );
                        error!(
                            core = self.core_id, entry_index,
                            error = %detail,
                            "transaction undo: edge re-insert failed; shard state unknown"
                        );
                        (entry_index, detail)
                    })?;
                let weight =
                    crate::engine::graph::csr::extract_weight_from_properties(&old_properties);
                let partition = self.csr_partition_mut(did, tid);
                let csr_res = if weight != 1.0 {
                    partition.add_edge_weighted_in_collection(
                        &src_id,
                        &label,
                        &dst_id,
                        &collection,
                        weight,
                    )
                } else {
                    partition.add_edge_in_collection(&src_id, &label, &dst_id, &collection)
                };
                csr_res.map_err(|e| {
                    let detail = format!("CSR re-insert {src_id}-[{label}]->{dst_id}: {e}");
                    error!(
                        core = self.core_id, entry_index,
                        error = %detail,
                        "transaction undo: CSR re-insert failed after edge_store restore; \
                         shard state unknown"
                    );
                    (entry_index, detail)
                })
            }
            _ => Err((
                entry_index,
                "apply_undo_edge called with non-edge entry".to_string(),
            )),
        }
    }

    // ── Columnar ─────────────────────────────────────────────────────────────

    pub(super) fn apply_undo_columnar(
        &mut self,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::ColumnarInsert {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
            } => {
                match self.columnar_engines.get_mut(&collection_key) {
                    Some(engine) => {
                        engine.rollback_memtable_inserts(
                            row_count_before,
                            &inserted_pks,
                            &displaced,
                        );
                        Ok(())
                    }
                    None => {
                        // Engine absent: no in-memory state to roll back.
                        // This is safe — if the engine was never created, no rows were inserted.
                        Ok(())
                    }
                }
            }
            UndoEntry::ColumnarUpdate {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
                restored,
            } => {
                if let Some(engine) = self.columnar_engines.get_mut(&collection_key) {
                    // 1. Remove the appended replacement rows (mirrors ColumnarInsert).
                    engine.rollback_memtable_inserts(row_count_before, &inserted_pks, &displaced);
                    // 2. Restore the tombstoned originals.
                    engine.restore_deleted_rows(&restored);
                }
                // Engine absent: no in-memory state to roll back.
                Ok(())
            }
            UndoEntry::ColumnarDelete {
                collection_key,
                restored,
            } => {
                if let Some(engine) = self.columnar_engines.get_mut(&collection_key) {
                    engine.restore_deleted_rows(&restored);
                }
                // Engine absent: no in-memory state to roll back.
                Ok(())
            }
            _ => Err((
                entry_index,
                "apply_undo_columnar called with non-columnar entry".to_string(),
            )),
        }
    }

    // ── Timeseries ───────────────────────────────────────────────────────────

    pub(super) fn apply_undo_timeseries(
        &mut self,
        entry_index: usize,
        entry: UndoEntry,
    ) -> Result<(), (usize, String)> {
        match entry {
            UndoEntry::TimeseriesIngest(token) => {
                self.restore_timeseries_ingest_preimage(entry_index, token)
            }
            _ => Err((
                entry_index,
                "apply_undo_timeseries called with non-timeseries entry".to_string(),
            )),
        }
    }

    fn restore_timeseries_ingest_preimage(
        &mut self,
        entry_index: usize,
        token: TimeseriesIngestUndo,
    ) -> Result<(), (usize, String)> {
        let TimeseriesIngestUndo {
            collection_key,
            memtable_before,
            memtable_config_before,
            memtable_memory_bytes_before,
            last_value_cache_before,
            max_ingested_lsn_before,
            last_ts_ingest_before,
            reservation_bytes_before,
        } = token;

        // Commit-deferred ingest must not touch reservations. Treat a mismatch
        // as fatal rather than dropping/recharging a token and corrupting the
        // governor's accounting during a failed transaction.
        let reservation_now = self
            .columnar_memtable_mem
            .get(&collection_key)
            .map(nodedb_mem::ReservationToken::size);
        if reservation_now != reservation_bytes_before {
            return Err((
                entry_index,
                format!(
                    "timeseries reservation changed during deferred ingest for {:?}: before {:?}, now {:?}",
                    collection_key, reservation_bytes_before, reservation_now
                ),
            ));
        }

        match (
            memtable_before,
            memtable_config_before,
            memtable_memory_bytes_before,
        ) {
            (Some(snapshot), Some(config), Some(memory_bytes)) => {
                let mut restored =
                    crate::engine::timeseries::columnar_memtable::ColumnarMemtable::from_snapshot(
                        snapshot, config,
                    )
                    .map_err(|error| {
                        (
                            entry_index,
                            format!("timeseries memtable snapshot restore failed: {error}"),
                        )
                    })?;
                restored.restore_memory_bytes_for_undo(memory_bytes);
                self.columnar_memtables
                    .insert(collection_key.clone(), restored);
            }
            (None, None, None) => {
                self.columnar_memtables.remove(&collection_key);
            }
            _ => {
                return Err((
                    entry_index,
                    "timeseries undo token has inconsistent memtable pre-image fields".into(),
                ));
            }
        }

        match last_value_cache_before {
            Some(cache) => {
                self.ts_last_value_caches
                    .insert(collection_key.clone(), cache);
            }
            None => {
                self.ts_last_value_caches.remove(&collection_key);
            }
        }
        match max_ingested_lsn_before {
            Some(lsn) => {
                self.ts_max_ingested_lsn.insert(collection_key, lsn);
            }
            None => {
                self.ts_max_ingested_lsn.remove(&collection_key);
            }
        }
        self.last_ts_ingest = last_ts_ingest_before;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::engine::timeseries::columnar_memtable::{
        ColumnType, ColumnValue, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
    };
    use crate::engine::timeseries::last_value_cache::LastValueCache;
    use crate::types::{DatabaseId, TenantId};
    use nodedb_types::QualifiedCollection;

    const DB: u64 = 0;
    const TID: u64 = 1;

    fn timeseries_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 1024 * 1024,
            hard_memory_limit: 2 * 1024 * 1024,
            max_tag_cardinality: 100,
        }
    }

    fn timeseries_memtable() -> ColumnarMemtable {
        ColumnarMemtable::new(
            ColumnarSchema {
                columns: vec![
                    ("timestamp".into(), ColumnType::Timestamp),
                    ("value".into(), ColumnType::Float64),
                    ("host".into(), ColumnType::Symbol),
                ],
                timestamp_idx: 0,
                codecs: vec![nodedb_codec::ColumnCodec::Auto; 3],
            },
            timeseries_config(),
        )
    }

    #[test]
    fn timeseries_undo_restores_schema_dictionary_lvc_lsn_and_timer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (
            crate::types::DatabaseId::new(DB),
            TenantId::new(TID),
            "metrics".into(),
        );
        let mut memtable = timeseries_memtable();
        memtable
            .ingest_row(
                1,
                &[
                    ColumnValue::Timestamp(10),
                    ColumnValue::Float64(1.0),
                    ColumnValue::Symbol("old-host".into()),
                ],
            )
            .expect("seed ingest");
        let snapshot = memtable.export_snapshot();
        let config = memtable.config();
        let memory_bytes = memtable.memory_bytes();
        core.columnar_memtables.insert(key.clone(), memtable);
        let mut cache = LastValueCache::new();
        cache.update(1, 10, 1.0);
        core.ts_last_value_caches.insert(key.clone(), cache.clone());
        core.ts_max_ingested_lsn.insert(key.clone(), 7);
        let prior_timer = std::time::Instant::now();
        core.last_ts_ingest = Some(prior_timer);

        let token = TimeseriesIngestUndo {
            collection_key: key.clone(),
            memtable_before: Some(snapshot),
            memtable_config_before: Some(config),
            memtable_memory_bytes_before: Some(memory_bytes),
            last_value_cache_before: Some(cache),
            max_ingested_lsn_before: Some(7),
            last_ts_ingest_before: Some(prior_timer),
            reservation_bytes_before: None,
        };
        let memtable = core.columnar_memtables.get_mut(&key).expect("memtable");
        memtable.add_column("region".into(), ColumnType::Symbol);
        memtable
            .ingest_row(
                2,
                &[
                    ColumnValue::Timestamp(20),
                    ColumnValue::Float64(2.0),
                    ColumnValue::Symbol("new-host".into()),
                    ColumnValue::Symbol("west".into()),
                ],
            )
            .expect("mutate ingest");
        core.ts_last_value_caches
            .get_mut(&key)
            .expect("cache")
            .update(1, 20, 2.0);
        core.ts_max_ingested_lsn.insert(key.clone(), 99);
        core.last_ts_ingest = Some(std::time::Instant::now());

        core.apply_undo_timeseries(0, UndoEntry::TimeseriesIngest(token))
            .expect("undo");
        let restored = core
            .columnar_memtables
            .get(&key)
            .expect("restored memtable");
        assert_eq!(restored.row_count(), 1);
        assert_eq!(restored.memory_bytes(), memory_bytes);
        assert_eq!(restored.schema().columns.len(), 3);
        assert_eq!(
            restored.symbol_dict(2).expect("dictionary").get(0),
            Some("old-host")
        );
        assert_eq!(
            core.ts_last_value_caches
                .get(&key)
                .and_then(|cache| cache.get(1))
                .map(|entry| (entry.ts, entry.value)),
            Some((10, 1.0))
        );
        assert_eq!(core.ts_max_ingested_lsn.get(&key), Some(&7));
        assert_eq!(core.last_ts_ingest, Some(prior_timer));
    }

    #[test]
    fn timeseries_undo_removes_newly_created_collection_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (
            crate::types::DatabaseId::new(DB),
            TenantId::new(TID),
            "new_metrics".into(),
        );
        let token = TimeseriesIngestUndo {
            collection_key: key.clone(),
            memtable_before: None,
            memtable_config_before: None,
            memtable_memory_bytes_before: None,
            last_value_cache_before: None,
            max_ingested_lsn_before: None,
            last_ts_ingest_before: None,
            reservation_bytes_before: None,
        };
        core.columnar_memtables
            .insert(key.clone(), timeseries_memtable());
        core.ts_last_value_caches
            .insert(key.clone(), LastValueCache::new());
        core.ts_max_ingested_lsn.insert(key.clone(), 1);
        core.last_ts_ingest = Some(std::time::Instant::now());

        core.apply_undo_timeseries(0, UndoEntry::TimeseriesIngest(token))
            .expect("undo");
        assert!(!core.columnar_memtables.contains_key(&key));
        assert!(!core.ts_last_value_caches.contains_key(&key));
        assert!(!core.ts_max_ingested_lsn.contains_key(&key));
        assert!(core.last_ts_ingest.is_none());
    }

    #[test]
    fn repeated_timeseries_ingests_restore_the_initial_preimage_on_abort() {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::TimeseriesOp;

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let plans = [
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"metrics value=1i 1000000000\n".to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"other_measurement value=2i 2000000000\n".to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
        ];

        let response = core.execute_transaction_batch(&task, TID, &plans, &[], None);

        assert_eq!(response.status, crate::bridge::envelope::Status::Error);
        assert!(
            !core.columnar_memtables.contains_key(&(
                crate::types::DatabaseId::DEFAULT,
                TenantId::new(TID),
                "metrics".to_string(),
            )),
            "reverse-order rollback must restore the pre-transaction absence after repeated ingests"
        );
        assert!(
            !core.ts_last_value_caches.contains_key(&(
                crate::types::DatabaseId::DEFAULT,
                TenantId::new(TID),
                "metrics".to_string(),
            )),
            "the last-value cache must follow the same initial pre-image"
        );
    }

    #[test]
    fn transactional_timeseries_flush_uses_the_enclosing_wal_lsn() {
        use std::time::{Duration, Instant};

        use crate::bridge::envelope::{
            Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
        };
        use crate::data::executor::task::ExecutionTask;
        use crate::types::{Lsn, RequestId, TraceId, VShardId};
        use nodedb_physical::physical_plan::{MetaOp, TimeseriesOp};

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let lsn = 42;
        let task = ExecutionTask::with_wal_lsn(
            Request {
                request_id: RequestId::new(1),
                tenant_id: TenantId::new(TID),
                database_id: crate::types::DatabaseId::new(DB),
                vshard_id: VShardId::new(0),
                plan: PhysicalPlan::Meta(MetaOp::Cancel {
                    target_request_id: RequestId::new(0),
                }),
                deadline: Instant::now() + Duration::from_secs(5),
                priority: Priority::Normal,
                trace_id: TraceId::ZERO,
                consistency: crate::types::ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
                txn_id: None,
                wal_lsn: Some(Lsn::new(lsn)),
                resolved_now_ms: None,
                admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
            },
            Some(Lsn::new(lsn)),
        );
        let plans = [PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            payload: b"metrics value=1i 1000000000\n".to_vec(),
            format: "ilp".into(),
            // Buffered transaction plans normally have no per-op LSN. The
            // transaction record's LSN above must become the partition stamp.
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        })];

        let response = core.execute_transaction_batch(&task, TID, &plans, &[], None);
        assert_eq!(response.status, Status::Ok);
        let key = (
            crate::types::DatabaseId::new(DB),
            TenantId::new(TID),
            "metrics".to_string(),
        );
        assert_eq!(core.ts_max_ingested_lsn.get(&key), Some(&lsn));

        core.flush_ts_collection(
            TenantId::new(TID),
            crate::types::DatabaseId::new(DB),
            "metrics",
            0,
        )
        .expect("flush committed transaction rows");
        let max_flushed_lsn = core
            .ts_registries
            .get(&key)
            .expect("partition registry")
            .iter()
            .map(|(_, entry)| entry.meta.last_flushed_wal_lsn)
            .max();
        assert_eq!(max_flushed_lsn, Some(lsn));
    }

    // ── Columnar predicate UPDATE / DELETE undo ─────────────────────────────
    //
    // A columnar predicate UPDATE / DELETE is staged at statement time and
    // replayed durably at COMMIT through `execute_tx_sub_plan`. Before the undo
    // parity fix, that replay hit the undo-less passthrough arm, so a SIBLING
    // sub-plan failing later in the same COMMIT batch left the columnar mutation
    // applied — a partial, non-atomic commit. These tests drive the real capture
    // path (`execute_tx_sub_plan`) then reverse via `rollback_undo_log` — the same
    // reverse-order driver `execute_transaction_batch` runs on a sibling failure —
    // and assert the columnar state is fully restored.
    //
    // PRE-FIX the `undo_log.len() == 1` assertion fails (the passthrough pushed no
    // undo entry), and the post-rollback state assertion fails (the mutation
    // survived the aborted batch).

    use nodedb_physical::physical_plan::{ColumnarOp, PhysicalPlan};

    fn columnar_key() -> (nodedb_types::DatabaseId, TenantId, String) {
        (
            nodedb_types::DatabaseId::DEFAULT,
            TenantId::new(TID),
            "m".to_string(),
        )
    }

    fn seed_columnar_engine(
        core: &mut crate::data::executor::core_loop::CoreLoop,
        rows: &[(i64, i64)],
    ) {
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        use nodedb_types::value::Value;

        let schema = ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("v", ColumnType::Int64),
            ],
            version: 1,
        };
        let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema);
        for (id, v) in rows {
            engine
                .insert(&[Value::Integer(*id), Value::Integer(*v)])
                .expect("seed insert");
        }
        core.columnar_engines.insert(columnar_key(), engine);
    }

    /// Current (non-tombstoned) memtable rows as `(id, v)` pairs, sorted by id.
    fn columnar_rows(core: &crate::data::executor::core_loop::CoreLoop) -> Vec<(i64, i64)> {
        use nodedb_types::value::Value;
        let engine = core
            .columnar_engines
            .get(&columnar_key())
            .expect("engine present");
        let mut out: Vec<(i64, i64)> = engine
            .scan_memtable_rows()
            .filter_map(|row| match (&row[0], &row[1]) {
                (Value::Integer(id), Value::Integer(v)) => Some((*id, *v)),
                _ => None,
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn columnar_predicate_update_rolls_back_on_sibling_failure() {
        use nodedb_types::value::Value;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        seed_columnar_engine(&mut core, &[(1, 10), (2, 20)]);
        assert_eq!(columnar_rows(&core), vec![(1, 10), (2, 20)]);

        // Durable COMMIT replay of `UPDATE m SET v = 999` (empty filter = all rows).
        let updates = vec![(
            "v".to_string(),
            nodedb_types::value_to_msgpack(&Value::Integer(999)).unwrap(),
        )];
        let plan = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            filters: Vec::new(),
            updates,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });

        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("columnar update sub-plan must succeed");

        // The mutation applied, and — critically — an undo entry was captured.
        assert_eq!(columnar_rows(&core), vec![(1, 999), (2, 999)]);
        assert_eq!(
            undo_log.len(),
            1,
            "columnar UPDATE must push exactly one undo entry (pre-fix: 0, on the undo-less passthrough)"
        );
        assert!(matches!(undo_log[0], UndoEntry::ColumnarUpdate { .. }));

        // A sibling sub-plan fails later in the same COMMIT: reverse the batch.
        core.rollback_undo_log(nodedb_types::DatabaseId::DEFAULT.as_u64(), TID, undo_log)
            .expect("rollback must succeed");

        assert_eq!(
            columnar_rows(&core),
            vec![(1, 10), (2, 20)],
            "rolled-back columnar UPDATE must restore the original values"
        );
    }

    #[test]
    fn columnar_predicate_delete_rolls_back_on_sibling_failure() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        seed_columnar_engine(&mut core, &[(1, 10), (2, 20), (3, 30)]);
        assert_eq!(columnar_rows(&core), vec![(1, 10), (2, 20), (3, 30)]);

        // Durable COMMIT replay of `DELETE FROM m` (empty filter = all rows).
        let plan = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "m"),
            filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });

        let mut undo_log = Vec::new();
        let mut crdt_deltas = Vec::new();
        core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
            .expect("columnar delete sub-plan must succeed");

        assert!(
            columnar_rows(&core).is_empty(),
            "all rows must be deleted by the durable replay"
        );
        assert_eq!(
            undo_log.len(),
            1,
            "columnar DELETE must push exactly one undo entry (pre-fix: 0, on the undo-less passthrough)"
        );
        assert!(matches!(undo_log[0], UndoEntry::ColumnarDelete { .. }));

        // A sibling sub-plan fails later in the same COMMIT: reverse the batch.
        core.rollback_undo_log(nodedb_types::DatabaseId::DEFAULT.as_u64(), TID, undo_log)
            .expect("rollback must succeed");

        assert_eq!(
            columnar_rows(&core),
            vec![(1, 10), (2, 20), (3, 30)],
            "rolled-back columnar DELETE must restore all deleted rows with their original values"
        );
    }

    // ── Vector undo (vector_doc_map symmetry) ───────────────────────────────

    fn vector_index_key() -> (nodedb_types::DatabaseId, TenantId, String) {
        crate::data::executor::core_loop::CoreLoop::vector_index_key(DB, TID, "c", "emb")
    }

    fn vector_doc_key() -> (
        nodedb_types::DatabaseId,
        TenantId,
        String,
        String,
        nodedb_types::StorageKey,
    ) {
        let key = vector_index_key();
        (
            key.0,
            key.1,
            "c".to_string(),
            "emb".to_string(),
            nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(1)),
        )
    }

    /// A rolled-back transactional document INSERT must remove the stale
    /// `vector_doc_map` entry the forward `apply_point_put_vector_indexes`
    /// insert created — otherwise the reverse doc→vector_id mapping leaks
    /// unboundedly (it never gets cleaned up since the document that would have
    /// triggered a delete cascade doesn't actually exist post-rollback). Mirrors
    /// `spatial_insert_undo_removes_entry_and_reverse_map`.
    #[test]
    fn vector_insert_undo_removes_stale_doc_map_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let index_key = vector_index_key();
        let coll = core
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| nodedb_vector::VectorCollection::new(2, Default::default()));
        let vector_id = coll.insert_with_surrogate(vec![1.0, 2.0], nodedb_types::Surrogate::ZERO);

        // Seed as though the forward `apply_point_put_vector_indexes` insert had
        // run: it populates `vector_doc_map` alongside the HNSW insert.
        core.vector_doc_map.insert(vector_doc_key(), vector_id);
        assert!(core.vector_doc_map.contains_key(&vector_doc_key()));

        let undo = UndoEntry::InsertVector {
            index_key,
            vector_id,
            collection: "c".to_string(),
            field: "emb".to_string(),
            doc_id: Some(vector_doc_key().4),
        };
        core.apply_undo_vector(TID, 0, undo).unwrap();

        assert!(
            !core.vector_doc_map.contains_key(&vector_doc_key()),
            "stale vector_doc_map entry must be removed on rolled-back insert"
        );
    }

    /// A rolled-back transactional document DELETE must restore the
    /// `vector_doc_map` entry the forward delete cascade removed — otherwise the
    /// doc→vector reverse lookup is permanently missing and a later delete of the
    /// same document can never find (and soft-delete) its vector: a permanent
    /// orphan. Mirrors `spatial_delete_undo_reinserts_entry_with_bbox`. Also
    /// verifies the restored mapping is immediately usable by a subsequent delete
    /// cascade lookup (the exact key `apply_point_delete` probes).
    #[test]
    fn vector_delete_undo_restores_doc_map_entry() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

        let index_key = vector_index_key();
        let coll = core
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| nodedb_vector::VectorCollection::new(2, Default::default()));
        let vector_id = coll.insert_with_surrogate(vec![3.0, 4.0], nodedb_types::Surrogate::ZERO);
        coll.delete(vector_id);

        // The forward delete cascade already removed the reverse-map entry (as
        // `apply_point_delete` does) — it must be absent before undo runs.
        assert!(!core.vector_doc_map.contains_key(&vector_doc_key()));

        let undo = UndoEntry::DeleteVector {
            index_key,
            vector_id,
            collection: "c".to_string(),
            field: "emb".to_string(),
            doc_id: Some(vector_doc_key().4),
        };
        core.apply_undo_vector(TID, 0, undo).unwrap();

        assert_eq!(
            core.vector_doc_map.get(&vector_doc_key()).copied(),
            Some(vector_id),
            "vector_doc_map entry must be restored so a later delete can find the vector again"
        );
    }

    // ── Graph edge-cascade undo ──────────────────────────────────────────────

    /// A rolled-back transactional document DELETE must restore every edge the
    /// unconditional graph-edge cascade removed — into BOTH the persistent edge
    /// store (`get_edge`) AND the in-memory CSR partition (`neighbors`), with the
    /// original edge properties intact. This exercises the full capture→restore
    /// path: `delete_edges_for_node` returns the removed edges, and
    /// `apply_undo_edge` re-inserts each via a `DeleteEdge` undo entry.
    #[test]
    fn edge_cascade_delete_rollback_restores_csr_and_edge_store() {
        use crate::engine::graph::csr::Direction;
        use crate::engine::graph::edge_store::EdgeRef;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let tenant = TenantId::new(TID);

        // Seed alice-[KNOWS]->bob in BOTH stores, as a forward EdgePut would.
        let seed_ord = core.hlc.next_ordinal();
        core.edge_store
            .put_edge_versioned(
                EdgeRef::new(
                    nodedb_types::DatabaseId::new(DB),
                    tenant,
                    "c",
                    "alice",
                    "KNOWS",
                    "bob",
                ),
                b"p1",
                seed_ord,
                nodedb_types::ordinal_to_ms(seed_ord),
                i64::MAX,
            )
            .unwrap();
        core.csr_partition_mut(DB, TID)
            .add_edge("alice", "KNOWS", "bob")
            .unwrap();

        // Sanity: edge present in both stores.
        assert_eq!(
            core.edge_store
                .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
                .unwrap(),
            Some(b"p1".to_vec())
        );
        assert_eq!(
            core.csr_partition_mut(DB, TID)
                .neighbors("alice", None, Direction::Out),
            vec![("KNOWS".to_string(), "bob".to_string())]
        );

        // Forward document-delete cascade (Cascade 3): remove from CSR + edge store,
        // capturing the removed edges for rollback.
        core.csr_partition_mut(DB, TID).remove_node_edges("alice");
        let cascade_ord = core.hlc.next_ordinal();
        let removed = core
            .edge_store
            .delete_edges_for_node(DB, tenant, "alice", cascade_ord)
            .unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(
            removed[0],
            (
                "c".to_string(),
                "alice".to_string(),
                "KNOWS".to_string(),
                "bob".to_string(),
                b"p1".to_vec()
            )
        );

        // Both stores now show the edge gone.
        assert!(
            core.edge_store
                .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
                .unwrap()
                .is_none()
        );
        assert!(
            core.csr_partition_mut(DB, TID)
                .neighbors("alice", None, Direction::Out)
                .is_empty()
        );

        // Rollback: push one DeleteEdge undo per captured edge and apply it.
        for (idx, (collection, src_id, label, dst_id, old_properties)) in
            removed.into_iter().enumerate()
        {
            let undo = UndoEntry::DeleteEdge {
                collection,
                src_id,
                label,
                dst_id,
                old_properties,
            };
            core.apply_undo_edge(DB, TID, idx, undo).unwrap();
        }

        // Both stores fully restored, properties intact.
        assert_eq!(
            core.edge_store
                .get_edge(DB, tenant, "c", "alice", "KNOWS", "bob")
                .unwrap(),
            Some(b"p1".to_vec()),
            "edge store must be restored with original properties"
        );
        assert_eq!(
            core.csr_partition_mut(DB, TID)
                .neighbors("alice", None, Direction::Out),
            vec![("KNOWS".to_string(), "bob".to_string())],
            "CSR adjacency must be restored"
        );
    }

    #[test]
    fn graph_edge_update_undo_restores_csr_weight() {
        use crate::engine::graph::edge_store::EdgeRef;

        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let tenant = TenantId::new(TID);
        let old_properties = nodedb_types::json_to_msgpack(&serde_json::json!({ "weight": 2.5 }))
            .expect("encode old edge properties");
        let new_properties = nodedb_types::json_to_msgpack(&serde_json::json!({ "weight": 9.0 }))
            .expect("encode new edge properties");
        let edge = EdgeRef::new(
            crate::types::DatabaseId::new(DB),
            tenant,
            "c",
            "alice",
            "KNOWS",
            "bob",
        );
        core.edge_store
            .put_edge_versioned(edge, &new_properties, 10, 10, i64::MAX)
            .expect("seed updated edge");
        core.csr_partition_mut(DB, TID)
            .add_edge_weighted_in_collection("alice", "KNOWS", "bob", "c", 9.0)
            .expect("seed updated CSR edge");

        core.apply_undo_edge(
            DB,
            TID,
            0,
            UndoEntry::PutEdge {
                collection: "c".into(),
                src_id: "alice".into(),
                label: "KNOWS".into(),
                dst_id: "bob".into(),
                old_properties: Some(old_properties),
            },
        )
        .expect("undo edge update");

        assert_eq!(
            core.csr_partition_mut(DB, TID)
                .edge_weight("alice", "KNOWS", "bob"),
            Some(2.5),
            "rollback must restore the committed CSR traversal weight"
        );
    }
}
