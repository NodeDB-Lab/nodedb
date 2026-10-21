// SPDX-License-Identifier: BUSL-1.1

//! `ColumnarOp::Truncate`: remove every row of a columnar or spatial
//! collection.
//!
//! The mutation engine's rows, every flushed segment and its surrogate
//! sidecar, and every R-tree entry the collection's geometry columns
//! produced are moved out whole. On the autocommit path they are dropped;
//! inside a transaction batch they ride an `UndoEntry::ColumnarTruncate`,
//! so an abort reinstalls them exactly — every bitemporal version, every
//! tombstone. Segment ids stay monotonic (`MutationEngine::truncate`).
//!
//! A bitemporal collection keeps its history instead: the truncate closes
//! every current key with a tombstone version at one statement ordinal,
//! raised per key, as a DELETE with no filter closes them. A backup restore
//! erases a bitemporal collection whole (`TruncateHistory::Erase`).

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_mutation_apply::{
    ColumnarDeleteMode, ColumnarEngineKey,
};
use crate::data::executor::handlers::columnar_resolve::{
    ResolveDeleteRowsParams, require_pk_column_index,
};
use crate::data::executor::handlers::transaction::undo::{ColumnarTruncateUndo, UndoEntry};
use crate::data::executor::task::ExecutionTask;

/// How a truncate treats a bitemporal collection's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum TruncateHistory {
    /// Close every current key of a bitemporal collection with a tombstone
    /// stamped as a DELETE stamps (see `ColumnarDeleteMode::Close`). A
    /// collection that is not bitemporal, or has no primary key, is erased.
    Close { system_from: Option<i64> },
    /// Erase every row and version, bitemporal or not.
    Erase,
}

impl TruncateHistory {
    /// The history mode `ColumnarOp::Truncate`'s fields name.
    pub(in crate::data::executor) fn of(erase_history: bool, system_from: Option<i64>) -> Self {
        if erase_history {
            Self::Erase
        } else {
            Self::Close { system_from }
        }
    }
}

impl CoreLoop {
    /// Handle `ColumnarOp::Truncate`. Reports the number of live rows that
    /// existed. A collection with no engine yet holds no row: reports zero.
    pub(in crate::data::executor) fn execute_columnar_truncate(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        history: TruncateHistory,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar truncate");
        let db = task.request.database_id;
        let tid = task.request.tenant_id;
        let key = (db, tid, collection.to_string());
        if let TruncateHistory::Close { system_from } = history
            && let Some(engine) = self.columnar_engines.get(&key)
            && engine.schema().is_bitemporal()
            && !engine.pk_col_indices().is_empty()
        {
            return self.close_columnar_collection(task, &key, system_from, undo_log);
        }

        let engine = match self.columnar_engines.get_mut(&key) {
            Some(engine) => engine,
            None => return self.truncate_response(task, 0),
        };
        let truncated = engine.live_row_count();
        let rows = engine.truncate();
        let flushed_segments = self.columnar_flushed_segments.remove(&key);
        let flushed_surrogates = self.columnar_flushed_surrogates.remove(&key);

        let spatial_keys: Vec<_> = self
            .spatial_indexes
            .keys()
            .filter(|(d, t, c, _)| *d == db && *t == tid && c == collection)
            .cloned()
            .collect();
        let mut spatial_indexes = Vec::with_capacity(spatial_keys.len());
        for skey in spatial_keys {
            if let Some(rtree) = self.spatial_indexes.remove(&skey) {
                spatial_indexes.push((skey, rtree));
            }
        }
        let doc_keys: Vec<_> = self
            .spatial_doc_map
            .keys()
            .filter(|(d, t, c, _, _)| *d == db && *t == tid && c == collection)
            .cloned()
            .collect();
        let mut spatial_doc_map = Vec::with_capacity(doc_keys.len());
        for dkey in doc_keys {
            if let Some(doc) = self.spatial_doc_map.remove(&dkey) {
                spatial_doc_map.push((dkey, doc));
            }
        }

        if let Some(log) = undo_log {
            log.push(UndoEntry::ColumnarTruncate(ColumnarTruncateUndo {
                collection_key: key,
                rows,
                flushed_segments,
                flushed_surrogates,
                spatial_indexes,
                spatial_doc_map,
            }));
        }

        self.checkpoint_coordinator
            .mark_dirty("columnar", truncated);
        // No wipe: columnar and spatial reads record only `Predicate` keys,
        // which the collection version bounds.
        self.note_collection_write(task, collection);
        self.invalidate_aggregate_cache_for_collection(db.as_u64(), tid.as_u64(), collection);

        debug!(core = self.core_id, %collection, truncated, "columnar truncate complete");
        self.truncate_response(task, truncated)
    }

    /// Truncate the bitemporal collection at `key` by closing every current
    /// key with a tombstone version, as a DELETE with no filter closes them.
    /// Every version stays as history. Reports the number of keys closed.
    fn close_columnar_collection(
        &mut self,
        task: &ExecutionTask,
        key: &ColumnarEngineKey,
        system_from: Option<i64>,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        let collection = key.2.as_str();
        let Some(schema) = self.columnar_engines.get(key).map(|e| e.schema().clone()) else {
            return self.truncate_response(task, 0);
        };
        let pk_col_idx = match require_pk_column_index(&schema, "TRUNCATE") {
            Ok(idx) => idx,
            Err(e) => return self.response_error(task, e),
        };
        let pks = match self.resolve_columnar_delete_rows(ResolveDeleteRowsParams {
            key,
            schema: &schema,
            pk_col_idx,
            filter_predicates: &[],
            rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
            tid: key.1.as_u64(),
            collection,
        }) {
            Ok(pks) => pks,
            Err(e) => return self.response_error(task, e),
        };
        let mut undo_log = undo_log;
        let outcome = match self.apply_columnar_delete_pks(
            key,
            &schema,
            &pks,
            ColumnarDeleteMode::Close { system_from },
            undo_log.as_deref_mut(),
        ) {
            Ok(outcome) => outcome,
            Err(e) => return self.response_error(task, e),
        };
        let closed = outcome.affected as usize;
        if let Some(log) = undo_log {
            log.push(outcome.undo_entry(key.clone()));
        }
        self.checkpoint_coordinator.mark_dirty("columnar", closed);
        self.note_collection_write(task, collection);
        self.invalidate_aggregate_cache_for_collection(key.0.as_u64(), key.1.as_u64(), collection);
        debug!(core = self.core_id, %collection, closed, "bitemporal columnar truncate complete");
        self.truncate_response(task, closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::ColumnarOp;
    use nodedb_types::{RlsWriteCheck, Value};

    const TID: u64 = 1;
    const COLLECTION: &str = "trunc_cols";

    fn task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TID),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            crate::bridge::envelope::PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                restart_identity: false,
                system_from: None,
                erase_history: false,
            }),
            None,
        )
    }

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TID),
            COLLECTION.to_string(),
        )
    }

    fn point_row(id: &str, x: f64, y: f64) -> Value {
        Value::Object(std::collections::HashMap::from([
            ("id".to_string(), Value::String(id.into())),
            (
                "loc".to_string(),
                Value::String(format!("{{\"type\":\"Point\",\"coordinates\":[{x},{y}]}}")),
            ),
        ]))
    }

    fn schema_bytes() -> Vec<u8> {
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::String).with_primary_key(),
            ColumnDef::nullable("loc", ColumnType::Geometry),
        ])
        .expect("valid schema");
        zerompk::to_msgpack_vec(&schema).expect("encode schema")
    }

    fn seed(core: &mut CoreLoop, task: &ExecutionTask, rows: Vec<Value>) {
        let payload =
            nodedb_types::value_to_msgpack(&Value::Array(rows)).expect("encode insert payload");
        let schema = schema_bytes();
        let resp = core.execute_columnar_insert(
            task,
            crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                collection: COLLECTION,
                payload: &payload,
                format: "msgpack",
                intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Insert,
                on_conflict_updates: &[],
                surrogates: &[],
                schema_bytes: &schema,
                provenance: None,
                system_from: None,
                rls_write_check: &RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: &[],
                spatial_undo: None,
            },
        );
        assert_eq!(
            resp.status,
            Status::Ok,
            "seed failed: {:?}",
            resp.error_code
        );
    }

    fn rtree_entries(core: &CoreLoop) -> usize {
        core.spatial_indexes
            .get(&(
                DatabaseId::DEFAULT,
                TenantId::new(TID),
                COLLECTION.to_string(),
                "loc".to_string(),
            ))
            .map(|rt| rt.entries().len())
            .unwrap_or(0)
    }

    fn live_rows(core: &CoreLoop) -> usize {
        core.columnar_engines
            .get(&key())
            .map(|e| e.live_row_count())
            .unwrap_or(0)
    }

    #[test]
    fn autocommit_truncate_empties_rows_and_rtree_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = task();
        seed(
            &mut core,
            &task,
            vec![point_row("a", 1.0, 1.0), point_row("b", 2.0, 2.0)],
        );
        assert_eq!(live_rows(&core), 2);
        assert_eq!(rtree_entries(&core), 2);

        let resp = core.execute_columnar_truncate(
            &task,
            COLLECTION,
            TruncateHistory::Close { system_from: None },
            None,
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(live_rows(&core), 0);
        assert_eq!(rtree_entries(&core), 0);
        assert!(core.spatial_doc_map.is_empty());

        seed(&mut core, &task, vec![point_row("z", 9.0, 9.0)]);
        assert_eq!(live_rows(&core), 1);
        assert_eq!(rtree_entries(&core), 1);
    }

    #[test]
    fn truncate_of_an_unknown_collection_reports_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let resp = core.execute_columnar_truncate(
            &task(),
            COLLECTION,
            TruncateHistory::Close { system_from: None },
            None,
        );
        assert_eq!(resp.status, Status::Ok);
    }

    #[test]
    fn batch_truncate_undo_restores_rows_and_rtree_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = task();
        seed(
            &mut core,
            &task,
            vec![point_row("a", 1.0, 1.0), point_row("b", 2.0, 2.0)],
        );

        let mut undo = Vec::new();
        let resp = core.execute_columnar_truncate(
            &task,
            COLLECTION,
            TruncateHistory::Close { system_from: None },
            Some(&mut undo),
        );
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(live_rows(&core), 0);
        assert_eq!(rtree_entries(&core), 0);
        // A later sub-plan in the same batch writes into the empty engine.
        seed(&mut core, &task, vec![point_row("z", 9.0, 9.0)]);
        assert_eq!(live_rows(&core), 1);

        core.rollback_undo_log(0, TID, undo).expect("rollback");
        assert_eq!(live_rows(&core), 2, "pre-truncate rows are back");
        assert_eq!(
            rtree_entries(&core),
            2,
            "pre-truncate R-tree entries are back"
        );
        assert!(
            core.spatial_doc_map.values().all(|d| d == "a" || d == "b"),
            "the post-truncate row's entry is gone: {:?}",
            core.spatial_doc_map
        );
    }

    mod bitemporal {
        use nodedb_columnar::pk_index::encode_pk;
        use nodedb_types::Value;
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

        use super::*;

        fn schema() -> ColumnarSchema {
            ColumnarSchema::new(vec![
                ColumnDef::required("_ts_system", ColumnType::Int64),
                ColumnDef::required("_ts_valid_from", ColumnType::Int64),
                ColumnDef::required("_ts_valid_until", ColumnType::Int64),
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("v", ColumnType::Int64),
            ])
            .expect("valid")
        }

        fn version(ts: i64, id: i64, v: i64) -> Vec<Value> {
            vec![
                Value::Integer(ts),
                Value::Integer(0),
                Value::Integer(i64::MAX),
                Value::Integer(id),
                Value::Integer(v),
            ]
        }

        fn seeded_core(dir: &std::path::Path) -> (CoreLoop, impl Sized, impl Sized) {
            let (mut core, tx, rx) = make_core_with_dir(dir);
            let mut engine = nodedb_columnar::MutationEngine::new(COLLECTION.to_string(), schema());
            engine.insert(&version(100, 1, 10)).expect("insert");
            engine.insert(&version(200, 2, 20)).expect("insert");
            core.columnar_engines.insert(key(), engine);
            (core, tx, rx)
        }

        /// The id and value each row a read at `cut` emits, sorted.
        fn read(core: &CoreLoop, cut: Option<i64>) -> Vec<(i64, i64)> {
            let gate = core
                .columnar_version_gate(&key(), cut, false)
                .expect("gate")
                .expect("bitemporal");
            let engine = core.columnar_engines.get(&key()).expect("engine");
            let mut out: Vec<(i64, i64)> = engine
                .scan_memtable_rows()
                .map(|row| row.expect("read"))
                .filter(|row| gate.admits(row))
                .map(|row| match (&row[3], &row[4]) {
                    (Value::Integer(id), Value::Integer(v)) => (*id, *v),
                    other => panic!("unexpected cells {other:?}"),
                })
                .collect();
            out.sort_unstable();
            out
        }

        /// A bitemporal truncate closes every current key with a tombstone
        /// at the statement's ordinal raised per key, and keeps every
        /// version: a read before it returns the rows. Its undo reverses it.
        #[test]
        fn a_bitemporal_truncate_closes_every_key_and_keeps_history() {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _tx, _rx) = seeded_core(dir.path());
            let mut undo = Vec::new();
            let resp = core.execute_columnar_truncate(
                &task(),
                COLLECTION,
                TruncateHistory::Close {
                    system_from: Some(150),
                },
                Some(&mut undo),
            );
            assert_eq!(resp.status, Status::Ok);

            let engine = core.columnar_engines.get(&key()).expect("engine");
            assert_eq!(engine.memtable().row_count(), 4, "2 versions, 2 tombstones");
            assert_eq!(engine.live_row_count(), 0);
            let stamps: Vec<i64> = engine
                .scan_memtable_rows()
                .map(|row| match row.expect("read").first() {
                    Some(Value::Integer(ts)) => *ts,
                    other => panic!("no stamp: {other:?}"),
                })
                .collect();
            assert_eq!(stamps, vec![100, 200, 150, 201], "raised per key");
            assert!(read(&core, None).is_empty(), "no current row");
            assert_eq!(read(&core, Some(149)), vec![(1, 10)], "before id 1 closes");
            assert_eq!(read(&core, Some(200)), vec![(2, 20)], "before id 2 closes");

            core.rollback_undo_log(0, TID, undo).expect("rollback");
            let engine = core.columnar_engines.get(&key()).expect("engine");
            assert_eq!(engine.memtable().row_count(), 2);
            assert_eq!(engine.live_row_count(), 2);
            assert_eq!(engine.closed_location(&encode_pk(&Value::Integer(1))), None);
            assert_eq!(read(&core, None), vec![(1, 10), (2, 20)]);
        }

        /// A truncate that erases history removes every version of a
        /// bitemporal collection.
        #[test]
        fn an_erasing_truncate_removes_every_version() {
            let dir = tempfile::tempdir().expect("tempdir");
            let (mut core, _tx, _rx) = seeded_core(dir.path());
            let resp =
                core.execute_columnar_truncate(&task(), COLLECTION, TruncateHistory::Erase, None);
            assert_eq!(resp.status, Status::Ok);
            let engine = core.columnar_engines.get(&key()).expect("engine");
            assert_eq!(engine.memtable().row_count(), 0);
        }
    }
}
