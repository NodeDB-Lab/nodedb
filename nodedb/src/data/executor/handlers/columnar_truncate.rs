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

use tracing::debug;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::{ColumnarTruncateUndo, UndoEntry};
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Handle `ColumnarOp::Truncate`. Reports the number of live rows that
    /// existed. A collection with no engine yet holds no row: reports zero.
    pub(in crate::data::executor) fn execute_columnar_truncate(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        undo_log: Option<&mut Vec<UndoEntry>>,
    ) -> Response {
        debug!(core = self.core_id, %collection, "columnar truncate");
        let db = task.request.database_id;
        let tid = task.request.tenant_id;
        let key = (db, tid, collection.to_string());

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
        self.note_collection_write_lsn(task, collection);
        self.invalidate_aggregate_cache_for_collection(db.as_u64(), tid.as_u64(), collection);

        debug!(core = self.core_id, %collection, truncated, "columnar truncate complete");
        self.truncate_response(task, truncated)
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

        let resp = core.execute_columnar_truncate(&task, COLLECTION, None);
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
        let resp = core.execute_columnar_truncate(&task(), COLLECTION, None);
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
        let resp = core.execute_columnar_truncate(&task, COLLECTION, Some(&mut undo));
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
}
