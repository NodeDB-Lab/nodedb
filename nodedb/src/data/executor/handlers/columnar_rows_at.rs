// SPDX-License-Identifier: BUSL-1.1

//! Every row of a columnar collection visible at one system-time ordinal.
//!
//! The cut-aware sibling of [`CoreLoop::for_each_current_columnar_row`]. A
//! predicate write inside a transaction block matches the rows its block's
//! snapshot sees. Visibility is the version stamps the engine keeps for every
//! collection: a row is visible at `S` when it was created at or below `S`
//! and no write ended it at or below `S`. A bitemporal row also passes its
//! `_ts_system` column against `S`, as the scan does.

use nodedb_columnar::VersionCut;
use nodedb_columnar::pk_index::RowLocation;
use nodedb_columnar::predicate::ScanPredicate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_mutation_apply::{
    ColumnarEngineKey, flushed_row_surrogate,
};
use crate::data::executor::handlers::columnar_read::bitemporal::bitemporal_row_visible;
use crate::data::executor::handlers::columnar_read::flushed_segment::SegmentBlocks;
use crate::data::executor::handlers::columnar_resolve::CurrentColumnarRow;

impl CoreLoop {
    /// Visit every row of the collection at `key` visible at
    /// `system_as_of`: each flushed segment in segment order, then the
    /// memtable. `None` visits the current rows
    /// ([`Self::for_each_current_columnar_row`]).
    ///
    /// A segment or a memtable block every row of which was created after
    /// the cut is never decoded. `Err` when a segment or a memtable cell
    /// does not read, or when `visit` fails. `site` names the read path in
    /// a corruption report.
    pub(in crate::data::executor) fn for_each_columnar_row_at(
        &self,
        key: &ColumnarEngineKey,
        system_as_of: Option<i64>,
        predicates: &[ScanPredicate],
        site: &'static str,
        mut visit: impl FnMut(CurrentColumnarRow) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let Some(at) = system_as_of else {
            return self.for_each_current_columnar_row(key, predicates, site, visit);
        };
        let Some(engine) = self.columnar_engines.get(key) else {
            return Ok(());
        };
        let cut = VersionCut::At(at);
        let schema = engine.schema();
        let collection = key.2.as_str();
        let ts_system_idx = schema.ts_system_idx();
        let visible_at = |values: &[nodedb_types::Value]| {
            bitemporal_row_visible(values, ts_system_idx, None, None, Some(at), None)
        };

        let segments = self
            .columnar_flushed_segments
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            // Segment ids are 1-based. Id 0 names the memtable.
            let segment_id = seg_idx as u64 + 1;
            if engine.segment_skippable(segment_id, cut) {
                continue;
            }
            let segment = self.decode_flushed_segment(
                key,
                segment_id,
                seg_bytes,
                schema.columns.len(),
                SegmentBlocks::Visible {
                    engine,
                    cut,
                    predicates,
                },
                site,
            )?;
            for row_idx in 0..segment.row_count() {
                if !segment.holds_row(row_idx) {
                    continue;
                }
                let row_index = u32::try_from(row_idx).map_err(|_| crate::Error::Internal {
                    detail: format!(
                        "columnar '{collection}': row {row_idx} of segment {segment_id} is \
                         past the u32 row index range"
                    ),
                })?;
                if !engine.row_visible(segment_id, row_index, cut) {
                    continue;
                }
                let values = segment.row(schema, row_idx)?;
                if !visible_at(values.as_slice()) {
                    continue;
                }
                let surrogate = flushed_row_surrogate(
                    &self.columnar_flushed_surrogates,
                    key,
                    RowLocation {
                        segment_id,
                        row_index,
                    },
                );
                visit(CurrentColumnarRow { surrogate, values })?;
            }
        }

        for scanned in engine.scan_memtable_versions(cut) {
            let (_, surrogate, values) = scanned.map_err(crate::Error::from)?;
            if !visible_at(values.as_slice()) {
                continue;
            }
            visit(CurrentColumnarRow { surrogate, values })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};

    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::types::{DatabaseId, TenantId};

    const COLL: &str = "plain_at";

    /// Insert one row per `(id, v, system_from)`, each at its ordinal.
    fn insert_at(core: &mut CoreLoop, rows: &[(i64, i64, i64)]) {
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid schema");
        let schema_bytes = zerompk::to_msgpack_vec(&schema).expect("encode schema");
        for (id, v, system_from) in rows {
            let row = Value::Object(std::collections::HashMap::from([
                ("id".to_string(), Value::Integer(*id)),
                ("v".to_string(), Value::Integer(*v)),
            ]));
            let payload =
                nodedb_types::value_to_msgpack(&Value::Array(vec![row])).expect("encode row");
            let resp = core.execute_columnar_insert(
                &make_default_task(),
                crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                    collection: COLL,
                    payload: &payload,
                    format: "msgpack",
                    intent: nodedb_physical::physical_plan::ColumnarInsertIntent::Insert,
                    on_conflict_updates: &[],
                    surrogates: &[],
                    schema_bytes: &schema_bytes,
                    provenance: None,
                    system_from: Some(*system_from),
                    restored_versions: &[],
                    rls_write_check: &nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                    returning: None,
                    rls_filters: &[],
                    spatial_undo: None,
                },
            );
            assert_eq!(
                resp.status,
                crate::bridge::envelope::Status::Ok,
                "insert: {:?}",
                resp.error_code
            );
        }
    }

    /// The `v` of every row visible at `system_as_of`, sorted.
    fn values_at(core: &CoreLoop, system_as_of: Option<i64>) -> Vec<i64> {
        let key = (
            make_default_task().request.database_id,
            TenantId::new(1),
            COLL.to_string(),
        );
        let mut out = Vec::new();
        core.for_each_columnar_row_at(&key, system_as_of, &[], "test", |row| {
            if let Some(Value::Integer(v)) = row.values.get(1) {
                out.push(*v);
            }
            Ok(())
        })
        .expect("rows read");
        out.sort_unstable();
        out
    }

    /// A read at an ordinal sees each key's version current then. A write
    /// after the ordinal is not seen. The current read sees the newest.
    #[test]
    fn rows_at_a_cut_are_the_versions_current_then() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(make_default_task().request.database_id, DatabaseId::DEFAULT);
        let base = 1_700_000_000_000 * nodedb_types::NANOS_PER_MS;
        insert_at(&mut core, &[(1, 10, base + 10), (2, 5, base + 10)]);
        // A concurrent commit after the snapshot at `base + 15`.
        insert_at(&mut core, &[(1, 20, base + 20), (3, 7, base + 20)]);

        assert!(values_at(&core, Some(base + 9)).is_empty());
        assert_eq!(values_at(&core, Some(base + 15)), vec![5, 10]);
        assert_eq!(values_at(&core, None), vec![5, 7, 20]);
    }
}
