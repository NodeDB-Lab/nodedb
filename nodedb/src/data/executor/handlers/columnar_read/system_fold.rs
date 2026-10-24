// SPDX-License-Identifier: BUSL-1.1

//! Fold the system-time ordinals an installed columnar engine holds into the
//! shared clock.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

/// The read path a corruption report names.
const SITE: &str = "columnar_system_fold";

impl CoreLoop {
    /// Fold the newest `_ts_system` ordinal of the columnar engine at `key`,
    /// memtable and flushed segments, into the shared clock. A snapshot
    /// install or a restore lands versions another node stamped, so a later
    /// write here stamps above every one of them. A collection that is not
    /// bitemporal folds nothing.
    ///
    /// `Err` when a flushed segment or a memtable row does not read.
    pub(in crate::data::executor) fn fold_columnar_system_ordinals(
        &self,
        key: &(DatabaseId, TenantId, String),
    ) -> crate::Result<()> {
        let Some(engine) = self.columnar_engines.get(key) else {
            return Ok(());
        };
        let Some(ts_idx) = engine.schema().ts_system_idx() else {
            return Ok(());
        };
        let collection = key.2.as_str();
        let mut newest: Option<i64> = None;
        let segments = self
            .columnar_flushed_segments
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for (seg_idx, seg_bytes) in segments.iter().enumerate() {
            let seg_id = seg_idx as u64 + 1;
            let segment =
                self.decode_flushed_segment(collection, seg_id, seg_bytes, ts_idx + 1, SITE)?;
            for row_idx in 0..segment.row_count() {
                if let Value::Integer(ordinal) =
                    segment.cell(ts_idx, row_idx, &ColumnType::Int64)?
                {
                    newest = newest.max(Some(ordinal));
                }
            }
        }
        for row_idx in 0..engine.memtable().row_count() {
            if let Some(row) = engine.get_memtable_row(row_idx)?
                && let Some(Value::Integer(ordinal)) = row.get(ts_idx)
            {
                newest = newest.max(Some(*ordinal));
            }
        }
        if let Some(ordinal) = newest {
            self.observe_bitemporal_ordinal(ordinal);
        }
        Ok(())
    }
}
