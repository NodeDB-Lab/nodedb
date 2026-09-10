// SPDX-License-Identifier: BUSL-1.1

//! Rescale the declared-instant cells a join materialized itself.
//!
//! A timeseries collection stores every timestamp column in epoch
//! milliseconds, and a client reads a `TIMESTAMP` cell as epoch microseconds.
//! A join scans its local sides through `scan_collection`, which hands back
//! the stored millisecond value, so the join rescales those cells as it emits
//! them — after every predicate has run, so the join itself still compares
//! milliseconds against milliseconds.
//!
//! Exactly once: a handler rescales ONLY the cells it read from a local
//! collection scan. Rows that arrive from a sub-plan response were already
//! rescaled by the handler that read them, and are never touched again.
//!
//! The rescale runs on the merged row, before any projection. A cell is named
//! by the key the join itself wrote, so a rename cannot hide it: the rename
//! happens strictly downstream, and a projected cell copies bytes the rescale
//! has already corrected.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::timeseries::raw_scan::scale_instant_cells;
use crate::types::{DatabaseId, TenantId};

/// One locally-scanned join side: the collection its rows come from, and the
/// qualifier the merged row prefixes that side's keys with.
pub(in crate::data::executor) struct JoinInstantSide<'a> {
    pub(in crate::data::executor) collection: &'a str,
    pub(in crate::data::executor) qualifier: &'a str,
}

impl CoreLoop {
    /// The merged-row keys of the declared-instant cells `sides` contribute.
    ///
    /// A merged join row keys every cell `<qualifier>.<column>`, so the
    /// returned names are what the row carries before any projection runs. A
    /// side that is not a timeseries collection contributes none, so a join
    /// over ordinary collections returns an empty list and pays nothing.
    pub(in crate::data::executor) fn join_instant_columns(
        &self,
        database_id: DatabaseId,
        tid: TenantId,
        sides: &[JoinInstantSide<'_>],
    ) -> Vec<String> {
        let mut merged_keys = Vec::new();
        for side in sides {
            for column in self.ts_instant_columns(database_id, tid, side.collection) {
                merged_keys.push(format!("{}.{column}", side.qualifier));
            }
        }
        merged_keys
    }
}

/// Rescale the named cells of merged join rows, in place.
///
/// Each row is a msgpack map. An empty `instant_columns` is a no-op, so a
/// join that touches no timeseries collection never decodes a row.
pub(in crate::data::executor) fn scale_join_instant_rows(
    rows: &mut [Vec<u8>],
    instant_columns: &[String],
) -> crate::Result<()> {
    if instant_columns.is_empty() {
        return Ok(());
    }
    for row in rows.iter_mut() {
        let mut decoded = [
            crate::util::bounded_msgpack::read_value(row.as_slice()).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("decode join row for instant rescale: {e}"),
                }
            })?,
        ];
        scale_instant_cells(&mut decoded, instant_columns)?;
        let mut buf = Vec::with_capacity(row.len());
        rmpv::encode::write_value(&mut buf, &decoded[0]).map_err(|e| {
            crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("encode join row after instant rescale: {e}"),
            }
        })?;
        *row = buf;
    }
    Ok(())
}
