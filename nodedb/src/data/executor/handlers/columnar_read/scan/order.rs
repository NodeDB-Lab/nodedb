// SPDX-License-Identifier: BUSL-1.1

//! Result ordering for a columnar base scan: `ORDER BY` keys, or audit-log
//! system-time order for an all-versions read.

use nodedb_physical::physical_plan::SortKeySpec;
use nodedb_types::columnar::ColumnarSchema;

use crate::data::executor::handlers::columnar_read::bitemporal::row_system_time;
use crate::data::executor::handlers::columnar_read::sort::{
    compare_sort_values, eval_row_sort_values,
};
use crate::data::executor::handlers::transaction::overlay::ColumnarMatchedRow;

/// Order `matched` in place.
///
/// With sort keys, every row's keys are evaluated first — `sort_by` has no
/// way to report an error — then the rows are ordered by them. A computed
/// key that divides by zero fails the statement. Without sort keys, an
/// all-versions read orders ascending by `_ts_system` (audit-log order) and a
/// current read keeps scan order.
pub(super) fn order_matched(
    matched: &mut Vec<ColumnarMatchedRow>,
    schema: &ColumnarSchema,
    sort_keys: &[SortKeySpec],
    all_versions: bool,
    ts_system_idx: Option<usize>,
) -> crate::Result<()> {
    if !sort_keys.is_empty() {
        let mut keyed = Vec::with_capacity(matched.len());
        for (_, row, _) in matched.iter() {
            keyed.push(eval_row_sort_values(row, schema, sort_keys)?);
        }
        let mut indexed: Vec<_> = keyed.into_iter().zip(matched.drain(..)).collect();
        indexed.sort_by(|a, b| compare_sort_values(&a.0, &b.0, sort_keys));
        matched.extend(indexed.into_iter().map(|(_, row)| row));
    } else if all_versions {
        matched.sort_by(|(_, a, _), (_, b, _)| {
            row_system_time(a, ts_system_idx).cmp(&row_system_time(b, ts_system_idx))
        });
    }
    Ok(())
}
