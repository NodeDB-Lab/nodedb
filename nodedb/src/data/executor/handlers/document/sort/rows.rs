// SPDX-License-Identifier: BUSL-1.1

//! Order fetched document rows: in memory when they fit one sort run, else
//! by external spill and merge.

use nodedb_physical::physical_plan::SortKeySpec;
use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Sort `rows` by `sort_keys`. `decimal_keys` flags the keys
    /// `decimal_sort_keys` found `DECIMAL`. `output_limit` bounds the rows an
    /// external merge keeps: the caller's `offset + limit`.
    pub(in crate::data::executor) fn sort_document_rows(
        &self,
        mut rows: Vec<(String, Vec<u8>)>,
        sort_keys: &[SortKeySpec],
        decimal_keys: &[bool],
        output_limit: usize,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        if rows.len() <= self.query_tuning.sort_run_size {
            let crs = self.crs_context();
            // A zero divisor in a sort key is a `22012` statement error, not
            // an internal fault, so the typed error propagates.
            super::sort_rows(&mut rows, sort_keys, decimal_keys, &crs)?;
            return Ok(rows);
        }
        self.external_sort(rows, sort_keys, decimal_keys, output_limit)
            .inspect_err(|e| warn!(core = self.core_id, error = %e, "external sort failed"))
    }
}
