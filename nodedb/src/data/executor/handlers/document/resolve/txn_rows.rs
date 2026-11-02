// SPDX-License-Identifier: BUSL-1.1

//! The rows a predicate document write matches: the base rows its filters
//! match, and, inside a transaction, that transaction's own staged writes.

use nodedb_types::SystemTimeScope;

use crate::bridge::envelope::ErrorCode;
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::write_match_cut::write_match_cut;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::StorageKey;
use crate::types::TenantId;

use super::context::DocResolveCtx;

impl CoreLoop {
    /// Every `(row key, body)` that `filters` match in `collection`, each at
    /// the version `system_time` selects: the current version, or the
    /// version visible at a transaction block's snapshot. A historical
    /// scope is refused. The mutation's precondition reads the current
    /// version, so a row changed after the snapshot fails it.
    ///
    /// A request that carries a transaction id reads its own writes: a
    /// staged delete drops its row, a staged put is matched again against
    /// `filters`, and a row only the transaction wrote joins the set.
    pub(super) fn resolve_matched_rows(
        &self,
        task: &ExecutionTask,
        ctx: &DocResolveCtx,
        collection: &str,
        filters: &[ScanFilter],
        system_time: &SystemTimeScope,
    ) -> Result<Vec<(StorageKey, Vec<u8>)>, ErrorCode> {
        let system_as_of = write_match_cut(system_time, "a predicate document write")?;
        let rows = match task.request.txn_id {
            Some(txn_id) => {
                let coll_key = (
                    task.request.database_id,
                    TenantId::new(ctx.tid),
                    collection.to_string(),
                );
                self.txn_matching_rows(txn_id, &coll_key, filters, system_as_of)
            }
            None => match system_as_of {
                None => self.current_matching_rows(ctx.database_id, ctx.tid, collection, filters),
                // A snapshot with no transaction to merge reads the
                // committed versions at the snapshot.
                Some(_) => self.matching_rows_at(
                    ctx.database_id,
                    ctx.tid,
                    collection,
                    filters,
                    system_as_of,
                ),
            },
        };
        rows.map_err(ErrorCode::from)
    }
}
