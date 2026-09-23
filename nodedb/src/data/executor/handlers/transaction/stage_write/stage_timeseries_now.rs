// SPDX-License-Identifier: BUSL-1.1

//! The default timestamp a staged timeseries batch records.

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

impl CoreLoop {
    /// Record `now_ms` as the default timestamp of the staged batch whose
    /// first row is `surrogates[0]`.
    pub(super) fn note_staged_ingest_now(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        collection: &str,
        surrogates: &[Surrogate],
        now_ms: i64,
    ) {
        let Some(first) = surrogates.first() else {
            return;
        };
        let coll_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        self.txn_overlay_mut(txn_id)
            .note_ingest_now(&coll_key, first.as_u32(), now_ms);
    }
}
