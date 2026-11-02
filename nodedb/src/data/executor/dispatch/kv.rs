// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for KvOp variants: engine pressure check, refusal of an unbound
//! row write, declared-column re-typing of incoming row bodies, then
//! delegation to execute_kv inside one KV apply scope.

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::KvOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_kv(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let is_kv_write = matches!(
            op,
            KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::BatchPut { .. }
                | KvOp::Expire { .. }
                | KvOp::FieldSet { .. }
                | KvOp::Incr { .. }
                | KvOp::IncrFloat { .. }
                | KvOp::Cas { .. }
                | KvOp::GetSet { .. }
                | KvOp::Transfer { .. }
                | KvOp::TransferItem { .. }
                | KvOp::PredicateUpdate { .. }
                | KvOp::PredicateDelete { .. }
        );
        if is_kv_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Kv) {
            return r;
        }
        if let Some(refusal) =
            crate::data::executor::handlers::kv::unbound::refuse_unbound_kv_write(op)
        {
            return self.response_error(task, refusal);
        }
        // Every row body the op supplies whole holds its declared numeric
        // columns' values before any handler sees it.
        let coerced = match self.coerce_kv_op_bodies(did, tid, op) {
            Ok(coerced) => coerced,
            Err(e) => return self.response_error(task, e),
        };
        // Every write of the op stamps one system-time ordinal, so a read at
        // any cut sees all of its rows or none.
        let apply_ordinal = self.kv_apply_ordinal();
        let scope = self.kv_engine.open_write_scope(apply_ordinal);
        let response = self.execute_kv(task, did, tid, coerced.as_ref().unwrap_or(op));
        self.kv_engine.close_write_scope(scope);
        response
    }

    /// The ordinal a KV apply scope fixes: the Calvin transaction's, else the
    /// one the write's WAL record carries (the replicated entry's leader
    /// assigned it, or the local append reserved it). `None` lets the scope
    /// draw a fresh ordinal of the shared clock at its first write.
    pub(in crate::data::executor) fn kv_apply_ordinal(&self) -> Option<i64> {
        self.apply_scope
            .calvin_txn_ordinal
            .or(self.apply_scope.replicated_ordinal)
    }
}
