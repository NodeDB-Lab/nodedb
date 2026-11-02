// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::KvDueExpiries`: the KV keys whose TTL passed on this core.
//!
//! The core hands its wheel's passed entries to its due set, then answers
//! at most `limit` of them as one msgpack array of `DueExpiry`. It reaps
//! nothing: the data group's leader proposes each reap through write
//! admission (`control::kv_expiry`).

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::DueExpiry;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_due_expiries(
        &mut self,
        task: &ExecutionTask,
        limit: u32,
    ) -> Response {
        self.kv_engine
            .collect_due_expiries(crate::engine::kv::current_ms());
        let due: Vec<DueExpiry> = self
            .kv_engine
            .due_expiries(usize::try_from(limit).unwrap_or(usize::MAX));
        match zerompk::to_msgpack_vec(&due) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("kv due expiries: {e}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::kv::DueExpiry;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_physical::physical_plan::MetaOp;

    /// The probe answers the due entries and leaves the keys stored.
    #[test]
    fn the_probe_answers_due_keys_and_reaps_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) =
            crate::data::executor::core_loop::tests::make_core_with_dir(dir.path());
        core.kv_engine
            .put(crate::engine::kv::KvPutParams {
                database_id: 0,
                tenant_id: 1,
                collection: "c",
                key: b"s",
                value: b"v",
                ttl_ms: 1,
                now_ms: 1_000,
                surrogate: crate::engine::kv::test_support::row_surrogate(b"s"),
            })
            .expect("a bound row writes");
        let task = CoreLoop::replay_task(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::KvDueExpiries { limit: 8 }),
            None,
        );
        let resp = core.execute_kv_due_expiries(&task, 8);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        let due: Vec<DueExpiry> = zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode");
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].expire_at_ms, 1_001);
        assert!(core.kv_engine.get(0, 1, "c", b"s", 0).is_some());
    }
}
