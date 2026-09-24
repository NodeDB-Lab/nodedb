// SPDX-License-Identifier: BUSL-1.1

use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

use super::super::task::ExecutionTask;
use super::CoreLoop;

impl CoreLoop {
    /// Hand a finished response back to the Control Plane, or report its loss.
    /// The response ring is bounded; if the push is refused, a caller that
    /// retries a committed `write` on timeout double-applies it, so the drop
    /// is recorded here — the only place that still knows what happened.
    pub(in crate::data::executor) fn send_response(
        &mut self,
        response: Response,
        write: crate::diag::LostResponseWrite,
    ) {
        if let Err(e) = self
            .response_tx
            .try_push(crate::bridge::dispatch::BridgeResponse { inner: response })
        {
            tracing::error!(
                core = self.core_id,
                error = %e,
                write = ?write,
                "failed to send response — caller can only learn a deadline"
            );
            crate::diag::data_plane_response_lost(self.core_id, write);
        }
    }

    pub(in crate::data::executor) fn response_ok(&self, task: &ExecutionTask) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    pub(in crate::data::executor) fn response_with_payload(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Build the response for a write reporting an affected-row count. A
    /// handler rendering `INSERT n` / `UPDATE n` / `DELETE n` must return
    /// through here — [`response_ok`](Self::response_ok) has no count to render.
    pub(in crate::data::executor) fn response_affected(
        &self,
        task: &ExecutionTask,
        affected: u64,
    ) -> Response {
        let payload = super::super::response_codec::encode_affected(affected);
        self.response_with_payload(task, payload)
    }

    /// Build the response for a write whose affected count is fixed but whose
    /// command verb is decided by the handler: `op` is `"insert"` or
    /// `"update"`. Read by `extract_kv_conflict_op` on the Control Plane to
    /// render `INSERT 0 n` or `UPDATE n`.
    pub(in crate::data::executor) fn response_affected_with_op(
        &self,
        task: &ExecutionTask,
        affected: u64,
        op: &str,
    ) -> Response {
        let payload = super::super::response_codec::encode_affected_with_op(affected, op);
        self.response_with_payload(task, payload)
    }

    pub(in crate::data::executor) fn response_partial(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Per-collection read-version LSN for `task`'s plan (distinct from the
    /// core-global `watermark`). On a write response this is the POST-write
    /// version. A resolved KV write's version is the max across its
    /// mutations, which may span two collections.
    pub(in crate::data::executor) fn read_version_lsn(
        &self,
        task: &ExecutionTask,
    ) -> crate::types::Lsn {
        if let crate::bridge::envelope::PhysicalPlan::Kv(
            nodedb_physical::physical_plan::KvOp::ResolvedWrite { mutations, .. },
        ) = task.plan()
        {
            return mutations
                .iter()
                .map(|m| self.collection_read_version(task, m.collection().as_str()))
                .max()
                .unwrap_or(crate::types::Lsn::ZERO);
        }
        task.plan()
            .collection()
            .map(|c| self.collection_read_version(task, c))
            .unwrap_or(crate::types::Lsn::ZERO)
    }

    /// One collection's recorded write LSN on this core, or `Lsn::ZERO` when
    /// it has none.
    fn collection_read_version(&self, task: &ExecutionTask, collection: &str) -> crate::types::Lsn {
        self.write_index
            .collection_write_lsn(&super::write_index::CollKey {
                db: task.request.database_id,
                tenant: task.request.tenant_id,
                collection: Box::from(collection),
            })
            .unwrap_or(crate::types::Lsn::ZERO)
    }

    pub(in crate::data::executor) fn response_error(
        &self,
        task: &ExecutionTask,
        error_code: impl Into<ErrorCode>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            read_version_lsn: crate::types::Lsn::ZERO,
            error_code: Some(Box::new(error_code.into())),
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Map key for the four vector in-memory maps. Returns
    /// `(DatabaseId, TenantId, collection_key)`, where `collection_key` is
    /// `collection`, or `"{collection}:{field_name}"` when named.
    pub(in crate::data::executor) fn vector_index_key(
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> (DatabaseId, TenantId, String) {
        let coll_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };
        (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            coll_key,
        )
    }

    /// In-memory build key for a vector collection: `"{db}:{tid}:{coll}"`.
    /// `coll` may itself contain `:` — parsing uses `splitn(3, ':')`.
    ///
    /// This key never becomes a filename; the on-disk name comes from
    /// `vector_ckpt_stem`, which hex-encodes the collection.
    pub(in crate::data::executor) fn vector_build_key(
        key: &(DatabaseId, TenantId, String),
    ) -> String {
        format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2)
    }

    pub(in crate::data::executor) fn get_crdt_engine(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> crate::Result<&mut TenantCrdtEngine> {
        match self.crdt_engines.entry((database_id, tenant_id)) {
            std::collections::hash_map::Entry::Occupied(engine) => Ok(engine.into_mut()),
            std::collections::hash_map::Entry::Vacant(slot) => {
                tracing::debug!(
                    core = self.core_id,
                    %database_id,
                    %tenant_id,
                    "creating CRDT engine for database tenant"
                );
                let mut engine =
                    TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())?;
                // Rejected deltas leave no replayable record, so their
                // dead-letter entries come back from storage.
                engine.restore_dead_letters(
                    self.sparse
                        .load_crdt_dead_letters(database_id.as_u64(), tenant_id.as_u64())?,
                );
                Ok(slot.insert(engine))
            }
        }
    }

    /// Release the per-collection validation candidates every CRDT engine on
    /// this core is holding, once a run of delta applies ends. Keeping them
    /// past the run would just be a second document per collection sitting idle.
    pub(in crate::data::executor) fn release_crdt_apply_candidates(&mut self) {
        for engine in self.crdt_engines.values_mut() {
            engine.clear_apply_candidates();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_core_with_dir, make_default_task};
    use crate::diag::{LostResponseWrite, data_plane_responses_lost};

    /// A response the ring accepts reaches the Control Plane unchanged and
    /// records nothing.
    #[test]
    fn a_deliverable_response_is_handed_over_and_reports_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, mut resp_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let response = core.response_ok(&task);
        let before = data_plane_responses_lost();

        core.send_response(response, LostResponseWrite::Committed);

        let delivered = resp_rx.try_pop().expect("response delivered");
        assert_eq!(delivered.inner.request_id, task.request_id());
        assert_eq!(data_plane_responses_lost(), before);
    }

    /// The drop is unavoidable once the ring is full, but must never be
    /// silent — a swallowed response leaves a committed write untraceable.
    #[test]
    fn a_refused_response_is_counted_rather_than_swallowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Never drained, so the ring saturates and then refuses.
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        // Comfortably past the ring's capacity, so the last push below is
        // guaranteed to be refused regardless of the configured depth.
        for _ in 0..256 {
            let filler = core.response_ok(&task);
            core.send_response(filler, LostResponseWrite::Committed);
        }
        let before = data_plane_responses_lost();

        let overflow = core.response_ok(&task);
        core.send_response(overflow, LostResponseWrite::Committed);

        assert_eq!(
            data_plane_responses_lost(),
            before + 1,
            "a response the ring refused must be reported, not discarded"
        );
    }
}
