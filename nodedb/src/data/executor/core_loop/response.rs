use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::types::TenantId;

use super::super::task::ExecutionTask;
use super::CoreLoop;

impl CoreLoop {
    pub(in crate::data::executor) fn response_ok(&self, task: &ExecutionTask) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            error_code: None,
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
            error_code: None,
        }
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
            error_code: None,
        }
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
            error_code: Some(error_code.into()),
        }
    }

    /// Build the map key for the four vector in-memory maps
    /// (`vector_collections`, `vector_params`, `index_configs`, `ivf_indexes`).
    ///
    /// Returns `(TenantId, collection_key)` where `collection_key` is:
    /// - `collection` when `field_name` is empty, or
    /// - `"{collection}:{field_name}"` when a named field is specified.
    ///
    /// This replaces the old `format!("{tid}:{collection}")` string key with a
    /// structured tuple so tenant scoping is structural rather than lexical.
    pub(in crate::data::executor) fn vector_index_key(
        tenant_id: u32,
        collection: &str,
        field_name: &str,
    ) -> (TenantId, String) {
        let coll_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };
        (TenantId::new(tenant_id), coll_key)
    }

    /// Checkpoint filename for a vector collection key.
    ///
    /// Produces the same `"{tid}:{coll}"` string that was used before the
    /// tuple-key migration so existing on-disk checkpoint files remain valid.
    pub(in crate::data::executor) fn vector_checkpoint_filename(
        key: &(TenantId, String),
    ) -> String {
        format!("{}:{}", key.0.as_u32(), key.1)
    }

    pub(in crate::data::executor) fn get_crdt_engine(
        &mut self,
        tenant_id: TenantId,
    ) -> crate::Result<&mut TenantCrdtEngine> {
        if !self.crdt_engines.contains_key(&tenant_id) {
            tracing::debug!(core = self.core_id, %tenant_id, "creating CRDT engine for tenant");
            let engine =
                TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())?;
            self.crdt_engines.insert(tenant_id, engine);
        }
        Ok(self
            .crdt_engines
            .get_mut(&tenant_id)
            .expect("just inserted"))
    }
}
