// SPDX-License-Identifier: BUSL-1.1

use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

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

    /// Per-collection read-version LSN for `task`'s plan: the scanned
    /// collection's `coll_write_lsn` at read time — a WAL LSN, the single domain
    /// the version index is fed in — and the sound comparand for cross-shard OCC
    /// read validation. `Lsn::ZERO` when the plan maps to no single collection or
    /// the collection has no recorded write on this core. Distinct from the
    /// core-global `watermark`.
    ///
    /// On a WRITE response this is the POST-write version: every write handler
    /// records its LSN into the index before building its response, so the value
    /// read back here already includes the write. That is what lets the apply
    /// path hand a committed write's own version back to its proposer.
    pub(in crate::data::executor) fn read_version_lsn(
        &self,
        task: &ExecutionTask,
    ) -> crate::types::Lsn {
        task.plan()
            .collection()
            .map(|c| {
                self.write_index
                    .collection_write_lsn(&super::write_index::CollKey {
                        db: task.request.database_id,
                        tenant: task.request.tenant_id,
                        collection: Box::from(c),
                    })
                    .unwrap_or(crate::types::Lsn::ZERO)
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

    /// Build the map key for the four vector in-memory maps
    /// (`vector_collections`, `vector_params`, `index_configs`, `ivf_indexes`).
    ///
    /// Returns `(DatabaseId, TenantId, collection_key)` where `collection_key` is:
    /// - `collection` when `field_name` is empty, or
    /// - `"{collection}:{field_name}"` when a named field is specified.
    ///
    /// This replaces the old `format!("{tid}:{collection}")` string key with a
    /// structured tuple so database + tenant scoping is structural rather than
    /// lexical.
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

    /// Checkpoint filename for a vector collection key.
    ///
    /// Produces a `"{db}:{tid}:{coll}"` string. The `coll` component may itself
    /// contain `:` (it is `collection` or `collection:field`) — that is fine
    /// because parsing uses `splitn(3, ':')` and treats the remainder verbatim.
    pub(in crate::data::executor) fn vector_checkpoint_filename(
        key: &(DatabaseId, TenantId, String),
    ) -> String {
        format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2)
    }

    pub(in crate::data::executor) fn get_crdt_engine(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> crate::Result<&mut TenantCrdtEngine> {
        let key = (database_id, tenant_id);
        if !self.crdt_engines.contains_key(&key) {
            tracing::debug!(
                core = self.core_id,
                %database_id,
                %tenant_id,
                "creating CRDT engine for database tenant"
            );
            let engine =
                TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())?;
            self.crdt_engines.insert(key, engine);
        }
        Ok(self.crdt_engines.get_mut(&key).expect("just inserted"))
    }
}
