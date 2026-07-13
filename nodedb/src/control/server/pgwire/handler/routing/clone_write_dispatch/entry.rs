// SPDX-License-Identifier: BUSL-1.1

//! Single hooked-in clone CoW write-interception entry point. Routes by plan
//! shape to the Document or KV copy-up/tombstone protocol.

use pgwire::error::PgWireResult;

use nodedb_types::TenantId;

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::core::NodeDbPgHandler;

/// Outcome of write-path clone interception.
pub(in crate::control::server::pgwire::handler::routing) enum CloneWriteOutcome {
    /// No interception needed — caller must dispatch normally.
    Passthrough,
    /// The write was fully handled by the clone path. Caller uses this response.
    Handled(Response),
}

impl NodeDbPgHandler {
    /// Intercept a single write task for a cloned collection.
    ///
    /// Must be called for every `PointUpdate` and `PointDelete` task before
    /// normal dispatch. Returns `Passthrough` when the collection is not a
    /// Shadowed/Materializing clone (zero overhead for non-clone paths).
    pub(in crate::control::server::pgwire::handler::routing) async fn maybe_intercept_clone_write(
        &self,
        task: &PhysicalTask,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate { .. })
            | PhysicalPlan::Document(DocumentOp::PointDelete { .. }) => {
                self.intercept_doc_clone_write(task, identity, tenant_id)
                    .await
            }
            PhysicalPlan::Kv(KvOp::FieldSet { .. }) | PhysicalPlan::Kv(KvOp::Delete { .. }) => {
                self.intercept_kv_clone_write(task, identity, tenant_id)
                    .await
            }
            _ => Ok(CloneWriteOutcome::Passthrough),
        }
    }
}
