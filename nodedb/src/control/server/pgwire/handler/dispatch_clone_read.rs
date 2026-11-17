// SPDX-License-Identifier: BUSL-1.1

//! Clone-check helpers for [`super::dispatch`]'s single-task dispatch path,
//! split out to keep `dispatch.rs` under the file-size cap.

use std::sync::Arc;

use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::clone_read::CloneReadAnswer;
use nodedb_physical::physical_task::PhysicalTask;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Clone-check, then authorize, one task at the Data-Plane dispatch
    /// boundary — the only way to obtain a `CloneCheckedTask` on this path.
    pub(super) async fn intercept_and_authorize_for_dispatch(
        &self,
        identity: &AuthenticatedIdentity,
        task: PhysicalTask,
    ) -> crate::Result<crate::control::server::shared::clone_write::CloneCheckedOutcome> {
        let tenant_id = task.tenant_id;
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.state.audit));
        crate::control::server::shared::clone_write::intercept_and_authorize(
            crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
                state: &self.state,
                task,
                identity,
                tenant_id,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                emitter: &emitter,
            },
        )
        .await
    }

    /// `Some(answer)` when a `Shadowed`/`Materializing` clone read was fully
    /// handled — the caller returns it directly. `None` otherwise.
    pub(super) async fn maybe_intercept_clone_read_early(
        &self,
        task: &PhysicalTask,
        identity: &AuthenticatedIdentity,
        perm: Permission,
        linearizable: bool,
    ) -> crate::Result<Option<CloneReadAnswer>> {
        if !matches!(perm, Permission::Read) {
            return Ok(None);
        }
        let tenant_id = task.tenant_id;
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.state.audit));
        match crate::control::server::shared::clone_read::maybe_intercept_clone_read(
            crate::control::server::shared::clone_read::CloneReadInterceptParams {
                state: &self.state,
                task,
                identity,
                tenant_id,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                emitter: &emitter,
                linearizable,
            },
        )
        .await?
        {
            crate::control::server::shared::clone_read::CloneReadOutcome::Handled(answer) => {
                Ok(Some(answer))
            }
            crate::control::server::shared::clone_read::CloneReadOutcome::Passthrough => Ok(None),
        }
    }
}
