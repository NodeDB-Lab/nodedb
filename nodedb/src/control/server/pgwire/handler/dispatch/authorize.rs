// SPDX-License-Identifier: BUSL-1.1

//! Task authorization and the CRDT admission-gate check shared by every
//! dispatch routing path.

use std::sync::Arc;

use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::security::identity::AuthenticatedIdentity;

use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    pub(super) fn authorize_for_dispatch(
        &self,
        identity: &AuthenticatedIdentity,
        task: &PhysicalTask,
    ) -> crate::Result<crate::control::server::shared::authorization::AuthorizedTask> {
        let emitter =
            crate::control::security::audit::ArcAuditEmitter(Arc::clone(&self.state.audit));
        crate::control::server::shared::authorization::authorize_task_set(
            identity,
            std::slice::from_ref(task),
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(crate::Error::from)?
        .into_tasks()
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Internal {
            detail: "pgwire authorization returned no capability".into(),
        })
    }
}

pub(super) fn reject_unadmitted_crdt_apply(plan: &PhysicalPlan) -> crate::Result<()> {
    if matches!(
        plan,
        PhysicalPlan::Crdt(
            CrdtOp::Apply { .. }
                | CrdtOp::ApplyAuthenticated { .. }
                | CrdtOp::ImportSnapshot { .. }
        )
    ) {
        return Err(crate::Error::CrdtApplyRequiresAdmission);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;

    #[test]
    fn generic_pgwire_dispatch_rejects_unadmitted_apply() {
        let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "docs",
            ),
            document_id: "doc-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 1,
            surrogate: Surrogate::ZERO,
            provenance: None,
            constraint_version_required: 0,
            expected_frontier_digest: None,
        });
        assert!(matches!(
            reject_unadmitted_crdt_apply(&plan),
            Err(crate::Error::CrdtApplyRequiresAdmission)
        ));
    }

    #[test]
    fn dispatch_task_compile_check() {
        // Confirms the dispatch module compiles.
        let _: () = ();
    }
}
