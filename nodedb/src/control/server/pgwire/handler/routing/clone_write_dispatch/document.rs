// SPDX-License-Identifier: BUSL-1.1

//! Document engine CoW write interception (PointUpdate / PointDelete).

use std::sync::Arc;

use pgwire::error::PgWireResult;

use nodedb_types::{CloneStatus, Lsn, TenantId};

use crate::control::clone::copyup::{CopyUpParams, perform_clone_copyup};
use crate::control::clone::tombstone::{TombstoneParams, perform_clone_tombstone};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::auth::pgwire_authorization_error;
use super::super::super::core::NodeDbPgHandler;
use super::entry::CloneWriteOutcome;
use super::probes::{fetch_source_row, probe_row_in_target};
use super::util::{strip_db_prefix, synthetic_ok_response, write_err};

impl NodeDbPgHandler {
    /// Handle Document CoW write interception (PointUpdate / PointDelete).
    pub(super) async fn intercept_doc_clone_write(
        &self,
        task: &PhysicalTask,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let (collection_qualified, document_id, surrogate, is_delete) = match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                ..
            }) => (collection.as_str(), document_id.as_str(), *surrogate, false),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            }) => (collection.as_str(), document_id.as_str(), *surrogate, true),
            _ => return Ok(CloneWriteOutcome::Passthrough),
        };

        let catalog = self.state.credentials.catalog();

        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, collection_qualified);

        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone write: get_collection: {e}")))?;
        let Some(desc) = desc else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let Some(ref origin) = desc.cloned_from else {
            return Ok(CloneWriteOutcome::Passthrough);
        };
        match desc.clone_status {
            CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_collection(
            identity,
            origin.source_database,
            &origin.source_collection,
            Permission::Read,
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(pgwire_authorization_error)?;

        let row_in_target = probe_row_in_target(
            &self.state,
            tenant_id,
            db_id,
            collection_qualified,
            document_id,
            surrogate,
        )
        .await
        .map_err(|e| write_err(&format!("clone write probe: {e}")))?;

        if row_in_target {
            return Ok(CloneWriteOutcome::Passthrough);
        }

        if is_delete {
            perform_clone_tombstone(TombstoneParams {
                state: &self.state,
                target_db_id: db_id,
                target_collection: coll_name,
                source_surrogate: surrogate,
            })
            .map_err(|e| write_err(&format!("clone tombstone: {e}")))?;

            let synthetic_resp = synthetic_ok_response(self.next_request_id(), Lsn::new(0));
            return Ok(CloneWriteOutcome::Handled(synthetic_resp));
        }

        let source_db_id = origin.source_database;
        let source_coll = origin.source_collection.as_str();
        let source_coll_qualified =
            crate::control::planner::sql_plan_convert::convert::db_qualified(
                source_db_id,
                source_coll,
            );

        let source_row_bytes = fetch_source_row(
            &self.state,
            tenant_id,
            source_db_id,
            &source_coll_qualified,
            document_id,
            surrogate,
        )
        .await
        .map_err(|e| write_err(&format!("clone write fetch source: {e}")))?;

        let Some(source_row_bytes) = source_row_bytes else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        perform_clone_copyup(CopyUpParams {
            state: &Arc::clone(&self.state),
            tenant_id,
            target_db_id: db_id,
            target_collection: coll_name,
            origin,
            source_surrogate: surrogate,
            source_doc_id: document_id.to_string(),
            source_row_bytes,
        })
        .await
        .map_err(|e| write_err(&format!("clone copyup: {e}")))?;

        Ok(CloneWriteOutcome::Passthrough)
    }
}
