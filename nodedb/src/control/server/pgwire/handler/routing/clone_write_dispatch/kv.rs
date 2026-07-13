// SPDX-License-Identifier: BUSL-1.1

//! KV engine CoW write interception (FieldSet / Delete).

use std::sync::Arc;

use pgwire::error::PgWireResult;

use nodedb_types::{CloneStatus, Lsn, TenantId};

use crate::control::clone::copyup::{KvCopyUpParams, perform_kv_clone_copyup};
use crate::control::clone::tombstone::{KvTombstoneParams, perform_kv_clone_tombstone};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::types::VShardId;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::auth::pgwire_authorization_error;
use super::super::super::core::NodeDbPgHandler;
use super::entry::CloneWriteOutcome;
use super::probes::{dispatch_data_plane_raw, fetch_kv_source_value, probe_kv_key_in_target};
use super::util::{strip_db_prefix, synthetic_ok_response, write_err};

impl NodeDbPgHandler {
    /// Handle KV CoW write interception (FieldSet / Delete).
    pub(super) async fn intercept_kv_clone_write(
        &self,
        task: &PhysicalTask,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let (collection_qualified, kv_key, is_delete) = match &task.plan {
            PhysicalPlan::Kv(KvOp::FieldSet {
                collection, key, ..
            }) => (collection.as_str(), key.clone(), false),
            PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => {
                // Delete may have multiple keys; handle each. We serialize here
                // (one tombstone per key) and return Handled with synthetic OK.
                let collection_qualified = collection.as_str();
                let db_id = task.database_id;
                let coll_name = strip_db_prefix(db_id, collection_qualified);

                let catalog = self.state.credentials.catalog();

                let desc = catalog
                    .get_collection(db_id, tenant_id.as_u64(), coll_name)
                    .map_err(|e| write_err(&format!("clone kv delete: get_collection: {e}")))?;
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

                // Split each key into one of two paths:
                //   • key absent in target (source-only) → record a tombstone
                //     so future scans hide the source row.
                //   • key present in target (already copied up or written
                //     in this clone) → dispatch a real KV Delete to remove
                //     the target row, then ALSO record a tombstone so any
                //     surviving source row remains hidden after deletion.
                //
                // Tombstoning unconditionally for target-resident keys is
                // safe: the source row (if any) must always be hidden in
                // this clone after the user has issued a DELETE.
                let mut keys_to_dispatch: Vec<Vec<u8>> = Vec::new();
                for key in keys {
                    let key_str = String::from_utf8_lossy(key).into_owned();
                    let key_in_target = probe_kv_key_in_target(
                        &self.state,
                        tenant_id,
                        db_id,
                        collection_qualified,
                        key,
                    )
                    .await
                    .map_err(|e| write_err(&format!("clone kv delete probe: {e}")))?;

                    perform_kv_clone_tombstone(KvTombstoneParams {
                        state: &self.state,
                        target_db_id: db_id,
                        target_collection: coll_name,
                        kv_key: key_str,
                    })
                    .map_err(|e| write_err(&format!("clone kv tombstone: {e}")))?;

                    if key_in_target {
                        keys_to_dispatch.push(key.clone());
                    }
                }

                if !keys_to_dispatch.is_empty() {
                    // Dispatch a real Delete for keys that exist in target.
                    let delete_plan = PhysicalPlan::Kv(KvOp::Delete {
                        collection: collection_qualified.to_string(),
                        keys: keys_to_dispatch,
                    });
                    let vshard_id =
                        VShardId::from_collection_in_database(db_id, collection_qualified);
                    let resp = dispatch_data_plane_raw(
                        &self.state,
                        tenant_id,
                        vshard_id,
                        db_id,
                        delete_plan,
                    )
                    .await
                    .map_err(|e| write_err(&format!("clone kv delete dispatch: {e}")))?;
                    return Ok(CloneWriteOutcome::Handled(resp));
                }

                let synthetic_resp = synthetic_ok_response(self.next_request_id(), Lsn::new(0));
                return Ok(CloneWriteOutcome::Handled(synthetic_resp));
            }
            _ => return Ok(CloneWriteOutcome::Passthrough),
        };

        // FieldSet path: check clone status, copy-up if needed.
        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, collection_qualified);

        let catalog = self.state.credentials.catalog();

        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone kv write: get_collection: {e}")))?;
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

        // FieldSet is not a delete.
        let _ = is_delete;

        let key_in_target =
            probe_kv_key_in_target(&self.state, tenant_id, db_id, collection_qualified, &kv_key)
                .await
                .map_err(|e| write_err(&format!("clone kv write probe: {e}")))?;

        if key_in_target {
            // Row exists in target — let the normal FieldSet proceed.
            return Ok(CloneWriteOutcome::Passthrough);
        }

        // Fetch source KV row and copy it up to target.
        let source_db_id = origin.source_database;
        let source_coll = origin.source_collection.as_str();
        let source_coll_qualified =
            crate::control::planner::sql_plan_convert::convert::db_qualified(
                source_db_id,
                source_coll,
            );

        let source_value = fetch_kv_source_value(
            &self.state,
            tenant_id,
            source_db_id,
            &source_coll_qualified,
            &kv_key,
        )
        .await
        .map_err(|e| write_err(&format!("clone kv copyup fetch: {e}")))?;

        let Some(source_value) = source_value else {
            // Row absent in source — let normal FieldSet run (no-op or error from DP).
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let kv_key_str = String::from_utf8_lossy(&kv_key).into_owned();

        perform_kv_clone_copyup(KvCopyUpParams {
            state: &Arc::clone(&self.state),
            tenant_id,
            target_db_id: db_id,
            target_collection: coll_name,
            kv_key,
            source_value_bytes: source_value,
        })
        .await
        .map_err(|e| write_err(&format!("clone kv copyup: {e}")))?;

        // Tombstone the source key so future clone reads do not merge in the
        // now-superseded source row.  The copy-up wrote the row to the target
        // and the FieldSet will overwrite it; the source copy must be hidden.
        perform_kv_clone_tombstone(KvTombstoneParams {
            state: &self.state,
            target_db_id: db_id,
            target_collection: coll_name,
            kv_key: kv_key_str,
        })
        .map_err(|e| write_err(&format!("clone kv tombstone after copyup: {e}")))?;

        // Fall through: let the original FieldSet dispatch to the target.
        Ok(CloneWriteOutcome::Passthrough)
    }
}
