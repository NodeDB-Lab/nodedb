// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `ALTER SCHEDULE` DDL handler.
//!
//! The registry lookup, the `propose_and_apply` catalog write, and the
//! in-memory registry update run here. The result is the protocol-neutral
//! [`DdlResult`] / [`DdlError`].
//!
//! Supports: ENABLE, DISABLE, SET CRON 'expr'.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::status;

/// Handle `ALTER SCHEDULE <name> ENABLE | DISABLE | SET CRON '<expr>'`.
///
/// `name`, `action`, and `cron_expr` come from the typed
/// `AutomationStmt::AlterSchedule` variant.
pub async fn alter_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    name: &str,
    action: &str,
    cron_expr: Option<&str>,
) -> Result<Vec<DdlResult>, DdlError> {
    crate::control::server::shared::ddl::neutral::object_permission::require_tenant_wide(
        state,
        identity,
        database_id,
        "alter schedules",
    )?;
    let tenant_id = identity.tenant_id.as_u64();

    // Look up the schedule in the registry.
    let mut def = state
        .schedule_registry
        .get(database_id, tenant_id, name)
        .ok_or_else(|| DdlError::new("42704", format!("schedule \"{name}\" does not exist")))?;

    match action {
        "ENABLE" => {
            def.enabled = true;
        }
        "DISABLE" => {
            def.enabled = false;
        }
        "SET" => {
            let new_cron = cron_expr.ok_or_else(|| {
                DdlError::new(
                    "42601",
                    "ALTER SCHEDULE SET CRON requires a quoted cron expression",
                )
            })?;

            CronExpr::parse(new_cron)
                .map_err(|e| DdlError::new("22023", format!("invalid cron expression: {e}")))?;

            def.cron_expr = new_cron.to_string();
        }
        _ => {
            return Err(DdlError::new(
                "42601",
                "ALTER SCHEDULE supports: ENABLE, DISABLE, SET CRON 'expr'",
            ));
        }
    }

    // Persist the updated definition through the same metadata-raft
    // propose path every other parent-replicated ALTER uses, so a
    // cluster deployment converges on the new state cluster-wide and
    // the single-node fallback writes both the primary row and its
    // OWNERS row. The earlier direct `catalog.put_schedule(&def)`
    // call did neither — divergence on replicas, orphan on disk.
    let entry = crate::control::catalog_entry::CatalogEntry::PutSchedule(Box::new(def.clone()));
    super::super::super::catalog::propose_and_apply_async(state, &entry).await?;

    // Update in-memory registry.
    state.schedule_registry.update(def);

    Ok(status("ALTER SCHEDULE"))
}
