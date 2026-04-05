//! Shared trigger evaluation logic: WHEN clause evaluation, body execution.
//!
//! Used by BEFORE, AFTER, and INSTEAD OF trigger fire paths.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::{MAX_CASCADE_DEPTH, StatementExecutor};
use crate::control::security::catalog::trigger_types::{StoredTrigger, TriggerSecurity};
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

/// Check cascade depth and return a clear error if exceeded.
pub fn check_cascade_depth(cascade_depth: u32, collection: &str) -> crate::Result<()> {
    if cascade_depth >= MAX_CASCADE_DEPTH {
        return Err(crate::Error::BadRequest {
            detail: format!(
                "trigger cascade depth exceeded ({MAX_CASCADE_DEPTH}): \
                 possible infinite loop on collection '{collection}'"
            ),
        });
    }
    Ok(())
}

/// Execute a list of triggers with the given bindings.
///
/// Evaluates WHEN clauses, parses trigger bodies, and executes each
/// matching trigger body through the statement executor.
///
/// Handles SECURITY DEFINER: when a trigger has `security = Definer`,
/// the executor runs with the trigger owner's identity instead of the caller's.
/// Tenant boundary is enforced — DEFINER identity always uses the trigger's tenant.
pub async fn fire_triggers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    triggers: &[StoredTrigger],
    bindings: &RowBindings,
    cascade_depth: u32,
) -> crate::Result<()> {
    for trigger in triggers {
        if let Some(ref when_cond) = trigger.when_condition {
            let bound_cond = bindings.substitute(when_cond);
            if !evaluate_simple_condition(&bound_cond) {
                continue;
            }
        }

        let block = match state.block_cache.get_or_parse(&trigger.body_sql) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    trigger = %trigger.name,
                    error = %e,
                    "failed to parse trigger body, skipping"
                );
                continue;
            }
        };

        // Resolve effective identity based on security mode.
        let effective_identity = resolve_trigger_identity(trigger, identity, tenant_id);

        // Audit log the trigger invocation with effective identity.
        info!(
            trigger = %trigger.name,
            collection = collection,
            timing = trigger.timing.as_str(),
            security = trigger.security.as_str(),
            caller = %identity.username,
            effective_user = %effective_identity.username,
            cascade_depth = cascade_depth,
            "trigger invoked"
        );

        let executor = StatementExecutor::with_source(
            state,
            effective_identity,
            tenant_id,
            cascade_depth + 1,
            crate::event::EventSource::Trigger,
        );

        if let Err(e) = executor.execute_block(&block, bindings).await {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "trigger '{}' on '{}' failed: {}",
                    trigger.name, collection, e
                ),
            });
        }
    }

    Ok(())
}

/// Resolve the effective identity for a trigger execution.
///
/// - INVOKER (default): uses the caller's identity (passthrough).
/// - DEFINER: creates a synthetic identity from the trigger's owner.
///   The tenant is always the trigger's tenant — DEFINER cannot cross tenants.
pub(crate) fn resolve_trigger_identity(
    trigger: &StoredTrigger,
    caller: &AuthenticatedIdentity,
    tenant_id: TenantId,
) -> AuthenticatedIdentity {
    match trigger.security {
        TriggerSecurity::Invoker => caller.clone(),
        TriggerSecurity::Definer => {
            // Create a synthetic identity for the trigger owner.
            // The owner is a superuser within the trigger's tenant scope.
            AuthenticatedIdentity {
                user_id: 0, // System-generated; not a real user ID
                username: trigger.owner.clone(),
                tenant_id,
                auth_method: AuthMethod::Trust,
                roles: vec![Role::TenantAdmin],
                is_superuser: true,
            }
        }
    }
}

/// Execute BEFORE triggers that may mutate the NEW row.
///
/// Returns the (possibly modified) NEW fields after all BEFORE triggers have run.
/// If a trigger executes `RAISE EXCEPTION`, the error propagates and the DML is aborted.
///
/// BEFORE triggers can modify NEW by executing `SET NEW.field = value` statements.
/// Currently, modification happens via RAISE EXCEPTION (abort) or by allowing the
/// trigger body to run side effects. True NEW mutation (returning modified row) is
/// handled by the caller interpreting ASSIGN statements targeting NEW.* fields.
#[allow(clippy::too_many_arguments)]
pub async fn fire_before_triggers_with_mutation(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    collection: &str,
    triggers: &[StoredTrigger],
    bindings: &RowBindings,
    cascade_depth: u32,
    mut new_fields: Option<HashMap<String, nodedb_types::Value>>,
) -> crate::Result<Option<HashMap<String, nodedb_types::Value>>> {
    for trigger in triggers {
        if let Some(ref when_cond) = trigger.when_condition {
            // Rebuild bindings with current (possibly mutated) NEW fields.
            let current_bindings = if let Some(ref fields) = new_fields {
                rebuild_bindings_with_new(bindings, fields)
            } else {
                bindings.clone()
            };
            let bound_cond = current_bindings.substitute(when_cond);
            if !evaluate_simple_condition(&bound_cond) {
                continue;
            }
        }

        let block = match state.block_cache.get_or_parse(&trigger.body_sql) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    trigger = %trigger.name,
                    error = %e,
                    "failed to parse BEFORE trigger body, skipping"
                );
                continue;
            }
        };

        // Build bindings with current NEW fields for this trigger.
        let current_bindings = if let Some(ref fields) = new_fields {
            rebuild_bindings_with_new(bindings, fields)
        } else {
            bindings.clone()
        };

        let effective_identity = resolve_trigger_identity(trigger, identity, tenant_id);

        info!(
            trigger = %trigger.name,
            collection = collection,
            timing = "BEFORE",
            security = trigger.security.as_str(),
            caller = %identity.username,
            effective_user = %effective_identity.username,
            cascade_depth = cascade_depth,
            "BEFORE trigger invoked"
        );

        let executor = StatementExecutor::with_source(
            state,
            effective_identity,
            tenant_id,
            cascade_depth + 1,
            crate::event::EventSource::Trigger,
        );

        // RAISE EXCEPTION in the body will propagate as an error, aborting the DML.
        if let Err(e) = executor.execute_block(&block, &current_bindings).await {
            return Err(crate::Error::BadRequest {
                detail: format!(
                    "BEFORE trigger '{}' on '{}' aborted DML: {}",
                    trigger.name, collection, e
                ),
            });
        }

        // Extract NEW.field mutations from ASSIGN statements (e.g., `NEW.total := NEW.total * 0.9`).
        let mutations = executor.take_new_mutations();
        if !mutations.is_empty()
            && let Some(fields) = new_fields.as_mut()
        {
            for (field, value) in mutations {
                fields.insert(field, value);
            }
        }
    }

    Ok(new_fields)
}

/// Rebuild a RowBindings with updated NEW fields (for BEFORE trigger chaining).
fn rebuild_bindings_with_new(
    original: &RowBindings,
    new_fields: &HashMap<String, nodedb_types::Value>,
) -> RowBindings {
    original.with_new_row(new_fields.clone())
}

/// Simple condition evaluation for WHEN clauses.
///
/// Handles common patterns without needing DataFusion:
/// - Constants: TRUE, FALSE, 1, 0, NULL
///
/// Falls back to `true` (fire the trigger) for complex conditions
/// that require DataFusion evaluation. The trigger body itself will
/// handle the condition via IF blocks.
pub fn evaluate_simple_condition(condition: &str) -> bool {
    super::try_eval_simple_condition(condition).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_condition_true() {
        assert!(evaluate_simple_condition("TRUE"));
        assert!(evaluate_simple_condition("1"));
    }

    #[test]
    fn simple_condition_false() {
        assert!(!evaluate_simple_condition("FALSE"));
        assert!(!evaluate_simple_condition("0"));
        assert!(!evaluate_simple_condition("NULL"));
    }

    #[test]
    fn complex_condition_defaults_true() {
        assert!(evaluate_simple_condition("'ord-1' IS NOT NULL"));
    }
}
