//! Post-apply side effects for a [`CatalogEntry`] — dispatched by
//! DDL family.
//!
//! Split into two phases so readers of `applied_index` observe a
//! consistent view:
//!
//! - [`apply_post_apply_side_effects_sync`] runs the synchronous
//!   in-memory cache updates (install_replicated_user,
//!   install_replicated_role, etc.) **inline** on the raft applier
//!   thread, BEFORE the metadata applier bumps the
//!   `AppliedIndexWatcher`. Once `applied_index = N`, readers are
//!   guaranteed to see every sync side-effect of every entry up to
//!   N — no tokio spawn race.
//! - [`spawn_post_apply_async_side_effects`] spawns a tokio task for
//!   the genuinely async work — today that is only Data Plane
//!   dispatches for `PutCollection`. Readers of the Data Plane
//!   register state still race with this, but no test relies on
//!   that synchronisation.
//!
//! Previously both were combined into a single `tokio::spawn`, so
//! a freshly-applied `PutUser` could bump the watcher while its
//! `install_replicated_user` task was still queued on the
//! scheduler. Tests that waited on `applied_index` and then
//! immediately polled `credentials.get_user` would flake whenever
//! the scheduler ran them in that order.

pub mod api_key;
pub mod change_stream;
pub mod collection;
pub mod function;
pub mod materialized_view;
pub mod owner;
pub mod permission;
pub mod procedure;
pub mod rls;
pub mod role;
pub mod schedule;
pub mod sequence;
pub mod tenant;
pub mod trigger;
pub mod user;

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

/// Run every **synchronous** post-apply side effect inline. Must be
/// called from the metadata applier BEFORE the watcher bump so
/// readers of the applied index see every in-memory cache update
/// that entry triggered. Best-effort per variant: the whole thing
/// is infallible today (all typed functions log on failure and
/// return).
pub fn apply_post_apply_side_effects_sync(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    // Gateway plan-cache invalidation: on any descriptor mutation, evict
    // stale cached plans that reference the changed descriptor.
    // This is a single, unconditional call per DDL commit — negligible overhead.
    invalidate_gateway_cache_for_entry(entry, shared);

    match entry {
        CatalogEntry::PutCollection(stored) => {
            // Owner record install is sync; Data Plane register is
            // the async part, handled by `spawn_post_apply_async_side_effects`.
            collection::put_owner_sync(stored, Arc::clone(shared));
        }
        CatalogEntry::DeactivateCollection { tenant_id, name } => {
            collection::deactivate(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSequence(stored) => {
            sequence::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            sequence::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSequenceState(state) => {
            sequence::put_state((**state).clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTrigger(stored) => {
            trigger::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteTrigger { tenant_id, name } => {
            trigger::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutFunction(stored) => {
            function::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteFunction { tenant_id, name } => {
            function::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutProcedure(stored) => {
            procedure::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteProcedure { tenant_id, name } => {
            procedure::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutSchedule(stored) => {
            schedule::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteSchedule { tenant_id, name } => {
            schedule::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutChangeStream(stored) => {
            change_stream::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteChangeStream { tenant_id, name } => {
            change_stream::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutUser(stored) => {
            user::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeactivateUser { username } => {
            user::deactivate(username.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutRole(stored) => {
            role::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteRole { name } => {
            role::delete(name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutApiKey(stored) => {
            api_key::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::RevokeApiKey { key_id } => {
            api_key::revoke(key_id.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutMaterializedView(stored) => {
            materialized_view::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            materialized_view::delete(*tenant_id, name.clone(), Arc::clone(shared));
        }
        CatalogEntry::PutTenant(stored) => {
            tenant::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteTenant { tenant_id } => {
            tenant::delete(*tenant_id, Arc::clone(shared));
        }
        CatalogEntry::PutRlsPolicy(stored) => {
            rls::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection,
            name,
        } => {
            rls::delete(
                *tenant_id,
                collection.clone(),
                name.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutPermission(stored) => {
            permission::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeletePermission {
            target,
            grantee,
            permission: perm,
        } => {
            permission::delete(
                target.clone(),
                grantee.clone(),
                perm.clone(),
                Arc::clone(shared),
            );
        }
        CatalogEntry::PutOwner(stored) => {
            owner::put((**stored).clone(), Arc::clone(shared));
        }
        CatalogEntry::DeleteOwner {
            object_type,
            tenant_id,
            object_name,
        } => {
            owner::delete(
                object_type.clone(),
                *tenant_id,
                object_name.clone(),
                Arc::clone(shared),
            );
        }
    }
}

/// Spawn the async post-apply side effects of `entry`. Today this is
/// limited to Data Plane dispatches for `PutCollection` (the only
/// genuinely `.await`-carrying variant). Best-effort: failures log
/// and drop.
pub fn spawn_post_apply_async_side_effects(entry: CatalogEntry, shared: Arc<SharedState>) {
    if let CatalogEntry::PutCollection(stored) = entry {
        tokio::spawn(async move {
            collection::put_async(*stored, shared).await;
        });
    }
}

/// Notify the gateway plan-cache invalidator after a DDL descriptor mutation.
///
/// Extracts the descriptor name and new version from the entry and calls
/// `PlanCacheInvalidator::invalidate`. This is best-effort: if the gateway
/// has not been constructed yet (`gateway_invalidator == None`) the call is
/// a no-op.
///
/// ## Invalidation decision table (all 31 variants — exhaustive, no `_ => {}`)
///
/// The gateway plan cache keys on `(sql_hash, ph_hash, GatewayVersionSet)`.
/// A `GatewayVersionSet` lists `(collection_name, descriptor_version)` pairs
/// extracted from the `PhysicalPlan` by `touched_collections`. A DDL entry
/// requires invalidation only if it changes the observable plan shape for
/// an already-cached plan. Verified against `planner/`, `rls_injection.rs`,
/// and the `PhysicalPlan` definition.
///
/// | Entry kind                              | Invalidate? | Reason |
/// |-----------------------------------------|-------------|--------|
/// | PutCollection / DeactivateCollection    | ✅ yes      | collection schema baked into plan |
/// | PutSequence / DeleteSequence            | ❌ no       | sequences resolved at handler level (pgwire `transaction_cmds.rs`), not in PhysicalPlan |
/// | PutSequenceState                        | ❌ no       | runtime counter state, not plan shape |
/// | PutTrigger / DeleteTrigger              | ❌ no       | triggers dispatched by Event Plane post-execution; no trigger fields in any PhysicalPlan variant |
/// | PutFunction / DeleteFunction            | ❌ no       | functions looked up at eval time, not inlined |
/// | PutProcedure / DeleteProcedure          | ❌ no       | same as functions |
/// | PutSchedule / DeleteSchedule            | ❌ no       | scheduler runs independently |
/// | PutChangeStream / DeleteChangeStream    | ❌ no       | CDC Event Plane concern |
/// | PutUser / DeactivateUser                | ❌ no       | authz checked at exec time |
/// | PutRole / DeleteRole                    | ❌ no       | same |
/// | PutApiKey / RevokeApiKey                | ❌ no       | same |
/// | PutMaterializedView / DeleteMaterializedView | ❌ no  | MV definition is its own catalog object; write-path `materialized_sum_sources` is set at collection-register time via PutCollection, not updated by PutMaterializedView independently |
/// | PutTenant / DeleteTenant                | ❌ no       | tenant identity does not affect plan shape |
/// | PutRlsPolicy / DeleteRlsPolicy          | ❌ no       | `execute_sql` is only called from CDC path (no RLS injection via `inject_rls`); per-session pgwire cache has its own DDL invalidation |
/// | PutPermission / DeletePermission        | ❌ no       | permission checked at exec time |
/// | PutOwner / DeleteOwner                  | ❌ no       | ownership does not affect plan shape |
pub(crate) fn invalidate_gateway_cache_for_entry(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    let Some(ref inv) = shared.gateway_invalidator else {
        return;
    };
    match entry {
        // ── Collection mutations that change the plan shape ──────────────────
        CatalogEntry::PutCollection(stored) => {
            inv.invalidate(&stored.name, stored.descriptor_version.max(1));
        }
        CatalogEntry::DeactivateCollection { name, .. } => {
            // Treat deactivation as version 0 (collection gone — any cached
            // plan for it is stale).
            inv.invalidate(name, 0);
        }

        // ── Sequence: resolved at handler level, not baked into PhysicalPlan ─
        CatalogEntry::PutSequence(_) => {
            // no-op: sequences resolved in pgwire transaction_cmds.rs before
            // planning; StoredSequence never appears in a PhysicalPlan variant.
        }
        CatalogEntry::DeleteSequence { .. } => {
            // no-op: same reason as PutSequence.
        }
        CatalogEntry::PutSequenceState(_) => {
            // no-op: runtime counter state — the planner never reads seq state.
        }

        // ── Trigger: dispatched by Event Plane post-execution ────────────────
        CatalogEntry::PutTrigger(_) => {
            // no-op: triggers are AFTER-fire; no trigger field exists in any
            // PhysicalPlan variant; Event Plane reads the trigger registry
            // directly at fire time.
        }
        CatalogEntry::DeleteTrigger { .. } => {
            // no-op: same as PutTrigger.
        }

        // ── Function / Procedure: looked up at eval time, not inlined ────────
        CatalogEntry::PutFunction(_) => {
            // no-op: UDFs looked up in function_registry at eval time via
            // `wasm/` executor; never inlined into a PhysicalPlan.
        }
        CatalogEntry::DeleteFunction { .. } => {
            // no-op: same as PutFunction.
        }
        CatalogEntry::PutProcedure(_) => {
            // no-op: stored procedures parsed and executed at CALL time via
            // `procedural/executor`; body not baked into any PhysicalPlan.
        }
        CatalogEntry::DeleteProcedure { .. } => {
            // no-op: same as PutProcedure.
        }

        // ── Schedule: cron runs independently of the plan cache ──────────────
        CatalogEntry::PutSchedule(_) => {
            // no-op: ScheduleRegistry drives the scheduler loop; no plan shape
            // changes result from a new/updated schedule definition.
        }
        CatalogEntry::DeleteSchedule { .. } => {
            // no-op: same as PutSchedule.
        }

        // ── Change stream: CDC Event Plane concern ────────────────────────────
        CatalogEntry::PutChangeStream(_) => {
            // no-op: CDC stream definitions route WriteEvents in the Event
            // Plane; they do not alter how a collection's plan is constructed.
        }
        CatalogEntry::DeleteChangeStream { .. } => {
            // no-op: same as PutChangeStream.
        }

        // ── User / Role / ApiKey: authz checked at exec, not baked into plan ─
        CatalogEntry::PutUser(_) => {
            // no-op: user identity checked in credential store at exec time.
        }
        CatalogEntry::DeactivateUser { .. } => {
            // no-op: same as PutUser.
        }
        CatalogEntry::PutRole(_) => {
            // no-op: role membership checked at exec time via RoleStore.
        }
        CatalogEntry::DeleteRole { .. } => {
            // no-op: same as PutRole.
        }
        CatalogEntry::PutApiKey(_) => {
            // no-op: API key checked at connection/exec time via ApiKeyStore.
        }
        CatalogEntry::RevokeApiKey { .. } => {
            // no-op: same as PutApiKey.
        }

        // ── Materialized view: MV definition is a separate catalog object ────
        CatalogEntry::PutMaterializedView(_) => {
            // no-op: MaterializedView metadata is its own catalog object and
            // does not directly modify any PhysicalPlan. The `materialized_sum_sources`
            // field in DocumentOp::Register is set at collection-register time
            // (driven by PutCollection), not updated independently by
            // PutMaterializedView. Any schema change that would affect plans
            // cascades through PutCollection instead.
        }
        CatalogEntry::DeleteMaterializedView { .. } => {
            // no-op: same as PutMaterializedView.
        }

        // ── Tenant: identity does not affect plan shape ───────────────────────
        CatalogEntry::PutTenant(_) => {
            // no-op: tenant identity used for quota enforcement at exec time.
        }
        CatalogEntry::DeleteTenant { .. } => {
            // no-op: same as PutTenant.
        }

        // ── RLS policy: execute_sql callers (CDC) do not inject RLS ──────────
        CatalogEntry::PutRlsPolicy(_) => {
            // no-op: the gateway execute_sql path (CDC consume_remote) calls
            // plan_sql without RLS injection; per-session pgwire plan cache
            // has its own DDL-aware invalidation that handles RLS changes.
        }
        CatalogEntry::DeleteRlsPolicy { .. } => {
            // no-op: same as PutRlsPolicy.
        }

        // ── Permission / Owner: not baked into plan ───────────────────────────
        CatalogEntry::PutPermission(_) => {
            // no-op: permission grants checked at exec time via PermissionStore.
        }
        CatalogEntry::DeletePermission { .. } => {
            // no-op: same as PutPermission.
        }
        CatalogEntry::PutOwner(_) => {
            // no-op: ownership does not influence plan structure.
        }
        CatalogEntry::DeleteOwner { .. } => {
            // no-op: same as PutOwner.
        }
    }
}
