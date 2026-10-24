// SPDX-License-Identifier: BUSL-1.1

//! Gateway plan-cache invalidation for DDL descriptor mutations.
//!
//! The gateway plan cache keys on `(sql_hash, ph_hash, GatewayVersionSet)`.
//! A `GatewayVersionSet` lists `(collection_name, descriptor_version)` pairs
//! extracted from the `PhysicalPlan` by `touched_collections`. A DDL entry
//! requires invalidation only if it changes the observable plan shape for
//! an already-cached plan.

use std::sync::Arc;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::state::SharedState;

/// Notify the gateway plan-cache invalidator after a DDL descriptor mutation.
///
/// Extracts the descriptor name and new version from the entry and calls
/// `PlanCacheInvalidator::invalidate`. This is best-effort: if the gateway
/// has not been constructed yet (`gateway_invalidator == None`) the call is
/// a no-op.
///
/// ## Invalidation decision table (exhaustive, no `_ => {}`)
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
/// | PutUser / DropUser                      | ❌ no       | authz checked at exec time |
/// | PutRole / DeleteRole                    | ❌ no       | same |
/// | PutApiKey / RevokeApiKey                | ❌ no       | same |
/// | PutAuthUser                             | ❌ no       | account status re-read at admission time |
/// | PutMaterializedView / DeleteMaterializedView | ❌ no  | MV definition is its own catalog object; write-path `materialized_sum_sources` is set at collection-register time via PutCollection, not updated by PutMaterializedView independently |
/// | PutContinuousAggregate / DeleteContinuousAggregate | ✅ yes | a read of the aggregate as a relation carries its version under a pseudo-entry |
/// | PutTenant / DeleteTenant                | ❌ no       | tenant identity does not affect plan shape |
/// | PutRlsPolicy / DeleteRlsPolicy          | ❌ no       | `execute_sql` is only called from CDC path (no RLS injection via `inject_rls`); per-session pgwire cache has its own DDL invalidation |
/// | PutRedactionPolicy / DeleteRedactionPolicy | ❌ no    | redaction rules are applied post-scan on the decoded document by role, so they are not baked into `PhysicalPlan` shape; the fail-closed refusal is re-evaluated against the live store on every execution, so no cached plan goes stale either |
/// | PutPermission / DeletePermission        | ❌ no       | permission checked at exec time |
/// | PutScopeGrant / DeleteScopeGrant        | ❌ no       | scope enrichment resolves grants per request against the live store; no scope field in any PhysicalPlan variant |
/// | PutOwner / DeleteOwner                  | ❌ no       | ownership does not affect plan shape |
/// | PutRetentionPolicy / DeleteRetentionPolicy | ✅ yes   | `auto_tier` rewrites a timeseries scan onto tier aggregates, so the policy is baked into the plan |
/// | PutAlertRule / DeleteAlertRule          | ❌ no       | alert rules drive their own eval loop and never enter a PhysicalPlan |
/// | Topic / consumer-group variants         | ❌ no       | Event Plane delivery identities that never enter a PhysicalPlan |
/// | PutArray / DeleteArray                  | ✅ yes      | array schema and routing prefix are baked into the plan |
/// | PutSpatialRefSys                        | ❌ no       | a new code is one no cached plan resolved: planning it failed |
/// | DropSpatialRefSys                       | ✅ yes, all | any cached plan can carry the dropped definition, and no version set names a CRS |
/// | PutCredential / DropCredential          | ❌ no       | a COPY reads the credential row when it runs; no plan carries it |
pub(crate) fn invalidate_gateway_cache_for_entry(entry: &CatalogEntry, shared: &Arc<SharedState>) {
    let Some(inv) = shared.gateway_invalidator.get() else {
        return;
    };
    match entry {
        // ── Collection mutations that change the plan shape ──────────────────
        CatalogEntry::PutCollection(stored) => {
            inv.invalidate(&stored.name, stored.descriptor_version.max(1));
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            inv.invalidate(&stored.name, stored.descriptor_version.max(1));
        }
        CatalogEntry::DeactivateCollection { name, .. } => {
            // Treat deactivation as version 0 (collection gone — any cached
            // plan for it is stale).
            inv.invalidate(name, 0);
        }
        CatalogEntry::PurgeCollection { name, .. } => {
            // Hard delete: same invalidation semantic as deactivate —
            // any cached plan for this name is stale.
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
        CatalogEntry::DropUser { .. } => {
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
        CatalogEntry::PutAuthUser(_) => {
            // no-op: account status is re-read from the auth-user store on
            // every request by the admission gate, never baked into a plan.
        }

        // ── Materialized view: MV definition is a separate catalog object ────
        CatalogEntry::PutMaterializedView(_) => {
            // no-op: MaterializedView metadata is its own catalog object and
            // does not directly modify any PhysicalPlan. The `materialized_sum_sources`
            // field in DocumentOp::Register is set at collection-register time
            // (driven by PutCollection), not updated independently by
            // PutMaterializedView. Any schema change that affects plans
            // cascades through PutCollection instead.
        }
        CatalogEntry::DeleteMaterializedView { .. } => {
            // no-op: same as PutMaterializedView.
        }
        CatalogEntry::PutStreamingMaterializedView(_)
        | CatalogEntry::DeleteStreamingMaterializedView { .. } => {
            // no-op: streaming MV definitions are consumed by the Event Plane
            // and never alter a PhysicalPlan's shape.
        }

        // ── Continuous aggregate: read as a relation through a provider scan ─
        // A plan that reads the aggregate carries its version under a
        // pseudo-entry, so a change evicts the plans keyed on the old one.
        CatalogEntry::PutContinuousAggregate(stored) => {
            inv.invalidate(
                &crate::control::gateway::version_set::continuous_aggregate_version_key(
                    stored.database_id,
                    stored.tenant_id,
                    &stored.name,
                ),
                stored.modification_hlc.wall_ns.max(1),
            );
        }
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
            ..
        } => {
            inv.invalidate(
                &crate::control::gateway::version_set::continuous_aggregate_version_key(
                    *database_id,
                    *tenant_id,
                    name,
                ),
                0,
            );
        }

        // ── Tenant: identity does not affect plan shape ───────────────────────
        CatalogEntry::PutTenant(_) | CatalogEntry::PutTenantWithAdmin { .. } => {
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

        // ── Redaction policy: applied post-scan, not baked into plan shape ───
        CatalogEntry::PutRedactionPolicy(_) => {
            // no-op: redaction rules are applied post-scan on the decoded
            // document by role, so they are not baked into `PhysicalPlan`
            // shape and need no gateway cache invalidation. The fail-closed
            // refusal for the shapes masking cannot cover is likewise
            // re-evaluated against the live store on every execution, cached
            // plan or not, so no plan cache goes stale on a policy write.
        }
        CatalogEntry::DeleteRedactionPolicy { .. } => {
            // no-op: same as PutRedactionPolicy.
        }

        // ── Permission / Owner: not baked into plan ───────────────────────────
        CatalogEntry::PutPermission(_) => {
            // no-op: permission grants checked at exec time via PermissionStore.
        }
        CatalogEntry::DeletePermission { .. } => {
            // no-op: same as PutPermission.
        }
        CatalogEntry::PutScopeGrant(_) => {
            // no-op: scope grants are resolved per request by scope
            // enrichment against the live store, so they are not baked into
            // `PhysicalPlan` shape and no cached plan goes stale on a write.
        }
        CatalogEntry::DeleteScopeGrant { .. } => {
            // no-op: same as PutScopeGrant.
        }
        // ── Index registry: index availability changes plan shape ────────────
        CatalogEntry::PutIndexRecord(record) => {
            // A newly registered index makes IndexLookup / vector-search
            // rewrites reachable for this collection; cached scans predate it.
            inv.invalidate(&record.collection, 0);
        }
        CatalogEntry::DeleteIndexRecord { collection, .. } => {
            // A cached plan still holding an IndexLookup against the dropped
            // index reads an index the engine no longer has.
            inv.invalidate(collection, 0);
        }

        CatalogEntry::PutOwner(_) => {
            // no-op: ownership does not influence plan structure.
        }
        CatalogEntry::DeleteOwner { .. } => {
            // no-op: same as PutOwner.
        }

        // ── Synonym group: registry-only change, no plan shape effect ─────────
        CatalogEntry::PutSynonymGroup(_) => {
            // no-op: synonym expansion happens at query time via the registry;
            // it does not alter the plan structure cached in the gateway.
        }
        CatalogEntry::DeleteSynonymGroup { .. } => {
            // no-op: same as PutSynonymGroup.
        }

        // ── Custom type: registry-only change, no plan shape effect ───────────
        CatalogEntry::PutCustomType(_) => {
            // no-op: type resolution happens at query time via the registry.
        }
        CatalogEntry::DeleteCustomType { .. } => {
            // no-op: same as PutCustomType.
        }

        // ── Database: descriptor and grants do not affect plan shape ──────────
        CatalogEntry::PutDatabase(_) => {
            // no-op: database descriptors are resolved at session bind, not
            // baked into cached plans.
        }
        CatalogEntry::DeleteDatabase { .. } => {
            // no-op: same as PutDatabase.
        }
        CatalogEntry::PutDatabaseGrant { .. } => {
            // no-op: database grants are checked at session bind, not in plans.
        }
        CatalogEntry::DeleteDatabaseGrant { .. } => {
            // no-op: same as PutDatabaseGrant.
        }
        CatalogEntry::PutOidcProvider(_) => {
            // no-op: OIDC providers are auth-layer concerns; they do not
            // affect the gateway plan cache shape.
        }
        CatalogEntry::DeleteOidcProvider { .. } => {
            // no-op: same as PutOidcProvider.
        }
        CatalogEntry::CloneDatabase { .. } => {
            // no-op: the new database has no cached plans yet; the source
            // database's plans are unaffected by the clone operation.
        }
        CatalogEntry::RecordWalTombstone { .. } => {
            // WAL replay barrier only; no plan shape is affected.
        }
        CatalogEntry::PutDatabaseQuota { .. } | CatalogEntry::DeleteDatabaseQuota { .. } => {
            // no-op: quotas gate admission and memory at exec time; they do
            // not change plan shape.
        }
        CatalogEntry::PutTenantQuota { .. } | CatalogEntry::DeleteTenantQuota { .. } => {
            // no-op: same as the database-scoped quota variants.
        }
        CatalogEntry::PutScopeQuota(_) | CatalogEntry::DeleteScopeQuota { .. } => {
            // no-op: token quotas gate dispatch admission, not plan shape.
        }
        CatalogEntry::PutRetentionPolicy(def) => {
            // `auto_tier` rewrites a scan onto tier aggregates, so the policy
            // is baked into the plan. Version 0 evicts every cached plan.
            inv.invalidate(&def.collection, 0);
        }
        CatalogEntry::DeleteRetentionPolicy { collection, .. } => {
            // Dropping the policy removes any tier rewrite from the plan.
            inv.invalidate(collection, 0);
        }
        CatalogEntry::PutAlertRule(_) | CatalogEntry::DeleteAlertRule { .. } => {
            // no-op: alert rules run their own eval loop and never enter a
            // query plan.
        }
        CatalogEntry::CreateTopicIfAbsent(_)
        | CatalogEntry::DeleteTopicWithConsumerGroups { .. }
        | CatalogEntry::PutConsumerGroupIfAbsent(_)
        | CatalogEntry::DeleteConsumerGroup { .. }
        | CatalogEntry::MigrateConsumerGroupStream { .. }
        | CatalogEntry::CommitConsumerOffsets(_) => {
            // no-op: topics and consumer groups are Event Plane delivery
            // identities and never enter a query plan.
        }
        CatalogEntry::PutBackupScheduleMark(_) => {
            // no-op: a backup schedule mark never enters a query plan.
        }
        CatalogEntry::PutSpatialRefSys(_) => {
            // no-op: no cached plan resolved a code before it was defined.
        }
        CatalogEntry::PutCredential(_) | CatalogEntry::DropCredential { .. } => {
            // no-op: a COPY reads the credential row when it runs.
        }
        CatalogEntry::DropSpatialRefSys { .. } => {
            // A plan resolves a CRS to its definition at planning time, so
            // a cached plan can still transform with the dropped one.
            inv.invalidate_all();
        }
        CatalogEntry::PutCheckpoint(_)
        | CatalogEntry::DeleteCheckpoint { .. }
        | CatalogEntry::CompactHistory { .. } => {
            // no-op: a checkpoint names a version vector and never enters a
            // query plan.
        }
        CatalogEntry::PutVectorModel(_)
        | CatalogEntry::DeleteVectorModel { .. }
        | CatalogEntry::PutVectorIndexParams(_)
        | CatalogEntry::DeleteVectorIndexParams { .. } => {
            // no-op: vector build parameters are read by the Data Plane index,
            // never by a cached plan.
        }
        CatalogEntry::PutCloneCopyup { .. }
        | CatalogEntry::PutCloneTombstone { .. }
        | CatalogEntry::PutKvCloneTombstone { .. } => {
            // no-op: a clone read consults these rows at execution, never
            // through a cached plan.
        }
        CatalogEntry::PutCloneSourceDrain(_) | CatalogEntry::DeleteCloneSourceDrain { .. } => {
            // no-op: drain claims reach no plan.
        }
        CatalogEntry::PutArray(stored) => {
            // An array plan carries its schema and routing prefix. Version 0
            // evicts every cached plan over the name.
            inv.invalidate(&stored.name, 0);
        }
        CatalogEntry::DeleteArray { name, .. } => {
            inv.invalidate(name, 0);
        }
        CatalogEntry::PutColumnStats(rows) => {
            // The join and aggregate cost models read these rows, so a cached
            // plan carries the previous figures. Version 0 evicts every plan
            // over the collection.
            if let Some(first) = rows.first() {
                inv.invalidate(&first.collection, 0);
            }
        }
        CatalogEntry::MoveTenantCutover { collections, .. } => {
            // Invalidate cached plans for each collection that moved databases.
            // This forces re-planning on the next query touching those collections.
            for coll in collections.iter() {
                inv.invalidate(&coll.name, coll.descriptor_version.max(1));
            }
        }
    }
}

/// Matchstick tests for `invalidate_gateway_cache_for_entry`.
///
/// The primary correctness guarantee is **compile-time exhaustiveness**: the
/// match in `post_apply::invalidate_gateway_cache_for_entry` has no `_ => {}`
/// catch-all, so adding a new `CatalogEntry` variant without handling it is a
/// compile error. These tests verify the **runtime behavior** — that the two
/// collection-level variants cause cache eviction and every other variant is a
/// no-op.
///
/// # Coverage strategy
///
/// Every variant is exercised either directly (using its concrete type) or via
/// the Delete/* variants (which share a `{ tenant_id, name }` shape and are
/// the simplest to construct without dependencies on complex nested types).
/// Complex `Put*` variants that wrap a Box<Stored*> with many required fields
/// are exercised by their corresponding `Delete*` counterpart — the match arm
/// for the Put variant is structurally identical (`// no-op`) and the compiler
/// guarantees both arms are present.
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::gateway::plan_cache::{PlanCache, PlanCacheKey, hash_sql};
    use crate::control::gateway::version_set::GatewayVersionSet;
    use crate::control::gateway::{Gateway, PlanCacheInvalidator};
    use crate::control::security::catalog::StoredCollection;
    use crate::wal::WalManager;

    /// Build a minimal SharedState with a gateway plan cache + invalidator installed.
    ///
    /// The SharedState owns the plan cache via `gateway`, and `gateway_invalidator`
    /// points to a weak-ref invalidator backed by the same cache. This mirrors
    /// the production wiring in `main.rs`.
    ///
    /// Returns state, plan cache, and the backing `TempDir` guard — the caller
    /// must keep the guard alive for as long as `shared` is in use.
    fn make_test_state() -> (Arc<SharedState>, Arc<PlanCache>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal_path = dir.path().join("test.wal");

        let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).unwrap();

        // Wire a real Gateway + PlanCacheInvalidator (mirrors main.rs).
        //
        // We use Arc::get_mut — valid here because SharedState::new returns a
        // fresh Arc with refcount=1 and we have not cloned it yet. The clone for
        // Gateway::new is made before the get_mut call; that makes the refcount 2,
        // so we need the raw-pointer write path instead.
        let shared_for_gw = Arc::clone(&shared);
        let gateway = Arc::new(Gateway::new(shared_for_gw));
        let plan_cache = Arc::clone(&gateway.plan_cache);
        let invalidator = Arc::new(PlanCacheInvalidator::new(&gateway.plan_cache));
        // SAFETY: `make_test_state` is single-threaded setup; no concurrent reads
        // of `gateway` / `gateway_invalidator` exist at this point. Fields start
        // as `None` and are written exactly once here.
        unsafe {
            let state = Arc::as_ptr(&shared) as *mut SharedState;
            let _ = (*state).gateway.set(gateway);
            let _ = (*state).gateway_invalidator.set(invalidator);
        }

        (shared, plan_cache, dir)
    }

    /// Insert a sentinel plan entry for collection `col` at version 1.
    fn plant_sentinel(cache: &PlanCache, col: &str) -> PlanCacheKey {
        use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
        let key = PlanCacheKey {
            sql_text_hash: hash_sql(&format!("SELECT * FROM {col}")),
            placeholder_types_hash: 0,
            version_set: GatewayVersionSet::from_pairs(vec![(col.into(), 1)]),
        };
        let plan = Arc::new(PhysicalPlan::Kv(KvOp::Get {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                col,
            ),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
        }));
        cache.insert(key.clone(), plan);
        key
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // PutCollection — must evict entries for the changed collection
    // ─────────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn put_collection_evicts_stale_plan_entries() {
        let (shared, cache, _dir) = make_test_state();
        let key = plant_sentinel(&cache, "orders");
        assert_eq!(cache.len(), 1);

        // PutCollection with a bumped descriptor_version.
        let mut col = StoredCollection::new(1, "orders", "alice");
        col.descriptor_version = 2;
        let entry = CatalogEntry::PutCollection(Box::new(col));

        invalidate_gateway_cache_for_entry(&entry, &shared);

        // Sentinel entry at version=1 must be evicted.
        assert_eq!(cache.len(), 0, "put_collection must evict stale entries");
        assert!(cache.get(&key).is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // DeactivateCollection — treats collection as gone (version 0)
    // ─────────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn deactivate_collection_evicts_plan_entries() {
        let (shared, cache, _dir) = make_test_state();
        let key = plant_sentinel(&cache, "products");
        assert_eq!(cache.len(), 1);

        let entry = CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 1,
            name: "products".into(),
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };

        invalidate_gateway_cache_for_entry(&entry, &shared);

        assert_eq!(cache.len(), 0, "deactivate_collection must evict entries");
        assert!(cache.get(&key).is_none());
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // All other variants — must be no-ops (cache unchanged)
    // ─────────────────────────────────────────────────────────────────────────────
    //
    // We test each Delete* variant directly (simple { tenant_id, name } shape) and
    // rely on the compiler's exhaustiveness check for the corresponding Put* arm.
    // The Put* variants for complex nested types (StoredTrigger, StoredFunction,
    // etc.) are covered by the same `// no-op` arm; constructing them
    // requires pages of boilerplate without adding behavioral coverage.

    fn assert_noop(
        shared: &Arc<SharedState>,
        cache: &Arc<PlanCache>,
        entry: CatalogEntry,
        label: &str,
    ) {
        // Plant a sentinel for "sentinel_col" and assert it survives.
        let key = plant_sentinel(cache, "sentinel_col");
        let size_before = cache.len();

        invalidate_gateway_cache_for_entry(&entry, shared);

        assert_eq!(cache.len(), size_before, "{label}: cache must not change");
        assert!(
            cache.get(&key).is_some(),
            "{label}: sentinel entry must survive"
        );
        // Remove sentinel to keep cache clean for next assertion.
        cache.invalidate_descriptor("sentinel_col", 0);
    }

    #[tokio::test]
    async fn no_op_variants_do_not_evict_plan_cache() {
        use crate::control::security::catalog::sequence_types::StoredSequence;

        let (shared, cache, _dir) = make_test_state();

        // DeleteSequence
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteSequence {
                database_id: 0,
                tenant_id: 1,
                name: "seq".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteSequence",
        );

        // PutSequence (using StoredSequence::new for minimal construction)
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::PutSequence(Box::new(StoredSequence::new(
                0,
                1,
                "seq2".into(),
                "alice".into(),
            ))),
            "PutSequence",
        );

        // PutSequenceState is tested via the sequence state type which has simple fields.
        // We skip direct construction here (requires epoch + period_key) — the compiler
        // guarantees the arm exists via exhaustiveness.

        // DeleteTrigger
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteTrigger {
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: 1,
                name: "trig".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteTrigger",
        );

        // DeleteFunction
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteFunction {
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: 1,
                name: "fn_".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteFunction",
        );

        // DeleteProcedure
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteProcedure {
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: 1,
                name: "proc".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteProcedure",
        );

        // DeleteSchedule
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteSchedule {
                database_id: crate::types::DatabaseId::DEFAULT,
                tenant_id: 1,
                name: "sched".into(),
            },
            "DeleteSchedule",
        );

        // DeleteChangeStream
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteChangeStream {
                database_id: crate::types::DatabaseId::DEFAULT.as_u64(),
                tenant_id: 1,
                name: "stream".into(),
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteChangeStream",
        );

        // DropUser
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DropUser {
                username: "bob".into(),
            },
            "DropUser",
        );

        // DeleteRole
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteRole {
                name: "analyst".into(),
            },
            "DeleteRole",
        );

        // RevokeApiKey
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::RevokeApiKey {
                key_id: "key_abc".into(),
            },
            "RevokeApiKey",
        );

        // DeleteMaterializedView
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteMaterializedView {
                database_id: 2,
                tenant_id: 1,
                name: "mv_orders".into(),
                target_descriptor_version: 0,
                target_hlc: nodedb_types::Hlc::ZERO,
            },
            "DeleteMaterializedView",
        );

        // DeleteTenant
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteTenant { tenant_id: 42 },
            "DeleteTenant",
        );

        // DeleteRlsPolicy
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteRlsPolicy {
                tenant_id: 1,
                collection: "orders".into(),
                name: "tenant_isolation".into(),
            },
            "DeleteRlsPolicy",
        );

        // DeletePermission
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeletePermission {
                target: "collection:1:orders".into(),
                grantee: "user:bob".into(),
                permission: "read".into(),
            },
            "DeletePermission",
        );

        // DeleteScopeGrant
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteScopeGrant {
                scope_name: "pro:all".into(),
                grantee_type: "org".into(),
                grantee_id: "acme".into(),
            },
            "DeleteScopeGrant",
        );

        // DeleteOwner
        assert_noop(
            &shared,
            &cache,
            CatalogEntry::DeleteOwner {
                object_type: "collection".into(),
                database_id: 0,
                tenant_id: 1,
                object_name: "orders".into(),
            },
            "DeleteOwner",
        );
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Retention policy — `auto_tier` rewrites the scan, so plans go stale
    // ─────────────────────────────────────────────────────────────────────────────

    fn retention_policy(
        collection: &str,
    ) -> crate::engine::timeseries::retention_policy::RetentionPolicyDef {
        crate::engine::timeseries::retention_policy::RetentionPolicyDef {
            database_id: 0,
            tenant_id: 1,
            name: "ret_pol".into(),
            collection: collection.into(),
            tiers: Vec::new(),
            auto_tier: true,
            enabled: true,
            eval_interval_ms: 3_600_000,
            owner: "alice".into(),
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn put_retention_policy_evicts_plans_for_its_collection() {
        let (shared, cache, _dir) = make_test_state();
        let key = plant_sentinel(&cache, "metrics");
        let other = plant_sentinel(&cache, "orders");

        invalidate_gateway_cache_for_entry(
            &CatalogEntry::PutRetentionPolicy(Box::new(retention_policy("metrics"))),
            &shared,
        );

        assert!(
            cache.get(&key).is_none(),
            "the policy's collection is evicted"
        );
        assert!(cache.get(&other).is_some(), "unrelated plans survive");
    }

    #[tokio::test]
    async fn delete_retention_policy_evicts_plans_for_its_collection() {
        let (shared, cache, _dir) = make_test_state();
        let key = plant_sentinel(&cache, "metrics");
        let other = plant_sentinel(&cache, "orders");

        invalidate_gateway_cache_for_entry(
            &CatalogEntry::DeleteRetentionPolicy {
                database_id: 0,
                tenant_id: 1,
                name: "ret_pol".into(),
                collection: "metrics".into(),
            },
            &shared,
        );

        assert!(
            cache.get(&key).is_none(),
            "the policy's collection is evicted"
        );
        assert!(cache.get(&other).is_some(), "unrelated plans survive");
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Verify that when gateway_invalidator is None, the function is a pure no-op
    // ─────────────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn no_gateway_invalidator_is_safe_noop() {
        // Build SharedState WITHOUT wiring the gateway_invalidator.
        // The WAL lives under the guard's directory rather than a fixed path, so
        // it is removed with the guard and two concurrent runs cannot collide on
        // the same file.
        let dir = tempfile::tempdir().expect("tmpdir");
        let wal_path = dir.path().join("test.wal");
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).unwrap();
        // gateway_invalidator is None by default.

        let entry = CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "x", "alice")));

        // Must not panic.
        invalidate_gateway_cache_for_entry(&entry, &shared);
    }
}
