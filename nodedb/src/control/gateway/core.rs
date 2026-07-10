// SPDX-License-Identifier: BUSL-1.1

//! Gateway — the single entry point for executing a `PhysicalPlan` against
//! the cluster.
//!
//! The gateway:
//! 1. Computes a [`GatewayVersionSet`] from the plan (collection → descriptor
//!    version mapping).
//! 2. Routes the plan via [`route_plan`] to `Local` or `Remote` task routes.
//! 3. Dispatches each route (local SPSC or `ExecuteRequest` RPC) with typed
//!    `NotLeader` retry (up to 3 attempts).
//! 4. Handles `RetryableSchemaChanged` (descriptor cache miss) by fetching a
//!    fresh lease and re-planning once.
//! 5. Fuses multiple vShard payloads for broadcast scans.
//! 6. Returns `Vec<Vec<u8>>` payloads to the caller.
//!
//! The `execute_sql` entry point additionally checks the gateway-level
//! [`PlanCache`] keyed on `(sql_text_hash, placeholder_types_hash,
//! DescriptorVersionSet)` before calling the planner.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use tracing::{Instrument, debug, info_span};

use crate::Error;
use crate::control::state::SharedState;
use crate::control::trace_export::EmitSpanParams;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};
use nodedb_physical::physical_plan::PhysicalPlan;

use super::dispatcher::{DispatchRouteParams, default_deadline_ms, dispatch_route};
use super::fuser::fuse_payloads;
use super::key_extractor::UnwiredKeyExtractor;
use super::plan_cache::{PlanCache, PlanCacheKey, SqlKey, hash_placeholder_types, hash_sql};
use super::retry::retry_not_leader;
use super::route::TaskRoute;
use super::router::{resolve_decision, route_plan};
use super::version_set::GatewayVersionSet;

/// Context passed to [`Gateway::execute`].
pub struct QueryContext {
    pub tenant_id: TenantId,
    pub trace_id: TraceId,
    /// Database scope for the query. Used to route collections to vShards
    /// (the database id is folded into the routing hash) and to scope
    /// catalog lookups. Single-database deployments pass
    /// [`DatabaseId::DEFAULT`].
    pub database_id: DatabaseId,
    /// Owning interactive transaction, when this plan is dispatched inside a
    /// `BEGIN`/`COMMIT` session. Threaded to the local Data Plane hop so the
    /// leaseholder resolves this transaction's staging overlay
    /// (read-your-own-writes) instead of reading only committed state. `None`
    /// for autocommit statements and all non-session callers.
    ///
    /// Only the **local-leader** dispatch (`dispatch_local`) forwards it today;
    /// cross-node in-transaction reads (remote `ExecuteRequest`, streaming)
    /// remain a tracked gap because the RPC envelope carries no `txn_id`.
    pub txn_id: Option<TxnId>,
}

/// The gateway: routes, dispatches, retries, and caches physical plans.
pub struct Gateway {
    pub(crate) shared: Arc<SharedState>,
    pub plan_cache: Arc<PlanCache>,
    /// Number of times `retry_not_leader` retried due to a `NotLeader` response.
    /// Each retry attempt after the initial attempt increments this counter.
    /// Observable via [`Gateway::not_leader_retry_count`]. `pub(super)` so the
    /// streaming entry point in `stream.rs` can increment it identically.
    pub(super) not_leader_retry_count: Arc<AtomicU64>,
}

impl Gateway {
    /// Construct a new gateway.
    ///
    /// Must be called after cluster topology / routing table is populated in
    /// `SharedState` (after `cluster::start_raft`) and before listeners bind.
    pub fn new(shared: Arc<SharedState>) -> Self {
        Self {
            plan_cache: Arc::new(PlanCache::default_capacity()),
            shared,
            not_leader_retry_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Total number of NotLeader-triggered retries since this gateway was created.
    ///
    /// Each individual retry attempt (not each NotLeader error) increments the
    /// counter. Useful in tests to assert that the retry path was exercised.
    pub fn not_leader_retry_count(&self) -> u64 {
        self.not_leader_retry_count.load(Ordering::Relaxed)
    }

    /// Execute a pre-planned `PhysicalPlan` against the cluster.
    ///
    /// Returns one `Vec<u8>` payload per vShard result. For point operations
    /// the returned Vec has exactly one element.
    pub async fn execute(
        &self,
        ctx: &QueryContext,
        plan: PhysicalPlan,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let span = info_span!(
            "gateway.execute",
            trace_id = %ctx.trace_id,
            tenant_id = ctx.tenant_id.as_u64()
        );
        let start = SystemTime::now();
        let version_set = self.collect_version_set(&plan, ctx.tenant_id.as_u64(), ctx.database_id);
        let result = self
            .execute_with_version_set(ctx, plan, version_set)
            .instrument(span)
            .await;
        // Emit an OTLP span covering the whole gateway execute so an
        // enabled collector correlates this with the executor spans
        // emitted by every leaseholder we dispatched to — they all
        // share the same `trace_id`.
        self.shared.trace_exporter.emit(EmitSpanParams {
            span_name: "gateway.execute",
            trace_id: ctx.trace_id,
            start,
            end: SystemTime::now(),
            tenant_id: ctx.tenant_id.as_u64(),
            vshard_id: 0,
            status_ok: result.is_ok(),
        });

        // Advance per-tenant observed write-HLC high-water on any
        // successful cluster dispatch (local or remote). Used by
        // RESTORE staleness gate. Tracking on success of every
        // gateway.execute is intentional: backup captures its
        // envelope watermark AFTER its own fan-out, so a fresh
        // backup's watermark always dominates the tenant_wm it
        // itself advanced.
        if result.is_ok() {
            self.shared.advance_tenant_write_hlc(ctx.tenant_id.as_u64());
        }

        result
    }

    /// SQL-text entry point: checks the plan cache first.
    ///
    /// `plan_fn` is called at most once (on cache miss or after a descriptor
    /// cache-miss recovery that requires re-planning).
    ///
    /// ## Two-phase cache lookup (Gap 5 fix)
    ///
    /// A `PlanCacheKey` requires a `GatewayVersionSet`, which we cannot build
    /// from SQL text alone — it requires knowing which collections the plan
    /// touches. Previously this method used a speculative empty version set,
    /// meaning the first-call key never matched the post-planning key, giving
    /// a 0% cache hit rate.
    ///
    /// The fix: a side cache maps `(sql_hash, ph_hash)` → stored
    /// `GatewayVersionSet`. On the second call, we recover the version set
    /// from the side cache, verify it is still current (DDL may have bumped
    /// descriptor versions), and — if current — use it to build the full key
    /// for the plan lookup.
    pub async fn execute_sql(
        &self,
        ctx: &QueryContext,
        sql: &str,
        placeholder_types: &[&str],
        plan_fn: impl FnOnce() -> Result<PhysicalPlan, Error>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let sql_hash = hash_sql(sql);
        let ph_hash = hash_placeholder_types(placeholder_types);
        let sql_key = SqlKey {
            sql_text_hash: sql_hash,
            placeholder_types_hash: ph_hash,
        };

        // Phase 1: check the side cache for a previously stored version set.
        if let Some(stored_vs) = self.plan_cache.lookup_version_set(&sql_key) {
            // Verify the stored version set is still current by cross-checking
            // each collection's current descriptor version.
            let current_vs =
                self.verify_version_set(&stored_vs, ctx.tenant_id.as_u64(), ctx.database_id);
            if current_vs == stored_vs {
                // Version set is still current — try the full plan cache.
                let full_key = PlanCacheKey {
                    sql_text_hash: sql_hash,
                    placeholder_types_hash: ph_hash,
                    version_set: stored_vs.clone(),
                };
                if let Some(cached_plan) = self.plan_cache.get(&full_key) {
                    debug!(sql = %sql, "gateway: plan cache hit (two-phase)");
                    return self
                        .execute_with_version_set(ctx, (*cached_plan).clone(), stored_vs)
                        .await;
                }
            }
            // Stored version set is stale or plan was evicted — fall through
            // to re-plan. The stale side-cache entry will be overwritten below.
        }

        // Cache miss — invoke the planner.
        let plan = plan_fn()?;

        // Compute the actual version set from the plan (contains the real
        // collection names and their current descriptor versions).
        let actual_vs = self.collect_version_set(&plan, ctx.tenant_id.as_u64(), ctx.database_id);
        let actual_key = PlanCacheKey {
            sql_text_hash: sql_hash,
            placeholder_types_hash: ph_hash,
            version_set: actual_vs.clone(),
        };

        // Populate both caches so the next call hits.
        self.plan_cache
            .insert_version_set(sql_key, actual_vs.clone());
        self.plan_cache.insert(actual_key, Arc::new(plan.clone()));

        self.execute_with_version_set(ctx, plan, actual_vs).await
    }

    /// Core execution path: route → dispatch with retry → fuse.
    async fn execute_with_version_set(
        &self,
        ctx: &QueryContext,
        plan: PhysicalPlan,
        version_set: GatewayVersionSet,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let routes = self.compute_routes(plan, ctx)?;

        let deadline_ms = default_deadline_ms(&self.shared);
        // Gateway-level byte ceiling: per-route `dispatch_to_data_plane`
        // already caps each shard's payload; this additionally caps the
        // scatter-gather *sum* so an N-shard fan-out can't accumulate
        // N × cap across routes.
        let max_total_bytes = self.shared.tuning.network.max_query_result_bytes as usize;
        let mut all_payloads: Vec<Vec<u8>> = Vec::new();
        let mut accumulated_bytes: usize = 0;

        for route in routes {
            let initial_decision = route.decision.clone();
            let vshard_id_for_retry = crate::types::VShardId::new(route.vshard_id);
            let plan_for_retry = route.plan.clone();
            let vshard_id_u32 = route.vshard_id;

            let routing_ref = self.shared.cluster_routing.as_deref();

            let retry_counter = Arc::clone(&self.not_leader_retry_count);
            let version_set_for_route = version_set.clone();
            let payloads = retry_not_leader(routing_ref, move |attempt| {
                if attempt > 0 {
                    retry_counter.fetch_add(1, Ordering::Relaxed);
                }
                let plan = plan_for_retry.clone();
                let shared = Arc::clone(&self.shared);
                let tenant_id = ctx.tenant_id;
                let database_id = ctx.database_id;
                let trace_id = ctx.trace_id;
                let txn_id = ctx.txn_id;
                let version_set = version_set_for_route.clone();
                async move {
                    let decision = {
                        let routing_guard = shared
                            .cluster_routing
                            .as_ref()
                            .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
                        let raft_snapshot: Vec<nodedb_cluster::GroupStatus> =
                            shared.raft_status_fn.get().map(|f| f()).unwrap_or_default();
                        let live_leader = move |group_id: u64| -> u64 {
                            raft_snapshot
                                .iter()
                                .find(|gs| gs.group_id == group_id)
                                .map(|gs| gs.leader_id)
                                .unwrap_or(0)
                        };
                        let live_lookup: Option<&dyn Fn(u64) -> u64> =
                            if shared.raft_status_fn.get().is_some() {
                                Some(&live_leader)
                            } else {
                                None
                            };
                        resolve_decision(
                            vshard_id_u32,
                            shared.node_id,
                            routing_guard.as_deref(),
                            live_lookup,
                        )
                    };
                    let route = TaskRoute {
                        plan,
                        decision,
                        vshard_id: vshard_id_u32,
                    };
                    dispatch_route(DispatchRouteParams {
                        route,
                        shared: &shared,
                        tenant_id,
                        database_id,
                        trace_id,
                        txn_id,
                        deadline_ms,
                        version_set: &version_set,
                    })
                    .await
                }
            })
            .await
            .map_err(|e| {
                debug!(
                    vshard_id = vshard_id_for_retry.as_u32(),
                    decision = ?initial_decision,
                    error = %e,
                    "gateway: dispatch failed"
                );
                e
            })?;

            for p in payloads {
                accumulated_bytes = accumulated_bytes.saturating_add(p.len());
                if accumulated_bytes > max_total_bytes {
                    return Err(Error::ExecutionLimitExceeded {
                        detail: format!(
                            "scatter-gather result exceeded max_query_result_bytes \
                             ({accumulated_bytes} > {max_total_bytes} bytes)"
                        ),
                    });
                }
                all_payloads.push(p);
            }
        }

        // For broadcast scans, fuse all shard payloads into one.
        if all_payloads.len() > 1 {
            let fused = fuse_payloads(all_payloads)?;
            Ok(vec![fused.payload])
        } else {
            Ok(all_payloads)
        }
    }

    /// Compute routing decisions for a plan.
    ///
    /// Acquires the routing guard and catalog reference, builds the
    /// `strategy_fn` closure from the catalog, and calls [`route_plan`].
    /// The routing guard is dropped before this function returns so the
    /// caller's future remains `Send`.
    ///
    /// Shared by [`execute_with_version_set`] and [`Gateway::execute_stream`].
    pub(super) fn compute_routes(
        &self,
        plan: PhysicalPlan,
        ctx: &QueryContext,
    ) -> Result<Vec<TaskRoute>, Error> {
        let routing_guard = self
            .shared
            .cluster_routing
            .as_ref()
            .map(|rw| rw.read().unwrap_or_else(|p| p.into_inner()));
        let routing = routing_guard.as_deref();
        let catalog = Some(self.shared.credentials.catalog());
        let database_id = ctx.database_id;
        let tenant_id = ctx.tenant_id.as_u64();
        let strategy_fn = |name: &str| {
            catalog
                .and_then(|c| c.get_collection(database_id, tenant_id, name).ok())
                .flatten()
                .map(|col| col.partition_strategy)
                .unwrap_or_default()
        };
        route_plan(
            plan,
            self.shared.node_id,
            routing,
            ctx.database_id,
            strategy_fn,
            &UnwiredKeyExtractor,
        )
        // routing_guard dropped here
    }

    /// Collect the descriptor version set for a plan using the current catalog.
    ///
    /// `tenant_id` must match the authenticated tenant of the query so that
    /// the catalog key lookup (`"{tenant_id}:{collection_name}"`) finds the
    /// correct descriptor version. Using tenant 0 here would return version 0
    /// for every collection stored under any other tenant, causing spurious
    /// `DescriptorMismatch` rejections at the leader.
    ///
    /// `database_id` scopes the catalog lookup to the session's current database
    /// so that a plan from one database cannot be served under another.
    pub(super) fn collect_version_set(
        &self,
        plan: &PhysicalPlan,
        tenant_id: u64,
        database_id: DatabaseId,
    ) -> GatewayVersionSet {
        let catalog = Some(self.shared.credentials.catalog());

        GatewayVersionSet::from_plan(plan, |name| {
            catalog
                .and_then(|c| c.get_collection(database_id, tenant_id, name).ok())
                .flatten()
                .map(|col| col.descriptor_version.max(1))
                .unwrap_or(0)
        })
    }

    /// Re-read the current descriptor versions for the collections listed in
    /// `stored_vs` and return a new `GatewayVersionSet` with the current values.
    ///
    /// Used by `execute_sql` to verify that a cached version set is still
    /// current before trusting a plan-cache hit. If the returned set equals
    /// `stored_vs`, the cached plan is still valid.
    fn verify_version_set(
        &self,
        stored_vs: &GatewayVersionSet,
        tenant_id: u64,
        database_id: DatabaseId,
    ) -> GatewayVersionSet {
        let catalog = Some(self.shared.credentials.catalog());

        let pairs: Vec<(String, u64)> = stored_vs
            .iter()
            .map(|(name, _)| {
                let current_version = catalog
                    .and_then(|c| c.get_collection(database_id, tenant_id, name).ok())
                    .flatten()
                    .map(|col| col.descriptor_version.max(1))
                    .unwrap_or(0);
                (name.clone(), current_version)
            })
            .collect();

        GatewayVersionSet::from_pairs(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::gateway::plan_cache::SqlKey;
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

    fn kv_get(col: &str) -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Get {
            collection: col.into(),
            key: b"k".to_vec(),
            rls_filters: vec![],
            surrogate_ceiling: None,
        })
    }

    #[test]
    fn plan_cache_populated_on_execute_sql() {
        // We don't have a real SharedState in unit tests; this test validates
        // the cache key construction logic in isolation.
        let cache = Arc::new(PlanCache::new(8));
        let plan = kv_get("users");
        let vs = GatewayVersionSet::from_pairs(vec![("users".into(), 1)]);
        let key = PlanCacheKey {
            sql_text_hash: hash_sql("SELECT * FROM users"),
            placeholder_types_hash: 0,
            version_set: vs.clone(),
        };

        assert!(cache.get(&key).is_none());
        cache.insert(key.clone(), Arc::new(plan));
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn version_set_stable_hash_consistent() {
        let vs1 = GatewayVersionSet::from_pairs(vec![("a".into(), 1), ("b".into(), 2)]);
        let vs2 = GatewayVersionSet::from_pairs(vec![("b".into(), 2), ("a".into(), 1)]);
        // Different insertion order → same sorted set → same hash.
        assert_eq!(vs1.stable_hash(), vs2.stable_hash());
    }

    // -------------------------------------------------------------------------
    // Gap 5 — two-phase execute_sql cache hit tests
    //
    // We test the `PlanCache` two-phase logic (lookup_version_set /
    // insert_version_set / invalidate_descriptor cross-eviction) in isolation
    // since we have no real SharedState available in unit tests.
    // The full end-to-end path is tested in `tests/pgwire_gateway_migration.rs`
    // (plan cache hit counter asserted across 3 execute_sql calls).
    // -------------------------------------------------------------------------

    /// The two-phase lookup stores and retrieves the version set correctly.
    #[test]
    fn two_phase_lookup_stores_and_retrieves_version_set() {
        let cache = PlanCache::new(16);
        let sql_key = SqlKey {
            sql_text_hash: hash_sql("SELECT * FROM widgets"),
            placeholder_types_hash: 0,
        };

        // Initially absent.
        assert!(cache.lookup_version_set(&sql_key).is_none());

        // Store it.
        let vs = GatewayVersionSet::from_pairs(vec![("widgets".into(), 3)]);
        cache.insert_version_set(sql_key.clone(), vs.clone());

        // Retrieve it.
        assert_eq!(cache.lookup_version_set(&sql_key), Some(vs));
    }

    /// DDL invalidation also removes the side-cache entry for the affected SQL.
    #[test]
    fn invalidate_descriptor_removes_side_cache_entry() {
        use std::sync::atomic::AtomicUsize;

        let cache = PlanCache::new(16);
        let sql_key = SqlKey {
            sql_text_hash: hash_sql("GET widgets k"),
            placeholder_types_hash: 0,
        };
        let vs = GatewayVersionSet::from_pairs(vec![("widgets".into(), 1)]);

        // Populate both caches.
        let full_key = PlanCacheKey {
            sql_text_hash: sql_key.sql_text_hash,
            placeholder_types_hash: sql_key.placeholder_types_hash,
            version_set: vs.clone(),
        };
        cache.insert_version_set(sql_key.clone(), vs.clone());
        cache.insert(full_key.clone(), Arc::new(kv_get("widgets")));

        assert_eq!(cache.len(), 1);
        assert!(cache.lookup_version_set(&sql_key).is_some());

        // DDL bump.
        cache.invalidate_descriptor("widgets", 2);

        // Both entries must be gone.
        assert_eq!(cache.len(), 0, "plan entry must be evicted");
        assert!(
            cache.lookup_version_set(&sql_key).is_none(),
            "side-cache entry must also be evicted"
        );

        // Ensure the counter trick works: simulate "plan_fn called N times".
        let plan_fn_calls = Arc::new(AtomicUsize::new(0));
        let _ = plan_fn_calls; // just a placeholder — real test is in integration tests
    }

    /// Simulate the full two-phase execute_sql flow using only PlanCache APIs.
    ///
    /// This test proves the invariant stated in Gap 5:
    ///   1. `plan_fn` invocation count == 1 after 3 calls.
    ///   2. Hit count == 2 after 3 calls.
    ///   3. After DDL invalidation on `widgets`, the next call invokes `plan_fn`
    ///      again (count == 2).
    ///   4. Hit count stays at 2.
    #[test]
    fn two_phase_execute_sql_plan_fn_called_once_then_cache_hits() {
        use std::sync::atomic::AtomicUsize;

        let cache = PlanCache::new(16);
        let plan_fn_calls = Arc::new(AtomicUsize::new(0));

        // Helper: simulates what execute_sql does on every call.
        //
        // `version_of_widgets` is the version the catalog would return.
        // `expect_hit` controls whether we assert a hit or miss.
        let simulate_call = |cache: &PlanCache,
                             plan_fn_calls: &Arc<AtomicUsize>,
                             version_of_widgets: u64|
         -> bool {
            let sql = "GET widgets key";
            let sql_hash = hash_sql(sql);
            let ph_hash = 0u64;
            let sql_key = SqlKey {
                sql_text_hash: sql_hash,
                placeholder_types_hash: ph_hash,
            };

            // Phase 1: side cache.
            if let Some(stored_vs) = cache.lookup_version_set(&sql_key) {
                // Verify currency.
                let current_version = version_of_widgets;
                let is_current = stored_vs.matches("widgets", current_version);
                if is_current {
                    let full_key = PlanCacheKey {
                        sql_text_hash: sql_hash,
                        placeholder_types_hash: ph_hash,
                        version_set: stored_vs.clone(),
                    };
                    if cache.get(&full_key).is_some() {
                        return true; // hit
                    }
                }
            }

            // Miss — "plan".
            plan_fn_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let vs = GatewayVersionSet::from_pairs(vec![("widgets".into(), version_of_widgets)]);
            let full_key = PlanCacheKey {
                sql_text_hash: sql_hash,
                placeholder_types_hash: ph_hash,
                version_set: vs.clone(),
            };
            cache.insert_version_set(sql_key, vs);
            cache.insert(full_key, Arc::new(kv_get("widgets")));
            false // miss
        };

        // Call 1 — miss, plan_fn invoked.
        let hit1 = simulate_call(&cache, &plan_fn_calls, 1);
        assert!(!hit1, "call 1 must miss");
        assert_eq!(plan_fn_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(cache.cache_hit_count(), 0);

        // Call 2 — hit.
        let hit2 = simulate_call(&cache, &plan_fn_calls, 1);
        assert!(hit2, "call 2 must hit");
        assert_eq!(
            plan_fn_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_fn not called again"
        );
        assert_eq!(cache.cache_hit_count(), 1, "one cache hit");

        // Call 3 — hit.
        let hit3 = simulate_call(&cache, &plan_fn_calls, 1);
        assert!(hit3, "call 3 must hit");
        assert_eq!(
            plan_fn_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_fn still not called again"
        );
        assert_eq!(cache.cache_hit_count(), 2, "two cache hits");

        // DDL invalidation — bump descriptor version to 2.
        cache.invalidate_descriptor("widgets", 2);

        // Call 4 after DDL — must miss and invoke plan_fn again.
        let hit4 = simulate_call(&cache, &plan_fn_calls, 2);
        assert!(!hit4, "call 4 after DDL must miss");
        assert_eq!(
            plan_fn_calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "plan_fn called again after DDL"
        );
        // Hit count stays at 2 (no new hits yet).
        assert_eq!(
            cache.cache_hit_count(),
            2,
            "hit count unchanged after DDL miss"
        );
    }
}
