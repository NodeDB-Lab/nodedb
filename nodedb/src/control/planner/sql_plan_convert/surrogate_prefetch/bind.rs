// SPDX-License-Identifier: BUSL-1.1

//! Execution conversion: a pure planning pass per plan, with an async bind
//! step that resolves every surrogate the pass needs before its tasks leave.
//!
//! Conversion stays a synchronous, deterministic function of the plan and
//! the resolved answers, so a plan converted again has the same structure.
//! Every draw and every request to a key's home is awaited here, and none
//! blocks a runtime worker, on any runtime flavor.

use nodedb_physical::physical_task::PhysicalTask;
use nodedb_sql::types::SqlPlan;

use super::super::convert::{ConvertContext, convert_plan};
use super::cache::SurrogateMisses;
use super::resolve::{prefetch_plan_surrogates, resolve_misses};
use crate::types::TenantId;

/// Convert `plans` to physical tasks with every surrogate they use resolved.
///
/// The keys read off the plans resolve first, in one pass. Each plan then
/// converts in order. A pass that records a miss is converted again once the
/// miss resolves, so a key the up-front read does not derive still resolves
/// without a synchronous request. Plans convert one at a time, so a plan
/// that persists catalog state converts once, before the plans after it.
pub async fn convert_bound(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &mut ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    ctx.prefetched = prefetch_plan_surrogates(plans, ctx).await?;
    convert_resolving_misses(plans, tenant_id, ctx).await
}

/// Convert each of `plans` with the answers `ctx` holds, resolving the misses
/// of each pass and converting that plan again until a pass records none.
pub(super) async fn convert_resolving_misses(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &mut ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        tasks.extend(convert_plan_bound(plan, tenant_id, ctx).await?);
    }
    Ok(tasks)
}

async fn convert_plan_bound(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &mut ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mark = ctx.prefetched.fresh_mark();
    let mut resolved: Option<SurrogateMisses> = None;
    loop {
        // A pass with misses planned placeholders, so its result, an error
        // included, is replaced by the next pass.
        let converted = convert_plan(plan, tenant_id, ctx);
        let misses = ctx.prefetched.take_misses();
        if misses.is_empty() {
            return converted;
        }
        drop(converted);
        // Each pass asks only for keys no earlier pass resolved. The same
        // misses twice means the answers do not reach conversion.
        if resolved.as_ref() == Some(&misses) {
            return Err(crate::Error::Internal {
                detail: format!(
                    "surrogate bind: conversion asked again for the keys of {} after they \
                     resolved",
                    misses.collection_names()
                ),
            });
        }
        ctx.prefetched.rewind_fresh(&mark);
        resolve_misses(ctx, &misses).await?;
        resolved = Some(misses);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, RwLock};

    use futures::FutureExt;
    use futures::future::BoxFuture;
    use nodedb_physical::physical_plan::{KvOp, TimeseriesOp};
    use nodedb_physical::physical_task::PhysicalTask;
    use nodedb_sql::types::{EngineType, SqlExpr, SqlPlan, SqlValue, TimeseriesIngestPlan};
    use nodedb_types::{CollectionKey, Surrogate};

    use super::{convert_bound, convert_resolving_misses};
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::planner::sql_plan_convert::{ConvertContext, PlanningPurpose};
    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};
    use crate::control::surrogate::{HomeSurrogateAuthority, SurrogateAssigner};
    use crate::types::{DatabaseId, TenantId};

    const TENANT: TenantId = TenantId::new(1);

    /// A cluster-mode assigner whose reserved batch is empty.
    fn cluster_assigner() -> Arc<SurrogateAssigner> {
        let credentials = Arc::new(CredentialStore::new().expect("in-memory credential store"));
        let registry = Arc::new(RwLock::new(SurrogateRegistry::from_persisted_cluster(0, 0)));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        Arc::new(SurrogateAssigner::new(registry, credentials, wal))
    }

    fn cluster_ctx(assigner: &Arc<SurrogateAssigner>) -> ConvertContext {
        ConvertContext {
            purpose: PlanningPurpose::Execute,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: Arc::clone(assigner),
            cluster_enabled: true,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TENANT,
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 0,
            shuffle_agg_threshold: 0,
            prefetched: Default::default(),
        }
    }

    fn timeseries_ingest(rows: usize) -> Vec<SqlPlan> {
        vec![SqlPlan::TimeseriesIngest(TimeseriesIngestPlan {
            collection: "metrics".to_string(),
            rows: vec![vec![("value".to_string(), SqlValue::Int(1))]; rows],
            volatile_defaults: false,
        })]
    }

    fn ingest_surrogates(tasks: &[PhysicalTask]) -> Vec<u32> {
        match &tasks[0].plan {
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest { surrogates, .. }) => {
                surrogates.iter().map(|s| s.as_u32()).collect()
            }
            other => panic!("expected a timeseries ingest, got {other:?}"),
        }
    }

    /// Install the reserved batch `[100, 116)` once the conversion is parked
    /// on the empty one. Fails when the conversion finished without it.
    async fn install_batch(assigner: &SurrogateAssigner, converted: &AtomicBool) {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert!(
            !converted.load(Ordering::SeqCst),
            "conversion finished with no batch installed"
        );
        let _waiter = assigner.await_reservation_for_test(1);
        assigner.complete_reservation(1, 100, 116);
    }

    /// A cluster conversion on a current-thread runtime awaits the refill of
    /// an empty batch: its one worker stays free to install the batch, and
    /// the rows draw from it.
    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_cluster_conversion_awaits_an_empty_batch() {
        let assigner = cluster_assigner();
        let mut ctx = cluster_ctx(&assigner);
        let plans = timeseries_ingest(3);
        let converted = AtomicBool::new(false);

        let conversion = async {
            let tasks = convert_bound(&plans, TENANT, &mut ctx).await;
            converted.store(true, Ordering::SeqCst);
            tasks
        };
        let (tasks, ()) = tokio::join!(conversion, install_batch(&assigner, &converted));

        let surrogates = ingest_surrogates(&tasks.expect("conversion"));
        assert_eq!(surrogates.len(), 3);
        assert!(surrogates.iter().all(|s| (100..116).contains(s)));
        let mut distinct = surrogates.clone();
        distinct.dedup();
        assert_eq!(distinct, surrogates, "each row draws its own surrogate");
    }

    /// A draw the up-front key read does not cover is recorded by the pass,
    /// awaited, and the plan converts again with the drawn values.
    #[tokio::test(flavor = "current_thread")]
    async fn a_missed_fresh_draw_is_awaited_and_the_plan_converts_again() {
        let assigner = cluster_assigner();
        let mut ctx = cluster_ctx(&assigner);
        let plans = timeseries_ingest(2);
        let converted = AtomicBool::new(false);

        let conversion = async {
            let tasks = convert_resolving_misses(&plans, TENANT, &mut ctx).await;
            converted.store(true, Ordering::SeqCst);
            tasks
        };
        let (tasks, ()) = tokio::join!(conversion, install_batch(&assigner, &converted));

        let surrogates = ingest_surrogates(&tasks.expect("conversion"));
        assert_eq!(surrogates.len(), 2);
        assert!(surrogates.iter().all(|s| (100..116).contains(s)));
        assert_ne!(surrogates[0], surrogates[1]);
        assert!(ctx.prefetched.take_misses().is_empty());
    }

    /// A home that binds each new key to the next value from 500, after a
    /// real await point.
    #[derive(Default)]
    struct CountingHome {
        bound: Mutex<HashMap<Vec<u8>, Surrogate>>,
    }

    impl HomeSurrogateAuthority for CountingHome {
        fn assign<'a>(
            &'a self,
            _key: CollectionKey<'a>,
            _tenant_id: nodedb_types::TenantId,
            pks: &'a [&'a [u8]],
        ) -> BoxFuture<'a, crate::Result<Vec<Surrogate>>> {
            async move {
                tokio::task::yield_now().await;
                let mut bound = self.bound.lock().unwrap_or_else(|p| p.into_inner());
                let mut answers = Vec::with_capacity(pks.len());
                for pk in pks {
                    let next = Surrogate::new(500 + bound.len() as u32);
                    answers.push(*bound.entry(pk.to_vec()).or_insert(next));
                }
                Ok(answers)
            }
            .boxed()
        }

        fn lookup_many<'a>(
            &'a self,
            _key: CollectionKey<'a>,
            _tenant_id: nodedb_types::TenantId,
            pks: &'a [&'a [u8]],
        ) -> BoxFuture<'a, crate::Result<Vec<Option<Surrogate>>>> {
            async move {
                tokio::task::yield_now().await;
                let bound = self.bound.lock().unwrap_or_else(|p| p.into_inner());
                Ok(pks.iter().map(|pk| bound.get(*pk).copied()).collect())
            }
            .boxed()
        }
    }

    /// A keyed write whose keys the up-front read misses asks the keys' home
    /// through an awaited request on a current-thread runtime, and plans with
    /// the home's values.
    #[tokio::test(flavor = "current_thread")]
    async fn missed_keys_are_bound_at_their_home_without_blocking() {
        let assigner = cluster_assigner();
        assigner.install_home_authority(Arc::new(CountingHome::default()));
        let mut ctx = cluster_ctx(&assigner);
        let plans = vec![SqlPlan::Update {
            collection: "cache".to_string(),
            engine: EngineType::KeyValue,
            assignments: vec![("v".to_string(), SqlExpr::Literal(SqlValue::Int(1)))],
            filters: Vec::new(),
            target_keys: vec![
                SqlValue::String("k1".to_string()),
                SqlValue::String("k2".to_string()),
            ],
            returning: false,
        }];

        let tasks = convert_resolving_misses(&plans, TENANT, &mut ctx)
            .await
            .expect("conversion");

        let cache = CollectionKey::from_bare(DatabaseId::DEFAULT, "cache");
        let mut planned = Vec::new();
        for task in &tasks {
            match &task.plan {
                PhysicalPlan::Kv(KvOp::FieldSet { key, surrogate, .. }) => {
                    assert_eq!(
                        assigner.lookup_bound(cache, TENANT, key).expect("lookup"),
                        Some(*surrogate),
                        "the home's value is kept in this node's catalog"
                    );
                    planned.push(surrogate.as_u32());
                }
                other => panic!("expected a KV field set, got {other:?}"),
            }
        }
        planned.sort_unstable();
        assert_eq!(planned, vec![500, 501]);
    }
}
