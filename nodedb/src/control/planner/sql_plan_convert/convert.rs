// SPDX-License-Identifier: BUSL-1.1

//! Convert nodedb-sql SqlPlan IR to NodeDB PhysicalPlan + PhysicalTask.
//!
//! This is the Origin-specific mapping layer. It adds vShard routing,
//! serializes filters to msgpack, and handles broadcast join decisions.

use nodedb_sql::types::SqlPlan;

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, QueryOp};

use crate::control::array_catalog::ArrayCatalogHandle;
use crate::control::security::credential::CredentialStore;
use crate::control::surrogate::SurrogateAssigner;
use crate::engine::bitemporal::BitemporalRetentionRegistry;
use crate::engine::timeseries::retention_policy::RetentionPolicyRegistry;
use crate::types::TenantId;
use crate::wal::WalManager;

use nodedb_physical::physical_task::PhysicalTask;

/// Qualify a raw collection name with its database ID so that storage keys
/// for collections in different databases never collide.
///
/// The resulting string is used as the `collection` field in every physical
/// plan variant that reaches the Data Plane. Storage engines key data on
/// `(tenant_id, collection, document_id)` — by embedding the database ID
/// into the collection token, isolation between databases is automatic.
pub fn db_qualified(database_id: crate::types::DatabaseId, collection: &str) -> String {
    nodedb_types::QualifiedCollection::new(database_id, collection)
        .as_str()
        .to_owned()
}

/// Whether conversion produces executable work or metadata used only for
/// authorization and response shaping.
///
/// Metadata conversion must never allocate durable identity or mutate planner
/// owned state. Its physical tasks are descriptive only and must not cross the
/// dispatch boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanningPurpose {
    Execute,
    Metadata,
}

/// Conversion context holding optional references needed during plan conversion.
pub struct ConvertContext {
    /// Execution conversion can allocate identities and apply converter-owned
    /// catalog changes. Metadata conversion is strictly side-effect-free.
    pub purpose: PlanningPurpose,
    pub retention_registry: Option<Arc<RetentionPolicyRegistry>>,
    /// Array DDL/DML targets — when `None`, array statements fail with a
    /// deterministic error so converters used by sub-planners (which do
    /// not own array state) cannot accidentally mutate the catalog.
    pub array_catalog: Option<ArrayCatalogHandle>,
    /// Used by `SqlPlan::CreateArray` / `DropArray` to persist or
    /// remove `_system.arrays` rows.
    pub credentials: Option<Arc<CredentialStore>>,
    /// LSN allocator for array Put/Delete dispatches.
    pub wal: Option<Arc<WalManager>>,
    /// CP-side surrogate assigner — bound to the same `Arc` held on
    /// `SharedState`. Threaded into INSERT/UPSERT/KV-INSERT converters
    /// to bind `(collection, pk_bytes)` → `Surrogate` before the op
    /// crosses the SPSC bridge.
    pub surrogate_assigner: Arc<SurrogateAssigner>,
    /// `true` when the node is running in cluster mode with a live
    /// topology. Array DML/query converters emit `ClusterArray` variants
    /// when this flag is set; single-node mode emits local `Array` variants.
    pub cluster_enabled: bool,
    /// Bitemporal retention registry — required by `ALTER ARRAY` to
    /// update the purge-scheduler's view of the array's retention policy.
    /// `None` for sub-planners that don't own array DDL.
    pub bitemporal_retention_registry: Option<Arc<BitemporalRetentionRegistry>>,
    /// Per-tenant maximum vector dimension (0 = unlimited). Checked in
    /// `VectorPrimaryInsert` conversion before the task is built.
    pub max_vector_dim: u32,
    /// Database scope for vShard computation. Every `CollectionKey` the
    /// converter builds uses this value, so collections in different
    /// databases route to distinct shards and the Data Plane isolates them.
    pub database_id: crate::types::DatabaseId,
    /// Tenant scope for surrogate identity. Threaded into every surrogate
    /// `assign`/`lookup` so two tenants with the same primary key in a
    /// same-named collection resolve to distinct surrogates.
    pub tenant_id: crate::types::TenantId,
    /// Permanent operator override (session var `nodedb.force_shuffle_join`):
    /// when `true` AND the node is in cluster mode, an equi hash join over two
    /// sharded sources is emitted as a whole-join `Exchange{Shuffle}` (both
    /// inputs left as bare scans) instead of the default broadcast-build-side
    /// plan. This is the manual hint layer; the automatic cost-model default is
    /// a separate follow-up. Ignored in single-node mode (no peers to shuffle
    /// across — the broadcast/local path is correct and cheaper).
    pub force_shuffle_join: bool,
    /// Target partition count for a forced shuffle join. Clamped to `>= 1` at
    /// emit time. Sourced from session var `nodedb.shuffle_num_parts`; defaults
    /// to the cluster's data-node count when unset.
    pub shuffle_num_parts: usize,
    /// Permanent operator override (session var `nodedb.force_shuffle_agg`):
    /// when `true` AND the node is in cluster mode, a GROUP BY aggregate over a
    /// sharded source is emitted as a whole-aggregate `Exchange{ShuffleAggregate}`
    /// (the input left as a bare per-shard scan) instead of the default
    /// Gather-merge plan. This is the manual hint layer; the automatic
    /// cost-model default is a separate follow-up. Ignored in single-node mode
    /// (no peers to shuffle across — the Gather path is correct and cheaper).
    pub force_shuffle_agg: bool,
    /// Target partition count for a forced shuffle aggregate. Sourced from
    /// session var `nodedb.shuffle_agg_num_parts`; `0` defaults to the cluster's
    /// data-node count at resolve time when unset.
    pub shuffle_agg_num_parts: usize,
    /// Broadcast-vs-shuffle cost threshold in bytes. When BOTH join sides have
    /// ANALYZE statistics and each side's estimated size exceeds this value
    /// (i.e. neither side is small enough to broadcast cheaply), the planner
    /// auto-selects a shuffle join. Defaults to the node's configured
    /// `[tuning.cluster_transport] broadcast_threshold_bytes`; overridable
    /// per-session via `nodedb.broadcast_threshold_bytes` for operator control
    /// and test determinism. See `nodedb_cluster::distributed_join::select_strategy`.
    pub broadcast_threshold_bytes: usize,
    /// Gather-vs-shuffle cost threshold in distinct-group units. When a GROUP BY
    /// aggregate over a sharded source has ANALYZE statistics and its estimated
    /// group cardinality (the product of the GROUP BY columns' `distinct_count`,
    /// capped at the collection row count) exceeds this value, the planner
    /// auto-selects a whole-aggregate shuffle (parallelizing the finalize across
    /// part-owners) instead of the default coordinator Gather-merge. Defaults to
    /// `DEFAULT_SHUFFLE_AGG_THRESHOLD`; overridable per-session via
    /// `nodedb.shuffle_agg_threshold` for operator control and test determinism.
    pub shuffle_agg_threshold: usize,
    /// The surrogate answers for this batch's keys, resolved async before
    /// conversion (`convert_bound`). Conversion reads them and never draws a
    /// surrogate or asks a key's home. A key with no answer is recorded here
    /// as a miss for the bind step to resolve.
    pub prefetched: super::surrogate_prefetch::PrefetchedSurrogates,
}

impl ConvertContext {
    pub fn is_metadata(&self) -> bool {
        self.purpose == PlanningPurpose::Metadata
    }

    /// The CRS context SRIDs resolve against. A context with no catalog
    /// reaches no user-defined CRS, so it resolves through the built-in
    /// registry only.
    pub fn crs_context(&self) -> nodedb_crs::CrsContext<'_> {
        match &self.credentials {
            Some(credentials) => {
                crate::control::crs_context::catalog_crs_context(credentials.catalog())
            }
            None => nodedb_crs::CrsContext::builtin_only(),
        }
    }

    /// The canonical key of `bare`, a catalog collection name in this
    /// context's database. Placement and surrogate identity use this key.
    pub fn collection_key<'a>(&self, bare: &'a str) -> nodedb_types::CollectionKey<'a> {
        nodedb_types::CollectionKey::from_bare(self.database_id, bare)
    }

    /// The surrogate a write that creates its row plans `pk_bytes` with.
    ///
    /// The bind step's answer or this node's catalog binding answers. A key
    /// neither answers is recorded as a miss and plans with the
    /// `Surrogate::ZERO` placeholder: the bind step binds it and converts the
    /// plan again. A metadata plan binds nothing, so it reads the existing
    /// binding and renders an unbound key as the placeholder.
    pub fn surrogate_for_pk(
        &self,
        key: nodedb_types::CollectionKey<'_>,
        pk_bytes: &[u8],
    ) -> crate::Result<nodedb_types::Surrogate> {
        if self.is_metadata() {
            return Ok(self
                .surrogate_for_existing_pk(key, pk_bytes)?
                .unwrap_or(nodedb_types::Surrogate::ZERO));
        }
        if let Some(bound) = self.prefetched.bound(key, pk_bytes) {
            return Ok(bound);
        }
        if let Some(bound) = self
            .surrogate_assigner
            .lookup_bound(key, self.tenant_id, pk_bytes)?
        {
            return Ok(bound);
        }
        self.prefetched.record_bind_miss(key, pk_bytes);
        Ok(nodedb_types::Surrogate::ZERO)
    }

    /// Resolve an EXISTING pk → surrogate binding read-only, or `None` when
    /// the key is unbound in this database. Used by reads and by writes that
    /// mutate rows they never create (PK UPDATE / DELETE): allocating there
    /// will mint a node-local phantom binding for a key no replica agrees on.
    ///
    /// A single node's catalog miss is the answer. A cluster node records a
    /// key neither the bind step nor its catalog answers as a miss, and plans
    /// it as unbound until the bind step asks the key's home.
    pub fn surrogate_for_existing_pk(
        &self,
        key: nodedb_types::CollectionKey<'_>,
        pk_bytes: &[u8],
    ) -> crate::Result<Option<nodedb_types::Surrogate>> {
        if let Some(answer) = self.prefetched.get(key, pk_bytes) {
            return Ok(answer);
        }
        let assigner = &self.surrogate_assigner;
        if let Some(bound) = assigner.lookup_bound(key, self.tenant_id, pk_bytes)? {
            return Ok(Some(bound));
        }
        if assigner.resolves_at_home()? {
            self.prefetched.record_lookup_miss(key, pk_bytes);
        }
        Ok(None)
    }

    /// The next fresh surrogate and its bound identity string the bind step
    /// drew for `key`, only while producing executable work.
    ///
    /// A metadata plan, and an execute plan whose drawn identities ran out,
    /// get a `Surrogate::ZERO` placeholder paired with its rendered identity,
    /// via
    /// [`RowIdentity::for_surrogate`](crate::engine::document::store::RowIdentity::for_surrogate),
    /// the same type the allocator renders through. The execute plan also
    /// records the shortfall as a miss, so the bind step draws one more
    /// identity and converts the plan again.
    pub fn fresh_surrogate(
        &self,
        key: nodedb_types::CollectionKey<'_>,
    ) -> crate::Result<(nodedb_types::Surrogate, String)> {
        let placeholder = || {
            let zero = nodedb_types::Surrogate::ZERO;
            Ok((
                zero,
                crate::engine::document::store::RowIdentity::for_surrogate(zero).into_string(),
            ))
        };
        if self.is_metadata() {
            return placeholder();
        }
        match self.prefetched.take_fresh(key) {
            Some(fresh) => Ok(fresh),
            None => {
                self.prefetched.record_fresh_miss(key);
                placeholder()
            }
        }
    }

    /// Reject converter paths whose conversion itself persists catalog state.
    pub fn require_execute(&self, operation: &str) -> crate::Result<()> {
        if self.is_metadata() {
            return Err(crate::Error::PlanError {
                detail: format!("{operation} is not available during metadata planning"),
            });
        }
        Ok(())
    }
}

/// Convert a list of SqlPlans to PhysicalTasks with the surrogate answers
/// `ctx` already holds. A pure planning pass: it draws nothing and asks no
/// home. A key it finds no answer for fails the conversion. Execution
/// planning converts through
/// [`convert_bound`](super::surrogate_prefetch::convert_bound), which awaits
/// every answer first.
pub fn convert(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        tasks.extend(convert_plan(plan, tenant_id, ctx)?);
    }
    let misses = ctx.prefetched.take_misses();
    if !misses.is_empty() {
        return Err(crate::Error::PlanError {
            detail: format!(
                "surrogate keys of {} have no answer; convert through `convert_bound`, which \
                 resolves them first",
                misses.collection_names()
            ),
        });
    }
    Ok(tasks)
}

/// Convert one SqlPlan to PhysicalTasks.
///
/// After each task is produced, any top-level read plan that is a sharded
/// source is wrapped in `Exchange{Gather}` so the coordinator knows to fan
/// it to all Data Plane cores and merge the results. Non-sharded plans
/// (point gets, writes, constant `ProviderScan`s, coordinator-local joins)
/// are left unwrapped.
pub(super) fn convert_plan(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = convert_one(plan, tenant_id, ctx)?;
    for task in &mut tasks {
        if task.plan.is_sharded_source() {
            let as_aggregate = matches!(
                &task.plan,
                PhysicalPlan::Query(QueryOp::Aggregate { .. })
                    | PhysicalPlan::Query(QueryOp::PartialAggregate { .. })
            );
            // Move the plan out, wrap it in Exchange{Gather}, put it back.
            let sentinel = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: Vec::new(),
                sources: Vec::new(),
                filters: Vec::new(),
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            let inner = std::mem::replace(&mut task.plan, sentinel);
            task.plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(inner),
                mode: ExchangeMode::Gather { as_aggregate },
            }));
        }
    }
    Ok(tasks)
}

pub(super) fn convert_one(
    plan: &SqlPlan,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut visitor = super::visitor::ConvertVisitor { tenant_id, ctx };
    nodedb_sql::dispatch(&mut visitor, plan)
}

#[cfg(test)]
mod tests {
    use super::{ConvertContext, PlanningPurpose};
    use std::sync::{Arc, RwLock};

    use crate::control::security::credential::CredentialStore;
    use crate::control::surrogate::SurrogateAssigner;
    use crate::control::surrogate::registry::SurrogateRegistry;
    use crate::control::surrogate::wal_appender::{NoopWalAppender, SurrogateWalAppender};
    use crate::types::{DatabaseId, TenantId};

    fn context(purpose: PlanningPurpose, assigner: Arc<SurrogateAssigner>) -> ConvertContext {
        ConvertContext {
            purpose,
            retention_registry: None,
            array_catalog: None,
            credentials: None,
            wal: None,
            surrogate_assigner: assigner,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: 0,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            force_shuffle_join: false,
            shuffle_num_parts: 0,
            force_shuffle_agg: false,
            shuffle_agg_num_parts: 0,
            broadcast_threshold_bytes: 0,
            shuffle_agg_threshold: 0,
            prefetched: Default::default(),
        }
    }

    #[test]
    fn metadata_surrogate_planning_never_creates_a_mapping_or_advances_counter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let credentials = Arc::new(
            CredentialStore::open(&dir.path().join("system.redb")).expect("credential store"),
        );
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let assigner = Arc::new(SurrogateAssigner::new(
            Arc::clone(&registry),
            credentials,
            wal,
        ));
        let metadata = context(PlanningPurpose::Metadata, Arc::clone(&assigner));

        assert_eq!(
            metadata
                .surrogate_for_pk(metadata.collection_key("users"), b"new-user")
                .unwrap()
                .as_u32(),
            0
        );
        assert_eq!(
            metadata
                .fresh_surrogate(metadata.collection_key("users"))
                .unwrap()
                .0
                .as_u32(),
            0
        );
        assert_eq!(
            assigner
                .lookup_bound(
                    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "users"),
                    TenantId::new(1),
                    b"new-user",
                )
                .unwrap(),
            None
        );
        assert_eq!(registry.read().expect("registry").current_hwm(), 0);
        assert!(metadata.prefetched.take_misses().is_empty());
    }

    /// Execute conversion never draws: an unanswered key plans with the
    /// placeholder and is recorded for the bind step, and the registry stays
    /// untouched.
    #[test]
    fn execute_conversion_records_unanswered_keys_instead_of_drawing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let credentials = Arc::new(
            CredentialStore::open(&dir.path().join("system.redb")).expect("credential store"),
        );
        let registry = Arc::new(RwLock::new(SurrogateRegistry::new()));
        let wal: Arc<dyn SurrogateWalAppender> = Arc::new(NoopWalAppender);
        let assigner = Arc::new(SurrogateAssigner::new(
            Arc::clone(&registry),
            credentials,
            wal,
        ));
        let execute = context(PlanningPurpose::Execute, Arc::clone(&assigner));
        let users = execute.collection_key("users");

        assert_eq!(
            execute
                .surrogate_for_pk(users, b"new-user")
                .unwrap()
                .as_u32(),
            0
        );
        assert_eq!(execute.fresh_surrogate(users).unwrap().0.as_u32(), 0);
        // A single node's catalog miss is the answer: no lookup is recorded.
        assert_eq!(
            execute.surrogate_for_existing_pk(users, b"ghost").unwrap(),
            None
        );
        assert_eq!(registry.read().expect("registry").current_hwm(), 0);

        let misses = execute.prefetched.take_misses();
        assert_eq!(misses.len(), 2);
        let (key, recorded) = misses.iter().next().expect("one collection");
        assert_eq!(key.name(), "users");
        assert!(recorded.binds.contains(b"new-user".as_slice()));
        assert!(recorded.lookups.is_empty());
        assert_eq!(recorded.fresh, 1);
        assert!(execute.prefetched.take_misses().is_empty());
    }
}
