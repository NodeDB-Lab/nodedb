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
    /// Execution conversion may allocate identities and apply converter-owned
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
    /// crosses the SPSC bridge. `None` only for converters used by
    /// sub-planners that never lower to the surrogate-bearing variants
    /// (e.g. CREATE/DROP/ARRAY paths).
    pub surrogate_assigner: Option<Arc<SurrogateAssigner>>,
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
    /// Database scope for vShard computation. All `VShardId::from_collection_in_database`
    /// calls must use this value so that collections in different databases are
    /// routed to distinct shards and data-plane isolates them correctly.
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
    /// The catalog the plan was built against. INSERT/UPSERT conversion
    /// evaluates each declared column DEFAULT here, and a sequence-backed
    /// DEFAULT (`nextval`, `currval`) reads its value through this handle.
    /// `None` for converters built by sub-planners that hold no catalog; a
    /// catalog-reading DEFAULT then raises instead of dropping the column.
    pub sql_catalog: Option<Arc<dyn nodedb_sql::catalog::SqlCatalog + Send + Sync>>,
}

impl ConvertContext {
    pub fn is_metadata(&self) -> bool {
        self.purpose == PlanningPurpose::Metadata
    }

    /// The catalog a column DEFAULT is evaluated against.
    ///
    /// Raises when the converter holds none. A DEFAULT that reads the catalog
    /// must fail loudly: omitting the column stores NULL where the
    /// declaration promised a value.
    pub fn sql_catalog(
        &self,
    ) -> crate::Result<&(dyn nodedb_sql::catalog::SqlCatalog + Send + Sync)> {
        self.sql_catalog
            .as_deref()
            .ok_or_else(|| crate::Error::PlanError {
                detail: "plan conversion holds no catalog, so a column DEFAULT that reads                          one cannot be evaluated"
                    .into(),
            })
    }

    /// Resolve an existing surrogate without creating a mapping while planning
    /// metadata. Execute planning retains the allocating assignment behavior.
    pub fn surrogate_for_pk(
        &self,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<nodedb_types::Surrogate> {
        let Some(assigner) = self.surrogate_assigner.as_ref() else {
            return Ok(nodedb_types::Surrogate::ZERO);
        };
        if self.is_metadata() {
            return Ok(assigner
                .lookup(self.database_id, self.tenant_id, collection, pk_bytes)?
                .unwrap_or(nodedb_types::Surrogate::ZERO));
        }
        assigner.assign(self.database_id, self.tenant_id, collection, pk_bytes)
    }

    /// Resolve an EXISTING pk → surrogate binding read-only, yielding
    /// `Surrogate::ZERO` when the key is unbound. Used by writes that mutate
    /// rows they never create (PK UPDATE / DELETE): allocating there would mint
    /// a node-local phantom binding for a key no replica agrees on.
    pub fn surrogate_for_existing_pk(
        &self,
        collection: &str,
        pk_bytes: &[u8],
    ) -> crate::Result<nodedb_types::Surrogate> {
        let Some(assigner) = self.surrogate_assigner.as_ref() else {
            return Ok(nodedb_types::Surrogate::ZERO);
        };
        Ok(assigner
            .lookup(self.database_id, self.tenant_id, collection, pk_bytes)?
            .unwrap_or(nodedb_types::Surrogate::ZERO))
    }

    /// Allocate a new surrogate and its bound identity string, only while
    /// producing executable work.
    ///
    /// A metadata plan, or a plan with no wired assigner, returns a
    /// `Surrogate::ZERO` placeholder. Its identity string comes from
    /// [`fresh_identity_string`](crate::control::surrogate::fresh_identity_string),
    /// the same function the allocator calls.
    pub fn fresh_surrogate(
        &self,
        collection: &str,
        kind: nodedb_physical::FreshSurrogateKind,
    ) -> crate::Result<(nodedb_types::Surrogate, String)> {
        let placeholder = || {
            Ok((
                nodedb_types::Surrogate::ZERO,
                crate::control::surrogate::fresh_identity_string(
                    kind,
                    nodedb_types::Surrogate::ZERO,
                ),
            ))
        };
        if self.is_metadata() {
            return placeholder();
        }
        match self.surrogate_assigner.as_ref() {
            Some(assigner) => {
                assigner.assign_fresh(self.database_id, self.tenant_id, collection, kind)
            }
            None => placeholder(),
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

    /// Build the deployment-neutral subset shared with `nodedb-physical`'s
    /// converter helpers. Cheap: 3 `Copy` fields + an `Arc` clone.
    pub fn shared(&self) -> nodedb_physical::SharedConvertContext {
        nodedb_physical::SharedConvertContext {
            database_id: self.database_id,
            max_vector_dim: self.max_vector_dim,
            cluster_enabled: self.cluster_enabled,
            surrogate_assigner: self
                .surrogate_assigner
                .as_ref()
                .map(|a| a.clone() as std::sync::Arc<dyn nodedb_physical::SurrogateAssigner>),
        }
    }
}

/// Convert a list of SqlPlans to PhysicalTasks.
///
/// After each task is produced, any top-level read plan that is a sharded
/// source is wrapped in `Exchange{Gather}` so the coordinator knows to fan
/// it to all Data Plane cores and merge the results. Non-sharded plans
/// (point gets, writes, constant `ProviderScan`s, coordinator-local joins)
/// are left unwrapped.
pub fn convert(
    plans: &[SqlPlan],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let mut tasks = Vec::new();
    for plan in plans {
        let mut one = convert_one(plan, tenant_id, ctx)?;
        for task in &mut one {
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
                    filters: Vec::new(),
                    projection: Vec::new(),
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
        tasks.extend(one);
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
            surrogate_assigner: Some(assigner),
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
            sql_catalog: None,
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
                .surrogate_for_pk("users", b"new-user")
                .unwrap()
                .as_u32(),
            0
        );
        assert_eq!(
            metadata
                .fresh_surrogate("users", nodedb_physical::FreshSurrogateKind::AutoRowId)
                .unwrap()
                .0
                .as_u32(),
            0
        );
        assert_eq!(
            assigner
                .lookup(DatabaseId::DEFAULT, TenantId::new(1), "users", b"new-user")
                .unwrap(),
            None
        );
        assert_eq!(registry.read().expect("registry").current_hwm(), 0);

        let execute = context(PlanningPurpose::Execute, Arc::clone(&assigner));
        let allocated = execute.surrogate_for_pk("users", b"new-user").unwrap();
        assert_ne!(allocated.as_u32(), 0);
        assert_eq!(
            assigner
                .lookup(DatabaseId::DEFAULT, TenantId::new(1), "users", b"new-user")
                .unwrap(),
            Some(allocated)
        );
        assert_eq!(
            registry.read().expect("registry").current_hwm(),
            allocated.as_u32()
        );
    }
}
