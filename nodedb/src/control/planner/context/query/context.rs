// SPDX-License-Identifier: BUSL-1.1

//! `QueryContext` struct definition and constructors.

use std::sync::Arc;

use crate::control::security::credential::CredentialStore;

use super::super::catalog_inputs::CatalogInputs;

/// Query context for the Control Plane.
///
/// SQL queries are parsed and planned via nodedb-sql, then converted
/// to `PhysicalPlan` variants for dispatch to the Data Plane.
///
/// This type is `Send + Sync` — lives on the Control Plane (Tokio).
///
/// The catalog adapter is **constructed per-plan**, not cached on
/// the context, because the adapter's `recorded_versions` field
/// is per-plan state. Holding a shared adapter across concurrent
/// plans would interleave their recorded descriptor sets and
/// poison the plan-cache keys. The context stores the inputs
/// needed to construct an adapter (credentials, optional
/// `Weak<SharedState>` for lease integration, tenant id,
/// retention registry) and builds a fresh one on every planning
/// call.
pub struct QueryContext {
    pub(super) catalog_inputs: Option<CatalogInputs>,
    /// Retention policy registry for auto-tier routing.
    pub(super) retention_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// Array catalog handle — required for `CREATE ARRAY` / `DROP ARRAY` /
    /// `INSERT INTO ARRAY` / `DELETE FROM ARRAY` to resolve and persist
    /// catalog entries. `None` for sub-planners that don't own array DDL.
    pub(super) array_catalog: Option<crate::control::array_catalog::ArrayCatalogHandle>,
    /// WAL allocator — required by array DML for `wal_lsn` allocation.
    pub(super) wal: Option<Arc<crate::wal::WalManager>>,
    /// Surrogate assigner — `Some` when the planner has access to
    /// `SharedState` (production path); `None` only for legacy
    /// `QueryContext::new()` test fixtures that never lower to
    /// surrogate-bearing variants.
    pub(super) surrogate_assigner: Option<Arc<crate::control::surrogate::SurrogateAssigner>>,
    /// Cluster mode flag — `true` when the node has a live cluster
    /// topology. Passed into `ConvertContext` so array converters can
    /// emit `ClusterArray` variants instead of local `Array` variants.
    pub(super) cluster_enabled: bool,
    /// Bitemporal retention registry — forwarded to `ConvertContext` so
    /// `ALTER ARRAY` can update the purge-scheduler's view of the
    /// array's retention policy. `None` for sub-planners.
    pub(super) bitemporal_retention_registry:
        Option<Arc<crate::engine::bitemporal::BitemporalRetentionRegistry>>,
    /// Per-tenant maximum vector dimension (0 = unlimited). Updated
    /// per-request by connection handlers via `set_max_vector_dim` so
    /// `VectorPrimaryInsert` conversion can reject oversized vectors without
    /// an extra `TenantIsolation` lock inside the planner hot path.
    pub(super) max_vector_dim: std::sync::atomic::AtomicU32,
    /// Per-request force-shuffle-join override (session var
    /// `nodedb.force_shuffle_join`). Updated by connection handlers via
    /// `set_force_shuffle_join` before each plan call; forwarded into
    /// `ConvertContext`. An atomic mirrors `max_vector_dim`: `&self` plan calls
    /// read it without an exclusive borrow, and handlers do not pipeline
    /// concurrent plans on one connection.
    pub(super) force_shuffle_join: std::sync::atomic::AtomicBool,
    /// Per-request forced-shuffle partition count (session var
    /// `nodedb.shuffle_num_parts`). `0` means "unset — let the emit default to
    /// the cluster data-node count". Updated alongside `force_shuffle_join`.
    pub(super) shuffle_num_parts: std::sync::atomic::AtomicU32,
    /// Per-request force-shuffle-aggregate override (session var
    /// `nodedb.force_shuffle_agg`). Updated by connection handlers via
    /// `set_force_shuffle_agg` before each plan call; forwarded into
    /// `ConvertContext`. When `true` AND the node is in cluster mode, a GROUP BY
    /// aggregate over a sharded source is emitted as a whole-aggregate
    /// `Exchange{ShuffleAggregate}` instead of the default Gather plan. An atomic
    /// mirrors `force_shuffle_join` (same `&self` plan-call contract).
    pub(super) force_shuffle_agg: std::sync::atomic::AtomicBool,
    /// Per-request forced-shuffle-aggregate partition count (session var
    /// `nodedb.shuffle_agg_num_parts`). `0` means "unset — let the emit default
    /// to the cluster data-node count". Updated alongside `force_shuffle_agg`.
    pub(super) shuffle_agg_num_parts: std::sync::atomic::AtomicU32,
    /// Broadcast-vs-shuffle cost threshold in bytes (session var
    /// `nodedb.broadcast_threshold_bytes`, defaulting to the node's
    /// `[tuning.cluster_transport] broadcast_threshold_bytes`). Connection
    /// handlers resolve the effective value (session override OR tuning default)
    /// and write it via `set_broadcast_threshold_bytes` before each plan call;
    /// forwarded into `ConvertContext` where the auto-shuffle cost model reads
    /// it. An atomic mirrors the other per-request knobs so `&self` plan calls
    /// read it without an exclusive borrow.
    pub(super) broadcast_threshold_bytes: std::sync::atomic::AtomicUsize,
    /// Gather-vs-shuffle cost threshold in distinct-group units (session var
    /// `nodedb.shuffle_agg_threshold`, defaulting to
    /// [`super::tuning::DEFAULT_SHUFFLE_AGG_THRESHOLD`]). When a GROUP BY's
    /// estimated group cardinality (from ANALYZE `distinct_count`) exceeds this
    /// value, the planner auto-selects a whole-aggregate shuffle even without
    /// `nodedb.force_shuffle_agg`. Connection handlers resolve the effective
    /// value (session override OR the default) and write it via
    /// `set_shuffle_agg_threshold` before each plan call; forwarded into
    /// `ConvertContext` where the auto-shuffle cost model reads it. An atomic
    /// mirrors `broadcast_threshold_bytes` so `&self` plan calls read it without
    /// an exclusive borrow.
    pub(super) shuffle_agg_threshold: std::sync::atomic::AtomicUsize,
    /// The calling connection's `currval` map, forwarded into the catalog
    /// adapter each plan call. Written per request by
    /// `apply_planning_session_overrides`, exactly like the knobs above.
    /// `None` for planning with no session behind it, which then reports
    /// that sequence access is unavailable rather than crossing sessions.
    pub(super) session_sequences:
        std::sync::Mutex<Option<Arc<crate::control::sequence::SessionSequenceValues>>>,
}

impl QueryContext {
    /// Create a new query context without catalog integration.
    pub fn new() -> Self {
        Self {
            catalog_inputs: None,
            retention_registry: None,
            array_catalog: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_join: std::sync::atomic::AtomicBool::new(false),
            shuffle_num_parts: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_agg: std::sync::atomic::AtomicBool::new(false),
            shuffle_agg_num_parts: std::sync::atomic::AtomicU32::new(0),
            broadcast_threshold_bytes: std::sync::atomic::AtomicUsize::new(
                default_broadcast_threshold_bytes(),
            ),
            shuffle_agg_threshold: std::sync::atomic::AtomicUsize::new(
                super::tuning::DEFAULT_SHUFFLE_AGG_THRESHOLD,
            ),
            session_sequences: std::sync::Mutex::new(None),
        }
    }

    /// Create a query context from `SharedState` without lease
    /// integration. Used by internal sub-planners (check
    /// constraints, type guards, ANALYZE, procedural DML, event
    /// trigger dispatch) that run inside a pgwire handler whose
    /// outer query already acquired leases. Re-acquiring via a
    /// sub-planner would be redundant — the lease store's fast
    /// path would return instantly anyway, but going through the
    /// sub-planner without a direct `Arc<SharedState>` reference
    /// would require threading one through every call site.
    pub fn for_state(state: &crate::control::state::SharedState) -> Self {
        let mut ctx = Self::with_catalog(
            Arc::clone(&state.credentials),
            Some(Arc::clone(&state.retention_policy_registry)),
        );
        ctx.surrogate_assigner = Some(Arc::clone(&state.surrogate_assigner));
        ctx.cluster_enabled = state.cluster_topology.is_some();
        ctx.bitemporal_retention_registry = Some(Arc::clone(&state.bitemporal_retention_registry));
        // max_vector_dim starts at 0 (unlimited); connection handlers call
        // set_max_vector_dim before each planning call.
        ctx.max_vector_dim
            .store(0, std::sync::atomic::Ordering::Relaxed);
        // Seed the broadcast-vs-shuffle byte threshold from the node's
        // configured tuning default. Connection handlers re-resolve it per
        // request (session override OR this default) via
        // `set_broadcast_threshold_bytes`.
        ctx.broadcast_threshold_bytes.store(
            state.tuning.cluster_transport.broadcast_threshold_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );
        ctx
    }

    /// Create a query context with descriptor lease integration.
    /// Used by the top-level pgwire dispatch so every user
    /// query's plan acquires descriptor leases before execution.
    /// Callers must hold an `Arc<SharedState>` — the adapter
    /// downgrades to `Weak` internally.
    pub fn for_state_with_lease(state: &Arc<crate::control::state::SharedState>) -> Self {
        let retention = Some(Arc::clone(&state.retention_policy_registry));
        Self {
            catalog_inputs: Some(CatalogInputs {
                credentials: Arc::clone(&state.credentials),
                shared: Some(Arc::downgrade(state)),
                retention_policy_registry: retention.clone(),
            }),
            retention_registry: retention,
            array_catalog: Some(state.array_catalog.clone()),
            wal: Some(Arc::clone(&state.wal)),
            surrogate_assigner: Some(Arc::clone(&state.surrogate_assigner)),
            cluster_enabled: state.cluster_topology.is_some(),
            bitemporal_retention_registry: Some(Arc::clone(&state.bitemporal_retention_registry)),
            // max_vector_dim is tenant-specific; callers supply it via
            // `with_tenant_quota` after construction so the context can be
            // reused across tenants on the same connection without carrying
            // stale quota values.
            max_vector_dim: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_join: std::sync::atomic::AtomicBool::new(false),
            shuffle_num_parts: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_agg: std::sync::atomic::AtomicBool::new(false),
            shuffle_agg_num_parts: std::sync::atomic::AtomicU32::new(0),
            broadcast_threshold_bytes: std::sync::atomic::AtomicUsize::new(
                state.tuning.cluster_transport.broadcast_threshold_bytes,
            ),
            shuffle_agg_threshold: std::sync::atomic::AtomicUsize::new(
                super::tuning::DEFAULT_SHUFFLE_AGG_THRESHOLD,
            ),
            session_sequences: std::sync::Mutex::new(None),
        }
    }

    /// Create a query context with catalog integration but no
    /// lease acquisition. Used by `for_state` and by callers
    /// that construct a context without an `Arc<SharedState>`.
    pub fn with_catalog(
        credentials: Arc<CredentialStore>,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        let catalog_inputs = Some(CatalogInputs {
            credentials,
            shared: None,
            retention_policy_registry: retention_policy_registry.clone(),
        });

        Self {
            catalog_inputs,
            retention_registry: retention_policy_registry,
            array_catalog: None,
            wal: None,
            surrogate_assigner: None,
            cluster_enabled: false,
            bitemporal_retention_registry: None,
            max_vector_dim: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_join: std::sync::atomic::AtomicBool::new(false),
            shuffle_num_parts: std::sync::atomic::AtomicU32::new(0),
            force_shuffle_agg: std::sync::atomic::AtomicBool::new(false),
            shuffle_agg_num_parts: std::sync::atomic::AtomicU32::new(0),
            broadcast_threshold_bytes: std::sync::atomic::AtomicUsize::new(
                default_broadcast_threshold_bytes(),
            ),
            shuffle_agg_threshold: std::sync::atomic::AtomicUsize::new(
                super::tuning::DEFAULT_SHUFFLE_AGG_THRESHOLD,
            ),
            session_sequences: std::sync::Mutex::new(None),
        }
    }
}

impl QueryContext {
    /// Bind the calling connection's `currval` map for the next plan call.
    pub fn set_session_sequences(
        &self,
        values: Option<Arc<crate::control::sequence::SessionSequenceValues>>,
    ) {
        let mut slot = self
            .session_sequences
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        *slot = values;
    }

    /// The `currval` map bound for the current plan call.
    pub(super) fn session_sequences(
        &self,
    ) -> Option<Arc<crate::control::sequence::SessionSequenceValues>> {
        self.session_sequences
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}

/// The node-default broadcast threshold (bytes) for fixtures that have no
/// `SharedState` tuning to read. Sourced from `ClusterTransportTuning::default()`
/// so the planner default and the config default never drift.
pub(super) fn default_broadcast_threshold_bytes() -> usize {
    nodedb_types::config::tuning::ClusterTransportTuning::default().broadcast_threshold_bytes
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::new()
    }
}
