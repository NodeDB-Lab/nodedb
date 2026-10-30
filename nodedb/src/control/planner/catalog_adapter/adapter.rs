// SPDX-License-Identifier: BUSL-1.1

//! `OriginCatalog` struct definition, construction, and small helpers.

use std::sync::{Arc, Mutex};

use crate::control::planner::descriptor_set::DescriptorVersionSet;
use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// Adapter bridging the NodeDB catalog to the `SqlCatalog` trait.
///
/// The adapter reads descriptors from the local `SystemCatalog`
/// redb and records each observed descriptor into
/// `recorded_versions` for use as the plan-cache key. It does
/// NOT acquire leases itself — `SharedState::acquire_plan_lease_scope`
/// is called by the pgwire handler after planning finishes
/// (for both cache hits and fresh plans) so leases are held
/// through the execute phase via a refcounted
/// `QueryLeaseScope`.
pub struct OriginCatalog {
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) tenant_id: u64,
    /// Database namespace to scope catalog lookups. Queries from a session
    /// that is bound to `db_alpha` must only see collections in `db_alpha`,
    /// even if a same-named collection exists in another database.
    pub(super) database_id: DatabaseId,
    pub(super) retention_policy_registry:
        Option<Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>>,
    /// The node's array catalog. Every adapter resolves array names against
    /// it, so a sub-planner plans array DML exactly as a client statement
    /// does.
    pub(super) array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
    /// Optional reference to the host's drain tracker. When
    /// present, `get_collection` checks for an active drain
    /// on each descriptor it reads and returns
    /// `RetryableSchemaChanged` so the planner's retry loop
    /// re-plans. When absent (sub-planners that don't thread
    /// an `Arc<SharedState>`), drain is not observable at
    /// plan time — the outer query's scope is still protecting
    /// the lease.
    pub(super) drain_tracker: Option<Arc<crate::control::lease::DescriptorDrainTracker>>,
    /// Descriptors read during planning, in stable order. Filled
    /// by `get_collection`, drained by the caller via
    /// `take_recorded_versions` once planning finishes. The
    /// resulting set becomes the cache key for the plan cache so
    /// DDL on unrelated descriptors does not invalidate cached
    /// plans.
    ///
    /// Wrapped in `Mutex` (not `RefCell`) because `SqlCatalog`
    /// is used through `&self` and the adapter must be `Sync`
    /// for axum / tokio handler bounds. Mutex overhead is
    /// negligible — `get_collection` is called only a handful
    /// of times per plan.
    pub(super) recorded_versions: Mutex<DescriptorVersionSet>,
    /// Node-wide sequence counters. `None` for adapters built without a
    /// `SharedState`; every sequence accessor then reports that sequence
    /// access is unavailable.
    pub(super) sequence_registry: Option<Arc<crate::control::sequence::SequenceRegistry>>,
    /// The calling session's `currval` map. `nextval` records into it and
    /// `currval` reads only from it, so one session never sees another
    /// session's allocation. `None` for planning with no session behind it.
    pub(super) session_sequences: Option<Arc<crate::control::sequence::SessionSequenceValues>>,
    /// Plans for a Describe, never for execution: sequence functions check
    /// their sequence exists and resolve to a typed placeholder, without
    /// advancing a counter or recording a session value.
    pub(super) describing: bool,
}

impl OriginCatalog {
    /// Construct an adapter that reads from the local redb
    /// catalog and records descriptor versions for the plan
    /// cache key. Lease acquisition happens in a separate,
    /// post-plan step — see
    /// `SharedState::acquire_plan_lease_scope`.
    /// Construct an adapter that reads from the local redb
    /// catalog WITHOUT drain observation. Used by internal
    /// sub-planners invoked inside a pgwire DDL handler
    /// whose outer query already holds leases through its
    /// `QueryLeaseScope`.
    pub fn new(
        credentials: Arc<CredentialStore>,
        array_catalog: crate::control::array_catalog::ArrayCatalogHandle,
        tenant_id: u64,
        database_id: DatabaseId,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials,
            tenant_id,
            database_id,
            retention_policy_registry,
            drain_tracker: None,
            recorded_versions: Mutex::new(DescriptorVersionSet::new()),
            array_catalog,
            sequence_registry: None,
            session_sequences: None,
            describing: false,
        }
    }

    /// Construct an adapter with drain observation. Used by
    /// the top-level pgwire dispatch so every user-initiated
    /// query's plan sees `RetryableSchemaChanged` when any
    /// descriptor it reads is being drained by an in-flight
    /// DDL; the pgwire handler's retry loop then re-plans.
    pub fn new_with_lease(
        shared: &Arc<SharedState>,
        tenant_id: u64,
        database_id: DatabaseId,
        retention_policy_registry: Option<
            Arc<crate::engine::timeseries::retention_policy::RetentionPolicyRegistry>,
        >,
    ) -> Self {
        Self {
            credentials: Arc::clone(&shared.credentials),
            tenant_id,
            database_id,
            retention_policy_registry,
            drain_tracker: Some(Arc::clone(&shared.lease_drain)),
            recorded_versions: Mutex::new(DescriptorVersionSet::new()),
            array_catalog: shared.array_catalog.clone(),
            sequence_registry: Some(Arc::clone(&shared.sequence_registry)),
            session_sequences: None,
            describing: false,
        }
    }

    /// Bind the node-wide sequence counters, so a column DEFAULT that calls
    /// `nextval` can allocate. For a planner built with [`OriginCatalog::new`]
    /// that plans rows outside a pgwire session.
    pub fn with_sequence_registry(
        mut self,
        registry: Arc<crate::control::sequence::SequenceRegistry>,
    ) -> Self {
        self.sequence_registry = Some(registry);
        self
    }

    /// Plan for a Describe only. Sequence functions keep their result type and
    /// their existence check, but advance nothing and record nothing, so
    /// describing a statement never consumes a value its execution would.
    pub fn describing(mut self) -> Self {
        self.describing = true;
        self
    }

    /// Bind the calling session's `currval` map to this adapter.
    pub fn with_session_sequences(
        mut self,
        values: Option<Arc<crate::control::sequence::SessionSequenceValues>>,
    ) -> Self {
        self.session_sequences = values;
        self
    }

    /// Drain the recorded descriptor-version set and return it.
    /// Callers capture this after planning finishes and use it
    /// as the plan cache key + freshness witness.
    pub fn take_recorded_versions(&self) -> DescriptorVersionSet {
        let mut guard = self
            .recorded_versions
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    /// Whether `collection` has a retention policy with AUTO_TIER on.
    /// Policies are keyed by policy name, so the lookup is by the
    /// collection the policy targets.
    pub(super) fn has_auto_tier(&self, collection: &str) -> bool {
        let registry = match &self.retention_policy_registry {
            Some(r) => r,
            None => return false,
        };
        registry
            .get_for_collection(self.database_id.as_u64(), self.tenant_id, collection)
            .is_some_and(|p| p.auto_tier)
    }
}
