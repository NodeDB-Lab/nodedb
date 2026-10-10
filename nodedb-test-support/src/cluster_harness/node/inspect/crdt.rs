// SPDX-License-Identifier: BUSL-1.1

//! CRDT constraint inspection methods on [`TestClusterNode`].

use std::sync::atomic::{AtomicU64, Ordering};

use crate::cluster_harness::node::lifecycle::TestClusterNode;

/// Monotonic request-id source for harness-issued Data Plane requests. Started
/// high so harness ids never collide with the small ids the pgwire client path
/// assigns through the same per-node request tracker.
static HARNESS_REQUEST_ID: AtomicU64 = AtomicU64::new(1 << 48);

impl TestClusterNode {
    /// Read the CRDT constraint set installed in THIS node's local per-core
    /// validator for `(tenant, collection)`.
    ///
    /// Dispatches a read-only `CrdtOp::ReadConstraints` plan to the local data
    /// core that homes the collection's vshard (the same core the
    /// `ConstraintChange` apply installed the set into) and decodes the
    /// zerompk-encoded `Vec<Constraint>` from the response payload. Unlike the
    /// catalog-backed inspectors, this reads the validator itself — proving the
    /// constraint was actually installed, not merely that the catalog row
    /// replicated. Returns an empty vec on dispatch error or non-Ok response.
    pub async fn crdt_constraints(
        &self,
        tenant: nodedb_types::TenantId,
        collection: &str,
    ) -> Vec<nodedb_crdt::Constraint> {
        use nodedb::bridge::envelope::{Priority, Request, Status};
        use nodedb::event::EventSource;
        use nodedb::types::{DatabaseId, ReadConsistency, RequestId, VShardId};
        use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};

        let request_id = RequestId::new(HARNESS_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(nodedb_cluster::routing::vshard_for_collection(
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection),
        ));
        let request = Request {
            request_id,
            tenant_id: tenant,
            database_id: DatabaseId::DEFAULT,
            vshard_id,
            plan: PhysicalPlan::Crdt(CrdtOp::ReadConstraints {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            }),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: nodedb::bridge::envelope::Admission::Exempt(
                nodedb::bridge::envelope::ExemptReason::Read,
            ),
        };

        // Register for response routing before dispatching, then submit through
        // the same SPSC bridge + response-poller path the session uses.
        let mut rx = self.shared.tracker.register(request_id);
        let dispatched = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if dispatched.is_err() {
            return Vec::new();
        }

        let response =
            match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
                Ok(Some(resp)) => resp,
                _ => return Vec::new(),
            };
        if response.status != Status::Ok {
            return Vec::new();
        }
        zerompk::from_msgpack::<Vec<nodedb_crdt::Constraint>>(response.payload.as_bytes())
            .unwrap_or_default()
    }

    /// Clear a collection's installed CRDT constraint set from THIS node's
    /// local per-core validator.
    ///
    /// Test hook used to prove snapshot RESTORE reinstalls a constraint set:
    /// a test captures a snapshot while a constraint is installed, drops it
    /// via this method, then restores the snapshot and asserts the
    /// constraint reappears in `crdt_constraints`. Dispatches
    /// `CrdtOp::DropConstraints` to the local data core that homes the
    /// collection's vshard. Returns `true` iff the response status is `Ok`.
    pub async fn crdt_drop_constraints(
        &self,
        tenant: nodedb_types::TenantId,
        collection: &str,
        constraint_version: u64,
    ) -> bool {
        use nodedb::bridge::envelope::{Priority, Request, Status};
        use nodedb::event::EventSource;
        use nodedb::types::{DatabaseId, ReadConsistency, RequestId, VShardId};
        use nodedb_physical::physical_plan::{CrdtOp, PhysicalPlan};

        let request_id = RequestId::new(HARNESS_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(nodedb_cluster::routing::vshard_for_collection(
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection),
        ));
        let request = Request {
            request_id,
            tenant_id: tenant,
            database_id: DatabaseId::DEFAULT,
            vshard_id,
            plan: PhysicalPlan::Crdt(CrdtOp::DropConstraints {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                constraint_version,
            }),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            entry_version: None,
            admission: nodedb::bridge::envelope::Admission::Admitted,
        };

        let mut rx = self.shared.tracker.register(request_id);
        let dispatched = match self.shared.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };
        if dispatched.is_err() {
            return false;
        }

        let response =
            match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
                Ok(Some(resp)) => resp,
                _ => return false,
            };
        response.status == Status::Ok
    }

    /// Drive ONE constraint-reconcile pass on this node (no-op unless this node
    /// is the metadata leader). Uses a fresh delivered-map so every changed
    /// collection is re-proposed, letting a test install constraints on demand
    /// instead of waiting on the background reconcile timer. Returns the number
    /// of proposals accepted.
    pub async fn run_constraint_reconcile_once(&self) -> usize {
        let mut delivered = std::collections::HashMap::new();
        nodedb::bootstrap::constraint_reconcile::reconcile_once(&self.shared, &mut delivered).await
    }
}
