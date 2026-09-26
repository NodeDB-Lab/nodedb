// SPDX-License-Identifier: BUSL-1.1

//! Tenant-snapshot capture/restore inspection methods on [`TestClusterNode`].
//!
//! These drive the two Data Plane handlers U6 changed
//! (`MetaOp::CreateTenantSnapshot` in `create.rs`, `MetaOp::RestoreTenantSnapshot`
//! in `restore.rs`) directly against a single node, without requiring a real
//! Raft `InstallSnapshot` round-trip.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::event::EventSource;
use nodedb::types::{DatabaseId, ReadConsistency, RequestId, TenantId, VShardId};
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::cluster_harness::node::lifecycle::TestClusterNode;

/// Monotonic request-id source for this file's harness-issued Data Plane
/// requests. `crdt.rs` already has its own `HARNESS_REQUEST_ID` starting at
/// `1 << 48`; rather than share that private static across files, this uses
/// an independently-seeded counter at a different high base (`1 << 49`) so
/// the two harness id spaces can never collide, without needing to widen
/// `crdt.rs`'s static to `pub(crate)`.
static SNAPSHOT_REQUEST_ID: AtomicU64 = AtomicU64::new(1 << 49);

impl TestClusterNode {
    /// Capture this node's tenant snapshot via `MetaOp::CreateTenantSnapshot`.
    ///
    /// Test hook for proving U6's capture path (`create.rs`): the returned
    /// bytes are a zerompk-encoded `nodedb::types::TenantDataSnapshot`. Since
    /// the harness runs one data core per node, the lone core's snapshot is
    /// the node's full tenant snapshot. Routed to the `"__system"` vshard
    /// (this op is not collection-scoped). Returns an empty `Vec` on dispatch
    /// error or non-Ok response.
    pub async fn create_tenant_snapshot(&self, tenant: TenantId) -> Vec<u8> {
        let request_id = RequestId::new(SNAPSHOT_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(vshard_for_collection(DatabaseId::DEFAULT, "__system"));
        let request = Request {
            request_id,
            tenant_id: tenant,
            database_id: DatabaseId::DEFAULT,
            vshard_id,
            plan: PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
                tenant_id: tenant.as_u64(),
                cut_watermark: None,
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
            admission: nodedb::bridge::envelope::Admission::Exempt(
                nodedb::bridge::envelope::ExemptReason::Read,
            ),
        };

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
        response.payload.as_bytes().to_vec()
    }

    /// Restore a captured tenant snapshot into this node's engines via
    /// `MetaOp::RestoreTenantSnapshot`.
    ///
    /// Test hook for proving U6's restore path (`restore.rs`). Mirrors the
    /// production `DataPlaneSnapshotApplier` dispatch shape: `tenant_id: 0`
    /// is only the routing/dispatch key (the merged Raft snapshot applies
    /// with dispatch tenant 0), and `replace_mode: true` because this
    /// simulates a Raft `InstallSnapshot` apply, which must overwrite local
    /// state rather than fail against it. The restore handler installs each
    /// snapshot entry (including `crdt_constraints`) by its own
    /// tenant-explicit fields, independent of the dispatch tenant. Routed to
    /// the `"__system"` vshard. Returns `true` iff the response status is
    /// `Ok`.
    pub async fn restore_tenant_snapshot(&self, snapshot_bytes: Vec<u8>) -> bool {
        let request_id = RequestId::new(SNAPSHOT_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let vshard_id = VShardId::new(vshard_for_collection(DatabaseId::DEFAULT, "__system"));
        let request = Request {
            request_id,
            tenant_id: TenantId::new(0),
            database_id: DatabaseId::DEFAULT,
            vshard_id,
            plan: PhysicalPlan::Meta(MetaOp::RestoreTenantSnapshot {
                tenant_id: 0,
                snapshot: snapshot_bytes,
                replace_mode: true,
                clear_vshards: Vec::new(),
                collections_to_clear: Vec::new(),
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
            admission: nodedb::bridge::envelope::Admission::Exempt(
                nodedb::bridge::envelope::ExemptReason::Read,
            ),
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
}
