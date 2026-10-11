// SPDX-License-Identifier: BUSL-1.1

//! Per-core row placement on [`TestClusterNode`]: which Data-Plane core holds
//! which documents, and which core a collection routes to.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::event::EventSource;
use nodedb::types::{DatabaseId, ReadConsistency, RequestId, TenantDataSnapshot, TenantId};
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};

use crate::cluster_harness::node::lifecycle::TestClusterNode;

/// Request ids for this file, on a base no other harness file uses.
static PLACEMENT_REQUEST_ID: AtomicU64 = AtomicU64::new(1 << 50);

impl TestClusterNode {
    /// Number of Data-Plane cores on this node.
    pub fn num_cores(&self) -> usize {
        self.shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .num_cores()
    }

    /// The core that reads and writes of `collection` in the default
    /// database route to.
    pub fn home_core_of(&self, collection: &str) -> usize {
        let vshard =
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard();
        self.shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .router()
            .resolve(vshard)
            .expect("every vshard routes to a core")
    }

    /// The document keys of `tenant` in the default database that core
    /// `core_id` stores, from that core's own tenant snapshot.
    pub async fn document_keys_on_core(&self, core_id: usize, tenant: TenantId) -> Vec<String> {
        self.tenant_snapshot_on_core(core_id, tenant)
            .await
            .documents
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    }

    /// The tenant snapshot of `tenant` in the default database that core
    /// `core_id` of this node holds. The read never leaves this node, so it
    /// shows this replica's own stored rows. Panics when the core answers
    /// with an error.
    pub async fn tenant_snapshot_on_core(
        &self,
        core_id: usize,
        tenant: TenantId,
    ) -> TenantDataSnapshot {
        let request_id = RequestId::new(PLACEMENT_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let request = Request {
            request_id,
            tenant_id: tenant,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb::types::VShardId::new(core_id as u32),
            plan: PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
                tenant_id: tenant.as_u64(),
                cut_watermark: None,
                cut_capture: None,
                arrays: false,
            }),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(10),
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
        let mut rx = self.shared.tracker.register(request_id);
        self.shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)
            .expect("dispatch the per-core snapshot");
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("core answers within 10s")
            .expect("response channel open");
        assert_eq!(
            response.status,
            Status::Ok,
            "core {core_id} snapshot failed"
        );
        zerompk::from_msgpack(response.payload.as_bytes()).expect("core snapshot decodes")
    }
}
