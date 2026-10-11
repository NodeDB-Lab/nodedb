// SPDX-License-Identifier: BUSL-1.1

//! Local timeseries reads on [`TestClusterNode`]: the rows this node's own
//! replica stores, read from its Data Plane without routing to a leader.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb::bridge::envelope::{Priority, Request, Status};
use nodedb::event::EventSource;
use nodedb::types::{DatabaseId, ReadConsistency, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp, UNBOUNDED_TIME_RANGE};

use crate::cluster_harness::node::lifecycle::TestClusterNode;

/// Request ids for this file, on a base no other harness file uses.
static TIMESERIES_REQUEST_ID: AtomicU64 = AtomicU64::new(1 << 51);

/// The most rows one local scan returns. A test collection holds far fewer.
const LOCAL_SCAN_LIMIT: usize = 100_000;

impl TestClusterNode {
    /// Every row of the timeseries `collection` in the default database of
    /// `tenant` that this node's own replica stores, memtable and flushed
    /// partitions both, as JSON objects. The scan runs on the collection's
    /// home core of this node and never leaves it. It returns at most
    /// `LOCAL_SCAN_LIMIT` rows.
    ///
    /// Panics with the scan's status when the core answers with an error.
    pub async fn timeseries_rows_local(
        &self,
        tenant: TenantId,
        collection: &str,
    ) -> Vec<serde_json::Value> {
        let request_id = RequestId::new(TIMESERIES_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
        let vshard =
            nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard();
        let request = Request {
            request_id,
            tenant_id: tenant,
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(vshard.as_u32()),
            plan: PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                time_range: UNBOUNDED_TIME_RANGE,
                projection: Vec::new(),
                limit: LOCAL_SCAN_LIMIT,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
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
        let core_id = self.home_core_of(collection);
        let mut rx = self.shared.tracker.register(request_id);
        self.shared
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)
            .expect("dispatch the local timeseries scan");
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv())
            .await
            .expect("the local timeseries scan answers within 10s")
            .expect("the local timeseries scan answers");
        assert_eq!(
            response.status,
            Status::Ok,
            "node {} local scan of '{collection}' failed: {:?}",
            self.node_id,
            response.error_code
        );
        let json = nodedb::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        match sonic_rs::from_str::<serde_json::Value>(&json) {
            Ok(serde_json::Value::Array(rows)) => rows,
            Ok(serde_json::Value::Null) => Vec::new(),
            Ok(other) => vec![other],
            Err(e) => panic!(
                "node {} local scan payload is not JSON: {e}: {json}",
                self.node_id
            ),
        }
    }
}
