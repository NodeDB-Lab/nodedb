// SPDX-License-Identifier: BUSL-1.1

//! A one-node server with its Data Plane core, and the write and read steps
//! the WAL catch-up cases drive it with.

use std::sync::Arc;

use nodedb::control::gateway::core::QueryContext;
use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::server::shared::clone_write::{
    CloneCheckedOutcome, CloneCheckedTask, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use nodedb::control::state::SharedState;
use nodedb::types::*;
use nodedb::wal::manager::{NO_APPLY_KEY, WalManager};
use nodedb_physical::physical_plan::{PhysicalPlan, TimeseriesOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_test_support::pgwire_harness::TestServer;

pub(super) fn ilp_payload(collection: &str, count: usize, start_ts_ns: i64) -> Vec<u8> {
    let mut lines = String::new();
    let qtypes = ["A", "AAAA", "MX", "CNAME"];
    for i in 0..count {
        let ts_ns = start_ts_ns + i as i64 * 1_000_000;
        let qtype = qtypes[i % qtypes.len()];
        lines.push_str(&format!(
            "{collection},qtype={qtype} elapsed_ms={}.0 {ts_ns}\n",
            i % 1000
        ));
    }
    lines.into_bytes()
}

/// The `TimeseriesBatch` WAL payload a live ILP ingest of `payload` into
/// `collection` writes: the seven-element tuple `("timeseries", collection,
/// payload, provenance, format, default_timestamp_ms, rls_write_check)`. No
/// RLS policy exists on these collections.
pub(super) fn ilp_wal_payload(collection: &str, payload: Vec<u8>) -> Vec<u8> {
    zerompk::to_msgpack_vec(&(
        "timeseries".to_string(),
        collection.to_string(),
        payload,
        None::<nodedb_types::sync::wire::SyncProvenance>,
        "ilp".to_string(),
        1_700_000_000_000i64,
        nodedb_types::RlsWriteCheck::NoPolicyApplies,
    ))
    .unwrap()
}

/// A one-node server with its Data Plane core, response poller and Raft
/// groups, as production boots it.
pub(super) struct TestStack {
    pub(super) shared: Arc<SharedState>,
    pub(super) wal: Arc<WalManager>,
    _server: TestServer,
}

impl TestStack {
    pub(super) async fn new() -> Self {
        let server = TestServer::start().await;
        Self {
            shared: Arc::clone(&server.shared),
            wal: Arc::clone(&server.shared.wal),
            _server: server,
        }
    }

    /// `plan` on `collection`, clone-checked and authorized for the superuser.
    async fn checked(&self, plan: PhysicalPlan, collection: &str) -> CloneCheckedTask {
        let tenant_id = TenantId::new(1);
        let task = PhysicalTask {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection)
                .vshard(),
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };
        let identity = nodedb_test_support::pgwire_auth_helpers::superuser();
        match intercept_and_authorize(InterceptAndAuthorizeParams {
            state: &self.shared,
            task,
            identity: &identity,
            tenant_id,
            permissions: &self.shared.permissions,
            roles: &self.shared.roles,
            emitter: &NoopAuditEmitter,
        })
        .await
        .expect("clone-check and authorize test task")
        {
            CloneCheckedOutcome::Proceed(checked) => checked,
            CloneCheckedOutcome::Handled(_) => {
                panic!("a timeseries task must not be clone-intercepted")
            }
        }
    }

    /// Run a read on this node's core and decode its answer.
    async fn dispatch(&self, plan: PhysicalPlan, collection: &str) -> serde_json::Value {
        let checked = self.checked(plan, collection).await;
        let resp = nodedb::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            &self.shared,
            checked,
            TraceId::ZERO,
        )
        .await
        .expect("dispatch failed");
        let json_str =
            nodedb::data::executor::response_codec::decode_payload_to_json(&resp.payload);
        serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
    }

    /// Ingest `payload`, ILP lines, into `collection` as a live write: the
    /// gateway proposes it and it applies through its replicated entry, which
    /// appends its own WAL record. Returns the apply's decoded answer.
    pub(super) async fn write_replicated(
        &self,
        collection: &str,
        payload: Vec<u8>,
    ) -> serde_json::Value {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            payload,
            format: "ilp".to_string(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            // No RLS policy exists on this collection.
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        let checked = self.checked(plan, collection).await;
        let gateway = nodedb::control::gateway::Gateway::new(Arc::clone(&self.shared));
        let payloads = gateway
            .execute(
                &QueryContext {
                    tenant_id: TenantId::new(1),
                    trace_id: TraceId::ZERO,
                    database_id: DatabaseId::DEFAULT,
                    txn_id: None,
                    linearizable: false,
                },
                checked,
            )
            .await
            .expect("replicated timeseries write");
        let json_str = payloads
            .first()
            .map(|payload| nodedb::data::executor::response_codec::decode_payload_to_json(payload))
            .unwrap_or_default();
        serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
    }

    pub(super) async fn query_count(&self, collection: &str) -> u64 {
        let resp = self
            .dispatch(
                PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                    collection: nodedb_types::QualifiedCollection::new(
                        nodedb_types::DatabaseId::DEFAULT,
                        collection,
                    ),
                    time_range: (0, i64::MAX),
                    projection: Vec::new(),
                    limit: usize::MAX,
                    filters: Vec::new(),
                    sort_keys: Vec::new(),
                    bucket_interval_ms: 0,
                    group_by: Vec::new(),
                    aggregates: vec![("count".into(), "*".into())],
                    gap_fill: String::new(),
                    rls_filters: Vec::new(),
                    system_time: nodedb_types::SystemTimeScope::Current,
                    valid_at_ms: None,
                    computed_columns: Vec::new(),
                    having: Vec::new(),
                    tiering: None,
                    output_columns: Vec::new(),
                }),
                collection,
            )
            .await;
        resp.as_array()
            .and_then(|a| a.first())
            .and_then(|r| r["count(*)"].as_u64())
            .unwrap_or(0)
    }

    /// Append `payload` to the WAL alone, as a dispatch the Data Plane never
    /// received.
    pub(super) fn write_to_wal(&self, collection: &str, payload: Vec<u8>) {
        let wal_payload = ilp_wal_payload(collection, payload);
        self.wal
            .appender(NO_APPLY_KEY)
            .with_event_source(nodedb::event::EventSource::User)
            .append_timeseries_batch(
                TenantId::new(1),
                nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard(),
                DatabaseId::DEFAULT,
                &wal_payload,
            )
            .unwrap();
        self.wal.sync().unwrap();
    }
}
