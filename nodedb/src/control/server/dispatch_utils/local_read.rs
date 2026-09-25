// SPDX-License-Identifier: BUSL-1.1

//! Read-only dispatch of an internal scan to this node's own Data Plane.
//!
//! A caller that must never re-enter the write funnel reads through here.
//! [`LocalRead`] can express only reads, so no write plan can reach a core
//! through this path, and no function here calls `submit_write`. Holding a
//! write's acknowledgement open can therefore never wait on a request that
//! passes through the write path again.
//!
//! The request carries `Admission::Exempt(Read)`: a read never takes the
//! write fence. It reads local state on the core that homes the vShard.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_types::{QualifiedCollection, SystemTimeScope};

use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Response};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::collect::{DeadlineCollect, collect_under_deadline};

/// A read an internal caller issues against its own node.
///
/// Every variant is a read. A write is not representable.
pub(crate) enum LocalRead {
    /// Every current row of one document collection.
    DocumentScan { collection: QualifiedCollection },
}

impl LocalRead {
    fn into_plan(self) -> PhysicalPlan {
        match self {
            LocalRead::DocumentScan { collection } => PhysicalPlan::Document(DocumentOp::Scan {
                collection,
                filters: Vec::new(),
                limit: usize::MAX,
                offset: 0,
                sort_keys: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            }),
        }
    }
}

/// Dispatch `read` to the core that homes `vshard_id` and collect its
/// bounded response before the request deadline.
pub(crate) async fn dispatch_local_read(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    read: LocalRead,
) -> crate::Result<Response> {
    let deadline =
        Instant::now() + Duration::from_secs(shared.tuning.network.default_deadline_secs);
    let request_id = shared.next_request_id();
    let request = Request {
        request_id,
        tenant_id,
        database_id,
        vshard_id,
        plan: read.into_plan(),
        deadline,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: Admission::Exempt(ExemptReason::Read),
    };

    let mut rx = shared.tracker.register(request_id);
    let dispatched = match shared.dispatcher.lock() {
        Ok(mut dispatcher) => dispatcher.dispatch(request),
        Err(poisoned) => poisoned.into_inner().dispatch(request),
    };
    if let Err(error) = dispatched {
        // No response will ever arrive for a refused request.
        shared.tracker.cancel(&request_id);
        return Err(error);
    }
    let collected = collect_under_deadline(
        &mut rx,
        DeadlineCollect {
            request_id,
            deadline,
            max_result_bytes: shared.tuning.network.max_query_result_bytes as usize,
            context: "internal local read",
        },
    )
    .await;
    if collected.is_err() {
        shared.tracker.cancel(&request_id);
    }
    collected
}
