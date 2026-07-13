// SPDX-License-Identifier: BUSL-1.1

//! Shared `Request` construction for already-sequenced Calvin sub-operations.

use std::time::{Duration, Instant};

use super::scheduler::Scheduler;
use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request};
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;

impl Scheduler {
    /// Builds a `Request` for an already-sequenced Calvin sub-operation.
    ///
    /// Calvin has already globally ordered the transaction, so the request is
    /// admitted `Exempt(AlreadyOrdered)` — it bypasses admission control. Only
    /// `request_id`, `tenant_id`, `plan`, and `wal_lsn` vary between call sites;
    /// everything else (vshard, deadline, and the fixed metadata constants) is
    /// derived from `&self` or constant. `wal_lsn` is `Some` only when the caller
    /// has already durably appended the record this request applies.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn build_exempt_request(
        &self,
        request_id: RequestId,
        tenant_id: TenantId,
        plan: PhysicalPlan,
        wal_lsn: Option<Lsn>,
    ) -> Request {
        Request {
            request_id,
            tenant_id,
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(self.vshard_id),
            plan,
            // no-determinism: scheduler deadline controls waiting, not ordered state.
            deadline: Instant::now()
                + Duration::from_millis(
                    self.config.epoch_duration_ms * u64::from(self.config.txn_deadline_multiplier),
                ),
            priority: Priority::Normal,
            trace_id: nodedb_types::TraceId([0u8; 16]),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::AlreadyOrdered),
        }
    }
}
