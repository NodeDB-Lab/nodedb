// SPDX-License-Identifier: BUSL-1.1

//! Request builders shared by the dispatcher's unit tests.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::DocumentOp;

use crate::bridge::envelope;
use crate::bridge::envelope::*;
use crate::types::*;

/// A read request with id 1 for tenant 1 in the default database on `vshard`.
pub(super) fn make_request(vshard: u32) -> envelope::Request {
    envelope::Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(vshard),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "users"),
            document_id: "u1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        deadline: Instant::now() + Duration::from_secs(5),
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
    }
}

/// A read request with id `req_id` for tenant 1 in database `db` on `vshard`.
pub(super) fn make_request_for_db(vshard: u32, db: u64, req_id: u64) -> envelope::Request {
    envelope::Request {
        request_id: RequestId::new(req_id),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::new(db),
        vshard_id: VShardId::new(vshard),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::new(db), "c"),
            document_id: "d".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        deadline: Instant::now() + Duration::from_secs(5),
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
    }
}
