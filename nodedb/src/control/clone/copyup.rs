// SPDX-License-Identifier: BUSL-1.1

//! Copy-up write helper for cloned collections.
//!
//! When an UPDATE targets a row that exists only in the source of a
//! `Shadowed` clone, this module performs the copy-up:
//!
//! 1. Allocate a fresh target surrogate.
//! 2. Write the source row to target with the fresh surrogate.
//! 3. Record `(target_collection, source_surrogate) → target_surrogate`
//!    in the `clone_copyups` catalog table.
//!
//! Steps 2-3 are performed inside the existing WAL group-commit boundary.

use std::time::Duration;

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::{Priority, Request, Status};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

/// Parameters for a KV copy-up operation.
pub struct KvCopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The KV key bytes (primary key).
    pub kv_key: Vec<u8>,
    /// Serialized KV value bytes (the stored value columns, msgpack).
    pub source_value_bytes: Vec<u8>,
}

/// Perform a KV copy-up: write `source_value_bytes` into target KV storage
/// under `kv_key`, making the row available for subsequent FieldSet or Delete
/// operations in the clone.
pub async fn perform_kv_clone_copyup(params: KvCopyUpParams<'_>) -> crate::Result<()> {
    let KvCopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        kv_key,
        source_value_bytes,
    } = params;

    let target_coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        target_db_id,
        target_collection,
    );

    // Allocate a surrogate for the target KV row.
    let surrogate = state
        .surrogate_assigner
        .assign(target_db_id, tenant_id, &target_coll_qualified, &kv_key)
        .map_err(|e| crate::Error::Storage {
            engine: "clone_kv_copyup".into(),
            detail: format!("surrogate alloc failed: {e}"),
        })?;

    let put_plan = PhysicalPlan::Kv(KvOp::Put {
        collection: nodedb_types::QualifiedCollection::new(target_db_id, target_collection),
        key: kv_key,
        value: source_value_bytes,
        ttl_ms: 0,
        surrogate,
        returning: None,
        rls_filters: Vec::new(),
        provenance: None,
    });

    let vshard_id = VShardId::from_collection_in_database(target_db_id, &target_coll_qualified);
    let req_id = RequestId::new(
        state
            .request_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    );

    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id: target_db_id,
        plan: put_plan,
        deadline: std::time::Instant::now() + deadline_dur,
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
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };

    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    };

    let resp = tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone_kv_copyup: response channel closed".into(),
        })?;

    if resp.status != Status::Ok {
        return Err(crate::Error::Storage {
            engine: "clone_kv_copyup".into(),
            detail: format!("Data Plane returned error status {:?}", resp.status),
        });
    }

    Ok(())
}

/// Parameters for a copy-up operation.
pub struct CopyUpParams<'a> {
    pub state: &'a SharedState,
    pub tenant_id: TenantId,
    pub target_db_id: DatabaseId,
    /// Plain (non-db_qualified) collection name.
    pub target_collection: &'a str,
    /// The source surrogate to copy up.
    pub source_surrogate: Surrogate,
    /// Serialized source row body (msgpack).  Must be obtained by the caller
    /// via a prior GET on the source collection.
    pub source_doc_id: String,
    pub source_row_bytes: Vec<u8>,
}

/// Perform a copy-up: write `source_row_bytes` into target storage with a
/// fresh surrogate and record the mapping in `clone_copyups`.
///
/// Returns the fresh target surrogate so the caller can apply the pending
/// UPDATE to it.
pub async fn perform_clone_copyup(params: CopyUpParams<'_>) -> crate::Result<Surrogate> {
    let CopyUpParams {
        state,
        tenant_id,
        target_db_id,
        target_collection,
        source_surrogate,
        source_doc_id,
        source_row_bytes,
    } = params;

    // Allocate a fresh target surrogate using the (collection, doc_id) key.
    let target_coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        target_db_id,
        target_collection,
    );
    let target_surrogate = state
        .surrogate_assigner
        .assign(
            target_db_id,
            tenant_id,
            &target_coll_qualified,
            source_doc_id.as_bytes(),
        )
        .map_err(|e| crate::Error::Storage {
            engine: "clone_copyup".into(),
            detail: format!("surrogate alloc failed: {e}"),
        })?;

    // Recorded BEFORE the KV put so the catalog is never less informed than
    // target storage; a failed put compensates by removing the mapping,
    // avoiding an orphaned target row that would cause duplicate surrogates.
    let catalog = state.credentials.catalog();

    // Keyed by the TARGET collection: every reader looks the mapping up
    // under the clone it belongs to.
    catalog
        .put_clone_copyup(
            &target_coll_qualified,
            source_surrogate.as_u32(),
            target_surrogate.as_u32(),
        )
        .map_err(|e| crate::Error::Storage {
            engine: "clone_copyup".into(),
            detail: format!("put_clone_copyup catalog write failed: {e}"),
        })?;

    // Rolls the catalog mapping back if a subsequent step fails. A failed
    // rollback is logged, not fatal: the read path treats a missing target
    // row as "fall through to source", same as no mapping at all.
    let rollback_mapping = |reason: &str| {
        if let Err(e) =
            catalog.delete_clone_copyup(&target_coll_qualified, source_surrogate.as_u32())
        {
            tracing::error!(
                target_collection = %target_coll_qualified,
                source_surrogate = source_surrogate.as_u32(),
                target_surrogate = target_surrogate.as_u32(),
                rollback_error = %e,
                trigger = reason,
                "clone_copyup: catalog rollback after put failure also failed; \
                 mapping will be reaped at next materialization sweep"
            );
        }
    };

    // Write the source row into target storage using PointPut.
    let put_plan = PhysicalPlan::Document(DocumentOp::PointPut {
        collection: nodedb_types::QualifiedCollection::new(target_db_id, target_collection),
        document_id: source_doc_id.clone(),
        value: source_row_bytes.clone(),
        surrogate: target_surrogate,
        pk_bytes: source_doc_id.as_bytes().to_vec(),
        // A copy-up is internal plumbing behind the caller's own statement; it
        // projects nothing and needs no read gate of its own.
        returning: None,
        rls_filters: Vec::new(),
        resolved_sum_targets: Vec::new(),
    });

    let vshard_id = VShardId::from_collection_in_database(target_db_id, &target_coll_qualified);
    let req_id = RequestId::new(
        state
            .request_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    );

    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id: target_db_id,
        plan: put_plan,
        deadline: std::time::Instant::now() + deadline_dur,
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
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    };

    let mut rx = state.tracker.register(req_id);
    let dispatch_outcome = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req),
        Err(p) => p.into_inner().dispatch(req),
    };
    if let Err(e) = dispatch_outcome {
        rollback_mapping("dispatch failed");
        return Err(e);
    }

    let resp = match tokio::time::timeout(deadline_dur, rx.recv()).await {
        Err(_) => {
            rollback_mapping("deadline exceeded");
            return Err(crate::Error::DeadlineExceeded { request_id: req_id });
        }
        Ok(None) => {
            rollback_mapping("response channel closed");
            return Err(crate::Error::Dispatch {
                detail: "clone_copyup: response channel closed".into(),
            });
        }
        Ok(Some(r)) => r,
    };

    if resp.status != Status::Ok {
        rollback_mapping("data plane returned non-Ok status");
        return Err(crate::Error::Storage {
            engine: "clone_copyup".into(),
            detail: format!("Data Plane returned error status {:?}", resp.status),
        });
    }

    Ok(target_surrogate)
}
