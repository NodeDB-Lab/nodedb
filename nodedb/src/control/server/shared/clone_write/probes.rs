// SPDX-License-Identifier: BUSL-1.1

//! Shared Data-Plane read helpers used by both the Document and KV clone
//! write-interception paths: presence probes on the target collection, and
//! source-row/value fetches for copy-up.

use std::time::Duration;

use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId};

use crate::bridge::envelope::{Priority, Request, Response, Status};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};

/// The clone collection a presence probe reads, and the transaction it runs
/// in.
#[derive(Clone, Copy)]
pub(super) struct ProbeTarget<'a> {
    pub tenant_id: TenantId,
    pub db_id: DatabaseId,
    pub collection_qualified: &'a str,
    /// The transaction whose overlay the probe reads, `None` outside one.
    pub txn_id: Option<TxnId>,
}

/// Probe whether `document_id` exists in target storage.
///
/// Issues a synchronous PointGet to the local Data Plane and returns `true`
/// if the row is present. A key with no target binding (`None`) names no
/// target row, so it is absent without a read. Inside transaction `txn_id`
/// the probe reads the transaction's overlay: a row it staged is present,
/// and a row it deleted is absent.
pub(super) async fn probe_row_in_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    target: ProbeTarget<'_>,
    document_id: &str,
    surrogate: Option<Surrogate>,
) -> crate::Result<bool> {
    if surrogate.is_none() {
        return Ok(false);
    }
    let ProbeTarget {
        tenant_id,
        db_id,
        collection_qualified,
        txn_id,
    } = target;
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::from_stored(collection_qualified.to_string()),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, db_id, plan)?;
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(db_id, collection_qualified)?.vshard();
    let resp = dispatch_data_plane_raw(
        state,
        RawTarget {
            tenant_id,
            vshard_id,
            database_id: db_id,
            txn_id,
        },
        plan,
    )
    .await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw msgpack bytes for a row from the source collection.
///
/// Returns `None` when the row is absent in source (PointGet returned empty).
pub(super) async fn fetch_source_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: QualifiedCollection::from_stored(source_coll_qualified.to_string()),
        document_id: document_id.to_string(),
        surrogate: Some(surrogate),
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, source_db_id, plan)?;
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(source_db_id, source_coll_qualified)?
            .vshard();
    let resp = dispatch_data_plane_raw(
        state,
        RawTarget {
            tenant_id,
            vshard_id,
            database_id: source_db_id,
            txn_id: None,
        },
        plan,
    )
    .await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Probe whether `kv_key` exists in target KV storage.
///
/// Issues a KvOp::Get to the local Data Plane and returns `true` if the key
/// is present. Inside a transaction the probe reads its overlay.
pub(super) async fn probe_kv_key_in_target(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    target: ProbeTarget<'_>,
    kv_key: &[u8],
) -> crate::Result<bool> {
    let ProbeTarget {
        tenant_id,
        db_id,
        collection_qualified,
        txn_id,
    } = target;
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::from_stored(collection_qualified.to_string()),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Internal probe of the clone's own target collection — never
        // delegated to source, so no isolation ceiling applies.
        surrogate_ceiling: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, db_id, plan)?;
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(db_id, collection_qualified)?.vshard();
    let resp = dispatch_data_plane_raw(
        state,
        RawTarget {
            tenant_id,
            vshard_id,
            database_id: db_id,
            txn_id,
        },
        plan,
    )
    .await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw value bytes for a KV row from the source collection.
///
/// Returns `None` when the key is absent in source (KvOp::Get returned empty).
pub(super) async fn fetch_kv_source_value(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    kv_key: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: QualifiedCollection::from_stored(source_coll_qualified.to_string()),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Copy-up reads must see every binding in the source — the
        // post-copy target write reflects the latest source state, and
        // a missed source row will silently drop data on the clone.
        surrogate_ceiling: None,
    });
    let plan = with_caller_rls(state, identity, tenant_id, source_db_id, plan)?;
    let vshard_id =
        nodedb_types::CollectionKey::from_qualified_str(source_db_id, source_coll_qualified)?
            .vshard();
    let resp = dispatch_data_plane_raw(
        state,
        RawTarget {
            tenant_id,
            vshard_id,
            database_id: source_db_id,
            txn_id: None,
        },
        plan,
    )
    .await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Apply the requesting principal's row-level security to a probe plan.
///
/// Copy-up reads a row out of the source collection and writes it into the
/// clone, where the source's policies no longer govern it. Without this the
/// clone will launder policy-excluded rows into readable ones. Presence probes
/// carry the same filters so a row the caller cannot see is not reported as
/// present either.
///
/// `database_id` is the database the probe plan actually targets (the
/// source database for copy-up reads, the clone's own database for presence
/// probes) — `RequestAuthScope::for_database` stamps it into `$auth.database_id`
/// in lockstep with the scalar so a database-scoped RLS predicate evaluates
/// against the database this probe reads, not the caller's session default.
fn with_caller_rls(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    mut plan: PhysicalPlan,
) -> crate::Result<PhysicalPlan> {
    let scope = crate::control::security::request_scope::RequestAuthScope::for_database(
        identity,
        state.auth_stores(),
        database_id,
    );
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        database_id,
        &mut plan,
        &state.rls,
        state.credentials.catalog(),
        scope.auth(),
    )?;
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        database_id,
        scope.auth(),
        &state.redaction,
    )?;
    Ok(plan)
}

/// Where [`dispatch_data_plane_raw`] sends a plan.
pub(super) struct RawTarget {
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    /// The transaction whose overlay a read consults, `None` outside one.
    pub txn_id: Option<TxnId>,
}

/// Dispatch a plan directly to the local Data Plane, bypassing WAL and Raft.
/// Used for the read probes and the autocommit KV delete inside the clone
/// write helper.
pub(super) async fn dispatch_data_plane_raw(
    state: &SharedState,
    target: RawTarget,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let RawTarget {
        tenant_id,
        vshard_id,
        database_id,
        txn_id,
    } = target;
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
        database_id,
        plan,
        deadline: std::time::Instant::now() + deadline_dur,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id,
        wal_lsn: None,
        resolved_now_ms: None,
        commit_hlc: None,
        entry_version: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ),
    };
    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    }
    tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone write probe: response channel closed".into(),
        })
}
