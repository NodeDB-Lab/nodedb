// SPDX-License-Identifier: BUSL-1.1

//! KV push dispatch for sync sessions.
//!
//! A Lite `KvPushMsg` becomes a `KvOp::Put` or `KvOp::Delete` carrying the
//! frame's sync provenance. It takes the durable route every client KV write
//! takes, tagged `EventSource::CrdtSync`:
//! - the funnel appends its redo record and a `SyncSeqAdvance` record under
//!   one outcome-floor window;
//! - a clustered write is proposed through Raft, and every replica gates it
//!   on the same stream mark;
//! - the Data Plane's sync gate answers the frame's ack.
//!
//! The session handler lives in `kv_session.rs`.

use async_trait::async_trait;

use nodedb_physical::physical_plan::KvOp;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::{QualifiedCollection, RlsWriteCheck};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use crate::event::EventSource;
use crate::types::{DatabaseId, TenantId, TraceId};

/// The write a KV push applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvPushWriteOp {
    /// Store `body` at the key. `ttl_ms` is `0` for an entry that never
    /// expires.
    Put { body: Vec<u8>, ttl_ms: u64 },
    /// Remove the key.
    Delete,
}

/// One KV push, ready to dispatch.
#[derive(Debug, Clone)]
pub struct KvPushWrite {
    pub collection: String,
    pub key: Vec<u8>,
    pub op: KvPushWriteOp,
    pub provenance: SyncProvenance,
}

/// Encapsulates the dispatch of a KV push.
#[async_trait]
pub trait KvPushDispatcher: Send + Sync {
    /// Apply `write` and return the Data Plane's `SyncAckResult` payload.
    async fn apply(&self, tenant_id: TenantId, write: KvPushWrite) -> crate::Result<Vec<u8>>;

    /// Move the stream mark past a frame Origin refused before it reached
    /// the Data Plane, so the producer's next frame is not a gap.
    async fn skip(
        &self,
        tenant_id: TenantId,
        collection: &str,
        provenance: SyncProvenance,
    ) -> crate::Result<Vec<u8>>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: runs admission, authorization and row-level
/// security, then the durable write route.
pub struct SharedStateKvDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
    /// The session's remote address, for the blacklist and risk checks.
    pub(crate) peer_addr: &'a str,
}

#[async_trait]
impl KvPushDispatcher for SharedStateKvDispatcher<'_> {
    async fn apply(&self, tenant_id: TenantId, write: KvPushWrite) -> crate::Result<Vec<u8>> {
        let identity = self.identity.ok_or_else(|| crate::Error::RejectedAuthz {
            tenant_id,
            resource: "authenticated sync identity required".into(),
        })?;
        let database_id = self.database_id;
        let request = crate::control::security::request_scope::ClientRequestScope::for_database(
            identity,
            self.shared.auth_stores(),
            database_id,
            self.peer_addr,
        )?;
        crate::control::server::session_auth::check_blacklist_and_status(self.shared, &request)?;
        self.shared.check_tenant_quota(tenant_id)?;

        let KvPushWrite {
            collection,
            key,
            op,
            provenance,
        } = write;
        let qualified = QualifiedCollection::new(database_id, &collection);
        let mut plan = match op {
            KvPushWriteOp::Put { body, ttl_ms } => {
                let surrogate =
                    crate::control::server::surrogate_exchange::assign_surrogate_routed(
                        self.shared,
                        nodedb_types::CollectionKey::from_bare(database_id, &collection),
                        tenant_id,
                        &key,
                        crate::types::TraceId::ZERO,
                    )
                    .await?;
                PhysicalPlan::Kv(KvOp::Put {
                    collection: qualified,
                    // Resolved from the catalog before the write is journaled.
                    declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::pending(),
                    key: key.clone(),
                    value: body,
                    ttl_ms,
                    surrogate,
                    returning: None,
                    rls_filters: Vec::new(),
                    provenance: Some(provenance.clone()),
                })
            }
            KvPushWriteOp::Delete => PhysicalPlan::Kv(KvOp::Delete {
                collection: qualified,
                keys: vec![key.clone()],
                rls_write_check: RlsWriteCheck::pending_injection(),
                returning: None,
                rls_filters: Vec::new(),
                provenance: Some(provenance.clone()),
            }),
        };
        crate::control::planner::rls_injection::inject_rls_for_single_plan(
            tenant_id.as_u64(),
            database_id,
            &mut plan,
            &self.shared.rls,
            self.shared.credentials.catalog(),
            request.scope().auth(),
        )?;

        let task = nodedb_physical::physical_task::PhysicalTask {
            tenant_id,
            vshard_id: nodedb_types::CollectionKey::from_bare(database_id, &collection).vshard(),
            database_id,
            plan,
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        };
        let emitter = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &self.shared.audit,
        ));
        let checked = match intercept_and_authorize(InterceptAndAuthorizeParams {
            state: self.shared,
            task,
            identity,
            tenant_id,
            permissions: &self.shared.permissions,
            roles: &self.shared.roles,
            emitter: &emitter,
        })
        .await?
        {
            CloneCheckedOutcome::Proceed(checked) => checked,
            // The clone copy-up path applied the write itself, outside the
            // gate. The frame still takes its sequence.
            CloneCheckedOutcome::Handled(response) => {
                crate::control::server::shared::response_payload::payload_or_typed_error(response)?;
                return self.skip(tenant_id, &collection, provenance).await;
            }
        };
        let response =
            crate::control::server::dispatch_utils::dispatch_authorized_durable_write_with_source(
                self.shared,
                checked,
                TraceId::ZERO,
                EventSource::CrdtSync,
            )
            .await?;
        crate::control::server::shared::response_payload::payload_or_typed_error(response)
    }

    async fn skip(
        &self,
        tenant_id: TenantId,
        collection: &str,
        provenance: SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        let database_id = self.database_id;
        // A delete of no keys applies nothing. Its provenance moves the mark,
        // and the replicated apply's record makes the move durable.
        let plan = PhysicalPlan::Kv(KvOp::Delete {
            collection: QualifiedCollection::new(database_id, collection),
            keys: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
            provenance: Some(provenance),
        });
        let response = super::raft_dispatch::dispatch_trusted_internal_sync_response(
            self.shared,
            tenant_id,
            database_id,
            nodedb_types::CollectionKey::from_bare(database_id, collection).vshard(),
            plan,
            EventSource::CrdtSync,
        )
        .await?;
        crate::control::server::shared::response_payload::payload_or_typed_error(response)
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable.
pub struct NoOpKvDispatcher;

#[async_trait]
impl KvPushDispatcher for NoOpKvDispatcher {
    async fn apply(&self, _tenant_id: TenantId, _write: KvPushWrite) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("kv push"))
    }

    async fn skip(
        &self,
        _tenant_id: TenantId,
        _collection: &str,
        _provenance: SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("kv push skip"))
    }
}
