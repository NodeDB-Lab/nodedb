// SPDX-License-Identifier: BUSL-1.1

//! FTS index/delete handler for sync sessions.
//!
//! Decodes `FtsIndexMsg` / `FtsDeleteMsg` from a Lite client,
//! allocates a surrogate for the document ID via `SurrogateAssigner`,
//! proposes `TextOp::FtsIndexDoc` / `TextOp::FtsDeleteDoc` through Raft (the
//! replicated apply journals the write), and returns an ACK frame carrying
//! the `SyncAckResult` from the gate.
//!
//! Handler methods (`handle_fts_index` / `handle_fts_delete`) live in the
//! sibling `fts_session.rs`.
//!
//! Structural pattern mirrors `vector_handler.rs`.

use async_trait::async_trait;

use nodedb_types::Surrogate;

use crate::control::server::dispatch_utils::RecordOwner;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates async Data Plane dispatch for FTS index/delete.
///
/// Returns the raw `Response.payload` bytes so the handler can decode the
/// [`SyncAckResult`] for gate status propagation to the Lite client.
#[async_trait]
pub trait FtsDispatcher: Send + Sync {
    /// Index a document's `(field, text)` pairs on the Data Plane. Empty
    /// `fields` remove the document from every index.
    async fn dispatch_index(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Surrogate,
        fields: Vec<(String, String)>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Remove a document from the FTS index on the Data Plane. `None` names
    /// a key its home never bound: the delete removes nothing and still
    /// commits the producer's sequence.
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Option<Surrogate>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Assign a stable surrogate for `(collection, doc_id)`.
    async fn assign_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Surrogate>;

    /// The surrogate `(collection, doc_id)` is bound to at the collection's
    /// home, or `None` when the home binds none. Never binds.
    async fn lookup_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Option<Surrogate>>;
}

// ── SharedState adapter ──────────────────────────────────────────────────────

/// Production dispatcher: routes FTS ops to the Data Plane via the SPSC bridge.
pub struct SharedStateFtsDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
}

#[async_trait]
impl<'a> FtsDispatcher for SharedStateFtsDispatcher<'a> {
    async fn dispatch_index(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Surrogate,
        fields: Vec<(String, String)>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::TextOp;

        let prov = provenance;
        let database_id = self.database_id;
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &collection,
        )?;

        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
            fields,
            provenance: Some(prov),
        });

        super::raft_dispatch::authorize_and_dispatch(self.shared, self.identity, owner, plan).await
    }

    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Option<Surrogate>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::TextOp;

        let prov = provenance;
        let database_id = self.database_id;
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &collection,
        )?;

        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
            provenance: Some(prov),
        });

        super::raft_dispatch::authorize_and_dispatch(self.shared, self.identity, owner, plan).await
    }

    async fn assign_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Surrogate> {
        crate::control::server::surrogate_exchange::assign_surrogate_routed(
            self.shared,
            nodedb_types::CollectionKey::from_bare(database_id, collection),
            tenant_id,
            doc_id.as_bytes(),
            crate::types::TraceId::ZERO,
        )
        .await
    }

    async fn lookup_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Option<Surrogate>> {
        crate::control::server::surrogate_exchange::lookup_surrogate_routed(
            self.shared,
            nodedb_types::CollectionKey::from_bare(database_id, collection),
            tenant_id,
            doc_id.as_bytes(),
            crate::types::TraceId::ZERO,
        )
        .await
    }
}

// ── NoOp dispatcher (loud failure) ──────────────────────────────────────────

/// Dispatcher used when `SharedState` is unavailable.
pub struct NoOpFtsDispatcher;

#[async_trait]
impl FtsDispatcher for NoOpFtsDispatcher {
    async fn dispatch_index(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _surrogate: Surrogate,
        _fields: Vec<(String, String)>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("FTS index"))
    }

    async fn dispatch_delete(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _surrogate: Option<Surrogate>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("FTS delete"))
    }

    async fn assign_surrogate(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _collection: &str,
        _doc_id: &str,
    ) -> crate::Result<Surrogate> {
        Err(super::raft_dispatch::noop_dispatch_error(
            "fts surrogate assignment",
        ))
    }

    async fn lookup_surrogate(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _collection: &str,
        _doc_id: &str,
    ) -> crate::Result<Option<Surrogate>> {
        Err(super::raft_dispatch::noop_dispatch_error(
            "fts surrogate lookup",
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "fts_handler_tests.rs"]
mod tests;
