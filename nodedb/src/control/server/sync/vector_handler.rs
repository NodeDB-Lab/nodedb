// SPDX-License-Identifier: BUSL-1.1

//! Vector insert/delete handler for sync sessions.
//!
//! Decodes `VectorInsertMsg` / `VectorDeleteMsg` from a Lite client,
//! allocates a surrogate for the document ID via `SurrogateAssigner`,
//! proposes `VectorOp::Insert` / `VectorOp::DeleteBySurrogate` through Raft
//! (the replicated apply journals the write), and returns an ACK frame.
//!
//! Structural pattern mirrors `columnar_handler.rs`:
//! a dispatcher trait ties ingest and ACK together so an ACK can never be
//! returned without at least attempting dispatch.

use async_trait::async_trait;

use nodedb_types::Surrogate;

use crate::control::server::dispatch_utils::RecordOwner;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Parameters for a single vector insert dispatch.
pub struct VectorInsertParams {
    pub collection: String,
    pub vector: Vec<f32>,
    pub dim: usize,
    pub field_name: String,
    pub surrogate: Surrogate,
    /// Raw PK bytes (the sync message's document id) used to bind the
    /// surrogate on follower apply. Always `Some` on this path — the sync
    /// producer always supplies a document id.
    pub pk_bytes: Option<Vec<u8>>,
}

/// Encapsulates async Data Plane dispatch for vector insert/delete.
///
/// Returns the raw `Response.payload` bytes from the Data Plane so that the
/// handler can decode the [`SyncAckResult`] for gate status propagation.
#[async_trait]
pub trait VectorDispatcher: Send + Sync {
    /// Insert a vector into the HNSW index on the Data Plane.
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        params: VectorInsertParams,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Delete a vector by surrogate from the HNSW index on the Data Plane.
    /// `None` names a key its home never bound: the delete removes nothing
    /// and still commits the producer's sequence.
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Option<Surrogate>,
        field_name: String,
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

/// Production dispatcher: routes vector ops to the Data Plane via the SPSC
/// bridge using `EventSource::CrdtSync` (suppresses AFTER triggers on synced
/// data).
pub struct SharedStateVectorDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
}

#[async_trait]
impl<'a> VectorDispatcher for SharedStateVectorDispatcher<'a> {
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        params: VectorInsertParams,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::VectorOp;

        let prov = provenance;
        let database_id = self.database_id;
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            &params.collection,
        )?;

        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &params.collection),
            vector: params.vector,
            dim: params.dim,
            field_name: params.field_name,
            surrogate: params.surrogate,
            pk_bytes: params.pk_bytes,
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
        field_name: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::VectorOp;

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
        let plan = PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
            field_name,
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
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            collection,
        )?;
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
        super::raft_dispatch::authorize_sync_collection(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            collection,
        )?;
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
pub struct NoOpVectorDispatcher;

#[async_trait]
impl VectorDispatcher for NoOpVectorDispatcher {
    async fn dispatch_insert(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _params: VectorInsertParams,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("vector insert"))
    }

    async fn dispatch_delete(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _surrogate: Option<Surrogate>,
        _field_name: String,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("vector delete"))
    }

    async fn assign_surrogate(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _collection: &str,
        _doc_id: &str,
    ) -> crate::Result<Surrogate> {
        Err(super::raft_dispatch::noop_dispatch_error(
            "vector surrogate assignment",
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
            "vector surrogate lookup",
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Session handler methods (`handle_vector_insert` / `handle_vector_delete`) live
// in `vector_session.rs`; tests below drive them via the trait + session API.

#[cfg(test)]
#[path = "vector_handler_tests.rs"]
mod tests;
