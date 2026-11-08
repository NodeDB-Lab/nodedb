// SPDX-License-Identifier: BUSL-1.1

//! Spatial geometry insert/delete handler for sync sessions.
//!
//! Decodes `SpatialInsertMsg` / `SpatialDeleteMsg` from a Lite client,
//! deserialises the geometry, allocates a surrogate for the document ID,
//! proposes `SpatialOp::Insert` / `SpatialOp::Delete` through Raft (the
//! replicated apply journals the write and runs the idempotency gate), and
//! returns an ACK frame.
//!
//! Handler methods live in `spatial_session.rs`.
//!
//! Structural pattern mirrors `vector_handler.rs`.

use async_trait::async_trait;

use nodedb_types::Surrogate;
use nodedb_types::geometry::SridGeometry;

use crate::control::server::dispatch_utils::RecordOwner;
use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Parameters bundling the spatial-index target for a single insert dispatch.
///
/// Groups the four fields that together identify the geometry being written:
/// which collection, which field index, the stable surrogate ID, and the
/// geometry value itself.
#[derive(Debug)]
pub struct SpatialInsertTarget {
    pub collection: String,
    pub field: String,
    pub surrogate: Surrogate,
    pub geometry: SridGeometry,
}

/// Encapsulates async Data Plane dispatch for spatial insert/delete.
///
/// Returns the raw `Response.payload` bytes so the handler can decode the
/// [`SyncAckResult`] for gate status propagation to the Lite client.
#[async_trait]
pub trait SpatialDispatcher: Send + Sync {
    /// Insert a geometry into the R-tree on the Data Plane.
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        target: SpatialInsertTarget,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Remove a document's geometry from the R-tree on the Data Plane.
    /// `None` names a key its home never bound: the delete removes nothing
    /// and still commits the producer's sequence.
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        field: String,
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

/// Production dispatcher: routes spatial ops to the Data Plane via the SPSC bridge.
pub struct SharedStateSpatialDispatcher<'a> {
    pub shared: &'a crate::control::state::SharedState,
    pub(crate) identity: Option<&'a crate::control::security::identity::AuthenticatedIdentity>,
    pub(crate) database_id: DatabaseId,
}

#[async_trait]
impl<'a> SpatialDispatcher for SharedStateSpatialDispatcher<'a> {
    async fn dispatch_insert(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        target: SpatialInsertTarget,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::SpatialOp;

        let prov = provenance;
        let database_id = self.database_id;
        let SpatialInsertTarget {
            collection,
            field,
            surrogate,
            geometry,
        } = target;

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
        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            field,
            surrogate,
            geometry,
            provenance: Some(prov),
        });

        super::raft_dispatch::authorize_and_dispatch(self.shared, self.identity, owner, plan).await
    }

    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        field: String,
        surrogate: Option<Surrogate>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use nodedb_physical::physical_plan::SpatialOp;

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
        let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            field,
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
pub struct NoOpSpatialDispatcher;

#[async_trait]
impl SpatialDispatcher for NoOpSpatialDispatcher {
    async fn dispatch_insert(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _target: SpatialInsertTarget,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("spatial insert"))
    }

    async fn dispatch_delete(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _field: String,
        _surrogate: Option<Surrogate>,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("spatial delete"))
    }

    async fn assign_surrogate(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _collection: &str,
        _doc_id: &str,
    ) -> crate::Result<Surrogate> {
        Err(super::raft_dispatch::noop_dispatch_error(
            "spatial surrogate assignment",
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
            "spatial surrogate lookup",
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "spatial_handler_tests.rs"]
mod tests;
