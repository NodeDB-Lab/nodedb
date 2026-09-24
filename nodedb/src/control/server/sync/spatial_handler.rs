// SPDX-License-Identifier: BUSL-1.1

//! Spatial geometry insert/delete handler for sync sessions.
//!
//! Decodes `SpatialInsertMsg` / `SpatialDeleteMsg` from a Lite client,
//! deserialises the geometry, allocates a surrogate for the document ID,
//! appends a WAL record on the Control Plane, dispatches
//! `SpatialOp::Insert` / `SpatialOp::Delete` to the Data Plane through
//! the idempotency gate, and returns an ACK frame.
//!
//! Handler methods live in `spatial_session.rs` to keep both files under
//! the 500-line limit.
//!
//! Structural pattern mirrors `vector_handler.rs`.

use async_trait::async_trait;

use nodedb_types::Surrogate;
use nodedb_types::geometry::Geometry;

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
    pub geometry: Geometry,
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
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        field: String,
        surrogate: Surrogate,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Assign a stable surrogate for `(collection, doc_id)`.
    fn assign_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Surrogate>;
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
        use crate::control::server::wal_dispatch::wal_append_spatial_put;
        use crate::control::server::wal_dispatch_fts_spatial::encode_spatial_put_payload;
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
        let spatial_put_payload =
            encode_spatial_put_payload(&collection, &field, surrogate, &geometry, &prov)?;
        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        // The record's outcome-floor window opens before the append and
        // closes from the dispatch's outcome.
        let (minted, _) = super::raft_dispatch::append_under_window(self.shared, owner, |wal| {
            wal_append_spatial_put(wal, tenant_id, vshard, database_id, &spatial_put_payload)
                .map(Some)
        })
        .await?;

        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            field,
            surrogate,
            geometry,
            provenance: Some(prov),
        });

        super::raft_dispatch::authorize_and_dispatch_minted(
            self.shared,
            self.identity,
            owner,
            plan,
            minted,
        )
        .await
    }

    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        field: String,
        surrogate: Surrogate,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::wal_append_spatial_delete;
        use crate::control::server::wal_dispatch_fts_spatial::encode_spatial_delete_payload;
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

        let spatial_delete_payload =
            encode_spatial_delete_payload(&collection, &field, surrogate, &prov);
        let owner = RecordOwner {
            tenant_id,
            database_id,
            vshard_id: vshard,
        };
        // The record's outcome-floor window opens before the append and
        // closes from the dispatch's outcome.
        let (minted, _) = super::raft_dispatch::append_under_window(self.shared, owner, |wal| {
            wal_append_spatial_delete(wal, tenant_id, vshard, database_id, &spatial_delete_payload)
                .map(Some)
        })
        .await?;

        let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            field,
            surrogate,
            provenance: Some(prov),
        });

        super::raft_dispatch::authorize_and_dispatch_minted(
            self.shared,
            self.identity,
            owner,
            plan,
            minted,
        )
        .await
    }

    fn assign_surrogate(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        doc_id: &str,
    ) -> crate::Result<Surrogate> {
        self.shared
            .surrogate_assigner
            .assign(database_id, tenant_id, collection, doc_id.as_bytes())
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
        _surrogate: Surrogate,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("spatial delete"))
    }

    fn assign_surrogate(
        &self,
        _database_id: DatabaseId,
        _tenant_id: TenantId,
        _collection: &str,
        _doc_id: &str,
    ) -> crate::Result<Surrogate> {
        Ok(Surrogate::ZERO)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::super::session::SyncSession;
    use super::super::wire::*;
    use super::*;

    type MockCallLog = Arc<Mutex<Vec<(TenantId, String, String)>>>;

    struct MockDispatcher {
        insert_calls: MockCallLog,
        delete_calls: MockCallLog,
        result: crate::Result<()>,
    }

    impl MockDispatcher {
        fn ok() -> (Self, MockCallLog, MockCallLog) {
            let inserts = Arc::new(Mutex::new(Vec::new()));
            let deletes = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    insert_calls: inserts.clone(),
                    delete_calls: deletes.clone(),
                    result: Ok(()),
                },
                inserts,
                deletes,
            )
        }

        fn err() -> Self {
            Self {
                insert_calls: Arc::new(Mutex::new(Vec::new())),
                delete_calls: Arc::new(Mutex::new(Vec::new())),
                result: Err(crate::Error::Internal {
                    detail: "mock failure".to_string(),
                }),
            }
        }
    }

    #[async_trait]
    impl SpatialDispatcher for MockDispatcher {
        async fn dispatch_insert(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            target: SpatialInsertTarget,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.insert_calls
                .lock()
                .unwrap()
                .push((tenant_id, target.collection, target.field));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        async fn dispatch_delete(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            field: String,
            _surrogate: Surrogate,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.delete_calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, field));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        fn assign_surrogate(
            &self,
            _database_id: DatabaseId,
            _tenant_id: TenantId,
            _collection: &str,
            _doc_id: &str,
        ) -> crate::Result<Surrogate> {
            Ok(Surrogate::ZERO)
        }
    }

    fn make_session() -> SyncSession {
        SyncSession::new("test-spatial-session".to_string())
    }

    fn make_point_geometry_bytes() -> Vec<u8> {
        let geom = nodedb_types::geometry::Geometry::point(10.0, 20.0);
        zerompk::to_msgpack_vec(&geom).unwrap()
    }

    fn make_insert_msg(collection: &str, field: &str, doc_id: &str) -> SpatialInsertMsg {
        SpatialInsertMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
            geometry_bytes: make_point_geometry_bytes(),
            batch_id: 1,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    fn make_delete_msg(collection: &str, field: &str, doc_id: &str) -> SpatialDeleteMsg {
        SpatialDeleteMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            field: field.to_string(),
            doc_id: doc_id.to_string(),
            batch_id: 2,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[tokio::test]
    async fn unauthenticated_insert_returns_rejection() {
        let mut session = make_session();
        let (mock, inserts, _) = MockDispatcher::ok();
        let msg = make_insert_msg("places", "loc", "d1");

        let frame = session.handle_spatial_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: SpatialInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(inserts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authenticated_insert_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, inserts, _) = MockDispatcher::ok();
        let msg = make_insert_msg("places", "loc", "d1");

        let frame = session.handle_spatial_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: SpatialInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.doc_id, "d1");
        assert_eq!(ack.field, "loc");
        assert_eq!(inserts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn insert_dispatch_failure_rejects() {
        let mut session = make_session();
        session.authenticated = true;
        let mock = MockDispatcher::err();
        let msg = make_insert_msg("places", "loc", "d1");

        let frame = session.handle_spatial_insert(&msg, &mock).await;
        let ack: SpatialInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(ack.reject_reason.is_some());
    }

    #[tokio::test]
    async fn authenticated_delete_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("places", "loc", "d1");

        let frame = session.handle_spatial_delete(&msg, &mock).await;
        let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.doc_id, "d1");
        assert_eq!(deletes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unauthenticated_delete_returns_rejection() {
        let mut session = make_session();
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("places", "loc", "d1");

        let frame = session.handle_spatial_delete(&msg, &mock).await;
        let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(deletes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalid_geometry_bytes_rejects_insert() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, inserts, _) = MockDispatcher::ok();

        let msg = SpatialInsertMsg {
            lite_id: "lite-test".to_string(),
            collection: "places".to_string(),
            field: "loc".to_string(),
            doc_id: "d1".to_string(),
            geometry_bytes: vec![0xFF, 0xFF, 0xFF], // invalid msgpack
            batch_id: 1,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        };

        let frame = session.handle_spatial_insert(&msg, &mock).await;
        let ack: SpatialInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(ack.reject_reason.is_some());
        assert!(inserts.lock().unwrap().is_empty());
    }
}
