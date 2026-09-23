// SPDX-License-Identifier: BUSL-1.1

//! Vector insert/delete handler for sync sessions.
//!
//! Decodes `VectorInsertMsg` / `VectorDeleteMsg` from a Lite client,
//! allocates a surrogate for the document ID via `SurrogateAssigner`,
//! dispatches `VectorOp::Insert` / `VectorOp::DeleteBySurrogate` to the
//! Data Plane, and returns an ACK frame.
//!
//! Structural pattern mirrors `columnar_handler.rs`:
//! a dispatcher trait ties ingest and ACK together so an ACK can never be
//! returned without at least attempting dispatch.

use async_trait::async_trait;

use nodedb_types::Surrogate;

use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::NO_APPLY_KEY;

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
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Surrogate,
        field_name: String,
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
        use crate::control::server::wal_dispatch::{VectorPutWalArgs, wal_append_vector_put};
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

        // Allocate WAL LSN on the Control Plane before dispatching to the
        // Data Plane. Sync path MUST write to WAL; non-sync path already does
        // this via `wal_append_if_write_with_creds` in the main dispatch.
        let wal_lsn = wal_append_vector_put(
            self.shared.wal.appender(NO_APPLY_KEY),
            tenant_id,
            vshard,
            database_id,
            VectorPutWalArgs {
                collection: &params.collection,
                vector: &params.vector,
                dim: params.dim,
                field_name: &params.field_name,
                surrogate: params.surrogate,
                provenance: Some(&prov),
            },
        )?;

        let plan = PhysicalPlan::Vector(VectorOp::Insert {
            collection: nodedb_types::QualifiedCollection::new(database_id, &params.collection),
            vector: params.vector,
            dim: params.dim,
            field_name: params.field_name,
            surrogate: params.surrogate,
            pk_bytes: params.pk_bytes,
            provenance: Some(prov),
        });

        let authorized = super::raft_dispatch::authorize_sync_task(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            vshard,
            plan,
        )?;
        super::raft_dispatch::dispatch_sync_payload(self.shared, authorized, Some(wal_lsn)).await
    }

    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Surrogate,
        field_name: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::{
            VectorDeleteWalArgs, wal_append_vector_delete_by_surrogate,
        };
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

        // Allocate WAL LSN on the Control Plane before dispatching to the
        // Data Plane.
        let wal_lsn = wal_append_vector_delete_by_surrogate(
            self.shared.wal.appender(NO_APPLY_KEY),
            tenant_id,
            vshard,
            database_id,
            VectorDeleteWalArgs {
                collection: &collection,
                surrogate,
                field_name: &field_name,
                provenance: Some(&prov),
            },
        )?;

        let plan = PhysicalPlan::Vector(VectorOp::DeleteBySurrogate {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
            field_name,
            provenance: Some(prov),
        });

        let authorized = super::raft_dispatch::authorize_sync_task(
            self.shared,
            self.identity,
            tenant_id,
            database_id,
            vshard,
            plan,
        )?;
        super::raft_dispatch::dispatch_sync_payload(self.shared, authorized, Some(wal_lsn)).await
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
        _surrogate: Surrogate,
        _field_name: String,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("vector delete"))
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
//
// Session handler methods (`handle_vector_insert` / `handle_vector_delete`) live
// in `vector_session.rs`; tests below drive them via the trait + session API.

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
    impl VectorDispatcher for MockDispatcher {
        async fn dispatch_insert(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            params: VectorInsertParams,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.insert_calls
                .lock()
                .unwrap()
                .push((tenant_id, params.collection, String::new()));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        async fn dispatch_delete(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            _surrogate: Surrogate,
            _field_name: String,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.delete_calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, String::new()));
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
        SyncSession::new("test-vector-session".to_string())
    }

    fn make_insert_msg(collection: &str, id: &str, vector: Vec<f32>) -> VectorInsertMsg {
        let dim = vector.len();
        VectorInsertMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            id: id.to_string(),
            vector,
            dim,
            field_name: String::new(),
            batch_id: 1,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    fn make_delete_msg(collection: &str, id: &str) -> VectorDeleteMsg {
        VectorDeleteMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            id: id.to_string(),
            field_name: String::new(),
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
        let msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0, 0.0]);

        let frame = session.handle_vector_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(inserts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authenticated_insert_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, inserts, _) = MockDispatcher::ok();
        let msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0, 0.0]);

        let frame = session.handle_vector_insert(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.id, "v1");
        assert_eq!(inserts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn insert_dimension_mismatch_rejects() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, _, _) = MockDispatcher::ok();
        let mut msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0, 0.0]);
        msg.dim = 5; // Mismatch: vector.len() == 3, dim == 5

        let frame = session.handle_vector_insert(&msg, &mock).await;
        let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(ack.reject_reason.unwrap().contains("dimension mismatch"));
    }

    #[tokio::test]
    async fn insert_dispatch_failure_rejects() {
        let mut session = make_session();
        session.authenticated = true;
        let mock = MockDispatcher::err();
        let msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0]);

        let frame = session.handle_vector_insert(&msg, &mock).await;
        let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(ack.reject_reason.is_some());
    }

    #[tokio::test]
    async fn authenticated_delete_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("vecs", "v1");

        let frame = session.handle_vector_delete(&msg, &mock).await;
        let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.id, "v1");
        assert_eq!(deletes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unauthenticated_delete_returns_rejection() {
        let mut session = make_session();
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("vecs", "v1");

        let frame = session.handle_vector_delete(&msg, &mock).await;
        let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(deletes.lock().unwrap().is_empty());
    }
}
