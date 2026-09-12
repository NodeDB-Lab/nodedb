// SPDX-License-Identifier: BUSL-1.1

//! FTS index/delete handler for sync sessions.
//!
//! Decodes `FtsIndexMsg` / `FtsDeleteMsg` from a Lite client,
//! allocates a surrogate for the document ID via `SurrogateAssigner`,
//! appends a WAL record on the Control Plane, dispatches
//! `TextOp::FtsIndexDoc` / `TextOp::FtsDeleteDoc` to the Data Plane,
//! and returns an ACK frame carrying the `SyncAckResult` from the gate.
//!
//! Handler methods (`handle_fts_index` / `handle_fts_delete`) live in the
//! sibling `fts_session.rs` to keep both files under 500 lines.
//!
//! Structural pattern mirrors `vector_handler.rs`.

use async_trait::async_trait;

use nodedb_types::Surrogate;

use crate::types::{DatabaseId, TenantId, VShardId};

// ── Dispatcher trait ─────────────────────────────────────────────────────────

/// Encapsulates async Data Plane dispatch for FTS index/delete.
///
/// Returns the raw `Response.payload` bytes so the handler can decode the
/// [`SyncAckResult`] for gate status propagation to the Lite client.
#[async_trait]
pub trait FtsDispatcher: Send + Sync {
    /// Index a document's text on the Data Plane.
    async fn dispatch_index(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
        surrogate: Surrogate,
        text: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>>;

    /// Remove a document from the FTS index on the Data Plane.
    async fn dispatch_delete(
        &self,
        tenant_id: TenantId,
        vshard: VShardId,
        collection: String,
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
        text: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::wal_append_fts_index;
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

        // Allocate WAL LSN on the Control Plane before dispatching to the
        // Data Plane. The doc_id for WAL purposes is the surrogate hex string
        // (same as what the DP uses for storage). We encode the original
        // doc_id (the Lite-side external key) into the WAL payload so replay
        // can re-derive the surrogate via the same assigner.
        let surrogate_hex =
            crate::engine::document::store::StorageKey::for_surrogate(surrogate).to_string();
        let fts_index_payload = nodedb_wal::record::FtsIndexPayload::new(
            prov.clone(),
            &collection,
            &surrogate_hex,
            &text,
        );
        let wal_lsn = wal_append_fts_index(
            &self.shared.wal,
            tenant_id,
            vshard,
            database_id,
            &fts_index_payload,
        )?;

        let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
            text,
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
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        use crate::bridge::envelope::PhysicalPlan;
        use crate::control::server::wal_dispatch::wal_append_fts_delete;
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

        let surrogate_hex =
            crate::engine::document::store::StorageKey::for_surrogate(surrogate).to_string();
        let fts_delete_payload =
            nodedb_wal::record::FtsDeletePayload::new(prov.clone(), &collection, &surrogate_hex);
        let wal_lsn = wal_append_fts_delete(
            &self.shared.wal,
            tenant_id,
            vshard,
            database_id,
            &fts_delete_payload,
        )?;

        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: nodedb_types::QualifiedCollection::new(database_id, &collection),
            surrogate,
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
pub struct NoOpFtsDispatcher;

#[async_trait]
impl FtsDispatcher for NoOpFtsDispatcher {
    async fn dispatch_index(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _surrogate: Surrogate,
        _text: String,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("FTS index"))
    }

    async fn dispatch_delete(
        &self,
        _tenant_id: TenantId,
        _vshard: VShardId,
        _collection: String,
        _surrogate: Surrogate,
        _provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        Err(super::raft_dispatch::noop_dispatch_error("FTS delete"))
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
        index_calls: MockCallLog,
        delete_calls: MockCallLog,
        result: crate::Result<()>,
    }

    impl MockDispatcher {
        fn ok() -> (Self, MockCallLog, MockCallLog) {
            let indexes = Arc::new(Mutex::new(Vec::new()));
            let deletes = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    index_calls: indexes.clone(),
                    delete_calls: deletes.clone(),
                    result: Ok(()),
                },
                indexes,
                deletes,
            )
        }

        fn err() -> Self {
            Self {
                index_calls: Arc::new(Mutex::new(Vec::new())),
                delete_calls: Arc::new(Mutex::new(Vec::new())),
                result: Err(crate::Error::Internal {
                    detail: "mock failure".to_string(),
                }),
            }
        }
    }

    #[async_trait]
    impl FtsDispatcher for MockDispatcher {
        async fn dispatch_index(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            _surrogate: Surrogate,
            _text: String,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.index_calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, String::new()));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        async fn dispatch_delete(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            _surrogate: Surrogate,
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
        SyncSession::new("test-fts-session".to_string())
    }

    fn make_index_msg(collection: &str, doc_id: &str, text: &str) -> FtsIndexMsg {
        FtsIndexMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            text: text.to_string(),
            batch_id: 1,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    fn make_delete_msg(collection: &str, doc_id: &str) -> FtsDeleteMsg {
        FtsDeleteMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            batch_id: 2,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[tokio::test]
    async fn unauthenticated_index_returns_rejection() {
        let mut session = make_session();
        let (mock, indexes, _) = MockDispatcher::ok();
        let msg = make_index_msg("docs", "d1", "hello world");

        let frame = session.handle_fts_index(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(indexes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn authenticated_index_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, indexes, _) = MockDispatcher::ok();
        let msg = make_index_msg("docs", "d1", "hello world");

        let frame = session.handle_fts_index(&msg, &mock).await;
        assert!(frame.is_some());
        let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.doc_id, "d1");
        assert_eq!(indexes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_text_acks_without_dispatch() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, indexes, _) = MockDispatcher::ok();
        let msg = make_index_msg("docs", "d1", "");

        let frame = session.handle_fts_index(&msg, &mock).await;
        let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert!(indexes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn index_dispatch_failure_rejects() {
        let mut session = make_session();
        session.authenticated = true;
        let mock = MockDispatcher::err();
        let msg = make_index_msg("docs", "d1", "hello");

        let frame = session.handle_fts_index(&msg, &mock).await;
        let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(ack.reject_reason.is_some());
    }

    #[tokio::test]
    async fn authenticated_delete_dispatches_and_acks() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("docs", "d1");

        let frame = session.handle_fts_delete(&msg, &mock).await;
        let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.doc_id, "d1");
        assert_eq!(deletes.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn unauthenticated_delete_returns_rejection() {
        let mut session = make_session();
        let (mock, _, deletes) = MockDispatcher::ok();
        let msg = make_delete_msg("docs", "d1");

        let frame = session.handle_fts_delete(&msg, &mock).await;
        let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(!ack.accepted);
        assert!(deletes.lock().unwrap().is_empty());
    }
}
