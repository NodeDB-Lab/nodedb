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
mod tests {
    use std::sync::{Arc, Mutex};

    use super::super::session::SyncSession;
    use super::super::wire::*;
    use super::*;

    type MockCallLog = Arc<Mutex<Vec<(TenantId, String, Vec<(String, String)>)>>>;

    struct MockDispatcher {
        index_calls: MockCallLog,
        delete_calls: MockCallLog,
        result: crate::Result<()>,
        /// The home's binding every lookup answers.
        bound: Option<Surrogate>,
        /// The doc ids `assign_surrogate` was called for.
        assigned: Arc<Mutex<Vec<String>>>,
        /// The target each `dispatch_delete` carried.
        deleted: Arc<Mutex<Vec<Option<Surrogate>>>>,
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
                    bound: None,
                    assigned: Arc::default(),
                    deleted: Arc::default(),
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
                bound: None,
                assigned: Arc::default(),
                deleted: Arc::default(),
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
            fields: Vec<(String, String)>,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.index_calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, fields));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        async fn dispatch_delete(
            &self,
            tenant_id: TenantId,
            _vshard: VShardId,
            collection: String,
            surrogate: Option<Surrogate>,
            provenance: nodedb_types::sync::wire::SyncProvenance,
        ) -> crate::Result<Vec<u8>> {
            let seq = provenance.seq;
            self.deleted.lock().unwrap().push(surrogate);
            self.delete_calls
                .lock()
                .unwrap()
                .push((tenant_id, collection, Vec::new()));
            super::super::test_support::mock_applied_ack(&self.result, seq)
        }

        async fn assign_surrogate(
            &self,
            _database_id: DatabaseId,
            _tenant_id: TenantId,
            _collection: &str,
            doc_id: &str,
        ) -> crate::Result<Surrogate> {
            self.assigned.lock().unwrap().push(doc_id.to_string());
            Ok(Surrogate::new(1))
        }

        async fn lookup_surrogate(
            &self,
            _database_id: DatabaseId,
            _tenant_id: TenantId,
            _collection: &str,
            _doc_id: &str,
        ) -> crate::Result<Option<Surrogate>> {
            Ok(self.bound)
        }
    }

    fn make_session() -> SyncSession {
        SyncSession::new("test-fts-session".to_string())
    }

    /// An index message whose only field is `body`; empty `text` carries no
    /// fields at all.
    fn make_index_msg(collection: &str, doc_id: &str, text: &str) -> FtsIndexMsg {
        let fields = if text.is_empty() {
            Vec::new()
        } else {
            vec![("body".to_string(), text.to_string())]
        };
        FtsIndexMsg {
            lite_id: "lite-test".to_string(),
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            fields,
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
        let calls = indexes.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].2,
            vec![("body".to_string(), "hello world".to_string())],
            "the message's fields reach the Data Plane unchanged"
        );
    }

    /// Empty fields are an update that stripped every string field: they
    /// dispatch, so the Data Plane removes the document's prior postings.
    #[tokio::test]
    async fn empty_fields_dispatch_as_a_removal() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, indexes, _) = MockDispatcher::ok();
        let msg = make_index_msg("docs", "d1", "");

        let frame = session.handle_fts_index(&msg, &mock).await;
        let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        let calls = indexes.lock().unwrap();
        assert_eq!(calls.len(), 1, "empty fields must reach the Data Plane");
        assert!(calls[0].2.is_empty());
        assert_eq!(*mock.assigned.lock().unwrap(), vec!["d1".to_string()]);
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

    #[tokio::test]
    async fn delete_of_an_unbound_key_looks_up_and_binds_nothing() {
        let mut session = make_session();
        session.authenticated = true;
        let (mock, _, deletes) = MockDispatcher::ok();

        let frame = session
            .handle_fts_delete(&make_delete_msg("docs", "d1"), &mock)
            .await;
        let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(deletes.lock().unwrap().len(), 1);
        assert_eq!(*mock.deleted.lock().unwrap(), vec![None]);
        assert!(
            mock.assigned.lock().unwrap().is_empty(),
            "a delete must never bind its key"
        );
    }

    #[tokio::test]
    async fn delete_of_a_bound_key_carries_the_home_surrogate() {
        let mut session = make_session();
        session.authenticated = true;
        let (mut mock, _, _) = MockDispatcher::ok();
        mock.bound = Some(Surrogate::new(7));

        let frame = session
            .handle_fts_delete(&make_delete_msg("docs", "d1"), &mock)
            .await;
        let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
        assert!(ack.accepted);
        assert_eq!(*mock.deleted.lock().unwrap(), vec![Some(Surrogate::new(7))]);
        assert!(mock.assigned.lock().unwrap().is_empty());
    }
}
