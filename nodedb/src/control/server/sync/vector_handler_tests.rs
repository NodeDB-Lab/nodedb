// SPDX-License-Identifier: BUSL-1.1

use nodedb_types::sync::wire::AckStatus;

use std::sync::{Arc, Mutex};

use super::super::session::SyncSession;
use super::super::wire::*;
use super::*;

type MockCallLog = Arc<Mutex<Vec<(TenantId, String, String)>>>;

struct MockDispatcher {
    insert_calls: MockCallLog,
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
        let inserts = Arc::new(Mutex::new(Vec::new()));
        let deletes = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                insert_calls: inserts.clone(),
                delete_calls: deletes.clone(),
                result: Ok(()),
                bound: None,
                assigned: Arc::default(),
                deleted: Arc::default(),
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
            bound: None,
            assigned: Arc::default(),
            deleted: Arc::default(),
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
        if params.dim == 0 || params.dim != params.vector.len() {
            return Err(crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::SyncRejected {
                    violation: nodedb_types::sync::violation::ViolationType::SchemaViolation {
                        field: params.field_name,
                        reason: "dimension mismatch".into(),
                    },
                    applied_seq: seq,
                    provenance,
                },
            ));
        }
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
        surrogate: Option<Surrogate>,
        _field_name: String,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        let seq = provenance.seq;
        self.deleted.lock().unwrap().push(surrogate);
        self.delete_calls
            .lock()
            .unwrap()
            .push((tenant_id, collection, String::new()));
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
async fn unauthenticated_insert_is_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, inserts, _) = MockDispatcher::ok();
    let msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0, 0.0]);

    let frame = session.handle_vector_insert(&msg, &mock).await;
    assert!(frame.is_some());
    let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(mock.assigned.lock().unwrap().is_empty());
    assert!(inserts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn authenticated_insert_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
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
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, _) = MockDispatcher::ok();
    let mut msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0, 0.0]);
    msg.dim = 5; // Mismatch: vector.len() == 3, dim == 5
    msg.seq = 7;

    let frame = session.handle_vector_insert(&msg, &mock).await;
    let ack: VectorInsertAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert!(ack.reject_reason.unwrap().contains("dimension mismatch"));
    assert_eq!(ack.applied_seq, 7);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
}

#[tokio::test]
async fn an_unknown_insert_dispatch_outcome_closes_without_an_ack() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let mock = MockDispatcher::err();
    let msg = make_insert_msg("vecs", "v1", vec![1.0, 0.0]);

    assert!(session.handle_vector_insert(&msg, &mock).await.is_none());
}

#[tokio::test]
async fn authenticated_delete_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("vecs", "v1");

    let frame = session.handle_vector_delete(&msg, &mock).await;
    let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(ack.accepted);
    assert_eq!(ack.id, "v1");
    assert_eq!(deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unauthenticated_delete_is_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("vecs", "v1");

    let frame = session.handle_vector_delete(&msg, &mock).await;
    let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delete_of_an_unbound_key_looks_up_and_binds_nothing() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, deletes) = MockDispatcher::ok();

    let frame = session
        .handle_vector_delete(&make_delete_msg("embeddings", "v1"), &mock)
        .await;
    let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
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
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.bound = Some(Surrogate::new(7));

    let frame = session
        .handle_vector_delete(&make_delete_msg("embeddings", "v1"), &mock)
        .await;
    let ack: VectorDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(ack.accepted);
    assert_eq!(*mock.deleted.lock().unwrap(), vec![Some(Surrogate::new(7))]);
    assert!(mock.assigned.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_surrogate_service_error_closes_without_a_terminal_ack() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let insert = make_insert_msg("vecs", "v1", vec![1.0]);
    assert!(
        session
            .handle_vector_insert(&insert, &NoOpVectorDispatcher)
            .await
            .is_none()
    );
    let delete = make_delete_msg("vecs", "v1");
    assert!(
        session
            .handle_vector_delete(&delete, &NoOpVectorDispatcher)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn an_invalid_collection_qualifier_is_fenced_without_surrogate_assignment() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, inserts, _) = MockDispatcher::ok();
    let msg = make_insert_msg("other/db/vecs", "v1", vec![1.0]);
    let ack: VectorInsertAckMsg = session
        .handle_vector_insert(&msg, &mock)
        .await
        .unwrap()
        .decode_body()
        .unwrap();
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(mock.assigned.lock().unwrap().is_empty());
    assert!(inserts.lock().unwrap().is_empty());
}
