// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex};

use super::super::session::SyncSession;
use super::super::wire::*;
use super::*;
use nodedb_types::sync::wire::SyncAckResult;

type MockCallLog = Arc<Mutex<Vec<(TenantId, String, Vec<(String, String)>)>>>;

struct MockDispatcher {
    index_calls: MockCallLog,
    delete_calls: MockCallLog,
    result: crate::Result<()>,
    payload: Option<Vec<u8>>,
    error_code: Option<crate::bridge::envelope::ErrorCode>,
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
                payload: None,
                error_code: None,
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
            payload: None,
            error_code: None,
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
        if let Some(code) = &self.error_code {
            return Err(crate::Error::DataPlane(code.clone()));
        }
        if let Some(payload) = &self.payload {
            return Ok(payload.clone());
        }
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
        if let Some(code) = &self.error_code {
            return Err(crate::Error::DataPlane(code.clone()));
        }
        if let Some(payload) = &self.payload {
            return Ok(payload.clone());
        }
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
async fn unauthenticated_index_returns_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, indexes, _) = MockDispatcher::ok();
    let msg = make_index_msg("docs", "d1", "hello world");

    let frame = session.handle_fts_index(&msg, &mock).await;
    assert!(frame.is_some());
    let ack: FtsIndexAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(indexes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn authenticated_index_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
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
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
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
async fn unknown_index_dispatch_outcome_requires_reconnect() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    let mock = MockDispatcher::err();
    let msg = make_index_msg("docs", "d1", "hello");

    assert!(session.handle_fts_index(&msg, &mock).await.is_none());
}

#[tokio::test]
async fn authenticated_delete_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("docs", "d1");

    let frame = session.handle_fts_delete(&msg, &mock).await;
    let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(ack.accepted);
    assert_eq!(ack.doc_id, "d1");
    assert_eq!(deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unauthenticated_delete_returns_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("docs", "d1");

    let frame = session.handle_fts_delete(&msg, &mock).await;
    let ack: FtsDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delete_of_an_unbound_key_looks_up_and_binds_nothing() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
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
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
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

#[tokio::test]
async fn unreadable_index_and_delete_outcomes_require_reconnect() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.payload = Some(b"invalid msgpack".to_vec());
    assert!(
        session
            .handle_fts_index(&make_index_msg("docs", "d1", "hello"), &mock)
            .await
            .is_none()
    );
    assert!(
        session
            .handle_fts_delete(&make_delete_msg("docs", "d1"), &mock)
            .await
            .is_none()
    );
    assert_eq!(session.mutations_processed, 0);
}

#[tokio::test]
async fn surrogate_errors_require_reconnect_without_an_ack() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    assert!(
        session
            .handle_fts_index(&make_index_msg("docs", "d1", "hello"), &NoOpFtsDispatcher)
            .await
            .is_none()
    );
    assert!(
        session
            .handle_fts_delete(&make_delete_msg("docs", "d1"), &NoOpFtsDispatcher)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn terminal_index_and_delete_payloads_preserve_the_consumed_sequence() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.payload = Some(
        zerompk::to_msgpack_vec(&SyncAckResult::rejected(
            nodedb_types::sync::violation::ViolationType::PermissionDenied,
            7,
        ))
        .unwrap(),
    );
    let index = session
        .handle_fts_index(&make_index_msg("docs", "d1", "hello"), &mock)
        .await
        .unwrap();
    let ack: FtsIndexAckMsg = index.decode_body().unwrap();
    assert_eq!(ack.applied_seq, 7);
    assert!(!ack.accepted);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    let delete = session
        .handle_fts_delete(&make_delete_msg("docs", "d1"), &mock)
        .await
        .unwrap();
    let ack: FtsDeleteAckMsg = delete.decode_body().unwrap();
    assert_eq!(ack.applied_seq, 7);
    assert!(!ack.accepted);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
}

#[tokio::test]
async fn typed_index_and_delete_refusals_preserve_the_consumed_frontier() {
    use crate::bridge::envelope::ErrorCode;
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.error_code = Some(ErrorCode::SyncRejected {
        violation: nodedb_types::sync::violation::ViolationType::PermissionDenied,
        applied_seq: 7,
        provenance: SyncProvenance {
            producer_id: 2,
            epoch: 1,
            stream_id: 3,
            seq: 7,
        },
    });
    let index = session
        .handle_fts_index(&make_index_msg("docs", "d1", "hello"), &mock)
        .await
        .unwrap();
    let ack: FtsIndexAckMsg = index.decode_body().unwrap();
    assert_eq!(ack.applied_seq, 7);
    assert!(!ack.accepted);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    let delete = session
        .handle_fts_delete(&make_delete_msg("docs", "d1"), &mock)
        .await
        .unwrap();
    let ack: FtsDeleteAckMsg = delete.decode_body().unwrap();
    assert_eq!(ack.applied_seq, 7);
    assert!(!ack.accepted);
    assert!(matches!(ack.status, AckStatus::Rejected { .. }));
}

#[tokio::test]
async fn unknown_delete_dispatch_outcome_requires_reconnect() {
    let mut session = make_session();
    session.authenticated = true;
    session
        .tenant_id
        .get_or_insert(crate::types::TenantId::new(1));
    assert!(
        session
            .handle_fts_delete(&make_delete_msg("docs", "d1"), &MockDispatcher::err())
            .await
            .is_none()
    );
}
