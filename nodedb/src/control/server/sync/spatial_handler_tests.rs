// SPDX-License-Identifier: BUSL-1.1

use std::sync::{Arc, Mutex};

use super::super::session::SyncSession;
use super::super::wire::*;
use super::*;
use nodedb_types::sync::wire::SyncAckResult;

type MockCallLog = Arc<Mutex<Vec<(TenantId, String, String)>>>;

struct MockDispatcher {
    insert_calls: MockCallLog,
    delete_calls: MockCallLog,
    result: crate::Result<()>,
    payload: Option<Vec<u8>>,
    error_code: Option<crate::bridge::envelope::ErrorCode>,
    /// The home's binding every lookup answers.
    bound: Option<Surrogate>,
    /// The doc ids `assign_surrogate` was called for.
    assigned: Arc<Mutex<Vec<String>>>,
    /// The surrogate each `dispatch_delete` carried.
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
                payload: None,
                error_code: None,
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
        field: String,
        surrogate: Option<Surrogate>,
        provenance: nodedb_types::sync::wire::SyncProvenance,
    ) -> crate::Result<Vec<u8>> {
        let seq = provenance.seq;
        self.deleted.lock().unwrap().push(surrogate);
        self.delete_calls
            .lock()
            .unwrap()
            .push((tenant_id, collection, field));
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
    SyncSession::new("test-spatial-session".to_string())
}

fn make_point_geometry_bytes() -> Vec<u8> {
    let geom = SridGeometry::wgs84(nodedb_types::geometry::Geometry::point(10.0, 20.0));
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
async fn unauthenticated_insert_returns_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, inserts, _) = MockDispatcher::ok();
    let msg = make_insert_msg("places", "loc", "d1");

    let frame = session.handle_spatial_insert(&msg, &mock).await;
    assert!(frame.is_some());
    let ack: SpatialInsertAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(inserts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn authenticated_insert_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
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
async fn unknown_insert_dispatch_outcome_requires_reconnect() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let mock = MockDispatcher::err();
    let msg = make_insert_msg("places", "loc", "d1");

    let frame = session.handle_spatial_insert(&msg, &mock).await;
    assert!(frame.is_none());
}

#[tokio::test]
async fn authenticated_delete_dispatches_and_acks() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("places", "loc", "d1");

    let frame = session.handle_spatial_delete(&msg, &mock).await;
    let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(ack.accepted);
    assert_eq!(ack.doc_id, "d1");
    assert_eq!(deletes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unauthenticated_delete_returns_fenced_without_dispatch() {
    let mut session = make_session();
    let (mock, _, deletes) = MockDispatcher::ok();
    let msg = make_delete_msg("places", "loc", "d1");

    let frame = session.handle_spatial_delete(&msg, &mock).await;
    let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(deletes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unreadable_geometry_requires_reconnect_without_dispatch() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
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
    assert!(frame.is_none());
    assert!(inserts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delete_of_an_unbound_key_looks_up_and_binds_nothing() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mock, _, deletes) = MockDispatcher::ok();

    let frame = session
        .handle_spatial_delete(&make_delete_msg("places", "loc", "d1"), &mock)
        .await;
    let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
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
        .handle_spatial_delete(&make_delete_msg("places", "loc", "d1"), &mock)
        .await;
    let ack: SpatialDeleteAckMsg = frame.unwrap().decode_body().unwrap();
    assert!(ack.accepted);
    assert_eq!(*mock.deleted.lock().unwrap(), vec![Some(Surrogate::new(7))]);
    assert!(mock.assigned.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unreadable_core_and_unknown_delete_outcomes_require_reconnect() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.payload = Some(b"invalid msgpack".to_vec());
    assert!(
        session
            .handle_spatial_insert(&make_insert_msg("places", "loc", "a"), &mock)
            .await
            .is_none()
    );
    assert!(
        session
            .handle_spatial_delete(&make_delete_msg("places", "loc", "a"), &mock)
            .await
            .is_none()
    );
    assert!(
        session
            .handle_spatial_delete(
                &make_delete_msg("places", "loc", "a"),
                &MockDispatcher::err()
            )
            .await
            .is_none()
    );
    assert_eq!(session.mutations_processed, 0);
}

#[tokio::test]
async fn surrogate_errors_require_reconnect_without_an_ack() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    assert!(
        session
            .handle_spatial_insert(
                &make_insert_msg("places", "loc", "a"),
                &NoOpSpatialDispatcher
            )
            .await
            .is_none()
    );
    assert!(
        session
            .handle_spatial_delete(
                &make_delete_msg("places", "loc", "a"),
                &NoOpSpatialDispatcher
            )
            .await
            .is_none()
    );
}

#[tokio::test]
async fn typed_core_payloads_and_errors_preserve_exact_spatial_frontiers() {
    use crate::bridge::envelope::ErrorCode;
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    for typed_error in [false, true] {
        let (mut mock, _, _) = MockDispatcher::ok();
        if typed_error {
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
        } else {
            mock.payload = Some(
                zerompk::to_msgpack_vec(&SyncAckResult::rejected(
                    nodedb_types::sync::violation::ViolationType::PermissionDenied,
                    7,
                ))
                .unwrap(),
            );
        }
        let frame = session
            .handle_spatial_insert(&make_insert_msg("places", "loc", "a"), &mock)
            .await
            .unwrap();
        let ack: SpatialInsertAckMsg = frame.decode_body().unwrap();
        assert_eq!(ack.applied_seq, 7);
        assert!(!ack.accepted);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
        let frame = session
            .handle_spatial_delete(&make_delete_msg("places", "loc", "a"), &mock)
            .await
            .unwrap();
        let ack: SpatialDeleteAckMsg = frame.decode_body().unwrap();
        assert_eq!(ack.applied_seq, 7);
        assert!(!ack.accepted);
        assert!(matches!(ack.status, AckStatus::Rejected { .. }));
    }
}

#[tokio::test]
async fn pre_admission_authorization_refusals_are_fenced_without_a_frontier() {
    let mut session = make_session();
    session.authenticated = true;
    session.tenant_id.get_or_insert(crate::types::TenantId::new(1));
    let (mut mock, _, _) = MockDispatcher::ok();
    mock.error_code = Some(crate::bridge::envelope::ErrorCode::RejectedAuthz {
        resource: "not authorized".to_owned(),
    });
    let frame = session
        .handle_spatial_insert(&make_insert_msg("places", "loc", "a"), &mock)
        .await
        .unwrap();
    let ack: SpatialInsertAckMsg = frame.decode_body().unwrap();
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(!ack.accepted);
    let frame = session
        .handle_spatial_delete(&make_delete_msg("places", "loc", "a"), &mock)
        .await
        .unwrap();
    let ack: SpatialDeleteAckMsg = frame.decode_body().unwrap();
    assert_eq!(ack.status, AckStatus::Fenced);
    assert_eq!(ack.applied_seq, 0);
    assert!(!ack.accepted);
}
