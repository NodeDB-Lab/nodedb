//! Integration tests for document operations (PointGet/Put/Delete, RangeScan, CRDT).

use nodedb::bridge::envelope::{ErrorCode, PhysicalPlan, Status};

use crate::helpers::*;

#[test]
fn point_get_not_found() {
    let (mut core, mut tx, mut rx) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointGet {
            collection: "users".into(),
            document_id: "nonexistent".into(),
        },
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code, Some(ErrorCode::NotFound));
}

#[test]
fn point_put_and_get() {
    let (mut core, mut tx, mut rx) = make_core();

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointPut {
            collection: "docs".into(),
            document_id: "d1".into(),
            value: b"hello world".to_vec(),
        },
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointGet {
            collection: "docs".into(),
            document_id: "d1".into(),
        },
    );
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(&*resp.payload, b"hello world");
}

#[test]
fn point_delete_removes() {
    let (mut core, mut tx, mut rx) = make_core();

    // Insert then delete via SPSC.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointPut {
            collection: "docs".into(),
            document_id: "d1".into(),
            value: b"data".to_vec(),
        },
    );

    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointDelete {
            collection: "docs".into(),
            document_id: "d1".into(),
        },
    );

    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointGet {
            collection: "docs".into(),
            document_id: "d1".into(),
        },
    );
    assert_eq!(resp.error_code, Some(ErrorCode::NotFound));
}

#[test]
fn crdt_read_not_found() {
    let (mut core, mut tx, mut rx) = make_core();
    let resp = send_raw(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::CrdtRead {
            collection: "sessions".into(),
            document_id: "s1".into(),
        },
    );
    assert_eq!(resp.status, Status::Error);
    assert_eq!(resp.error_code, Some(ErrorCode::NotFound));
}

#[test]
fn range_scan_returns_results() {
    let (mut core, mut tx, mut rx) = make_core();

    // Insert documents with indexed fields via PointPut.
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointPut {
            collection: "users".into(),
            document_id: "u1".into(),
            value: b"{\"name\":\"alice\",\"age\":25}".to_vec(),
        },
    );
    send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::PointPut {
            collection: "users".into(),
            document_id: "u2".into(),
            value: b"{\"name\":\"bob\",\"age\":30}".to_vec(),
        },
    );

    // DocumentScan should return both.
    let payload = send_ok(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::DocumentScan {
            collection: "users".into(),
            limit: 10,
            offset: 0,
            sort_keys: Vec::new(),
            filters: Vec::new(),
            distinct: false,
            projection: Vec::new(),
            computed_columns: Vec::new(),
        },
    );
    let json = payload_json(&payload);
    assert!(json.contains("alice"), "payload: {json}");
    assert!(json.contains("bob"), "payload: {json}");
}
