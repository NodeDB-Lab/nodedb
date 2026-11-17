// SPDX-License-Identifier: BUSL-1.1

//! A PointPut whose submitted body is not a document object.

use nodedb_types::{StorageKey, Surrogate};

use crate::bridge::envelope::{ErrorCode, Status};
use crate::data::executor::core_loop::tests::{make_core, make_default_task};
use crate::data::executor::handlers::point::put::PointPutExec;

/// Raw bytes that are neither MessagePack nor JSON are the request's fault:
/// the put answers `BadRequest` and stores nothing.
#[test]
fn a_put_of_raw_bytes_is_a_bad_request_and_writes_nothing() {
    let (mut core, _req_tx, _resp_rx, _dir) = make_core();
    let task = make_default_task();
    let response = core.execute_point_put(
        &task,
        PointPutExec {
            tid: 1,
            collection: "docs",
            document_id: "doc1",
            surrogate: Surrogate::new(1),
            value: b"original",
            returning: None,
            rls_filters: &[],
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: &[],
        },
    );
    assert_eq!(response.status, Status::Error, "{response:?}");
    assert!(
        matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::BadRequest { detail }) if detail.contains("docs")
        ),
        "a non-object body is a bad request; got {:?}",
        response.error_code
    );
    let stored = core
        .sparse
        .get(0, 1, "docs", &StorageKey::for_surrogate(Surrogate::new(1)))
        .expect("read the row key");
    assert!(stored.is_none(), "the refused put stored a row");
    let rows = core
        .scan_collection(0, 1, "docs", 10)
        .expect("scan the collection");
    assert!(
        rows.is_empty(),
        "the refused put stored {} rows",
        rows.len()
    );
}
