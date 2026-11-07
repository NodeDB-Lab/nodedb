// SPDX-License-Identifier: BUSL-1.1

//! The native `ArrayPutBatch` opcode checks the tenant quota before it loads.

use nodedb::control::security::tenant::TenantQuota;
use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{ArrayCellWrite, ArrayPutBatch, HelloFrame, NativeResponse, OpCode};
use nodedb_types::value::Value;
use tokio::net::TcpStream;

async fn put_cell(stream: &mut TcpStream, seq: u64, x: i64) -> NativeResponse {
    let cells = ArrayPutBatch::from_cells(&[ArrayCellWrite {
        coords: vec![Value::Integer(x)],
        attrs: vec![Value::Float(0.5)],
    }])
    .expect("cell batch");
    let fields = TextFields {
        collection: Some("quota_grid".to_string()),
        payload: Some(zerompk::to_msgpack_vec(&cells).expect("encode cells")),
        ..TextFields::default()
    };
    send_request(stream, seq, OpCode::ArrayPutBatch, fields).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_over_its_concurrency_quota_is_refused_before_the_load() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    let created = send_sql(
        &mut stream,
        1,
        "CREATE ARRAY quota_grid DIMS (x INT64 [0..63]) ATTRS (v FLOAT64) TILE_EXTENTS (16)",
    )
    .await;
    assert_eq!(created.status, ResponseStatus::Ok, "{created:?}");

    let admitted = put_cell(&mut stream, 2, 1).await;
    assert_eq!(admitted.status, ResponseStatus::Ok, "{admitted:?}");

    // One request held open against a quota of one fills the tenant.
    let tenant = server
        .shared
        .credentials
        .get_user("nodedb")
        .expect("the trust superuser")
        .tenant_id;
    {
        let mut tenants = server.shared.tenants.lock().expect("tenants");
        tenants.set_quota(
            tenant,
            TenantQuota {
                max_concurrent_requests: 1,
                ..TenantQuota::default()
            },
        );
        tenants.request_start(tenant);
    }

    let refused = put_cell(&mut stream, 3, 2).await;
    assert_eq!(refused.status, ResponseStatus::Error, "{refused:?}");
    let error = refused.error.expect("a refusal carries its error");
    assert_eq!(error.code, "42601", "{error:?}");
    assert!(
        error.message.contains("1/1 concurrent requests"),
        "{error:?}"
    );

    server
        .shared
        .tenants
        .lock()
        .expect("tenants")
        .request_end(tenant);
    let released = put_cell(&mut stream, 4, 3).await;
    assert_eq!(released.status, ResponseStatus::Ok, "{released:?}");
}
