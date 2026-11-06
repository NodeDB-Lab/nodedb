// SPDX-License-Identifier: BUSL-1.1

//! A write to a mirror draws no surrogate at any door that draws before its
//! write gate: native `plan_builder` ops and the columnar sync dispatcher.
//!
//! A drawn surrogate binds at once. The write gate then refuses the write, and
//! the binding stays with no row behind it. So each test checks the refusal and
//! that the registry's watermark did not move.

use nodedb::control::server::session_auth::configured_trust_identity;
use nodedb::control::server::sync::columnar_handler::{
    ColumnarDispatcher, SharedStateColumnarDispatcher,
};
use nodedb_test_support::native_harness::send_request;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::MirrorStatus;
use nodedb_types::protocol::{BatchVector, OpCode, TextFields};
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_types::value::Value;
use nodedb_types::{CollectionKey, DatabaseId};

use super::helpers::mark_database_mirror;
use super::test_read_only_protocols::{READ_ONLY, assert_native_refused, native_session};

/// A server whose client session uses a mirror holding one collection per door.
async fn fixture() -> (TestServer, String, DatabaseId) {
    let (server, name) = TestServer::with_database("mirror_draw").await;
    for statement in [
        "CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)",
        "CREATE COLLECTION points (ts BIGINT TIME_KEY, v BIGINT) WITH (engine='timeseries')",
        "CREATE COLLECTION keyless (ts BIGINT, v BIGINT) WITH (engine='columnar')",
        "CREATE COLLECTION keyed (id TEXT, v BIGINT) WITH (engine='columnar')",
    ] {
        server
            .exec(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    mark_database_mirror(&server, &name, MirrorStatus::Following).await;
    let database = server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(&name)
        .expect("catalog lookup")
        .expect("database exists");
    (server, name, database)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_plan_builder_doors_draw_nothing_on_a_mirror() {
    let (server, name, _database) = fixture().await;
    let mut stream = native_session(&server, &name).await;
    let before = server.shared.surrogate_assigner.current_hwm();

    let doors = [
        (
            "timeseries point put",
            OpCode::PointPut,
            TextFields {
                collection: Some("points".to_string()),
                document_id: Some("p1".to_string()),
                data: Some(b"1".to_vec()),
                ..Default::default()
            },
        ),
        (
            "keyless columnar insert",
            OpCode::ColumnarInsert,
            TextFields {
                collection: Some("keyless".to_string()),
                payload: Some(br#"[{"ts":1,"v":2}]"#.to_vec()),
                ..Default::default()
            },
        ),
        (
            "headless vector insert",
            OpCode::VectorInsert,
            TextFields {
                collection: Some("docs".to_string()),
                vector: Some(vec![0.5, 0.5]),
                ..Default::default()
            },
        ),
        (
            "headless vector batch",
            OpCode::VectorBatchInsert,
            TextFields {
                collection: Some("docs".to_string()),
                vectors: Some(vec![BatchVector {
                    id: "v1".to_string(),
                    embedding: vec![0.5, 0.5],
                    metadata: None,
                }]),
                ..Default::default()
            },
        ),
    ];
    for (seq, (door, op, fields)) in (2u64..).zip(doors) {
        eprintln!("door: {door}");
        let response = send_request(&mut stream, seq, op, fields).await;
        assert_native_refused(&response, READ_ONLY);
        assert_eq!(
            server.shared.surrogate_assigner.current_hwm(),
            before,
            "{door} drew a surrogate"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_columnar_sync_dispatcher_draws_nothing_on_a_mirror() {
    let (server, _name, database) = fixture().await;
    let identity = configured_trust_identity(&server.shared).expect("trust identity");
    let dispatcher = SharedStateColumnarDispatcher::new(&server.shared, &identity, database);
    let before = server.shared.surrogate_assigner.current_hwm();

    let doors = [
        ("keyless", vec![Value::Integer(1), Value::Integer(2)]),
        ("keyed", vec![Value::String("a".into()), Value::Integer(2)]),
    ];
    for (collection, row) in doors {
        let refused = dispatcher
            .dispatch_insert(
                identity.tenant_id,
                CollectionKey::from_bare(database, collection).vshard(),
                collection.to_string(),
                vec![row],
                Vec::new(),
                SyncProvenance {
                    producer_id: 1,
                    epoch: 1,
                    stream_id: 1,
                    seq: 1,
                },
            )
            .await
            .expect_err("a write to a mirror is refused");
        assert!(
            refused.to_string().contains(READ_ONLY),
            "{collection}: {refused}"
        );
        assert_eq!(
            server.shared.surrogate_assigner.current_hwm(),
            before,
            "{collection} drew a surrogate"
        );
    }
}
