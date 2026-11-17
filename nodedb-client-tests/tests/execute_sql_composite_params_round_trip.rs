// SPDX-License-Identifier: BUSL-1.1

//! `NodeDbRemote::execute_sql` binds an array, an object, a duration and a
//! geometry parameter, and the server stores each typed.
//!
//! - A same-kind array binds as the array type of its kind and reads back
//!   as an array of exactly those typed elements.
//! - An object binds as `jsonb` and reads back as an object, its integers,
//!   exact decimals and strings typed.
//! - An object or mixed array with a leaf JSON cannot hold exactly (a whole
//!   decimal, a UUID, an instant, bytes, a duration, a non-finite float)
//!   binds as a tagged value and reads back with every leaf typed.
//! - A duration binds as `interval` and reads back as a duration.
//! - A geometry binds as GeoJSON text, which `ST_GeomFromGeoJSON($n)`
//!   stores as a geometry.

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().expect("decimal literal"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_params_store_typed_values() {
    let server = TestServer::start().await;
    let remote = NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed");
    let native = native(&server);
    remote
        .execute_sql("CREATE COLLECTION composite_params", &[])
        .await
        .expect("CREATE COLLECTION composite_params");

    let prices = Value::Array(vec![decimal("12.50"), Value::Null, decimal("3.00")]);
    let uids = Value::Array(vec![Value::Uuid(UUID.into())]);
    let meta = Value::Object(HashMap::from([
        ("n".to_string(), Value::Integer(2)),
        ("d".to_string(), decimal("1.50")),
        ("s".to_string(), Value::String("x".into())),
        (
            "l".to_string(),
            Value::Array(vec![Value::Integer(1), Value::String("y".into())]),
        ),
    ]));
    let span = Value::Duration(nodedb_types::NdbDuration::from_micros(90_000_000));
    remote
        .execute_sql(
            "INSERT INTO composite_params (id, prices, uids, meta, span) \
             VALUES ('r1', $1, $2, $3, $4)",
            &[prices.clone(), uids.clone(), meta.clone(), span.clone()],
        )
        .await
        .expect("INSERT with composite params");

    let row = native
        .document_get("composite_params", "r1")
        .await
        .expect("native get")
        .expect("row r1 exists");
    assert_eq!(row.get("prices"), Some(&prices), "prices");
    assert_eq!(row.get("uids"), Some(&uids), "uids");
    assert_eq!(row.get("meta"), Some(&meta), "meta");
    assert_eq!(row.get("span"), Some(&span), "span");

    // A geometry value as the server stores it, read back typed.
    remote
        .execute_sql(
            "INSERT INTO composite_params (id, at) VALUES ('src', ST_Point(1.0, 2.0))",
            &[],
        )
        .await
        .expect("INSERT a geometry literal");
    let point = native
        .document_get("composite_params", "src")
        .await
        .expect("native get")
        .expect("row src exists")
        .get("at")
        .cloned()
        .expect("src holds a geometry");
    assert!(matches!(point, Value::Geometry(_)), "{point:?}");
    remote
        .execute_sql(
            "INSERT INTO composite_params (id, at) VALUES ('g1', ST_GeomFromGeoJSON($1))",
            std::slice::from_ref(&point),
        )
        .await
        .expect("INSERT with a geometry param");
    let row = native
        .document_get("composite_params", "g1")
        .await
        .expect("native get")
        .expect("row g1 exists");
    assert_eq!(row.get("at"), Some(&point), "the geometry round-trips");

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn objects_with_leaves_json_cannot_hold_keep_every_type() {
    let server = TestServer::start().await;
    let remote = NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed");
    let native = native(&server);
    remote
        .execute_sql("CREATE COLLECTION tagged_params", &[])
        .await
        .expect("CREATE COLLECTION tagged_params");

    let meta = Value::Object(HashMap::from([
        ("whole".to_string(), decimal("5")),
        ("u".to_string(), Value::Uuid(UUID.into())),
        (
            "at".to_string(),
            Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                1_700_000_000_000_000,
            )),
        ),
        ("raw".to_string(), Value::Bytes(vec![0, 0xff])),
        (
            "span".to_string(),
            Value::Duration(nodedb_types::NdbDuration::from_micros(90)),
        ),
        ("inf".to_string(), Value::Float(f64::INFINITY)),
        (
            "l".to_string(),
            Value::Array(vec![decimal("7"), Value::String("y".into())]),
        ),
    ]));
    let mixed = Value::Array(vec![
        Value::Integer(1),
        decimal("2"),
        Value::Uuid(UUID.into()),
    ]);
    remote
        .execute_sql(
            "INSERT INTO tagged_params (id, meta, mixed) VALUES ('t1', $1, $2)",
            &[meta.clone(), mixed.clone()],
        )
        .await
        .expect("INSERT with tagged params");

    let row = native
        .document_get("tagged_params", "t1")
        .await
        .expect("native get")
        .expect("row t1 exists");
    assert_eq!(row.get("meta"), Some(&meta), "meta");
    assert_eq!(row.get("mixed"), Some(&mixed), "mixed");

    server.graceful_shutdown().await;
}
