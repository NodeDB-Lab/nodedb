// SPDX-License-Identifier: BUSL-1.1

//! End-to-end cell encoding for `NodeDbRemote`.
//!
//! A strict document holds a `BYTEA`, a `VECTOR`, a `TIMESTAMP`, a
//! `DECIMAL` and a `UUID` field. Read over the extended protocol, where
//! `tokio_postgres` asks for binary results, every field comes back as its
//! own typed `Value`, equal to what was written. A parameterless `SELECT`
//! runs on the extended protocol too. A statement with no result shape at
//! Parse runs on the simple protocol, where every cell is its PostgreSQL
//! text form.

use nodedb_client::{NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

async fn remote(server: &TestServer) -> NodeDbRemote {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed")
}

/// Create the strict collection and write row `r1` with every typed field
/// set and row `r2` with every typed field NULL.
async fn seed(remote: &NodeDbRemote) {
    remote
        .execute_sql(
            "CREATE COLLECTION typed_cells (id TEXT PRIMARY KEY, payload BYTEA, \
             embedding VECTOR(3), at TIMESTAMP, price DECIMAL(10,2), uid UUID) \
             WITH (engine='document_strict')",
            &[],
        )
        .await
        .expect("CREATE COLLECTION typed_cells");
    // A string written to a BYTEA column is read as base64: `AQL/` is the
    // bytes 0x01 0x02 0xff.
    remote
        .execute_sql(
            &format!(
                "INSERT INTO typed_cells (id, payload, embedding, at, price, uid) VALUES \
                 ('r1', 'AQL/', '[0.5, 1.25, -2]', '2024-01-02T03:04:05Z', 12.50, '{UUID}')"
            ),
            &[],
        )
        .await
        .expect("INSERT the typed row");
    remote
        .execute_sql("INSERT INTO typed_cells (id) VALUES ('r2')", &[])
        .await
        .expect("INSERT the NULL row");
}

const SELECT_TYPED: &str =
    "SELECT payload, embedding, at, price, uid FROM typed_cells WHERE id = $1";

#[tokio::test]
async fn remote_reads_bytes_vector_timestamp_decimal_and_uuid_typed() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    seed(&remote).await;

    let result = remote
        .execute_sql(SELECT_TYPED, &[Value::String("r1".into())])
        .await
        .expect("extended-protocol read of the typed row");
    assert_eq!(
        result.columns,
        vec!["payload", "embedding", "at", "price", "uid"]
    );
    let at = NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant");
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Bytes(vec![0x01, 0x02, 0xff]),
            Value::Array(vec![
                Value::Float(0.5),
                Value::Float(1.25),
                Value::Float(-2.0)
            ]),
            Value::NaiveDateTime(at),
            Value::Decimal("12.50".parse().expect("decimal literal")),
            Value::Uuid(UUID.into()),
        ]],
        "every field reads back as the typed value written"
    );

    let nulls = remote
        .execute_sql(SELECT_TYPED, &[Value::String("r2".into())])
        .await
        .expect("extended-protocol read of the NULL row");
    assert_eq!(nulls.rows, vec![vec![Value::Null; 5]]);

    server.graceful_shutdown().await;
}

/// A parameterless row query runs on the extended protocol, so its cells
/// decode by column type exactly as a parameterized read's do. A statement
/// with no result shape at Parse, such as `DESCRIBE`, stays on the simple
/// protocol, and its cells are PostgreSQL text.
#[tokio::test]
async fn parameterless_select_cells_are_typed() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    seed(&remote).await;

    let result = remote
        .execute_sql(
            "SELECT payload, embedding, price, uid FROM typed_cells WHERE id = 'r1'",
            &[],
        )
        .await
        .expect("parameterless read of the typed row");
    assert_eq!(
        result.rows,
        vec![vec![
            Value::Bytes(vec![0x01, 0x02, 0xff]),
            Value::Array(vec![
                Value::Float(0.5),
                Value::Float(1.25),
                Value::Float(-2.0)
            ]),
            Value::Decimal("12.50".parse().expect("decimal literal")),
            Value::Uuid(UUID.into()),
        ]],
        "a parameterless SELECT reads every field as its typed value"
    );

    let described = remote
        .execute_sql("DESCRIBE typed_cells", &[])
        .await
        .expect("DESCRIBE on the simple protocol");
    assert!(
        described
            .rows
            .iter()
            .flatten()
            .all(|cell| matches!(cell, Value::String(_) | Value::Null)),
        "simple-protocol cells are text: {:?}",
        described.rows
    );

    server.graceful_shutdown().await;
}

/// `document_get` reads a declared `DECIMAL`, `UUID` and `TIMESTAMP` field
/// from its exact JSON text to its typed value, scale included.
#[tokio::test]
async fn document_get_reads_declared_fields_typed() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    seed(&remote).await;

    let doc = remote
        .document_get("typed_cells", "r1")
        .await
        .expect("document_get of the typed row")
        .expect("row r1 exists");
    let price = match doc.get("price") {
        Some(Value::Decimal(d)) => *d,
        other => panic!("price is a Decimal, got {other:?}"),
    };
    assert_eq!(price.to_string(), "12.50");
    assert_eq!(doc.get("uid"), Some(&Value::Uuid(UUID.into())));
    let at = NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant");
    assert_eq!(doc.get("at"), Some(&Value::NaiveDateTime(at)));

    server.graceful_shutdown().await;
}
