// SPDX-License-Identifier: BUSL-1.1

//! Writes to a `crdt='true'` collection keep their exact types, and a
//! block-list insert is visible to reads at once.
//!
//! An SQL `INSERT` and `UPSERT` of a Decimal `12.50`, a UUID and a timestamp
//! read back as exactly those typed values. A native `list_insert` of typed
//! block fields reads back, through the document read, as the row's block
//! list holding exactly those values.

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

async fn remote(server: &TestServer) -> NodeDbRemote {
    NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed")
}

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

fn instant() -> NdbDateTime {
    NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant")
}

/// Assert `field` of `fields` is the Decimal `text`, scale included.
fn assert_decimal(fields: &HashMap<String, Value>, field: &str, text: &str, how: &str) {
    match fields.get(field) {
        Some(Value::Decimal(d)) => assert_eq!(d.to_string(), text, "{how}: {field} scale"),
        other => panic!("{how}: {field} is a Decimal, got {other:?}"),
    }
}

const DDL: &str = "CREATE COLLECTION crdt_typed (id TEXT PRIMARY KEY, price DECIMAL(10,2), \
                   uid UUID, at TIMESTAMP) WITH (crdt='true')";

/// An SQL INSERT and UPSERT into a CRDT collection store every value typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_writes_to_a_crdt_collection_keep_every_type() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote.execute_sql(DDL, &[]).await.expect(DDL);

    for (verb, id, price) in [("INSERT", "r1", "12.50"), ("UPSERT", "r2", "7.10")] {
        let sql = format!(
            "{verb} INTO crdt_typed (id, price, uid, at) \
             VALUES ('{id}', {price}, '{UUID}'::UUID, TIMESTAMP '2024-01-02 03:04:05')"
        );
        remote.execute_sql(&sql, &[]).await.expect(&sql);
        let read = native
            .document_get("crdt_typed", id)
            .await
            .expect("native get")
            .unwrap_or_else(|| panic!("{verb}: row {id} exists"));
        assert_decimal(&read.fields, "price", price, verb);
        assert_eq!(
            read.get("uid"),
            Some(&Value::Uuid(UUID.into())),
            "{verb}: uid"
        );
        assert_eq!(
            read.get("at"),
            Some(&Value::NaiveDateTime(instant())),
            "{verb}: at"
        );
    }

    server.graceful_shutdown().await;
}

/// A native list insert of typed fields is in the row a document read
/// returns right after it, every field typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_insert_is_read_back_typed_at_once() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote.execute_sql(DDL, &[]).await.expect(DDL);
    remote
        .execute_sql(
            "INSERT INTO crdt_typed (id, price) VALUES ('r1', 1.00)",
            &[],
        )
        .await
        .expect("seed the CRDT row");

    let block: HashMap<String, Value> = [
        ("price".to_string(), decimal("12.50")),
        ("uid".to_string(), Value::Uuid(UUID.into())),
        ("at".to_string(), Value::DateTime(instant())),
        ("blob".to_string(), Value::Bytes(vec![0x01, 0xff])),
    ]
    .into();
    native
        .list_insert(
            "crdt_typed",
            "r1",
            "items",
            0,
            &Value::Object(block.clone()),
        )
        .await
        .expect("native list insert of typed fields");

    let read = native
        .document_get("crdt_typed", "r1")
        .await
        .expect("native get")
        .expect("the CRDT row exists");
    let Some(Value::Array(items)) = read.get("items") else {
        panic!("the block list is in the row: {:?}", read.get("items"));
    };
    let [Value::Object(got)] = items.as_slice() else {
        panic!("one block: {items:?}");
    };
    assert_eq!(got, &block, "every block field keeps its type");
    assert_decimal(got, "price", "12.50", "list insert");

    // SQL reads the same projection: the row's block list holds one block.
    let selected = native
        .execute_sql("SELECT items FROM crdt_typed WHERE id = 'r1'", &[])
        .await
        .expect("SQL select of the block list");
    let column = selected
        .columns
        .iter()
        .position(|c| c == "items")
        .expect("an items column");
    let [row] = selected.rows.as_slice() else {
        panic!("one row: {:?}", selected.rows);
    };
    match row.get(column) {
        Some(Value::Array(blocks)) => assert_eq!(blocks.len(), 1, "SQL sees the block"),
        other => panic!("SQL reads the block list, got {other:?}"),
    }

    server.graceful_shutdown().await;
}
