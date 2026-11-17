// SPDX-License-Identifier: BUSL-1.1

//! Values a client sends keep their exact type.
//!
//! A Decimal `12.50`, a UUID, a timestamp and bytes, written through each
//! client's document write, read back as exactly the typed values written,
//! into declared and undeclared fields alike.

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, NodeDbRemote, Value};
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

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

fn decimal() -> Value {
    Value::Decimal("12.50".parse().expect("decimal literal"))
}

fn instant() -> NdbDateTime {
    NdbDateTime::parse("2024-01-02T03:04:05Z").expect("valid instant")
}

/// The four typed fields, with `at` as the instant kind `at` names.
fn typed_fields(at: Value) -> HashMap<String, Value> {
    [
        ("price".to_string(), decimal()),
        ("uid".to_string(), Value::Uuid(UUID.into())),
        ("at".to_string(), at),
        ("blob".to_string(), Value::Bytes(vec![0x01, 0x02, 0xff])),
    ]
    .into()
}

fn document(id: &str, fields: &HashMap<String, Value>) -> Document {
    let mut doc = Document::new(id);
    for (name, value) in fields {
        doc.set(name.clone(), value.clone());
    }
    doc
}

/// Assert `doc` holds every field of `fields` exactly, the Decimal's scale
/// included.
fn assert_typed(doc: &Document, fields: &HashMap<String, Value>, how: &str) {
    for (name, value) in fields {
        assert_eq!(doc.get(name), Some(value), "{how}: field {name}");
    }
    let Some(Value::Decimal(price)) = doc.get("price") else {
        panic!("{how}: price is a Decimal");
    };
    assert_eq!(price.to_string(), "12.50", "{how}: the scale survives");
}

/// A native put sends a native object body: a schemaless document keeps
/// every field's type, read back through either client's typed read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_document_put_keeps_every_type() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql("CREATE COLLECTION typed_docs", &[])
        .await
        .expect("CREATE COLLECTION typed_docs");

    let fields = typed_fields(Value::DateTime(instant()));
    native
        .document_put("typed_docs", document("d1", &fields))
        .await
        .expect("native put of typed fields");
    let read = native
        .document_get("typed_docs", "d1")
        .await
        .expect("native get")
        .expect("the document exists");
    assert_typed(&read, &fields, "native put, native get");

    server.graceful_shutdown().await;
}

/// A pgwire put binds a Decimal as `numeric` and a UUID, a timestamp and
/// bytes as their exact text into columns the collection declares of those
/// types. The document reads back typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_document_put_keeps_declared_types() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    remote
        .execute_sql(
            "CREATE COLLECTION typed_strict (id TEXT PRIMARY KEY, price DECIMAL(10,2), \
             uid UUID, at TIMESTAMP, blob BYTEA) WITH (engine='document_strict')",
            &[],
        )
        .await
        .expect("CREATE COLLECTION typed_strict");

    let fields = typed_fields(Value::NaiveDateTime(instant()));
    remote
        .document_put("typed_strict", document("d1", &fields))
        .await
        .expect("remote put of typed fields");
    let read = remote
        .document_get("typed_strict", "d1")
        .await
        .expect("remote get")
        .expect("the document exists");
    assert_typed(&read, &fields, "remote put, remote get");

    server.graceful_shutdown().await;
}

/// A pgwire put into a schemaless document binds every typed value under
/// its exact OID, so each undeclared field reads back typed through the
/// native client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_document_put_into_a_schemaless_document() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql("CREATE COLLECTION loose_docs", &[])
        .await
        .expect("CREATE COLLECTION loose_docs");

    for (id, at) in [
        ("utc", Value::DateTime(instant())),
        ("naive", Value::NaiveDateTime(instant())),
    ] {
        let fields = typed_fields(at);
        remote
            .document_put("loose_docs", document(id, &fields))
            .await
            .expect("remote put of typed fields into undeclared fields");
        let read = native
            .document_get("loose_docs", id)
            .await
            .expect("native get")
            .expect("the document exists");
        assert_typed(&read, &fields, "remote put, native get");
    }

    server.graceful_shutdown().await;
}
