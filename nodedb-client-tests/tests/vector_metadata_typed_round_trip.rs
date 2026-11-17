// SPDX-License-Identifier: BUSL-1.1

//! Vector metadata keeps its exact type end to end.
//!
//! A Decimal `12.50`, a UUID, a timestamp and bytes, written as the metadata
//! of `vector_insert` / `vector_insert_field`, read back as exactly the typed
//! values written. A metadata filter on the Decimal matches the row written
//! with it and no other.
//!
//! A `VectorBatchInsert` whose vectors name their ids writes each vector and
//! its typed metadata as the row under its id, in one page.
//!
//! The native client sends the metadata as a native object in its
//! `VectorInsert` request. The remote client binds each metadata field as a
//! typed parameter, so a UUID, an instant or bytes stays typed in a declared
//! column and in an undeclared field alike.

use std::collections::HashMap;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, MetadataFilter, NativeClient, NodeDb, NodeDbRemote, Value};
use nodedb_test_support::native_harness::{do_handshake, send_request_msgpack};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{
    AuthMethod, BatchDocument, BatchVector, HelloFrame, OpCode, TextFields,
};

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

/// The four typed metadata fields, with `at` as the instant kind `at` names.
fn typed_metadata(at: Value) -> HashMap<String, Value> {
    [
        ("price".to_string(), decimal("12.50")),
        ("uid".to_string(), Value::Uuid(UUID.into())),
        ("at".to_string(), at),
        ("blob".to_string(), Value::Bytes(vec![0x01, 0x02, 0xff])),
    ]
    .into()
}

fn metadata_doc(fields: &HashMap<String, Value>) -> Document {
    let mut doc = Document::new("metadata");
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

/// A native `vector_insert` into a document collection stores the metadata
/// typed in the row, and a filter on the Decimal selects exactly that row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vector_metadata_keeps_every_type_and_filters_typed() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql(
            "CREATE COLLECTION typed_vecs FIELDS (id TEXT, embedding VECTOR(2)) \
             WITH (engine='document_schemaless', m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION typed_vecs");

    let fields = typed_metadata(Value::DateTime(instant()));
    native
        .vector_insert("typed_vecs", "v1", &[0.5, 1.0], Some(metadata_doc(&fields)))
        .await
        .expect("native vector_insert with typed metadata");
    let mut other = Document::new("metadata");
    other.set("price", decimal("3.00"));
    native
        .vector_insert("typed_vecs", "v2", &[0.5, 0.9], Some(other))
        .await
        .expect("native vector_insert of a second row");

    let read = native
        .document_get("typed_vecs", "v1")
        .await
        .expect("native get")
        .expect("the vector row exists");
    assert_typed(&read, &fields, "native vector_insert, native get");

    let filter = MetadataFilter::eq("price", decimal("12.50"));
    let hits = native
        .vector_search("typed_vecs", &[0.5, 1.0], 5, Some(&filter), None)
        .await
        .expect("native vector_search filtered on a Decimal");
    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["v1"], "the Decimal filter matches only v1");

    server.graceful_shutdown().await;
}

/// A native `vector_insert_field` into a vector-primary collection stores
/// the metadata typed in the row's sidecar.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_vector_primary_sidecar_keeps_every_type() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql(
            "CREATE COLLECTION typed_primary FIELDS (id TEXT, body_embedding VECTOR(3)) \
             WITH (engine='vector', primary='vector', vector_field='body_embedding', \
                   dim=3, m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION typed_primary");

    let fields = typed_metadata(Value::DateTime(instant()));
    native
        .vector_insert_field(
            "typed_primary",
            "body_embedding",
            "v1",
            &[0.1, 0.2, 0.3],
            Some(metadata_doc(&fields)),
        )
        .await
        .expect("native vector_insert_field with typed metadata");
    let read = native
        .document_get("typed_primary", "v1")
        .await
        .expect("native get")
        .expect("the vector row exists");
    assert_typed(&read, &fields, "native vector_insert_field, native get");

    let err = native
        .vector_insert_field(
            "typed_primary",
            "other_embedding",
            "v2",
            &[0.1, 0.2, 0.3],
            None,
        )
        .await
        .expect_err("a field that is not the vector column is refused");
    assert!(err.to_string().contains("other_embedding"), "{err}");

    server.graceful_shutdown().await;
}

/// A remote `vector_insert` binds each metadata field typed into the columns
/// the collection declares, and a UUID into an undeclared field stays a UUID.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_vector_metadata_binds_declared_types() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql(
            "CREATE COLLECTION typed_strict_vecs (id TEXT PRIMARY KEY, embedding VECTOR(2), \
             price DECIMAL(10,2), uid UUID, at TIMESTAMP, blob BYTEA) \
             WITH (engine='document_strict')",
            &[],
        )
        .await
        .expect("CREATE COLLECTION typed_strict_vecs");

    let fields = typed_metadata(Value::NaiveDateTime(instant()));
    remote
        .vector_insert(
            "typed_strict_vecs",
            "v1",
            &[0.5, 1.0],
            Some(metadata_doc(&fields)),
        )
        .await
        .expect("remote vector_insert with typed metadata");
    let read = remote
        .document_get("typed_strict_vecs", "v1")
        .await
        .expect("remote get")
        .expect("the vector row exists");
    assert_typed(&read, &fields, "remote vector_insert, remote get");

    remote
        .execute_sql(
            "CREATE COLLECTION loose_vecs FIELDS (id TEXT, embedding VECTOR(2)) \
             WITH (engine='document_schemaless', m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION loose_vecs");
    let mut uid = Document::new("metadata");
    uid.set("uid", Value::Uuid(UUID.into()));
    remote
        .vector_insert("loose_vecs", "v1", &[0.5, 1.0], Some(uid))
        .await
        .expect("remote vector_insert of a UUID into an undeclared field");
    let read = native
        .document_get("loose_vecs", "v1")
        .await
        .expect("native get")
        .expect("the vector row exists");
    assert_eq!(
        read.get("uid"),
        Some(&Value::Uuid(UUID.into())),
        "an undeclared field keeps its UUID"
    );

    server.graceful_shutdown().await;
}

/// Send one `op` request of `fields` over a raw MessagePack native session,
/// the encoding the native client uses, and require it to succeed. A JSON
/// session would carry the typed metadata as plain JSON. `what` names the
/// request in the failure message.
async fn send_batch(server: &TestServer, op: OpCode, fields: TextFields, what: &str) {
    let addr = format!("127.0.0.1:{}", server.native_port)
        .parse()
        .expect("native addr");
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request_msgpack(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: "nodedb".into(),
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native auth: {auth:?}");
    let response = send_request_msgpack(&mut stream, 2, op, fields).await;
    assert_eq!(response.status, ResponseStatus::Ok, "{what}: {response:?}");
}

/// Send one `VectorBatchInsert` of `vectors` into `collection`.
async fn send_vector_batch(server: &TestServer, collection: &str, vectors: Vec<BatchVector>) {
    let fields = TextFields {
        collection: Some(collection.to_string()),
        vectors: Some(vectors),
        ..Default::default()
    };
    send_batch(
        server,
        OpCode::VectorBatchInsert,
        fields,
        &format!("vector batch into {collection}"),
    )
    .await;
}

/// A keyed batch writes every vector and its typed metadata as the row under
/// its id, in a document collection and in a vector-primary collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_vector_batch_writes_typed_rows_under_their_ids() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    for ddl in [
        "CREATE COLLECTION batch_docs FIELDS (id TEXT, embedding VECTOR(2)) \
         WITH (engine='document_schemaless', m=8, ef_construction=50)",
        "CREATE COLLECTION batch_primary FIELDS (id TEXT, embedding VECTOR(2)) \
         WITH (engine='vector', primary='vector', vector_field='embedding', \
               dim=2, m=8, ef_construction=50)",
    ] {
        remote.execute_sql(ddl, &[]).await.expect(ddl);
    }

    let first = typed_metadata(Value::DateTime(instant()));
    let mut second = typed_metadata(Value::NaiveDateTime(instant()));
    second.insert("price".to_string(), decimal("3.00"));
    for collection in ["batch_docs", "batch_primary"] {
        let vectors = vec![
            BatchVector {
                id: "b1".into(),
                embedding: vec![0.5, 1.0],
                metadata: Some(Value::Object(first.clone())),
            },
            BatchVector {
                id: "b2".into(),
                embedding: vec![1.0, 0.5],
                metadata: Some(Value::Object(second.clone())),
            },
        ];
        send_vector_batch(&server, collection, vectors).await;
        for (id, fields) in [("b1", &first), ("b2", &second)] {
            let read = native
                .document_get(collection, id)
                .await
                .expect("native get")
                .unwrap_or_else(|| panic!("{collection}: row {id} exists under its id"));
            for (name, value) in fields {
                assert_eq!(
                    read.get(name),
                    Some(value),
                    "{collection}/{id}: field {name}"
                );
            }
        }
    }

    server.graceful_shutdown().await;
}

/// A `DocumentBatchInsert` stores every document's fields typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_document_batch_keeps_every_type() {
    let server = TestServer::start().await;
    let remote = remote(&server).await;
    let native = native(&server);
    remote
        .execute_sql("CREATE COLLECTION batch_typed_docs", &[])
        .await
        .expect("CREATE COLLECTION batch_typed_docs");

    let first = typed_metadata(Value::DateTime(instant()));
    let mut second = typed_metadata(Value::NaiveDateTime(instant()));
    second.insert("price".to_string(), decimal("3.00"));
    let documents = vec![
        BatchDocument {
            id: "d1".into(),
            fields: Value::Object(first.clone()),
        },
        BatchDocument {
            id: "d2".into(),
            fields: Value::Object(second.clone()),
        },
    ];
    send_batch(
        &server,
        OpCode::DocumentBatchInsert,
        TextFields {
            collection: Some("batch_typed_docs".to_string()),
            documents: Some(documents),
            ..Default::default()
        },
        "document batch",
    )
    .await;
    for (id, fields) in [("d1", &first), ("d2", &second)] {
        let read = native
            .document_get("batch_typed_docs", id)
            .await
            .expect("native get")
            .unwrap_or_else(|| panic!("document {id} exists"));
        for (name, value) in fields {
            assert_eq!(read.get(name), Some(value), "{id}: field {name}");
        }
    }

    server.graceful_shutdown().await;
}
