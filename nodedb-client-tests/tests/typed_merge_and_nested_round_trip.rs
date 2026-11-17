// SPDX-License-Identifier: BUSL-1.1

//! A MERGE arm's assignments and a nested object literal keep exact types
//! in a schemaless collection.
//!
//! - `MERGE ... WHEN MATCHED THEN UPDATE SET` and `WHEN NOT MATCHED THEN
//!   INSERT` with a UUID cast and a decimal cast read back as a `Uuid` and a
//!   `Decimal`, and a field the arm leaves alone keeps its stored type.
//! - `INSERT INTO c { m: { b: $1 } }` with a UUID parameter stores the nested
//!   field as a `Uuid`, and a nested typed literal likewise.
//! - `UPDATE ... FROM` with a UUID cast and a decimal cast stores both typed,
//!   and an untouched typed field keeps its type.
//! - A `VALUES` cell whose quoted text holds `)` or `,` stays one cell.

use std::collections::HashMap;
use std::error::Error;

use bytes::BytesMut;
use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use tokio_postgres::types::{IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, NoTls};

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const OTHER: &str = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

/// A binary `uuid` parameter.
#[derive(Debug)]
struct BinaryUuid([u8; 16]);

impl ToSql for BinaryUuid {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(&self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::UUID
    }

    to_sql_checked!();
}

async fn pg(server: &TestServer) -> Client {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("pgwire connect to harness must succeed");
    tokio::spawn(connection);
    client
}

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

async fn fields(native: &NativeClient, collection: &str, id: &str) -> HashMap<String, Value> {
    native
        .document_get(collection, id)
        .await
        .expect("native get")
        .unwrap_or_else(|| panic!("document {id} exists"))
        .fields
}

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().expect("decimal literal"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_arms_keep_every_type() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    pg.batch_execute("CREATE COLLECTION merge_typed_tgt; CREATE COLLECTION merge_typed_src")
        .await
        .expect("create collections");
    pg.batch_execute(&format!(
        "INSERT INTO merge_typed_tgt (id, keep) VALUES ('hit', '{OTHER}'::uuid); \
         INSERT INTO merge_typed_src (id) VALUES ('hit'), ('new')"
    ))
    .await
    .expect("seed rows");

    pg.batch_execute(&format!(
        "MERGE INTO merge_typed_tgt t USING merge_typed_src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET u = '{UUID}'::uuid, d = 12.50::decimal \
         WHEN NOT MATCHED THEN INSERT (id, u, d) VALUES (s.id, '{UUID}'::uuid, 12.50::decimal)"
    ))
    .await
    .expect("MERGE with typed assignments");

    for id in ["hit", "new"] {
        let row = fields(&native, "merge_typed_tgt", id).await;
        assert_eq!(row.get("u"), Some(&Value::Uuid(UUID.into())), "{id}: u");
        assert_eq!(row.get("d"), Some(&decimal("12.50")), "{id}: d");
        let Some(Value::Decimal(d)) = row.get("d") else {
            panic!("{id}: d is a Decimal");
        };
        assert_eq!(d.to_string(), "12.50", "{id}: the scale survives");
    }
    assert_eq!(
        fields(&native, "merge_typed_tgt", "hit").await.get("keep"),
        Some(&Value::Uuid(OTHER.into())),
        "a field the arm leaves alone keeps its type"
    );

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_object_literal_fields_keep_their_type() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    pg.batch_execute("CREATE COLLECTION nested_typed")
        .await
        .expect("create collection");

    let bytes = nodedb_types::id_gen::uuid_to_bytes(UUID).expect("valid uuid");
    let statement = pg
        .prepare_typed(
            "INSERT INTO nested_typed { id: $1, m: { b: $2, c: { n: 1 } } }",
            &[Type::TEXT, Type::UUID],
        )
        .await
        .expect("prepare the nested brace insert");
    pg.execute(&statement, &[&"param", &BinaryUuid(bytes)])
        .await
        .expect("nested brace insert with a UUID parameter");

    pg.batch_execute(&format!(
        "INSERT INTO nested_typed {{ id: 'literal', m: {{ b: '{UUID}'::uuid, c: {{ n: 1 }} }} }}"
    ))
    .await
    .expect("nested brace insert with a UUID literal");

    let expected = Value::Object(
        [
            ("b".to_string(), Value::Uuid(UUID.into())),
            (
                "c".to_string(),
                Value::Object([("n".to_string(), Value::Integer(1))].into()),
            ),
        ]
        .into(),
    );
    for id in ["param", "literal"] {
        assert_eq!(
            fields(&native, "nested_typed", id).await.get("m"),
            Some(&expected),
            "{id}"
        );
    }

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_from_keeps_every_type() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    pg.batch_execute("CREATE COLLECTION upd_from_tgt; CREATE COLLECTION upd_from_src")
        .await
        .expect("create collections");
    pg.batch_execute(&format!(
        "INSERT INTO upd_from_tgt (id, keep) VALUES ('hit', '{OTHER}'::uuid); \
         INSERT INTO upd_from_src (id) VALUES ('hit')"
    ))
    .await
    .expect("seed rows");

    pg.batch_execute(&format!(
        "UPDATE upd_from_tgt SET u = '{UUID}'::uuid, d = 12.50::decimal \
         FROM upd_from_src WHERE upd_from_tgt.id = upd_from_src.id"
    ))
    .await
    .expect("UPDATE ... FROM with typed assignments");

    let row = fields(&native, "upd_from_tgt", "hit").await;
    assert_eq!(row.get("u"), Some(&Value::Uuid(UUID.into())), "u");
    let Some(Value::Decimal(d)) = row.get("d") else {
        panic!("d is a Decimal: {:?}", row.get("d"));
    };
    assert_eq!(d.to_string(), "12.50", "the scale survives");
    assert_eq!(
        row.get("keep"),
        Some(&Value::Uuid(OTHER.into())),
        "a field the statement leaves alone keeps its type"
    );

    server.graceful_shutdown().await;
}

/// A `VALUES` cell whose quoted text holds `)` or `,` is one cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quoted_commas_and_parens_stay_inside_their_cell() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    pg.batch_execute("CREATE COLLECTION split_cells")
        .await
        .expect("create collection");

    pg.batch_execute(
        "INSERT INTO split_cells { id: 'obj', m: { k: 'a),b' } }; \
         INSERT INTO split_cells (id, note, up) VALUES ('fn', 'x, y)', upper('p, q'))",
    )
    .await
    .expect("insert cells holding quoted punctuation");

    assert_eq!(
        fields(&native, "split_cells", "obj").await.get("m"),
        Some(&Value::Object(
            [("k".to_string(), Value::String("a),b".into()))].into()
        ))
    );
    let row = fields(&native, "split_cells", "fn").await;
    assert_eq!(row.get("note"), Some(&Value::String("x, y)".into())));
    assert_eq!(row.get("up"), Some(&Value::String("P, Q".into())));

    server.graceful_shutdown().await;
}
