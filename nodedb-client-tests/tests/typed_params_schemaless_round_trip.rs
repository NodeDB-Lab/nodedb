// SPDX-License-Identifier: BUSL-1.1

//! A UUID, an instant and bytes stay typed into an undeclared field of a
//! schemaless collection, whichever way SQL carries them.
//!
//! Each case writes the fields, then reads them back two ways: the native
//! client's typed get, and a pgwire `SELECT` of the field text. The cases:
//! - raw pgwire parameters in binary format, under their declared OIDs;
//! - raw pgwire parameters in text format, under their declared OIDs;
//! - typed SQL literals (`'...'::uuid`, `TIMESTAMPTZ '...'`, ...);
//! - native-protocol SQL parameters;
//! - `UPDATE ... SET` parameters and literals;
//! - `INSERT ... { }` and `[{ }]` with `$N` parameters.
//!
//! Text bound for a typed parameter, or a typed literal, that spells no value
//! of the type is refused with PostgreSQL's SQLSTATE. Bytes equality finds
//! stored bytes by scan and by index. A hostile native text parameter is
//! stored verbatim.

use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::NdbDateTime;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{Client, NoTls};

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";
const AT: &str = "2024-01-02T03:04:05Z";
/// `AT` as Unix seconds.
const AT_UNIX_SECS: u64 = 1_704_164_645;
const BLOB: [u8; 3] = [0x01, 0x02, 0xff];

/// A parameter sent in text format, as its raw PostgreSQL text.
#[derive(Debug)]
struct TextParam(&'static str);

impl ToSql for TextParam {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

/// A binary `uuid` parameter: its 16 bytes.
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

fn at() -> NdbDateTime {
    NdbDateTime::parse(AT).expect("valid instant")
}

fn at_system_time() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(AT_UNIX_SECS)
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

const INSERT: &str =
    "INSERT INTO loose_params (id, uid, at, naive, blob) VALUES ($1, $2, $3, $4, $5)";

fn insert_types() -> [Type; 5] {
    [
        Type::TEXT,
        Type::UUID,
        Type::TIMESTAMPTZ,
        Type::TIMESTAMP,
        Type::BYTEA,
    ]
}

/// Assert the document `id` holds every field typed, through the native
/// typed get and the pgwire field text.
async fn assert_typed(native: &NativeClient, pg: &Client, id: &str, how: &str) {
    let doc = native
        .document_get("loose_params", id)
        .await
        .expect("native get")
        .unwrap_or_else(|| panic!("{how}: document {id} exists"));
    assert_eq!(
        doc.get("uid"),
        Some(&Value::Uuid(UUID.into())),
        "{how}: uid"
    );
    assert_eq!(doc.get("at"), Some(&Value::DateTime(at())), "{how}: at");
    assert_eq!(
        doc.get("naive"),
        Some(&Value::NaiveDateTime(at())),
        "{how}: naive"
    );
    assert_eq!(
        doc.get("blob"),
        Some(&Value::Bytes(BLOB.to_vec())),
        "{how}: blob"
    );

    let messages = pg
        .simple_query(&format!(
            "SELECT uid, at, blob FROM loose_params WHERE id = '{id}'"
        ))
        .await
        .expect("pgwire select");
    let row = messages
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{how}: one row"));
    assert_eq!(row.get("uid"), Some(UUID), "{how}: uid text");
    assert_eq!(
        row.get("at"),
        Some(at().to_iso8601().as_str()),
        "{how}: at text"
    );
    assert_eq!(row.get("blob"), Some("\\x0102ff"), "{how}: blob text");
}

async fn create_collection(pg: &Client) {
    pg.batch_execute("CREATE COLLECTION loose_params")
        .await
        .expect("CREATE COLLECTION loose_params");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_and_text_parameters_store_typed() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;

    let statement = pg
        .prepare_typed(INSERT, &insert_types())
        .await
        .expect("prepare the typed insert");
    let uuid = BinaryUuid(*uuid_bytes());
    pg.execute(
        &statement,
        &[
            &"binary",
            &uuid,
            &at_system_time(),
            &at_system_time(),
            &BLOB.to_vec(),
        ],
    )
    .await
    .expect("insert binary parameters");
    assert_typed(&native, &pg, "binary", "binary parameters").await;

    pg.execute(
        &statement,
        &[
            &TextParam("text"),
            &TextParam("550E8400-E29B-41D4-A716-446655440000"),
            &TextParam("2024-01-02 03:04:05+00"),
            &TextParam("2024-01-02 03:04:05"),
            &TextParam("\\x0102ff"),
        ],
    )
    .await
    .expect("insert text parameters");
    assert_typed(&native, &pg, "text", "text parameters").await;

    server.graceful_shutdown().await;
}

fn uuid_bytes() -> &'static [u8; 16] {
    &[
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_sql_literals_store_typed() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;

    pg.batch_execute(&format!(
        "INSERT INTO loose_params (id, uid, at, naive, blob) VALUES ('lit', '{UUID}'::uuid, \
         TIMESTAMPTZ '{AT}', TIMESTAMP '2024-01-02 03:04:05', '\\x0102ff'::bytea)"
    ))
    .await
    .expect("insert typed literals");
    assert_typed(&native, &pg, "lit", "typed literals").await;

    pg.batch_execute(&format!(
        "INSERT INTO loose_params (id, uid, at, naive, blob) VALUES ('lit2', UUID '{UUID}', \
         '{AT}'::timestamptz, '2024-01-02 03:04:05'::timestamp, BYTEA '\\x0102ff')"
    ))
    .await
    .expect("insert typed strings");
    assert_typed(&native, &pg, "lit2", "typed strings").await;

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_parameters_store_typed() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;

    let price = Value::Decimal("12.50".parse().expect("decimal literal"));
    native
        .execute_sql(
            "INSERT INTO loose_params (id, uid, at, naive, blob, price) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                Value::String("native".into()),
                Value::Uuid(UUID.into()),
                Value::DateTime(at()),
                Value::NaiveDateTime(at()),
                Value::Bytes(BLOB.to_vec()),
                price.clone(),
            ],
        )
        .await
        .expect("native insert with typed parameters");
    assert_typed(&native, &pg, "native", "native parameters").await;
    let doc = native
        .document_get("loose_params", "native")
        .await
        .expect("native get")
        .expect("the document exists");
    let Some(Value::Decimal(read)) = doc.get("price") else {
        panic!("price stays a Decimal: {:?}", doc.get("price"));
    };
    assert_eq!(read.to_string(), "12.50", "the scale survives");

    server.graceful_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_typed_text_refuses_with_its_sqlstate() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    create_collection(&pg).await;

    for (ty, text, code) in [
        (Type::UUID, "not-a-uuid", "22P02"),
        (Type::BYTEA, "\\x0", "22P02"),
        (Type::TIMESTAMPTZ, "not a time", "22007"),
        (Type::TIMESTAMP, "not a time", "22007"),
    ] {
        let statement = pg
            .prepare_typed(
                "INSERT INTO loose_params (id, v) VALUES ($1, $2)",
                &[Type::TEXT, ty.clone()],
            )
            .await
            .expect("prepare");
        let err = pg
            .execute(&statement, &[&"bad", &TextParam(text)])
            .await
            .expect_err("invalid text for a typed parameter is refused");
        assert_eq!(
            err.code().map(|state| state.code()),
            Some(code),
            "{ty}: {err:?}"
        );
    }

    let err = pg
        .batch_execute("INSERT INTO loose_params (id, v) VALUES ('bad', 'not-a-uuid'::uuid)")
        .await
        .expect_err("an invalid uuid literal is refused");
    assert_eq!(
        err.code().map(|state| state.code()),
        Some("22P02"),
        "{err:?}"
    );

    server.graceful_shutdown().await;
}

/// `UPDATE ... SET` a UUID, instants, bytes and a decimal into undeclared
/// fields stores each typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_parameters_and_literals_store_typed() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;
    pg.batch_execute("INSERT INTO loose_params (id) VALUES ('up'), ('uplit')")
        .await
        .expect("insert the rows to update");

    let statement = pg
        .prepare_typed(
            "UPDATE loose_params SET uid = $2, at = $3, naive = $4, blob = $5, price = $6 \
             WHERE id = $1",
            &[
                Type::TEXT,
                Type::UUID,
                Type::TIMESTAMPTZ,
                Type::TIMESTAMP,
                Type::BYTEA,
                Type::NUMERIC,
            ],
        )
        .await
        .expect("prepare the typed update");
    let uuid = BinaryUuid(*uuid_bytes());
    pg.execute(
        &statement,
        &[
            &"up",
            &uuid,
            &at_system_time(),
            &at_system_time(),
            &BLOB.to_vec(),
            &TextParam("12.50"),
        ],
    )
    .await
    .expect("update with typed parameters");
    assert_typed(&native, &pg, "up", "update parameters").await;

    pg.batch_execute(&format!(
        "UPDATE loose_params SET uid = '{UUID}'::uuid, at = TIMESTAMPTZ '{AT}', \
         naive = TIMESTAMP '2024-01-02 03:04:05', blob = '\\x0102ff'::bytea, price = 12.50 \
         WHERE id = 'uplit'"
    ))
    .await
    .expect("update with typed literals");
    assert_typed(&native, &pg, "uplit", "update literals").await;

    for id in ["up", "uplit"] {
        let doc = native
            .document_get("loose_params", id)
            .await
            .expect("native get")
            .expect("the document exists");
        let Some(Value::Decimal(price)) = doc.get("price") else {
            panic!("{id}: price stays a Decimal: {:?}", doc.get("price"));
        };
        assert_eq!(price.to_string(), "12.50", "{id}: the scale survives");
    }

    server.graceful_shutdown().await;
}

/// The ids `sql` selects, in order.
async fn ids(pg: &Client, sql: &str) -> Vec<String> {
    pg.simple_query(sql)
        .await
        .expect("select")
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => row.get("id").map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// `WHERE blob = '\x0102ff'::bytea` finds the row that stores those bytes,
/// by a scan and through a secondary index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bytes_equality_matches_stored_bytes() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    create_collection(&pg).await;
    pg.batch_execute(
        "INSERT INTO loose_params (id, blob) VALUES ('hit', '\\x0102ff'::bytea), \
         ('miss', '\\x0102'::bytea)",
    )
    .await
    .expect("insert bytes rows");

    let query = "SELECT id FROM loose_params WHERE blob = '\\x0102ff'::bytea";
    assert_eq!(ids(&pg, query).await, vec!["hit".to_string()], "scan");

    pg.batch_execute("CREATE INDEX loose_params_blob ON loose_params (blob)")
        .await
        .expect("create the bytes index");
    assert_eq!(ids(&pg, query).await, vec!["hit".to_string()], "indexed");

    server.graceful_shutdown().await;
}

/// A typed timestamp literal whose text spells no instant is 22007.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_timestamp_literals_are_22007() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    create_collection(&pg).await;
    for literal in ["TIMESTAMP 'nope'", "'nope'::timestamptz"] {
        let err = pg
            .batch_execute(&format!(
                "INSERT INTO loose_params (id, v) VALUES ('bad', {literal})"
            ))
            .await
            .expect_err("an invalid timestamp literal is refused");
        assert_eq!(
            err.code().map(|state| state.code()),
            Some("22007"),
            "{literal}: {err:?}"
        );
    }
    server.graceful_shutdown().await;
}

/// `INSERT ... { }` and `[{ }]` with `$N` bind each parameter typed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brace_insert_parameters_store_typed() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;

    let uuid = BinaryUuid(*uuid_bytes());
    let types = insert_types();
    let object = pg
        .prepare_typed(
            "INSERT INTO loose_params { id: $1, uid: $2, at: $3, naive: $4, blob: $5 }",
            &types,
        )
        .await
        .expect("prepare the brace insert");
    pg.execute(
        &object,
        &[
            &"brace",
            &uuid,
            &at_system_time(),
            &at_system_time(),
            &BLOB.to_vec(),
        ],
    )
    .await
    .expect("brace insert with typed parameters");
    assert_typed(&native, &pg, "brace", "brace parameters").await;

    let array = pg
        .prepare_typed(
            "INSERT INTO loose_params [{ id: $1, uid: $2, at: $3, naive: $4, blob: $5 }]",
            &types,
        )
        .await
        .expect("prepare the array brace insert");
    pg.execute(
        &array,
        &[
            &"array",
            &uuid,
            &at_system_time(),
            &at_system_time(),
            &BLOB.to_vec(),
        ],
    )
    .await
    .expect("array brace insert with typed parameters");
    assert_typed(&native, &pg, "array", "array brace parameters").await;

    server.graceful_shutdown().await;
}

/// A native SQL text parameter holding quotes, a backslash, a NUL, `$1`
/// and braces is stored as exactly that text, in the VALUES and the brace
/// form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_text_parameters_are_stored_verbatim() {
    let server = TestServer::start().await;
    let pg = pg(&server).await;
    let native = native(&server);
    create_collection(&pg).await;

    let hostile = "x'); DROP COLLECTION loose_params; --\\ \u{0} $1 {a: 1} '' }";
    for (id, sql) in [
        (
            "values",
            "INSERT INTO loose_params (id, note) VALUES ($1, $2)",
        ),
        ("brace", "INSERT INTO loose_params { id: $1, note: $2 }"),
    ] {
        native
            .execute_sql(
                sql,
                &[Value::String(id.into()), Value::String(hostile.into())],
            )
            .await
            .expect("native insert with a hostile text parameter");
        let doc = native
            .document_get("loose_params", id)
            .await
            .expect("native get")
            .expect("the document exists");
        assert_eq!(
            doc.get("note"),
            Some(&Value::String(hostile.into())),
            "{id}"
        );
    }

    let err = native
        .execute_sql("SELECT 'open $1", &[Value::Integer(1)])
        .await
        .expect_err("an untokenizable statement with parameters is refused");
    assert!(err.to_string().contains("parameter bind"), "{err}");

    server.graceful_shutdown().await;
}
