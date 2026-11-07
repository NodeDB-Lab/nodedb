// SPDX-License-Identifier: BUSL-1.1

//! A collection in a non-default database homes to one vShard, and each row
//! keeps one surrogate, on every write and read path.
//!
//! The vShard hash and the surrogate allocator take the canonical key
//! `(database_id, bare_name)`. The SQL planner carries the qualified name
//! `"{database_id}/{name}"` and de-qualifies it. The native direct ops carry
//! the bare name. Each collection here is picked so the two strings hash to
//! different Data Plane cores. A path that hashed the qualified string would
//! place its rows on a core the other path never reads. It would also bind
//! their surrogates under a key the other path never looks up.
//!
//! Rows are written through SQL (pgwire) and through native `PointPut`. Each
//! row is then read and overwritten through the other path.

use std::collections::BTreeMap;

use nodedb_test_support::native_harness::{do_handshake, send_request};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, NativeResponse, OpCode};
use nodedb_types::{CollectionKey, DatabaseId, QualifiedCollection, Surrogate, TenantId};
use tokio::net::TcpStream;

const NUM_CORES: usize = 4;

/// The tenant the harness trust superuser belongs to.
const TENANT: TenantId = TenantId::new(1);

/// Rows the SQL path writes, with their first values.
const SQL_ROWS: [(&str, i64); 2] = [("s1", 7101), ("s2", 7102)];

/// Rows the native path writes, with their first values.
const NATIVE_ROWS: [(&str, i64); 2] = [("n1", 7201), ("n2", 7202)];

/// The engine under test and the name of its primary-key column.
#[derive(Clone, Copy)]
enum Engine {
    Document,
    Kv,
}

impl Engine {
    fn pk_column(self) -> &'static str {
        match self {
            Engine::Document => "id",
            Engine::Kv => "key",
        }
    }

    fn create_sql(self, name: &str) -> String {
        match self {
            Engine::Document => format!(
                "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, v INT) \
                 WITH (engine='document_schemaless')"
            ),
            Engine::Kv => {
                format!("CREATE COLLECTION {name} (key TEXT PRIMARY KEY, v INT) WITH (engine='kv')")
            }
        }
    }

    /// The value bytes a native `PointPut` carries for row `pk`.
    fn native_value(self, pk: &str, v: i64) -> Vec<u8> {
        let value = match self {
            Engine::Document => serde_json::json!({ "id": pk, "v": v }),
            Engine::Kv => serde_json::json!({ "v": v }),
        };
        nodedb_types::json_to_msgpack(&value).expect("encode native value")
    }
}

/// The Data Plane core a vShard runs on.
fn core_of(vshard: nodedb_types::id::VShardId) -> usize {
    vshard.as_u32() as usize % NUM_CORES
}

/// A collection name whose canonical key and raw qualified string hash to
/// different cores in `database_id`. Without the canonical key, the SQL path
/// and the native path would place its rows on different cores.
///
/// The hash is `h * 31 + b`, so the core distance between the two keys is fixed by the database id.
/// A database id with distance 0 has no such name, and this returns `None`.
fn split_placement_name(database_id: DatabaseId, prefix: &str) -> Option<String> {
    (0..256u32).map(|i| format!("{prefix}_{i}")).find(|name| {
        let qualified = QualifiedCollection::new(database_id, name);
        let canonical = CollectionKey::from_bare(database_id, name).vshard();
        let raw_qualified = CollectionKey::from_bare(database_id, qualified.as_str()).vshard();
        core_of(canonical) != core_of(raw_qualified)
    })
}

/// Create databases until one has a collection name that splits placement, and enter it.
async fn enter_database_with_split_name(
    srv: &TestServer,
    prefix: &str,
) -> (String, DatabaseId, String) {
    for attempt in 0..32u32 {
        let database = format!("{prefix}_db_{attempt}");
        srv.exec(&format!("CREATE DATABASE {database}"))
            .await
            .expect("create database");
        let database_id = srv
            .shared
            .credentials
            .catalog()
            .get_database_id_by_name(&database)
            .expect("read database id")
            .expect("database exists");
        assert_ne!(database_id, DatabaseId::DEFAULT);
        if let Some(collection) = split_placement_name(database_id, prefix) {
            srv.exec(&format!("USE DATABASE {database}"))
                .await
                .expect("use database");
            return (database, database_id, collection);
        }
    }
    panic!("no database id under '{prefix}' lets a collection name split placement across cores");
}

/// Open a native session bound to `database`.
async fn native_session(srv: &TestServer, database: &str) -> TcpStream {
    let username = srv
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in trust mode");
    let addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (mut stream, _ack) = do_handshake(addr, &HelloFrame::current())
        .await
        .expect("native handshake");
    let auth = send_request(
        &mut stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust { username }),
            database: Some(database.to_owned()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(auth.status, ResponseStatus::Ok, "native auth: {auth:?}");
    stream
}

async fn native_put(
    stream: &mut TcpStream,
    seq: u64,
    engine: Engine,
    collection: &str,
    pk: &str,
    v: i64,
) {
    let put = send_request(
        stream,
        seq,
        OpCode::PointPut,
        TextFields {
            collection: Some(collection.to_owned()),
            document_id: Some(pk.to_owned()),
            data: Some(engine.native_value(pk, v)),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        put.status,
        ResponseStatus::Ok,
        "native PointPut {pk}: {put:?}"
    );
}

async fn native_get(
    stream: &mut TcpStream,
    seq: u64,
    collection: &str,
    pk: &str,
) -> NativeResponse {
    let get = send_request(
        stream,
        seq,
        OpCode::PointGet,
        TextFields {
            collection: Some(collection.to_owned()),
            document_id: Some(pk.to_owned()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        get.status,
        ResponseStatus::Ok,
        "native PointGet {pk}: {get:?}"
    );
    get
}

/// Assert a native `PointGet` of `pk` finds the row and carries `v`.
async fn assert_native_reads(stream: &mut TcpStream, seq: u64, collection: &str, pk: &str, v: i64) {
    let get = native_get(stream, seq, collection, pk).await;
    let rows = get.rows.clone().unwrap_or_default();
    assert!(
        !rows.is_empty(),
        "native PointGet must find row '{pk}' in '{collection}': {get:?}"
    );
    assert!(
        format!("{rows:?}").contains(&v.to_string()),
        "native PointGet of '{pk}' must carry v = {v}: {get:?}"
    );
}

/// Assert a SQL point read of `pk` finds exactly one row with value `v`.
async fn assert_sql_reads(srv: &TestServer, engine: Engine, collection: &str, pk: &str, v: i64) {
    let pk_column = engine.pk_column();
    let rows = srv
        .query_rows(&format!(
            "SELECT {pk_column}, v FROM {collection} WHERE {pk_column} = '{pk}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("SQL point read of '{pk}': {e}"));
    assert_eq!(
        rows,
        vec![vec![pk.to_owned(), v.to_string()]],
        "SQL point read must find row '{pk}' in '{collection}' with v = {v}"
    );
}

/// The surrogate bound to every row, keyed by primary key. Each binding
/// lives under the canonical key, never under the raw qualified string.
fn surrogates(
    srv: &TestServer,
    database_id: DatabaseId,
    collection: &str,
) -> BTreeMap<String, Surrogate> {
    let canonical = CollectionKey::from_bare(database_id, collection);
    let qualified = QualifiedCollection::new(database_id, collection);
    let raw_qualified = CollectionKey::from_bare(database_id, qualified.as_str());
    let mut out = BTreeMap::new();
    for (pk, _) in SQL_ROWS.iter().chain(NATIVE_ROWS.iter()) {
        let surrogate = srv
            .shared
            .surrogate_assigner
            .lookup_bound(canonical, TENANT, pk.as_bytes())
            .expect("surrogate lookup")
            .unwrap_or_else(|| panic!("row '{pk}' has no surrogate under the canonical key"));
        let stray = srv
            .shared
            .surrogate_assigner
            .lookup_bound(raw_qualified, TENANT, pk.as_bytes())
            .expect("stray surrogate lookup");
        assert_eq!(
            stray, None,
            "row '{pk}' must not bind a surrogate under the qualified string"
        );
        out.insert((*pk).to_owned(), surrogate);
    }
    out
}

/// Run the cross-path scenario for one engine.
async fn cross_path_rows_share_placement_and_identity(engine: Engine, prefix: &str) {
    let srv = TestServer::start_multicores(NUM_CORES).await;
    let (database, database_id, collection) = enter_database_with_split_name(&srv, prefix).await;
    srv.exec(&engine.create_sql(&collection))
        .await
        .expect("create collection");

    let pk_column = engine.pk_column();
    for (pk, v) in SQL_ROWS {
        srv.exec(&format!(
            "INSERT INTO {collection} ({pk_column}, v) VALUES ('{pk}', {v})"
        ))
        .await
        .unwrap_or_else(|e| panic!("SQL INSERT of '{pk}': {e}"));
    }

    let mut stream = native_session(&srv, &database).await;
    let mut seq = 2;
    for (pk, v) in NATIVE_ROWS {
        native_put(&mut stream, seq, engine, &collection, pk, v).await;
        seq += 1;
    }

    // Each path reads the rows the other path wrote.
    for (pk, v) in SQL_ROWS {
        assert_native_reads(&mut stream, seq, &collection, pk, v).await;
        seq += 1;
    }
    for (pk, v) in NATIVE_ROWS {
        assert_sql_reads(&srv, engine, &collection, pk, v).await;
    }

    let before = surrogates(&srv, database_id, &collection);

    // Each path overwrites a row the other path wrote. The overwrite must
    // land on the stored row, never beside it.
    let (sql_pk, _) = SQL_ROWS[0];
    let native_over_sql = 7301;
    native_put(
        &mut stream,
        seq,
        engine,
        &collection,
        sql_pk,
        native_over_sql,
    )
    .await;
    seq += 1;
    let (native_pk, _) = NATIVE_ROWS[0];
    let sql_over_native = 7302;
    srv.exec(&format!(
        "UPSERT INTO {collection} ({pk_column}, v) VALUES ('{native_pk}', {sql_over_native})"
    ))
    .await
    .unwrap_or_else(|e| panic!("SQL UPSERT of '{native_pk}': {e}"));

    assert_sql_reads(&srv, engine, &collection, sql_pk, native_over_sql).await;
    assert_native_reads(&mut stream, seq, &collection, native_pk, sql_over_native).await;

    let after = surrogates(&srv, database_id, &collection);
    assert_eq!(
        before, after,
        "every row must keep its surrogate when the other path overwrites it"
    );

    let mut keys: Vec<String> = srv
        .query_rows(&format!("SELECT {pk_column} FROM {collection}"))
        .await
        .expect("full scan")
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec!["n1", "n2", "s1", "s2"],
        "each row must be stored once, on one core"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_rows_share_placement_and_surrogate_across_sql_and_native() {
    cross_path_rows_share_placement_and_identity(Engine::Document, "ckp_doc").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_rows_share_placement_and_surrogate_across_sql_and_native() {
    cross_path_rows_share_placement_and_identity(Engine::Kv, "ckp_kv").await;
}
