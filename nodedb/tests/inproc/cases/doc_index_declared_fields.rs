// SPDX-License-Identifier: BUSL-1.1

//! A declared field's auto-index is data-log state on every core.
//!
//! A RANGE scan over `$.city` reads through the field's index when the core
//! maintains it, and through every row when it does not. Either way it
//! returns every row in range. The cases cover two DDL steps on a
//! schemaless collection, which maintains every top-level key:
//!
//! - `DEFINE FIELD` on a collection that already holds rows ranges over
//!   those rows.
//! - `DROP INDEX` of an explicit index on a declared field leaves the
//!   top-level entries in place.
//!
//! The index lookup cases cover how the index reaches a core:
//!
//! - A declared build the DDL lost leaves the DDL answered, and the lookup
//!   refused as retryable until the reconcile pass delivers it.
//! - A schemaless collection maintains every top-level key from the first
//!   write, so a new key needs no build.

use nodedb::control::security::audit::NoopAuditEmitter;
use nodedb::control::server::shared::clone_write::{
    CloneCheckedOutcome, InterceptAndAuthorizeParams, intercept_and_authorize,
};
use nodedb::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};
use nodedb_test_support::native_harness::{NativeTestServer, do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{HelloFrame, NativeResponse, OpCode};
use tokio::net::TcpStream;

const CITIES: [&str; 4] = ["berlin", "lima", "oslo", "rome"];

async fn sql_ok(stream: &mut TcpStream, seq: u64, sql: &str) {
    let response = send_sql(stream, seq, sql).await;
    assert_ne!(
        response.status,
        ResponseStatus::Error,
        "{sql} must succeed: {response:?}"
    );
}

/// A native RANGE scan of `$.city` over `[a, z)`, which every city holds.
async fn range_scan(stream: &mut TcpStream, seq: u64, collection: &str) -> NativeResponse {
    send_request(
        stream,
        seq,
        OpCode::RangeScan,
        TextFields {
            collection: Some(collection.to_string()),
            field: Some("$.city".to_string()),
            lower_bound: Some(b"a".to_vec()),
            upper_bound: Some(b"z".to_vec()),
            limit: Some(100),
            ..Default::default()
        },
    )
    .await
}

fn row_count(response: &NativeResponse) -> usize {
    response.rows.as_ref().map(Vec::len).unwrap_or(0)
}

/// A native point put, which records no inferred field in the catalog.
async fn put_row(stream: &mut TcpStream, seq: u64, collection: &str, id: &str, city: &str) {
    let value = serde_json::json!({ "id": id, "city": city });
    let response = send_request(
        stream,
        seq,
        OpCode::PointPut,
        TextFields {
            collection: Some(collection.to_owned()),
            document_id: Some(id.to_owned()),
            data: Some(nodedb_types::json_to_msgpack(&value).expect("encode row")),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        response.status,
        ResponseStatus::Ok,
        "native PointPut {id}: {response:?}"
    );
}

#[tokio::test]
async fn define_field_on_a_populated_collection_ranges_over_every_row() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    sql_ok(
        &mut stream,
        1,
        "CREATE COLLECTION dfi_rows WITH (engine='document_schemaless')",
    )
    .await;
    let mut seq = 2;
    for (i, city) in CITIES.iter().enumerate() {
        put_row(&mut stream, seq, "dfi_rows", &format!("r{i}"), city).await;
        seq += 1;
    }

    sql_ok(
        &mut stream,
        seq,
        "DEFINE FIELD city ON dfi_rows TYPE string",
    )
    .await;
    seq += 1;
    let scanned = range_scan(&mut stream, seq, "dfi_rows").await;
    assert_eq!(scanned.status, ResponseStatus::Ok, "{scanned:?}");
    assert_eq!(
        row_count(&scanned),
        CITIES.len(),
        "the field's index holds every row written before DEFINE FIELD: {scanned:?}"
    );

    // A row written after the build is maintained.
    seq += 1;
    put_row(&mut stream, seq, "dfi_rows", "r9", "paris").await;
    seq += 1;
    let scanned = range_scan(&mut stream, seq, "dfi_rows").await;
    assert_eq!(row_count(&scanned), CITIES.len() + 1, "{scanned:?}");
}

#[tokio::test]
async fn drop_index_on_a_declared_field_keeps_the_declared_entries() {
    let server = NativeTestServer::start().await;
    let (mut stream, _ack) = do_handshake(server.addr, &HelloFrame::current())
        .await
        .expect("handshake");
    sql_ok(
        &mut stream,
        1,
        "CREATE COLLECTION dfd_rows (id STRING PRIMARY KEY, city STRING) \
         WITH (engine='document_schemaless')",
    )
    .await;
    let mut seq = 2;
    for (i, city) in CITIES.iter().enumerate() {
        sql_ok(
            &mut stream,
            seq,
            &format!("INSERT INTO dfd_rows (id, city) VALUES ('r{i}', '{city}')"),
        )
        .await;
        seq += 1;
    }

    sql_ok(
        &mut stream,
        seq,
        "CREATE INDEX dfd_by_city ON dfd_rows (city)",
    )
    .await;
    seq += 1;
    sql_ok(&mut stream, seq, "DROP INDEX dfd_by_city").await;
    seq += 1;

    let scanned = range_scan(&mut stream, seq, "dfd_rows").await;
    assert_eq!(scanned.status, ResponseStatus::Ok, "{scanned:?}");
    assert_eq!(
        row_count(&scanned),
        CITIES.len(),
        "the declared field keeps its entries after DROP INDEX: {scanned:?}"
    );

    // The declared field's index keeps following writes.
    seq += 1;
    sql_ok(
        &mut stream,
        seq,
        "INSERT INTO dfd_rows (id, city) VALUES ('r9', 'paris')",
    )
    .await;
    seq += 1;
    let scanned = range_scan(&mut stream, seq, "dfd_rows").await;
    assert_eq!(row_count(&scanned), CITIES.len() + 1, "{scanned:?}");
}

/// What one index lookup answered.
#[derive(Debug)]
enum Lookup {
    /// The ids of the rows the index holds under the value.
    Rows(Vec<serde_json::Value>),
    /// The core refused the read as retryable: it does not maintain the
    /// path yet.
    Refused(String),
}

/// An `IndexLookup` of `path = value` on `collection`, dispatched to its
/// core as the superuser of tenant 1.
async fn index_lookup(server: &TestServer, collection: &str, path: &str, value: &str) -> Lookup {
    let tenant_id = TenantId::new(1);
    let task = PhysicalTask {
        tenant_id,
        database_id: DatabaseId::DEFAULT,
        vshard_id: nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard(),
        plan: PhysicalPlan::Document(DocumentOp::IndexLookup {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            path: path.to_string(),
            value: value.to_string(),
            system_time: nodedb_types::SystemTimeScope::Current,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let identity = nodedb_test_support::pgwire_auth_helpers::superuser();
    let checked = match intercept_and_authorize(InterceptAndAuthorizeParams {
        state: &server.shared,
        task,
        identity: &identity,
        tenant_id,
        permissions: &server.shared.permissions,
        roles: &server.shared.roles,
        emitter: &NoopAuditEmitter,
    })
    .await
    .expect("authorize the lookup")
    {
        CloneCheckedOutcome::Proceed(checked) => checked,
        CloneCheckedOutcome::Handled(_) => panic!("a document lookup is never clone-intercepted"),
    };
    let dispatched = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        nodedb::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
            &server.shared,
            checked,
            TraceId::ZERO,
        ),
    )
    .await
    .expect("the lookup answers");
    match dispatched {
        Ok(response) => match response.error_code {
            Some(code) => Lookup::Refused(format!("{code:?}")),
            None => {
                let json = nodedb::data::executor::response_codec::decode_payload_to_json(
                    &response.payload,
                );
                match serde_json::from_str::<serde_json::Value>(&json) {
                    Ok(serde_json::Value::Array(ids)) => Lookup::Rows(ids),
                    other => panic!("an index lookup answers an id array, got {other:?}"),
                }
            }
        },
        Err(error) => Lookup::Refused(format!("{error:?}")),
    }
}

/// The ids a lookup found. Panics on a refusal.
fn rows(lookup: Lookup) -> Vec<serde_json::Value> {
    match lookup {
        Lookup::Rows(ids) => ids,
        Lookup::Refused(reason) => panic!("the lookup was refused: {reason}"),
    }
}

/// How many document index builds the cores ran.
fn backfills(server: &TestServer) -> u64 {
    server
        .shared
        .system_metrics
        .as_ref()
        .expect("system metrics")
        .document_index_backfills
        .load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_declared_build_is_refused_until_the_reconcile_delivers_it() {
    use nodedb::bootstrap::constraint_reconcile::{ReconcileDelivered, reconcile_once};
    use nodedb::fail_point::{FailAction, FailGuard};

    let server = TestServer::start().await;
    let guard = FailGuard::install(
        "doc_index::before_propose",
        FailAction::Fail("injected: the build proposal is lost".into()),
    );
    server
        .exec(
            "CREATE COLLECTION dfr_rows (id STRING PRIMARY KEY, city STRING) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE answers OK once its descriptor committed");
    for (i, city) in CITIES.iter().enumerate() {
        server
            .exec(&format!(
                "INSERT INTO dfr_rows (id, city) VALUES ('r{i}', '{city}')"
            ))
            .await
            .expect("insert");
    }
    match index_lookup(&server, "dfr_rows", "$.city", "lima").await {
        Lookup::Refused(reason) => assert!(
            reason.contains("RetryableRefusal") || reason.contains("does not maintain"),
            "the refusal is retryable: {reason}"
        ),
        Lookup::Rows(ids) => panic!("a core without the build answered {ids:?}"),
    }
    drop(guard);

    let mut delivered = ReconcileDelivered::default();
    let pass = reconcile_once(&server.shared, &mut delivered).await;
    assert!(
        pass.doc_index_steps > 0,
        "the reconcile delivers the lost build: {pass:?}"
    );
    for city in CITIES {
        assert_eq!(
            rows(index_lookup(&server, "dfr_rows", "$.city", city).await).len(),
            1,
            "the delivered build holds the row of {city}"
        );
    }

    let again = reconcile_once(&server.shared, &mut delivered).await;
    assert_eq!(
        again.doc_index_steps, 0,
        "an unchanged descriptor is not delivered again: {again:?}"
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconcile_build_read_before_a_drop_index_leaves_the_index_dropped() {
    use std::time::{Duration, Instant};

    use nodedb::bootstrap::constraint_reconcile::{ReconcileDelivered, reconcile_once};
    use nodedb::fail_point::{FailAction, FailGuard};

    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION sdr_rows (id STRING PRIMARY KEY, email STRING, active BOOL) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create");
    for (id, email, active) in [("a", "x@t.test", true), ("b", "y@t.test", false)] {
        server
            .exec(&format!(
                "INSERT INTO sdr_rows (id, email, active) VALUES ('{id}', '{email}', {active})"
            ))
            .await
            .expect("insert");
    }
    server
        .exec("CREATE INDEX sdr_active ON sdr_rows (email) WHERE active = true")
        .await
        .expect("create the partial index");
    assert!(
        rows(index_lookup(&server, "sdr_rows", "$.email", "y@t.test").await).is_empty(),
        "the partial index decides the entries of the path"
    );

    // A reconcile pass reads the catalog while the index is `Ready`, and
    // parks before it proposes.
    let dir = tempfile::tempdir().expect("tempdir");
    let release = dir.path().join("release");
    let parked = dir.path().join("release.parked");
    let _guard = FailGuard::install(
        "reconcile::before_doc_index_delivery",
        FailAction::WaitForFile(release.clone()),
    );
    let shared = std::sync::Arc::clone(&server.shared);
    let pass = tokio::spawn(async move {
        let mut delivered = ReconcileDelivered::default();
        reconcile_once(&shared, &mut delivered).await
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while !parked.exists() {
        assert!(Instant::now() < deadline, "the reconcile pass never parked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // The DROP commits and retires the owner on the core first.
    server
        .exec("DROP INDEX sdr_active")
        .await
        .expect("drop the index");
    std::fs::write(&release, b"").expect("release the pass");
    let pass = tokio::time::timeout(Duration::from_secs(60), pass)
        .await
        .expect("the reconcile pass answers")
        .expect("the reconcile task ran");
    assert!(
        pass.doc_index_steps > 0,
        "the pass proposed the steps of the older descriptor: {pass:?}"
    );

    // The build of the dropped index derived from an older descriptor than
    // its retire, so the declared field's full entries stay.
    assert_eq!(
        rows(index_lookup(&server, "sdr_rows", "$.email", "y@t.test").await).len(),
        1,
        "the dropped partial index did not come back"
    );
    assert_eq!(
        rows(index_lookup(&server, "sdr_rows", "$.email", "x@t.test").await).len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_schemaless_key_needs_no_build() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION skn_rows WITH (engine='document_schemaless')")
        .await
        .expect("create");
    for (i, city) in CITIES.iter().enumerate() {
        server
            .exec(&format!(
                "INSERT INTO skn_rows {{ id: 'r{i}', city: '{city}' }}"
            ))
            .await
            .expect("insert");
    }
    let before = backfills(&server);
    server
        .exec("INSERT INTO skn_rows { id: 'k1', fresh: 'yes' }")
        .await
        .expect("insert a new key");
    assert_eq!(
        backfills(&server),
        before,
        "an inferred key proposes no build"
    );
    assert_eq!(
        rows(index_lookup(&server, "skn_rows", "$.fresh", "yes").await).len(),
        1,
        "the first write of a key indexes it"
    );
    assert_eq!(
        rows(index_lookup(&server, "skn_rows", "$.city", "oslo").await).len(),
        1
    );

    // The counter follows the cores: an explicit index build counts.
    server
        .exec("CREATE INDEX skn_by_city ON skn_rows (city)")
        .await
        .expect("create index");
    assert!(backfills(&server) > before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_an_update_stored_is_indexed_before_an_insert_infers_it() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION sku_rows WITH (engine='document_schemaless')")
        .await
        .expect("create");
    server
        .exec("INSERT INTO sku_rows { id: 'a', n: 1 }")
        .await
        .expect("insert a");
    server
        .exec("UPDATE sku_rows SET k = 'va' WHERE id = 'a'")
        .await
        .expect("update adds k");
    server
        .exec("INSERT INTO sku_rows { id: 'b', k: 'vb' }")
        .await
        .expect("insert infers k");
    let found = rows(index_lookup(&server, "sku_rows", "$.k", "va").await);
    assert_eq!(found.len(), 1, "the index finds row a by k: {found:?}");
    let other = rows(index_lookup(&server, "sku_rows", "$.k", "vb").await);
    assert_eq!(other.len(), 1);
    assert_ne!(found, other, "the two values find two rows");
}
