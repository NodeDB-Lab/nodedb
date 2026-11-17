// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the vector engine (`primary='vector'` collections).
//!
//! A vector-primary row lives in two stores: the vector itself in the HNSW
//! graph, and every other column in a sparse-store sidecar keyed by the row's
//! surrogate in hex. Only the sidecar is readable as a row — `attach_body`
//! fetches it by that key and the response translator flattens it — so the
//! sidecar is what a `RETURNING` projection must report.
//!
//! The sidecar holds `zerompk` TAGGED bytes (`Value::String(s)` encodes as
//! `[4,"…"]`), stored verbatim by the upsert handler. Decoding them as an
//! ordinary document body yields tag arrays instead of values, which is the
//! same failure that once made a stored `"v1"` read back as the integer 118.

use crate::harness::TestServer;
use crate::harness::insert_returning_engines;
use tokio_postgres::SimpleQueryMessage;

/// Number of RESULT SETS in a simple-query response: one `CommandComplete` per
/// result set, which is what a driver counts for the statement.
fn result_set_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
        .count()
}

/// Every row in `collection` with its FULL column set, rendered as sorted
/// `name=value` pairs.
///
/// The same shape-capture the timeseries agreement test uses. For this engine
/// it answers a question that cannot be settled by reading the flatten path
/// with confidence: which columns a `SELECT *` on a vector-primary collection
/// actually produces — whether the vector field is projectable at all, and
/// whether the surrogate surfaces as a column or stays internal identity.
/// `RETURNING *` has to mean exactly what `SELECT *` means, so this is the
/// definition, not a convenience.
async fn full_rows(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_named_rows(&format!("SELECT * FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT * FROM {collection}: {e}"))
        .into_iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            format!("{{{}}}", cells.join(", "))
        })
        .collect()
}

async fn create_vector_primary(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// A vector-primary payload column must read back as its VALUE, not as the
/// zerompk tag array it is stored as.
///
/// This is a regression guard for a live read-path defect, not a shape probe.
/// The sidecar holds `zerompk::to_msgpack_vec(&HashMap<String, Value>)` —
/// tagged form — written verbatim by the upsert handler. The scan path
/// normalizes sparse rows through `doc_format::json_to_msgpack`, whose
/// "already standard MessagePack?" guard inspects only the OUTER container:
/// a tagged map is a valid msgpack map, so the bytes pass through untouched
/// and the tagged VALUES reach the client as `[4,"alice"]`.
///
/// That makes a vector-primary collection unreadable in any useful sense, and
/// it is independent of `RETURNING`. The full column set is reported on failure
/// because the same output settles what `RETURNING *` must mean for this
/// engine: which columns exist, whether the vector is projectable, and whether
/// the surrogate surfaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_primary_payload_column_reads_back_as_its_value_not_a_tag_array() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_shape").await;

    server
        .exec(
            "INSERT INTO vec_shape (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert must succeed");

    let shape = full_rows(&server, "vec_shape").await;
    assert_eq!(
        shape.len(),
        1,
        "one stored row: {shape:?}\n\
         (if this is empty, a vector-primary collection is not scannable without a \
          vector search and RETURNING has no SELECT to agree with)"
    );

    let row = &shape[0];
    // The payload columns must survive the tagged-encoding round trip. A body
    // decoded as an ordinary document would render these as `[4,"alice"]`
    // rather than `alice`, so this assertion is the tag-decode check as much as
    // a storage check.
    assert!(
        row.contains("owner=alice"),
        "a payload column must read back as its value, not as a zerompk tag array: {shape:?}"
    );
    assert!(
        row.contains("id=r1"),
        "the declared primary-key column must read back: {shape:?}"
    );
}

/// The vector-primary marker must be rebuilt from the durable catalog at boot,
/// not only installed by the live `CREATE COLLECTION`.
///
/// The marker is what tells the read path that this collection's sparse rows
/// are tagged sidecars. Deriving it only on the live-DDL path would leave a
/// collection readable until its first restart and unreadable after it — the
/// same "decoder given the wrong format" defect, one layer over. Reading the
/// rows back through a real restart is the only check that both the live path
/// and the boot seed carry it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_primary_payload_column_still_reads_back_after_restart() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_restart").await;

    server
        .exec(
            "INSERT INTO vec_restart (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert must succeed");

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let shape = full_rows(&server, "vec_restart").await;
    assert_eq!(
        shape.len(),
        1,
        "the stored row must survive restart: {shape:?}"
    );
    assert!(
        shape[0].contains("owner=alice"),
        "the vector-primary marker must be re-seeded from the catalog at boot, \
         or payload columns come back as tag arrays after a restart: {shape:?}"
    );
    assert!(
        shape[0].contains("id=r1"),
        "the declared primary-key column must read back after restart: {shape:?}"
    );
}

/// `RETURNING *` returns the stored sidecar row, and the column set it reports
/// is exactly the one a `SELECT *` on the same row reports.
///
/// Both come from the same converter against the same bytes, so the column-set
/// comparison is what proves that rather than a restatement of it: a projection
/// that decided the encoding a second time would still produce `id` and `owner`
/// as keys while rendering their values as tag arrays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_insert_returning_star_returns_the_stored_row() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_ret_star").await;

    let returned = server
        .query_named_rows(
            "INSERT INTO vec_ret_star (id, vec, owner) \
             VALUES ('v1', ARRAY[1.0, 0.0, 0.0], 'alice') RETURNING *",
        )
        .await
        .expect("vector-primary INSERT RETURNING must return the stored row");

    assert_eq!(returned.len(), 1, "one upserted row: {returned:?}");
    assert_eq!(
        returned[0].get("id").map(String::as_str),
        Some("v1"),
        "the declared primary key must come back as its value: {returned:?}"
    );
    assert_eq!(
        returned[0].get("owner").map(String::as_str),
        Some("alice"),
        "a payload column must come back as its value, not a zerompk tag array: {returned:?}"
    );

    let stored = full_rows(&server, "vec_ret_star").await;
    assert_eq!(stored.len(), 1, "one stored row: {stored:?}");
    let mut returned_cells: Vec<String> = returned[0]
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    returned_cells.sort();
    assert_eq!(
        format!("{{{}}}", returned_cells.join(", ")),
        stored[0],
        "RETURNING * must report the same columns and values a SELECT * reports"
    );
}

/// Named columns and aliases project exactly what was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_insert_returning_named_columns() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_ret_named").await;

    let returned = server
        .query_named_rows(
            "INSERT INTO vec_ret_named (id, vec, owner) \
             VALUES ('v1', ARRAY[1.0, 0.0, 0.0], 'alice') RETURNING owner AS who",
        )
        .await
        .expect("named RETURNING must succeed");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(
        returned[0].get("who").map(String::as_str),
        Some("alice"),
        "the alias must name the column: {returned:?}"
    );
    assert!(
        !returned[0].contains_key("owner"),
        "an aliased column must not also appear under its source name: {returned:?}"
    );
}

/// A vector-primary multi-row insert plans one `DirectBatchWrite` page, so its
/// rows must fold into ONE result set in submission order.
///
/// This is the assertion that fails if the response shaper misses its
/// `returning: Some(_)` arm: the rows then travel the opaque passthrough path,
/// which neither folds nor redacts them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_multi_row_insert_returns_one_result_set_in_order() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_ret_multi").await;

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO vec_ret_multi (id, vec, owner) VALUES \
             ('v1', ARRAY[1.0, 0.0, 0.0], 'alice'), \
             ('v2', ARRAY[0.0, 1.0, 0.0], 'bob'), \
             ('v3', ARRAY[0.0, 0.0, 1.0], 'carol') RETURNING id",
        )
        .await
        .expect("multi-row vector-primary insert with RETURNING must succeed");

    assert_eq!(
        result_set_count(&msgs),
        1,
        "one statement is one result set, however many rows it upserted"
    );

    let returned: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect();

    // Compared against a `SELECT` of the same column rather than a literal, so
    // a genuine rendering divergence between the two paths fails loudly instead
    // of being absorbed by an expectation chosen to match one of them.
    let selected: Vec<String> = server
        .query_rows("SELECT id FROM vec_ret_multi ORDER BY id")
        .await
        .expect("read the same rows back")
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    assert_eq!(
        returned, selected,
        "RETURNING and SELECT must render the same stored values: \
         returned={returned:?} selected={selected:?}"
    );
    assert_eq!(
        returned.len(),
        3,
        "one row per upserted row, in submission order: {returned:?}"
    );
}

/// Whatever the upsert path materializes, `RETURNING` reports it — the returned
/// row is compared against a `SELECT`, before AND after a restart.
///
/// The restart half is load-bearing here rather than thorough: it exercises the
/// boot-seeded `vector_primary` marker that the sparse-body format resolves
/// from. A row that agreed before the restart and disagreed after it is the
/// tag-array defect one layer over — the same bytes handed to a decoder told
/// the wrong format, because the marker was installed only by the live DDL path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_insert_returning_agrees_with_select_across_a_restart() {
    let server = TestServer::start().await;
    create_vector_primary(&server, "vec_ret_agree").await;

    let returned = server
        .query_named_rows(
            "INSERT INTO vec_ret_agree (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice') RETURNING id, owner",
        )
        .await
        .expect("vector-primary INSERT RETURNING must return the stored row");
    assert_eq!(returned.len(), 1, "one upserted row: {returned:?}");

    let selected = server
        .query_named_rows("SELECT id, owner FROM vec_ret_agree")
        .await
        .expect("read the row back");
    assert_eq!(selected.len(), 1, "one stored row: {selected:?}");

    // The stored row's own column set, before the restart. Carried into every
    // message below so a failure shows what the row IS, not only what the
    // projection managed to pull out of it.
    let shape_before = full_rows(&server, "vec_ret_agree").await;

    for column in ["id", "owner"] {
        assert_eq!(
            returned[0].get(column),
            selected[0].get(column),
            "RETURNING and SELECT must agree on {column}: \
             returned={returned:?} selected={selected:?}\n\
             stored column set before restart: {shape_before:?}"
        );
    }
    // Non-empty on both sides, so the agreement above is not two empty rows
    // agreeing with each other.
    assert_eq!(returned[0].get("id").map(String::as_str), Some("r1"));
    assert_eq!(returned[0].get("owner").map(String::as_str), Some("alice"));

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let after = server
        .query_named_rows("SELECT id, owner FROM vec_ret_agree")
        .await
        .expect("read the row back after restart");
    let shape_after = full_rows(&server, "vec_ret_agree").await;
    assert_eq!(
        after.len(),
        1,
        "the row must have survived: {after:?}\n\
         stored column set after restart: {shape_after:?}"
    );
    for column in ["id", "owner"] {
        assert_eq!(
            returned[0].get(column),
            after[0].get(column),
            "the row a write handed back must survive a restart unchanged on {column}: \
             returned={returned:?} after={after:?}\n\
             stored column set BEFORE restart: {shape_before:?}\n\
             stored column set AFTER  restart: {shape_after:?}"
        );
    }
    // The restart must not change the row's SHAPE either. A column set that
    // differs across the boundary means the sidecar was decoded under a
    // different format after boot, which is a distinct defect from a value
    // being lost — and it would otherwise only ever surface as a confusing
    // NULL in the per-column assertions above.
    assert_eq!(
        shape_before, shape_after,
        "the stored row's column set must survive a restart unchanged"
    );
}

/// Every engine the shared list calls refused still refuses, and every engine
/// it calls supported still hands back its stored row. Vector-primary moved
/// from the first list to the second in this change, so both halves assert it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_refused_and_supported_engine_lists_both_still_hold() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_refused_engines_still_refuse(&server, "vec_ret_refused").await;
    insert_returning_engines::assert_supported_engines_return_their_row(&server, "vec_ret_ok")
        .await;
}

/// A CLASSIC collection with a vector index over a document field must be
/// unaffected by the vector-primary sidecar decoding.
///
/// Its rows are ordinary document bodies, and its crash-recovery rebuild reads
/// them raw to extract the vector field out of the body — a vector-primary
/// sidecar has no vector field at all. Decoding these rows as sidecars would
/// both corrupt the scan and break the rebuild, so this pins the boundary from
/// the other side: after a restart the documents still read back as values and
/// the rebuilt index still ranks them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_classic_vector_indexed_collection_survives_restart_unchanged() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vec_classic TYPE document")
        .await
        .expect("create vec_classic");
    server
        .exec(
            "CREATE VECTOR INDEX idx_vec_classic ON vec_classic (embedding) \
             METRIC cosine DIM 3",
        )
        .await
        .expect("create vector index");

    for (id, owner, emb) in [
        ("c1", "alice", "ARRAY[1.0, 0.0, 0.0]"),
        ("c2", "bob", "ARRAY[0.0, 1.0, 0.0]"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vec_classic (id, owner, embedding) \
                 VALUES ('{id}', '{owner}', {emb})"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert {id}: {e}"));
    }

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let shape = full_rows(&server, "vec_classic").await;
    assert_eq!(
        shape.len(),
        2,
        "both documents must survive restart: {shape:?}"
    );
    assert!(
        shape.iter().any(|r| r.contains("owner=alice")),
        "a classic document column must still read back as its value: {shape:?}"
    );

    // The index rebuild reads the same rows RAW; if that path were rerouted
    // through the sidecar normalizer it would extract no vector and this search
    // would rank nothing.
    let nearest = server
        .query_rows(
            "SELECT id FROM vec_classic \
             ORDER BY vector_distance(embedding, ARRAY[1.0, 0.0, 0.0]) LIMIT 1",
        )
        .await
        .expect("vector search after restart");
    assert_eq!(
        nearest[0][0], "c1",
        "the rebuilt index must still rank the classic collection's vectors: {nearest:?}"
    );
}
