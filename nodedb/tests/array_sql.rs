//! End-to-end test for the `CREATE / DROP / INSERT INTO / DELETE FROM`
//! ARRAY SQL surface.
//!
//! Spins up a single-core NodeDB server via the shared pgwire harness,
//! exercises every array DDL/DML statement over the wire, and verifies
//! both wire-level success and Control-Plane catalog state.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn create_insert_delete_drop_array_via_pgwire() {
    let srv = TestServer::start().await;

    // 1. CREATE ARRAY
    srv.exec(
        "CREATE ARRAY genome_variants \
         DIMS (chrom INT64 [1..23], pos INT64 [0..300000000]) \
         ATTRS (variant STRING, qual FLOAT64) \
         TILE_EXTENTS (1, 1000000) \
         CELL_ORDER HILBERT",
    )
    .await
    .expect("CREATE ARRAY");

    // Catalog state should reflect the new array.
    {
        let cat = srv.shared.array_catalog.read().unwrap();
        assert!(
            cat.lookup_by_name("genome_variants").is_some(),
            "array catalog must contain the newly created array"
        );
    }

    // 2. INSERT INTO ARRAY (multi-row)
    srv.exec(
        "INSERT INTO ARRAY genome_variants \
         COORDS (1, 12345) VALUES ('SNP', 99.5), \
         COORDS (1, 12346) VALUES ('SNP', 88.2), \
         COORDS (2, 5000)  VALUES ('INS', 75.0)",
    )
    .await
    .expect("INSERT INTO ARRAY");

    // 3. DELETE FROM ARRAY
    srv.exec("DELETE FROM ARRAY genome_variants WHERE COORDS IN ((1, 12345))")
        .await
        .expect("DELETE FROM ARRAY");

    // 4. DROP ARRAY
    srv.exec("DROP ARRAY genome_variants")
        .await
        .expect("DROP ARRAY");

    {
        let cat = srv.shared.array_catalog.read().unwrap();
        assert!(
            cat.lookup_by_name("genome_variants").is_none(),
            "array catalog must be empty after DROP ARRAY"
        );
    }
}

#[tokio::test]
async fn drop_unknown_array_without_if_exists_errors() {
    let srv = TestServer::start().await;
    srv.expect_error("DROP ARRAY does_not_exist", "not found")
        .await;
}

#[tokio::test]
async fn drop_unknown_array_with_if_exists_succeeds() {
    let srv = TestServer::start().await;
    srv.exec("DROP ARRAY IF EXISTS does_not_exist")
        .await
        .expect("DROP ARRAY IF EXISTS");
}

#[tokio::test]
async fn insert_unknown_array_errors() {
    let srv = TestServer::start().await;
    srv.expect_error("INSERT INTO ARRAY ghost COORDS (1) VALUES (1)", "not found")
        .await;
}

// ── NDARRAY_* function surface ────────────────────────────────────────

/// Helper: spin up a server with a 2-dim array preloaded with cells.
async fn prepare_genome(srv: &TestServer) {
    srv.exec(
        "CREATE ARRAY genome_variants \
         DIMS (chrom INT64 [1..23], pos INT64 [0..300000000]) \
         ATTRS (variant STRING, qual FLOAT64) \
         TILE_EXTENTS (1, 1000000) \
         CELL_ORDER HILBERT",
    )
    .await
    .expect("CREATE ARRAY");
    srv.exec(
        "INSERT INTO ARRAY genome_variants \
         COORDS (1, 12345) VALUES ('SNP', 99.5), \
         COORDS (1, 12346) VALUES ('SNP', 88.2), \
         COORDS (2, 5000)  VALUES ('INS', 75.0)",
    )
    .await
    .expect("INSERT INTO ARRAY");
    // Force a flush so reads exercise the segment scan path.
    srv.exec("SELECT NDARRAY_FLUSH('genome_variants')")
        .await
        .expect("NDARRAY_FLUSH");
}

#[tokio::test]
async fn ndarray_slice_returns_in_range_cells() {
    let srv = TestServer::start().await;
    prepare_genome(&srv).await;

    let rows = srv
        .query_text(
            "SELECT * FROM NDARRAY_SLICE('genome_variants', \
             '{chrom: [1, 1], pos: [0, 13000]}', ['variant', 'qual'], 100)",
        )
        .await
        .expect("NDARRAY_SLICE");
    assert_eq!(
        rows.len(),
        2,
        "expected two cells in chrom=1, pos<13000; got {rows:?}"
    );
    // Each row is a JSON cell `{"coords": [...], "attrs": [...]}`.
    // Reject any internal `[18, "<base64>"]` tagged-msgpack leak.
    for row in &rows {
        let cell: serde_json::Value =
            serde_json::from_str(row).unwrap_or_else(|e| panic!("row not JSON object: {row}: {e}"));
        let coords = cell
            .get("coords")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("missing coords array in {row}"));
        assert!(!coords.is_empty(), "coords empty in {row}");
        let attrs = cell
            .get("attrs")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("missing attrs array in {row}"));
        assert!(!attrs.is_empty(), "attrs empty in {row}");
    }
}

#[tokio::test]
async fn ndarray_project_streams_one_row_per_cell() {
    let srv = TestServer::start().await;
    prepare_genome(&srv).await;

    let rows = srv
        .query_text("SELECT * FROM NDARRAY_PROJECT('genome_variants', ['qual'])")
        .await
        .expect("NDARRAY_PROJECT");
    assert_eq!(
        rows.len(),
        3,
        "expected three projected cells; got {rows:?}"
    );
}

#[tokio::test]
async fn ndarray_agg_sum_scalar() {
    let srv = TestServer::start().await;
    prepare_genome(&srv).await;

    let rows = srv
        .query_text("SELECT * FROM NDARRAY_AGG('genome_variants', 'qual', 'sum')")
        .await
        .expect("NDARRAY_AGG sum");
    assert_eq!(rows.len(), 1, "scalar agg must return one row");
    let row = &rows[0];
    // The result row is JSON `{"result": <f64>}`. Cheap substring check
    // avoids dragging in serde_json — we only care that the sum of
    // 99.5 + 88.2 + 75.0 = 262.7 round-trips.
    assert!(row.contains("262.7"), "expected result 262.7, got: {row}");
}

#[tokio::test]
async fn ndarray_agg_group_by_chrom() {
    let srv = TestServer::start().await;
    prepare_genome(&srv).await;

    let rows = srv
        .query_text("SELECT * FROM NDARRAY_AGG('genome_variants', 'qual', 'sum', 'chrom')")
        .await
        .expect("NDARRAY_AGG group");
    assert_eq!(
        rows.len(),
        2,
        "two distinct chrom values → two rows; got {rows:?}"
    );
    let joined = rows.join("\n");
    assert!(
        joined.contains("187.7"),
        "chrom=1 sum 187.7 missing: {joined}"
    );
    assert!(joined.contains("75"), "chrom=2 sum 75 missing: {joined}");
}

/// `NDARRAY_ELEMENTWISE` runs across two structurally-identical arrays
/// even when their names differ — `schema_hash` is computed over the
/// array's *content* fields (dims/attrs/tile_extents/orders) only, so
/// two distinct-named but shape-identical arrays share a hash and pair
/// up cleanly. The arithmetic correctness of elementwise itself is
/// covered by the dispatch-level test
/// `dispatch::array::tests_dispatch::elementwise_add_two_arrays`.
#[tokio::test]
async fn ndarray_elementwise_accepts_distinct_named_same_shape() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE ARRAY arr_a \
         DIMS (k INT64 [0..15]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await
    .expect("CREATE ARRAY arr_a");
    srv.exec(
        "CREATE ARRAY arr_b \
         DIMS (k INT64 [0..15]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await
    .expect("CREATE ARRAY arr_b");
    srv.exec("INSERT INTO ARRAY arr_a COORDS (0) VALUES (1.0)")
        .await
        .unwrap();
    srv.exec("INSERT INTO ARRAY arr_b COORDS (0) VALUES (10.0)")
        .await
        .unwrap();
    // No error: identical shape ⇒ identical content hash.
    let _ = srv
        .query_text("SELECT * FROM NDARRAY_ELEMENTWISE('arr_a', 'arr_b', 'add', 'qual')")
        .await
        .expect("NDARRAY_ELEMENTWISE on identically-shaped arrays");
}

#[tokio::test]
async fn ndarray_flush_and_compact_succeed() {
    let srv = TestServer::start().await;
    prepare_genome(&srv).await;

    let _ = srv
        .query_text("SELECT NDARRAY_FLUSH('genome_variants')")
        .await
        .expect("NDARRAY_FLUSH");
    let _ = srv
        .query_text("SELECT NDARRAY_COMPACT('genome_variants')")
        .await
        .expect("NDARRAY_COMPACT");
    // Subsequent read still works → flush/compact didn't break state.
    let rows = srv
        .query_text("SELECT * FROM NDARRAY_PROJECT('genome_variants', ['qual'])")
        .await
        .expect("NDARRAY_PROJECT after flush+compact");
    assert_eq!(rows.len(), 3, "post-maintenance reads must still work");
}

/// `DROP ARRAY` must broadcast to every Data-Plane core so a subsequent
/// `CREATE ARRAY` of the same name (with a different schema) does not
/// carry stale per-core memtable or segment state.
#[tokio::test]
async fn drop_then_recreate_with_different_schema_starts_clean() {
    let srv = TestServer::start().await;

    // Original array with attr `qual FLOAT64`.
    srv.exec(
        "CREATE ARRAY recyc \
         DIMS (k INT64 [0..15]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await
    .expect("CREATE ARRAY recyc v1");
    srv.exec("INSERT INTO ARRAY recyc COORDS (3) VALUES (42.0)")
        .await
        .expect("INSERT v1");

    // DROP must scatter `ArrayOp::DropArray` so each core releases state.
    srv.exec("DROP ARRAY recyc")
        .await
        .expect("DROP ARRAY recyc");

    // Re-create with a completely different schema (different attr name
    // *and* type). If the per-core store from v1 leaked through, the
    // engine would either reject the schema-hash mismatch or surface
    // stale cells with the wrong type.
    srv.exec(
        "CREATE ARRAY recyc \
         DIMS (k INT64 [0..15]) \
         ATTRS (label STRING) \
         TILE_EXTENTS (16) \
         CELL_ORDER ROW_MAJOR",
    )
    .await
    .expect("CREATE ARRAY recyc v2");

    // Project on the v2 schema must return zero rows — no v1 data left.
    let rows = srv
        .query_text("SELECT * FROM NDARRAY_PROJECT('recyc', ['label'])")
        .await
        .expect("NDARRAY_PROJECT recyc v2");
    assert!(
        rows.is_empty(),
        "fresh array must be empty; got stale rows: {rows:?}"
    );
}

/// End-to-end fusion: `ORDER BY vector_distance(...) + JOIN NDARRAY_SLICE(...)`
/// must reach the data plane as a single fused `VectorOp::Search` whose
/// `inline_prefilter_plan` is an `ArrayOp::SurrogateBitmapScan` over the
/// requested slice. Asserts the wire executes end-to-end without error;
/// the per-cell semantics (correctly filtered hits) are covered at the
/// dispatch level in `data::executor::dispatch::array::tests_dispatch::
/// vector_search_with_array_surrogate_prefilter`.
#[tokio::test]
async fn vector_search_with_ndarray_slice_prefilter_fuses_e2e() {
    let srv = TestServer::start().await;

    // Document collection backing vector embeddings.
    srv.exec("CREATE COLLECTION genes TYPE document")
        .await
        .expect("CREATE COLLECTION genes");
    srv.exec("CREATE VECTOR INDEX idx_genes_emb ON genes FIELD embedding METRIC cosine DIM 3")
        .await
        .expect("CREATE VECTOR INDEX");

    // Genome array — chr × pos with one float attr.
    srv.exec(
        "CREATE ARRAY genome \
         DIMS (chrom INT64 [1..23], pos INT64 [0..300000000]) \
         ATTRS (qual FLOAT64) \
         TILE_EXTENTS (1, 1000000) \
         CELL_ORDER HILBERT",
    )
    .await
    .expect("CREATE ARRAY genome");
    srv.exec(
        "INSERT INTO ARRAY genome \
         COORDS (1, 1000) VALUES (10.0), \
         COORDS (1, 2000) VALUES (20.0), \
         COORDS (2, 1000) VALUES (30.0)",
    )
    .await
    .expect("INSERT INTO ARRAY genome");
    srv.exec("SELECT NDARRAY_FLUSH('genome')")
        .await
        .expect("NDARRAY_FLUSH");

    // Fused query: ORDER BY vector_distance + JOIN NDARRAY_SLICE.
    // Surrogate alignment between the empty vector index and the array's
    // cells is not arranged here — the assertion is that the query
    // wires through every layer without error (planner fusion → convert
    // layer → ArrayOp::SurrogateBitmapScan + VectorOp::Search with
    // inline_prefilter_plan → data plane sub-plan execution).
    let _ = srv
        .query_text(
            "SELECT id FROM genes \
             JOIN NDARRAY_SLICE('genome', '{chrom: [1, 1], pos: [0, 50000]}') AS s \
               ON id = s.qual \
             ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0]) \
             LIMIT 10",
        )
        .await
        .expect("fused vector+array prefilter query");
}
