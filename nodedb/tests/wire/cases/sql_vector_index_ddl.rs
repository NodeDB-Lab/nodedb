// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `CREATE VECTOR INDEX` / `ALTER VECTOR INDEX` DDL
//! quantization parameters: INDEX_TYPE, PQ_M, IVF_CELLS, IVF_NPROBE.
//!
//! Asserts that the SQL DDL surface recognizes and validates the quantization
//! keywords advertised in `docs/vectors.md`. Silent fall-through to FP32 HNSW
//! (unknown parameters ignored instead of rejected, validation skipped) is the
//! regression mode these tests guard.
//!
//! The same guard extends to the option surface as a whole: the handler scans
//! whitespace-split tokens for known keywords, so any option spelling it was
//! not written against is skipped rather than rejected, and the Data Plane's
//! rejection of the resulting garbage configuration is discarded instead of
//! surfaced. Both produce a reported-successful `CREATE VECTOR INDEX` that
//! builds no usable index and a `SEARCH` that returns an empty set with
//! nothing to diagnose from.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_unknown_index_type_errors() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_bogus TYPE document")
        .await
        .unwrap();

    // Unknown quantization tier must be rejected at the DDL layer, not silently
    // downgraded to FP32 HNSW. This is the core fall-through regression guard.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_bogus ON vi_bogus \
             METRIC cosine DIM 4 INDEX_TYPE bogus_type",
            "index_type",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_hnsw_pq_pq_m_must_divide_dim() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_bad_pqm TYPE document")
        .await
        .unwrap();

    // PQ subquantizer count must divide the vector dimension evenly — otherwise
    // the index cannot be constructed. Today this is silently accepted because
    // PQ_M is never parsed; the engine falls back to PQ_M=8 which also doesn't
    // divide 6, masking the bug until the first insert. DDL must validate up-front.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_bad_pqm ON vi_bad_pqm \
             METRIC cosine DIM 6 INDEX_TYPE hnsw_pq PQ_M 4",
            "pq_m",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_hnsw_pq_rejects_codebook_over_limit() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_big_pq TYPE document")
        .await
        .unwrap();

    // PQ_M 8 divides DIM 65536, but 8 × 256 centroids of 8192 floats
    // exceed the codebook cap. DDL refuses the shape the codec cannot train.
    let err = server
        .exec(
            "CREATE VECTOR INDEX idx_vi_big_pq ON vi_big_pq \
             METRIC cosine DIM 65536 INDEX_TYPE hnsw_pq PQ_M 8",
        )
        .await
        .expect_err("a codebook over the cap is refused");
    assert!(err.contains("SQLSTATE 22023"), "got: {err}");
    assert!(err.to_lowercase().contains("codebook"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_vector_index_to_hnsw_pq_checks_default_pq_m() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_alter_pqm TYPE document")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vi_alter_pqm ON vi_alter_pqm METRIC cosine DIM 12")
        .await
        .unwrap();

    // No pq_m in the SET list: the engine default 8 applies, and 8 does
    // not divide 12.
    let err = server
        .exec("ALTER VECTOR INDEX ON vi_alter_pqm SET (index_type = 'hnsw_pq')")
        .await
        .expect_err("the default pq_m does not divide dim 12");
    assert!(err.contains("SQLSTATE 22023"), "got: {err}");
    assert!(err.contains("pq_m"), "got: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_accepts_valid_hnsw_pq() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_hnsw_pq TYPE document")
        .await
        .unwrap();

    // Valid hnsw_pq configuration: PQ_M divides DIM. Must be accepted.
    // Positive lock-in: prevents the fix from over-rejecting valid syntax.
    server
        .exec(
            "CREATE VECTOR INDEX idx_vi_hnsw_pq ON vi_hnsw_pq \
             METRIC cosine DIM 4 INDEX_TYPE hnsw_pq PQ_M 2",
        )
        .await
        .expect("valid hnsw_pq configuration must be accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_accepts_valid_ivf_pq() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_ivf_pq TYPE document")
        .await
        .unwrap();

    // Valid ivf_pq configuration with IVF_CELLS and IVF_NPROBE.
    // Positive lock-in for the most memory-efficient documented tier.
    server
        .exec(
            "CREATE VECTOR INDEX idx_vi_ivf_pq ON vi_ivf_pq \
             METRIC cosine DIM 4 INDEX_TYPE ivf_pq PQ_M 2 IVF_CELLS 64 IVF_NPROBE 8",
        )
        .await
        .expect("valid ivf_pq configuration must be accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_per_column_two_embeddings_on_one_collection() {
    // GAP-9: `CREATE VECTOR INDEX ... ON <coll> (<column>) ...` names the
    // embedding column the index covers, so one collection can carry several
    // vector indexes (e.g. a text-embedding and an image-embedding column),
    // each with its own params. The `(<column>)` token must not be
    // silently discarded — discarding it lands every index's config on the
    // default (unnamed) field.
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_multi TYPE document")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vi_multi_text ON vi_multi (text_emb) METRIC cosine DIM 4")
        .await
        .expect("first per-column vector index must be accepted");
    // A second vector index on a *different* column of the same collection
    // must also be accepted (and use its own metric), not rejected as a
    // duplicate / param change.
    server
        .exec("CREATE VECTOR INDEX idx_vi_multi_img ON vi_multi (image_emb) METRIC l2 DIM 8")
        .await
        .expect("second per-column vector index on a different column must be accepted");

    for (id, t, i) in [
        (
            "a",
            [0.10f32, 0.20, 0.30, 0.40],
            [1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "b",
            [0.11, 0.21, 0.31, 0.41],
            [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
        (
            "c",
            [0.90, 0.80, 0.70, 0.60],
            [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ),
    ] {
        server
            .exec(&format!(
                "INSERT INTO vi_multi (id, text_emb, image_emb) VALUES \
                 ('{id}', ARRAY[{},{},{},{}], ARRAY[{},{},{},{},{},{},{},{}])",
                t[0], t[1], t[2], t[3], i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]
            ))
            .await
            .unwrap();
    }

    let by_text = server
        .query_text("SELECT id FROM vi_multi WHERE text_emb <=> ARRAY[0.1, 0.2, 0.3, 0.4] LIMIT 2")
        .await
        .unwrap();
    assert_eq!(
        by_text.len(),
        2,
        "search on the (text_emb) index must return its nearest rows; got {by_text:?}"
    );

    let by_image = server
        .query_text(
            "SELECT id FROM vi_multi WHERE image_emb <-> ARRAY[1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0] LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        by_image.len(),
        2,
        "search on the (image_emb) index must return its nearest rows; got {by_image:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejects_unrecognized_option_syntax() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_optsyn").await.unwrap();

    // The pgvector-shaped `WITH (dim = ..., metric = ...)` spelling is not the
    // supported option syntax. Tokens the handler does not recognize must be a
    // syntax error — skipping them yields an index configured from nothing.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_optsyn ON vi_optsyn (embedding) \
             WITH (dim = 3, metric = 'cosine')",
            "unrecognized option 'WITH'",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_surfaces_data_plane_rejection() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_dpreject").await.unwrap();

    // The metric name is validated in the Data Plane, whose response the DDL
    // handler discards — so an unusable configuration is reported as a
    // successful CREATE. A rejected configuration must reach the client.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_dpreject ON vi_dpreject (embedding) \
             METRIC euclidian DIM 4",
            "metric",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejected_options_leave_no_inert_index() {
    // End-to-end guard for the silent-empty failure mode: a rejected
    // CREATE must not leave behind a registered-but-unusable index that
    // swallows every later search. After the rejection the collection must
    // still be indexable, and the search must return the rows that are there.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_inert").await.unwrap();
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_inert ON vi_inert (embedding) \
             WITH (dim = 3, metric = 'cosine')",
            "unrecognized option 'WITH'",
        )
        .await;
    server
        .exec("CREATE VECTOR INDEX idx_vi_inert ON vi_inert (embedding) METRIC cosine DIM 3")
        .await
        .expect("supported option syntax must be accepted after a rejected attempt");
    for (id, v) in [("a1", "0.1,0.2,0.3"), ("a2", "0.4,0.5,0.6")] {
        server
            .exec(&format!(
                "INSERT INTO vi_inert (id, embedding) VALUES ('{id}', ARRAY[{v}])"
            ))
            .await
            .unwrap();
    }

    let hits = server
        .query_text("SEARCH vi_inert USING VECTOR(embedding, ARRAY[0.1,0.2,0.3], 2)")
        .await
        .expect("SEARCH over an indexed collection must succeed");
    assert_eq!(
        hits.len(),
        2,
        "stored rows must be reachable through the index; an empty result set is \
         the silent failure this guards: got {hits:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejects_zero_dim() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_dim0").await.unwrap();

    // An index whose declared dimension is zero can never match anything.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_dim0 ON vi_dim0 (embedding) METRIC cosine DIM 0",
            "dim",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejects_omitted_dim() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_nodim").await.unwrap();

    // Omitting DIM defaults it to zero — the same inert index as an explicit
    // `DIM 0`, reached without the user ever typing a bad value.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_nodim ON vi_nodim (embedding) METRIC cosine",
            "dim",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejects_non_numeric_dim() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_dimtext").await.unwrap();

    // A value that does not parse must be an error, not a silent fall back to
    // the zero default.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_dimtext ON vi_dimtext (embedding) METRIC cosine DIM three",
            "dim",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_rejects_non_numeric_ef_construction() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_eftext").await.unwrap();

    // Same silent-default path as DIM, on the build-quality knob: an unparsed
    // EF_CONSTRUCTION silently builds a 200-ef index the user did not ask for.
    server
        .expect_error(
            "CREATE VECTOR INDEX idx_vi_eftext ON vi_eftext (embedding) \
             METRIC cosine DIM 4 EF_CONSTRUCTION high",
            "ef_construction",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_accepts_if_not_exists() {
    // `CREATE VECTOR INDEX IF NOT EXISTS ...` is advertised in
    // `docs/query-language.md`; the positional token scan reads `IF` as the
    // index name and then fails to find `ON` where it expects it.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_ine").await.unwrap();
    server
        .exec(
            "CREATE VECTOR INDEX IF NOT EXISTS idx_vi_ine ON vi_ine (embedding) \
             METRIC cosine DIM 4",
        )
        .await
        .expect("documented IF NOT EXISTS form must be accepted");
    // Re-running must be a no-op rather than an error — that is what the
    // clause is for.
    server
        .exec(
            "CREATE VECTOR INDEX IF NOT EXISTS idx_vi_ine ON vi_ine (embedding) \
             METRIC cosine DIM 4",
        )
        .await
        .expect("IF NOT EXISTS must make a repeat CREATE a no-op");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_vector_index_enforces_declared_dim_on_insert() {
    // The declared DIM is persisted to the catalog but never compared against
    // the vectors that arrive: the index silently adopts the dimension of the
    // first inserted row. A row that disagrees with the declaration must be
    // rejected, otherwise DIM is decoration and a wrong-width embedding
    // pipeline is discovered only as bad search results.
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_dimmix").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vi_dimmix ON vi_dimmix (embedding) METRIC cosine DIM 4")
        .await
        .unwrap();

    let inserted = server
        .exec("INSERT INTO vi_dimmix (id, embedding) VALUES ('a1', ARRAY[0.1,0.2,0.3])")
        .await;
    assert!(
        inserted.is_err(),
        "a 3-wide vector must not be accepted into a DIM 4 index"
    );
    let message = inserted.unwrap_err().to_lowercase();
    assert!(
        message.contains("dimension") || message.contains("dim"),
        "rejection must name the dimension mismatch, got: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_vector_index_rejects_malformed_set_item() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION vi_setbad").await.unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vi_setbad ON vi_setbad (embedding) METRIC cosine DIM 4")
        .await
        .unwrap();

    // A list item carrying no `=` is a typo. Dropping it reports success for
    // the items around it while one of them never applied.
    server
        .expect_error(
            "ALTER VECTOR INDEX ON vi_setbad.embedding SET (m = 32, ef_construction)",
            "ef_construction",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_vector_index_set_index_type_accepted() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION vi_alter TYPE document")
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_vi_alter ON vi_alter METRIC cosine DIM 4")
        .await
        .unwrap();

    // ALTER accepts the same quantization keyword set as CREATE. Without it,
    // users who defaulted to FP32 have no SQL migration path to the documented
    // tiers.
    server
        .exec("ALTER VECTOR INDEX ON vi_alter SET (index_type = 'hnsw_pq', pq_m = 2)")
        .await
        .expect("ALTER VECTOR INDEX SET (index_type = ...) must be accepted");
}
