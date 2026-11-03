// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for the Vector engine.
//!
//! These tests cover vector search over a document collection: a
//! `document_schemaless` collection with an HNSW index on a field. The
//! vector engine itself, `WITH (engine='vector')`, is a vector-primary
//! collection; `sql_create_collection_type_clause` and the `vector_primary_*`
//! cases cover it.
//! Covers: index creation, ANN search via vector_distance, quantization option,
//! and the quantization hint check against a vector-primary collection's codec
//! and an IVF-PQ index's codec.

use crate::harness::TestServer;

#[tokio::test]
async fn create_vector_collection_and_insert() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_basic WITH (engine='document_schemaless')")
        .await
        .unwrap();

    srv.exec("INSERT INTO vec_basic { id: 'v1', embedding: [0.1, 0.2, 0.3], label: 'cat' }")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_basic { id: 'v2', embedding: [0.9, 0.8, 0.7], label: 'dog' }")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT COUNT(*) FROM vec_basic")
        .await
        .unwrap();
    assert_eq!(rows[0][0].parse::<u32>().unwrap(), 2);
}

#[tokio::test]
async fn create_vector_index_and_ann_search() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_ann WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_ann (embedding)")
        .await
        .unwrap();

    let vecs: &[(&str, [f32; 4])] = &[
        ("v1", [1.0, 0.0, 0.0, 0.0]),
        ("v2", [0.0, 1.0, 0.0, 0.0]),
        ("v3", [0.0, 0.0, 1.0, 0.0]),
        ("v4", [0.0, 0.0, 0.0, 1.0]),
        ("v5", [0.7, 0.7, 0.0, 0.0]),
    ];
    for (id, emb) in vecs {
        let arr = format!("[{},{},{},{}]", emb[0], emb[1], emb[2], emb[3]);
        srv.exec(&format!(
            "INSERT INTO vec_ann {{ id: '{id}', embedding: {arr} }}"
        ))
        .await
        .unwrap();
    }

    let rows = srv
        .query_rows(
            "SELECT id FROM vec_ann \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) \
             LIMIT 3",
        )
        .await
        .unwrap();
    let ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(ids.contains(&"v1"), "v1 not in top-3: {ids:?}");
}

#[tokio::test]
async fn vector_distance_with_quantization_option() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_quant WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_quant (vec)").await.unwrap();

    srv.exec("INSERT INTO vec_quant { id: 'q1', vec: [1.0, 2.0, 3.0, 4.0] }")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_quant { id: 'q2', vec: [4.0, 3.0, 2.0, 1.0] }")
        .await
        .unwrap();

    let rows = srv
        .query_rows(
            "SELECT id FROM vec_quant \
             ORDER BY vector_distance(vec, ARRAY[1.0,2.0,3.0,4.0], quantization => 'sq8') \
             LIMIT 2",
        )
        .await
        .unwrap();
    assert!(!rows.is_empty(), "expected at least one result");
    assert_eq!(rows[0][0], "q1");
}

/// Create a vector-primary collection `name` of dim 8 with `quantization`,
/// holding the eight unit vectors `u0`..`u7`.
async fn vector_primary_with_codec(srv: &TestServer, name: &str, quantization: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id STRING PRIMARY KEY, emb VECTOR(8)) \
         WITH (engine='vector', vector_field='emb', dim=8, metric='l2', \
               quantization='{quantization}')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {name} with {quantization}: {e}"));
    for i in 0..8 {
        let parts: Vec<&str> = (0..8).map(|j| if j == i { "1.0" } else { "0.0" }).collect();
        srv.exec(&format!(
            "INSERT INTO {name} (id, emb) VALUES ('u{i}', ARRAY[{}])",
            parts.join(", ")
        ))
        .await
        .unwrap_or_else(|e| panic!("insert u{i} into {name}: {e}"));
    }
}

/// The nearest id to the unit vector along axis 2, searched with `hint`.
async fn nearest_with_hint(srv: &TestServer, name: &str, hint: &str) -> Result<String, String> {
    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM {name} \
             ORDER BY vector_distance(emb, ARRAY[0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0], \
                                      quantization => '{hint}') \
             LIMIT 1"
        ))
        .await?;
    Ok(rows.first().map(|r| r[0].clone()).unwrap_or_default())
}

#[tokio::test]
async fn vector_primary_ternary_collection_answers_ternary_hint() {
    let srv = TestServer::start().await;
    vector_primary_with_codec(&srv, "vp_ternary", "ternary").await;
    let nearest = nearest_with_hint(&srv, "vp_ternary", "ternary")
        .await
        .expect("a ternary hint on a ternary collection is served");
    assert_eq!(nearest, "u2");
}

#[tokio::test]
async fn vector_primary_opq_collection_answers_opq_hint() {
    let srv = TestServer::start().await;
    vector_primary_with_codec(&srv, "vp_opq", "opq").await;
    let nearest = nearest_with_hint(&srv, "vp_opq", "opq")
        .await
        .expect("an opq hint on an opq collection is served");
    assert_eq!(nearest, "u2");
}

#[tokio::test]
async fn quantization_hint_mismatch_is_an_error() {
    let srv = TestServer::start().await;
    vector_primary_with_codec(&srv, "vp_mismatch", "ternary").await;
    // The codec is fixed at CREATE. A hint naming another codec is refused,
    // never answered with the configured codec.
    let err = nearest_with_hint(&srv, "vp_mismatch", "sq8")
        .await
        .expect_err("an sq8 hint on a ternary collection is refused");
    assert!(err.contains("quantization"), "got: {err}");
}

#[tokio::test]
async fn wal_restart_durability() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_wal WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("INSERT INTO vec_wal { id: 'w1', label: 'persisted' }")
        .await
        .unwrap();

    let (srv, dir) = srv.take_dir();
    srv.graceful_shutdown().await;

    let (srv2, _dir) = TestServer::open_on_path(dir).await;
    let rows = srv2
        .query_rows("SELECT label FROM vec_wal WHERE id = 'w1'")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "persisted");
}

#[tokio::test]
async fn search_using_vector_dsl_lowers_to_canonical_form() {
    // The `SEARCH <coll> USING VECTOR(<field>, ARRAY[...], <k>)` shorthand
    // must lower to `ORDER BY vector_distance(...) LIMIT k` so users can pick
    // either surface.
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vec_search_dsl WITH (engine='document_schemaless')")
        .await
        .unwrap();
    srv.exec("CREATE INDEX ON vec_search_dsl (embedding)")
        .await
        .unwrap();

    let vecs: &[(&str, [f32; 4])] = &[
        ("a", [1.0, 0.0, 0.0, 0.0]),
        ("b", [0.0, 1.0, 0.0, 0.0]),
        ("c", [0.0, 0.0, 1.0, 0.0]),
        ("d", [0.0, 0.0, 0.0, 1.0]),
        ("e", [0.7, 0.7, 0.0, 0.0]),
    ];
    for (id, emb) in vecs {
        let arr = format!("[{},{},{},{}]", emb[0], emb[1], emb[2], emb[3]);
        srv.exec(&format!(
            "INSERT INTO vec_search_dsl {{ id: '{id}', embedding: {arr} }}"
        ))
        .await
        .unwrap();
    }

    let canonical = srv
        .query_rows(
            "SELECT * FROM vec_search_dsl \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0]) LIMIT 3",
        )
        .await
        .unwrap();
    let rows = srv
        .query_rows("SEARCH vec_search_dsl USING VECTOR(embedding, ARRAY[1.0,0.0,0.0,0.0], 3)")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        canonical.len(),
        "SEARCH USING VECTOR DSL must match canonical form: canonical={}, dsl={}",
        canonical.len(),
        rows.len()
    );
}

/// The nearest id to `[1, 0, 0, 0]` in `vi_ivf_hint`, searched with `hint`.
async fn ivf_nearest_with_hint(srv: &TestServer, hint: &str) -> Result<String, String> {
    let rows = srv
        .query_rows(&format!(
            "SELECT id FROM vi_ivf_hint \
             ORDER BY vector_distance(embedding, ARRAY[1.0,0.0,0.0,0.0], \
                                      quantization => '{hint}') \
             LIMIT 1"
        ))
        .await?;
    Ok(rows.first().map(|r| r[0].clone()).unwrap_or_default())
}

#[tokio::test]
async fn ivf_pq_index_accepts_pq_hint_and_refuses_sq8_hint() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION vi_ivf_hint").await.unwrap();
    srv.exec(
        "CREATE VECTOR INDEX idx_vi_ivf_hint ON vi_ivf_hint (embedding) \
         METRIC l2 DIM 4 INDEX_TYPE ivf_pq PQ_M 2 IVF_CELLS 4 IVF_NPROBE 4",
    )
    .await
    .unwrap();
    let vecs: &[(&str, [f32; 4])] = &[
        ("a", [1.0, 0.0, 0.0, 0.0]),
        ("b", [0.0, 1.0, 0.0, 0.0]),
        ("c", [0.0, 0.0, 1.0, 0.0]),
        ("d", [0.0, 0.0, 0.0, 1.0]),
    ];
    for (id, emb) in vecs {
        srv.exec(&format!(
            "INSERT INTO vi_ivf_hint (id, embedding) VALUES ('{id}', ARRAY[{},{},{},{}])",
            emb[0], emb[1], emb[2], emb[3]
        ))
        .await
        .unwrap();
    }

    // An IVF-PQ index searches by PQ codes: the pq hint names its codec.
    let nearest = ivf_nearest_with_hint(&srv, "pq")
        .await
        .expect("a pq hint on an ivf_pq index is served");
    assert_eq!(nearest, "a");
    let err = ivf_nearest_with_hint(&srv, "sq8")
        .await
        .expect_err("an sq8 hint on an ivf_pq index is refused");
    assert!(err.contains("quantization"), "got: {err}");
}

#[tokio::test]
async fn graph_codecs_refuse_a_dimension_past_u16_at_create() {
    let srv = TestServer::start().await;
    // A graph codec stores the dimension as u16: the DDL refuses the shape
    // instead of leaving every segment without its codec.
    for codec in ["rabitq", "bbq", "binary", "ternary", "opq"] {
        let err = srv
            .exec(&format!(
                "CREATE COLLECTION vp_wide_{codec} (id STRING PRIMARY KEY, emb VECTOR(65536)) \
                 WITH (engine='vector', vector_field='emb', dim=65536, metric='l2', \
                       quantization='{codec}')"
            ))
            .await
            .expect_err("a graph codec past dim 65535 is refused");
        assert!(err.contains("SQLSTATE 22023"), "{codec}: {err}");
    }
}
