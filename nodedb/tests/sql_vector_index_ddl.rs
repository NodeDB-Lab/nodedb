//! Integration tests for `CREATE VECTOR INDEX` / `ALTER VECTOR INDEX` DDL
//! quantization parameters: INDEX_TYPE, PQ_M, IVF_CELLS, IVF_NPROBE.
//!
//! Asserts that the SQL DDL surface recognizes and validates the quantization
//! keywords advertised in `docs/vectors.md`. Silent fall-through to FP32 HNSW
//! (unknown parameters ignored instead of rejected, validation skipped) is the
//! regression mode these tests guard.

mod common;

use common::pgwire_harness::TestServer;

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

    // ALTER must accept the same quantization keyword set as CREATE — otherwise
    // users who defaulted to FP32 have no SQL migration path to the documented
    // tiers. Today ALTER errors with "unknown parameter 'index_type'".
    server
        .exec("ALTER VECTOR INDEX ON vi_alter SET (index_type = 'hnsw_pq', pq_m = 2)")
        .await
        .expect("ALTER VECTOR INDEX SET (index_type = ...) must be accepted");
}
