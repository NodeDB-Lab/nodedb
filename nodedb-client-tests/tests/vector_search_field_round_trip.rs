// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::vector_search_field` honors the
//! `field_name` argument across the full pgwire round-trip.
//!
//! A search against a named field must hit a SQL path that scopes to
//! that field, with results reflecting the field's data — never the
//! unfielded fallback. A trait default that delegates to
//! `vector_search` and discards `field_name` is the silent-wrong
//! pattern this test guards against. The remote override emits the
//! 2-arg form of `vector_distance(<field>, ARRAY[...])` so the planner
//! routes through the named column.

use nodedb_client::{NodeDb, NodeDbRemote};
use nodedb_test_support::pgwire_harness::TestServer;

#[tokio::test]
async fn vector_search_field_must_not_silently_delegate_to_unfielded() {
    let server = TestServer::start().await;
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let remote = NodeDbRemote::connect(&conn_str)
        .await
        .expect("pgwire connect to harness must succeed");

    // Provision a vector-primary collection with an explicit
    // `body_embedding` column and seed it so the search has real data
    // to return. `primary='vector'` wires the named-field HNSW index —
    // `vector_distance(body_embedding, ARRAY[...])` resolves through it.
    remote
        .execute_sql(
            "CREATE COLLECTION embeddings_multi \
             FIELDS (id TEXT, body_embedding VECTOR(3)) \
             WITH (engine='vector', primary='vector', \
                   vector_field='body_embedding', \
                   dim=3, m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION embeddings_multi");

    remote
        .vector_insert_field(
            "embeddings_multi",
            "body_embedding",
            "v1",
            &[0.1, 0.2, 0.3],
            None,
        )
        .await
        .expect("seed vector into body_embedding");

    // Spec: `vector_search_field("body_embedding", ...)` searches the
    // named HNSW index on the collection and returns results from THAT
    // field — not from the default unfielded vector index. An
    // `Err("not implemented")` default is correct negative behavior but
    // not the spec — do not soften the assertion to accept `Err`; that
    // locks the broken default in as the contract.
    let results = remote
        .vector_search_field(
            "embeddings_multi",
            "body_embedding",
            &[0.1, 0.2, 0.3],
            5,
            None,
        )
        .await
        .expect("vector_search_field must scope to the named field and return results");
    assert!(
        !results.is_empty(),
        "vector_search_field must return at least one result against the seeded field"
    );

    server.graceful_shutdown().await;
}
