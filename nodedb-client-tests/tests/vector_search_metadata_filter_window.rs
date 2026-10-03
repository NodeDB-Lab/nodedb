// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::vector_search` applies a metadata filter
//! before the top-k cut, identically on both Origin clients.
//!
//! The nearest rows to the query all fail the filter, and they outnumber
//! the search's initial candidate window. A filter applied to a fixed
//! window would return fewer than `k` rows. The candidates must narrow to
//! the matching rows before the cut: `k` rows come back, every one
//! matching the filter, nearest first, the same rows on both clients.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{MetadataFilter, NativeClient, NodeDb, NodeDbRemote, SearchResult, Value};
use nodedb_test_support::pgwire_harness::TestServer;

/// Rows near the query that fail the filter. More than the initial window
/// of a `k = 2` filtered search.
const NEAR_ROWS: usize = 40;

/// Seed `meta_filter_vecs`: `NEAR_ROWS` rows close to the query `[0, 0]`
/// in category `skip`, then three rows far from it in category `keep`.
async fn seed(remote: &NodeDbRemote) {
    remote
        .execute_sql(
            "CREATE COLLECTION meta_filter_vecs \
             FIELDS (id TEXT, embedding VECTOR(2), category TEXT) \
             WITH (engine='vector', m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION meta_filter_vecs");
    let near = (0..NEAR_ROWS).map(|i| (format!("near{i}"), i as f64 * 0.01, "skip"));
    let far = [("far1", 10.0), ("far2", 20.0), ("far3", 30.0)]
        .into_iter()
        .map(|(id, x)| (id.to_string(), x, "keep"));
    for (id, x, category) in near.chain(far) {
        remote
            .execute_sql(
                &format!(
                    "INSERT INTO meta_filter_vecs (id, embedding, category) \
                     VALUES ('{id}', ARRAY[{x}, 0.0], '{category}')"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
}

fn ids(hits: &[SearchResult]) -> Vec<&str> {
    hits.iter().map(|h| h.id.as_str()).collect()
}

#[tokio::test]
async fn vector_search_metadata_filter_narrows_candidates_before_top_k() {
    let server = TestServer::start().await;
    let remote = NodeDbRemote::connect(&format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    ))
    .await
    .expect("pgwire connect to harness must succeed");
    seed(&remote).await;
    let native = NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ));

    // Unfiltered, the near rows win: the filter below is what moves the
    // result to the far rows.
    let open = remote
        .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, None, None)
        .await
        .expect("remote unfiltered vector_search");
    assert_eq!(ids(&open), vec!["near0", "near1"]);

    let keep = MetadataFilter::eq("category", Value::String("keep".into()));
    let remote_hits = remote
        .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&keep), None)
        .await
        .expect("remote vector_search with a metadata filter");
    let native_hits = native
        .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&keep), None)
        .await
        .expect("native vector_search with a metadata filter");
    assert_eq!(
        ids(&remote_hits),
        vec!["far1", "far2"],
        "remote: k matching rows, nearest first"
    );
    assert_eq!(
        ids(&native_hits),
        ids(&remote_hits),
        "native and remote return the same rows"
    );

    // A compound filter matches the same rows on both clients.
    let far_two = MetadataFilter::and(vec![
        keep.clone(),
        MetadataFilter::Not(Box::new(MetadataFilter::eq(
            "id",
            Value::String("far1".into()),
        ))),
    ]);
    let remote_hits = remote
        .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&far_two), None)
        .await
        .expect("remote vector_search with a compound filter");
    let native_hits = native
        .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&far_two), None)
        .await
        .expect("native vector_search with a compound filter");
    assert_eq!(ids(&remote_hits), vec!["far2", "far3"]);
    assert_eq!(ids(&native_hits), ids(&remote_hits));

    // A filter no row matches returns no row on either client.
    let none = MetadataFilter::eq("category", Value::String("absent".into()));
    for (client, hits) in [
        (
            "remote",
            remote
                .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&none), None)
                .await
                .expect("remote vector_search matching nothing"),
        ),
        (
            "native",
            native
                .vector_search("meta_filter_vecs", &[0.0, 0.0], 2, Some(&none), None)
                .await
                .expect("native vector_search matching nothing"),
        ),
    ] {
        assert!(hits.is_empty(), "{client}: got {hits:?}");
    }

    server.graceful_shutdown().await;
}
