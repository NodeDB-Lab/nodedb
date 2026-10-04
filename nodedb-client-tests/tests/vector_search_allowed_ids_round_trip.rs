// SPDX-License-Identifier: BUSL-1.1

//! End-to-end test that `NodeDb::vector_search` ranks only within
//! `allowed_ids` on both Origin clients.
//!
//! The nearest vectors to the query lie outside the allowed set. A
//! restriction applied after a top-k cut would return nothing (or fewer
//! than `k` rows). The restriction must apply before ranking: `k` rows come
//! back, every one from the allowed set, nearest first.

use std::collections::HashSet;

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, NodeDbRemote, SearchResult};
use nodedb_test_support::pgwire_harness::TestServer;

/// Seed `allowed_ids_vecs`: three near rows the query prefers and three far
/// rows, increasingly distant from the query `[0, 0]`.
async fn seed(remote: &NodeDbRemote) {
    remote
        .execute_sql(
            "CREATE COLLECTION allowed_ids_vecs \
             FIELDS (id TEXT, embedding VECTOR(2)) \
             WITH (engine='vector', m=8, ef_construction=50)",
            &[],
        )
        .await
        .expect("CREATE COLLECTION allowed_ids_vecs");
    for (id, x) in [
        ("near1", 0.0),
        ("near2", 0.1),
        ("near3", 0.2),
        ("far1", 10.0),
        ("far2", 20.0),
        ("far3", 30.0),
    ] {
        remote
            .execute_sql(
                &format!(
                    "INSERT INTO allowed_ids_vecs (id, embedding) VALUES ('{id}', ARRAY[{x}, 0.0])"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }
}

fn far_ids() -> HashSet<String> {
    ["far1", "far2", "far3"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn ids(hits: &[SearchResult]) -> Vec<&str> {
    hits.iter().map(|h| h.id.as_str()).collect()
}

/// `k = 2` over the far rows returns the two nearest far rows, nearest first.
fn assert_ranked_within_allowed(client: &str, hits: &[SearchResult]) {
    assert_eq!(
        ids(hits),
        vec!["far1", "far2"],
        "{client}: k rows from the allowed set, nearest first"
    );
}

#[tokio::test]
async fn vector_search_ranks_only_within_allowed_ids() {
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
    let allowed = far_ids();

    // Unrestricted, the near rows win: the restriction below is what moves
    // the result into the allowed set.
    let open = remote
        .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, None)
        .await
        .expect("remote unrestricted vector_search");
    assert_eq!(ids(&open), vec!["near1", "near2"]);
    let open = native
        .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, None)
        .await
        .expect("native unrestricted vector_search");
    assert_eq!(ids(&open), vec!["near1", "near2"], "native unrestricted");

    let hits = remote
        .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, Some(&allowed))
        .await
        .expect("remote vector_search with allowed_ids");
    assert_ranked_within_allowed("remote", &hits);

    let hits = native
        .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, Some(&allowed))
        .await
        .expect("native vector_search with allowed_ids");
    assert_ranked_within_allowed("native", &hits);

    // An empty allowed set admits no row on either client.
    let none: HashSet<String> = HashSet::new();
    for (client, hits) in [
        (
            "remote",
            remote
                .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, Some(&none))
                .await
                .expect("remote empty allowed set"),
        ),
        (
            "native",
            native
                .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, Some(&none))
                .await
                .expect("native empty allowed set"),
        ),
    ] {
        assert!(hits.is_empty(), "{client}: got {hits:?}");
    }

    // An allowed id that names no row restricts to the ids that do.
    let with_ghost: HashSet<String> = ["far3", "ghost"].iter().map(|s| s.to_string()).collect();
    let hits = native
        .vector_search("allowed_ids_vecs", &[0.0, 0.0], 2, None, Some(&with_ghost))
        .await
        .expect("native vector_search with an unbound id");
    assert_eq!(ids(&hits), vec!["far3"]);

    server.graceful_shutdown().await;
}
