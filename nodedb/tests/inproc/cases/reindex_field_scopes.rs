// SPDX-License-Identifier: BUSL-1.1

//! A full-text REINDEX rebuilds every index of the collection: the
//! whole-document index and one index per text field. A field-scoped search
//! answers the same after the rebuild as before it.

use nodedb_test_support::pgwire_harness::TestServer;

/// Ids a full-text match on `field` returns, sorted.
async fn text_ids_on(
    server: &TestServer,
    collection: &str,
    field: &str,
    term: &str,
) -> Vec<String> {
    let mut ids = server
        .query_text(&format!(
            "SELECT id FROM {collection} WHERE text_match({field}, '{term}')"
        ))
        .await
        .unwrap();
    ids.sort();
    ids
}

/// The field scopes of `rx_fields`: `rust` in `d1`'s title and `d2`'s body.
async fn assert_scopes(server: &TestServer) {
    assert_eq!(
        text_ids_on(server, "rx_fields", "title", "rust").await,
        ["d1"]
    );
    assert_eq!(
        text_ids_on(server, "rx_fields", "body", "rust").await,
        ["d2"]
    );
    assert_eq!(
        text_ids_on(server, "rx_fields", "*", "rust").await,
        ["d1", "d2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_rebuild_preserves_field_scopes() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rx_fields WITH (engine='document_schemaless')")
        .await
        .unwrap();
    for (id, title, body) in [
        ("d1", "rust handbook", "an introduction"),
        ("d2", "cooking", "the rust belt"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO rx_fields {{ id: '{id}', title: '{title}', body: '{body}' }}"
            ))
            .await
            .unwrap();
    }
    assert_scopes(&server).await;

    // The plain form answers after the rebuild has cut over.
    server.exec("REINDEX INDEX fts rx_fields").await.unwrap();

    assert_scopes(&server).await;
}
