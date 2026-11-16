// SPDX-License-Identifier: BUSL-1.1

//! RESTORE of a vector-primary collection keeps every sidecar column's value.
//!
//! A vector-primary row keeps its columns in a tagged sidecar body. The
//! restored row reads the same columns and values as the source row.

use crate::harness::TestServer;

const TENANT: u64 = 1;

const ROWS: &str = "SELECT id, tag, emb FROM vp_restore ORDER BY id";
const FILTERED: &str = "SELECT id FROM vp_restore WHERE tag = 'first' \
                        ORDER BY vector_distance(emb, ARRAY[0.0,1.0,0.0]) LIMIT 10";

#[tokio::test]
async fn a_restored_vector_primary_row_reads_its_source_columns() {
    let source = TestServer::start().await;
    source
        .exec(
            "CREATE COLLECTION vp_restore (id TEXT PRIMARY KEY, tag TEXT, emb VECTOR(3)) \
             WITH (engine='vector', primary='vector', vector_field='emb', dim=3, \
             payload_indexes=['tag'])",
        )
        .await
        .expect("CREATE COLLECTION vp_restore");
    for (id, tag, emb) in [
        ("v1", "first", "[1.0,0.0,0.0]"),
        ("v2", "second", "[0.0,1.0,0.0]"),
    ] {
        source
            .exec(&format!(
                "INSERT INTO vp_restore (id, tag, emb) VALUES ('{id}', '{tag}', ARRAY{emb})"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT {id}: {e}"));
    }
    let expected = source.query_rows(ROWS).await.expect("source rows");
    assert_eq!(expected.len(), 2, "{expected:?}");
    assert_eq!(expected[0][0], "v1", "{expected:?}");
    assert_eq!(expected[0][1], "first", "{expected:?}");
    let expected_filtered = source.query_rows(FILTERED).await.expect("source search");
    assert_eq!(expected_filtered, vec![vec!["v1".to_string()]]);

    let backup = super::backup_support::drain_backup(&source.client, TENANT)
        .await
        .expect("BACKUP TENANT");
    drop(source);

    let restored = TestServer::start().await;
    super::backup_support::push_restore(&restored.client, TENANT, backup)
        .await
        .expect("RESTORE TENANT");
    assert_eq!(
        restored.query_rows(ROWS).await.expect("restored rows"),
        expected,
        "the restored rows read the source columns"
    );
    assert_eq!(
        restored
            .query_rows(FILTERED)
            .await
            .expect("restored search"),
        expected_filtered,
        "a filtered search on the restored collection finds the source rows"
    );
}
