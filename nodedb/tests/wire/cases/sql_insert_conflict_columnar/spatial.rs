// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_insert_duplicate_pk_keeps_latest() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION places (\
                id TEXT PRIMARY KEY, geom GEOMETRY SPATIAL_INDEX, label TEXT\
            ) WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX places_pk ON places (id)")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (id, geom, label) VALUES ('p1', ST_Point(0.0, 0.0), 'origin')")
        .await
        .unwrap();

    server
        .exec("INSERT INTO places (id, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved')")
        .await
        .unwrap();

    let rows = server
        .query_rows("SELECT id, label FROM places WHERE id = 'p1'")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "spatial duplicate PK must not produce two rows, got: {rows:?}"
    );
    // row[0]=id, row[1]=label
    assert_eq!(
        rows[0][1], "moved",
        "expected latest (moved), got: {:?}",
        rows[0]
    );
    assert_ne!(
        rows[0][1], "origin",
        "prior row must be tombstoned, got: {:?}",
        rows[0]
    );
}
