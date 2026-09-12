// SPDX-License-Identifier: BUSL-1.1

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_insert_duplicate_pk_refuses_with_23505() {
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

    // A declared PRIMARY KEY means uniqueness on every column, `id` included.
    match server
        .client
        .simple_query(
            "INSERT INTO places (id, geom, label) VALUES ('p1', ST_Point(1.0, 1.0), 'moved')",
        )
        .await
    {
        Ok(_) => panic!("expected unique_violation on the declared primary key, got success"),
        Err(e) => {
            let db_err = e.as_db_error().expect("expected DbError");
            assert_eq!(
                db_err.code().code(),
                "23505",
                "expected SQLSTATE 23505, got {}: {}",
                db_err.code().code(),
                db_err.message()
            );
        }
    }

    let rows = server
        .query_rows("SELECT id, label FROM places WHERE id = 'p1'")
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the refused insert must leave exactly one row, got: {rows:?}"
    );
    assert_eq!(
        rows[0][1], "origin",
        "the original row must be unchanged, got: {:?}",
        rows[0]
    );
}
