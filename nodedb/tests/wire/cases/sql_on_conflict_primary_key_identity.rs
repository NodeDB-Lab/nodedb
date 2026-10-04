// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... ON CONFLICT DO UPDATE` keeps the conflicting row's key on the
//! key-value, vector-primary and columnar engines.
//!
//! The conflict branch merges its assignments into the row stored under the
//! conflicting key, and that row keeps its key and surrogate. An assignment
//! that moves the key column is refused with `23000`. Assigning the key the
//! row holds, or `EXCLUDED.<key>`, is accepted.

use crate::harness::TestServer;

/// SQLSTATE of a statement that must fail, or `None` when it succeeded.
async fn sqlstate_of(server: &TestServer, sql: &str) -> Option<String> {
    match server.client.simple_query(sql).await {
        Ok(_) => None,
        Err(e) => Some(
            e.as_db_error()
                .unwrap_or_else(|| panic!("expected a DbError from {sql}, got: {e}"))
                .code()
                .code()
                .to_string(),
        ),
    }
}

async fn assert_key_change_refused(server: &TestServer, sql: &str) {
    let state = sqlstate_of(server, sql)
        .await
        .unwrap_or_else(|| panic!("a primary-key change must be refused, but ran: {sql}"));
    assert_eq!(
        state, "23000",
        "a primary-key change is an integrity violation, got SQLSTATE {state} for: {sql}"
    );
}

async fn query(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// KV shapes: the built-in `key` column, and a named key column the body
/// also carries.
const KV_SHAPES: [(&str, &str); 2] = [
    ("key", "(key TEXT PRIMARY KEY, v TEXT)"),
    ("k", "(k TEXT PRIMARY KEY, v TEXT)"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_on_conflict_update_refuses_a_new_key() {
    let server = TestServer::start().await;
    for (i, (key, shape)) in KV_SHAPES.iter().enumerate() {
        let name = format!("pk_conflict_kv_{i}");
        server
            .exec(&format!(
                "CREATE COLLECTION {name} {shape} WITH (engine='kv')"
            ))
            .await
            .expect("create kv collection");
        server
            .exec(&format!(
                "INSERT INTO {name} ({key}, v) VALUES ('k1', 'one'), ('k2', 'two')"
            ))
            .await
            .expect("seed rows");

        for assignment in [format!("{key} = 'moved'"), format!("{key} = EXCLUDED.v")] {
            assert_key_change_refused(
                &server,
                &format!(
                    "INSERT INTO {name} ({key}, v) VALUES ('k1', 'again') \
                     ON CONFLICT ({key}) DO UPDATE SET {assignment}"
                ),
            )
            .await;
        }
        assert_eq!(
            query(&server, &format!("SELECT v FROM {name} ORDER BY v")).await,
            vec!["one".to_string(), "two".to_string()],
            "a refused key change writes nothing"
        );

        server
            .exec(&format!(
                "INSERT INTO {name} ({key}, v) VALUES ('k1', 'again') \
                 ON CONFLICT ({key}) DO UPDATE SET {key} = EXCLUDED.{key}, v = EXCLUDED.v"
            ))
            .await
            .expect("assigning the conflicting key keeps the identity");
        server
            .exec(&format!(
                "INSERT INTO {name} ({key}, v) VALUES ('k2', 'ignored') \
                 ON CONFLICT ({key}) DO UPDATE SET {key} = 'k2', v = 'same'"
            ))
            .await
            .expect("assigning the current key keeps the identity");
        assert_eq!(
            query(&server, &format!("SELECT v FROM {name} WHERE {key} = 'k1'")).await,
            vec!["again".to_string()]
        );
        assert_eq!(
            query(&server, &format!("SELECT v FROM {name} WHERE {key} = 'k2'")).await,
            vec!["same".to_string()]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_primary_on_conflict_update_refuses_a_new_key() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION pk_conflict_vp (id STRING PRIMARY KEY, vec VECTOR(3), \
             owner STRING) WITH (engine='vector', primary='vector', vector_field='vec', \
             dim=3, payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary collection");
    server
        .exec(
            "INSERT INTO pk_conflict_vp (id, vec, owner) \
             VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("seed row");

    for assignment in ["id = 'moved'", "id = EXCLUDED.owner"] {
        assert_key_change_refused(
            &server,
            &format!(
                "INSERT INTO pk_conflict_vp (id, vec, owner) \
                 VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
                 ON CONFLICT (id) DO UPDATE SET {assignment}"
            ),
        )
        .await;
    }
    assert_eq!(
        query(&server, "SELECT owner FROM pk_conflict_vp WHERE id = 'r1'").await,
        vec!["alice".to_string()],
        "a refused key change writes nothing"
    );
    assert!(
        query(
            &server,
            "SELECT owner FROM pk_conflict_vp WHERE id = 'moved'"
        )
        .await
        .is_empty()
    );

    server
        .exec(
            "INSERT INTO pk_conflict_vp (id, vec, owner) \
             VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'bob') \
             ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, owner = EXCLUDED.owner",
        )
        .await
        .expect("assigning the conflicting key keeps the identity");
    server
        .exec(
            "INSERT INTO pk_conflict_vp (id, vec, owner) \
             VALUES ('r1', ARRAY[0.0, 1.0, 0.0], 'ignored') \
             ON CONFLICT (id) DO UPDATE SET id = 'r1'",
        )
        .await
        .expect("assigning the current key keeps the identity");
    assert_eq!(
        query(&server, "SELECT owner FROM pk_conflict_vp WHERE id = 'r1'").await,
        vec!["bob".to_string()]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_on_conflict_update_refuses_a_new_key() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pk_conflict_col (id TEXT PRIMARY KEY, v TEXT) WITH (engine='columnar')")
        .await
        .expect("create columnar collection");
    server
        .exec("INSERT INTO pk_conflict_col (id, v) VALUES ('k1', 'one'), ('k2', 'two')")
        .await
        .expect("seed rows");

    for assignment in ["id = 'moved'", "id = EXCLUDED.v"] {
        assert_key_change_refused(
            &server,
            &format!(
                "INSERT INTO pk_conflict_col (id, v) VALUES ('k1', 'again') \
                 ON CONFLICT (id) DO UPDATE SET {assignment}"
            ),
        )
        .await;
    }
    assert_eq!(
        query(&server, "SELECT id FROM pk_conflict_col ORDER BY id").await,
        vec!["k1".to_string(), "k2".to_string()],
        "a refused key change leaves one row per key"
    );

    server
        .exec(
            "INSERT INTO pk_conflict_col (id, v) VALUES ('k1', 'again') \
             ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, v = EXCLUDED.v",
        )
        .await
        .expect("assigning the conflicting key keeps the identity");
    assert_eq!(
        query(&server, "SELECT id FROM pk_conflict_col ORDER BY id").await,
        vec!["k1".to_string(), "k2".to_string()]
    );
    assert_eq!(
        query(&server, "SELECT v FROM pk_conflict_col WHERE id = 'k1'").await,
        vec!["again".to_string()]
    );
}
