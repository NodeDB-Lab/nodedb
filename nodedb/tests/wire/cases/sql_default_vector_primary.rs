// SPDX-License-Identifier: BUSL-1.1

//! Column DEFAULT evaluation on a `primary='vector'` collection.
//!
//! A vector-primary INSERT bypasses document encoding: the planner splits each
//! row into the vector field and a payload map before the shared DEFAULT pass
//! runs. A column the statement omits therefore reaches storage only if this
//! path materializes its declared DEFAULT itself.
//!
//! Companion coverage: `sql_default_expressions.rs` for DEFAULT evaluation on
//! the document, key-value, and columnar engines.

use crate::harness::TestServer;

/// Create the vector-primary collection every test in this file writes to.
async fn create_vector_primary(server: &TestServer, name: &str, key_type: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (\
                id {key_type} PRIMARY KEY, \
                vec VECTOR(3), \
                owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// Read the `id` column of every row, failing loudly on an absent or NULL value.
async fn sorted_ids(server: &TestServer, name: &str) -> Vec<String> {
    let rows = server
        .query_named_rows(&format!("SELECT * FROM {name}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT * FROM {name}: {e}"));
    let mut ids: Vec<String> = rows
        .iter()
        .map(|row| {
            let id = row
                .get("id")
                .unwrap_or_else(|| panic!("row must carry an `id` column, got {row:?}"));
            let trimmed = id.trim();
            assert!(
                !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("null"),
                "id: expected a value, got `{id}`"
            );
            trimmed.to_string()
        })
        .collect();
    ids.sort();
    ids
}

/// A `DEFAULT nextval('seq')` key column allocates 1 then 2 across two
/// vector-primary inserts that omit it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nextval_default_fills_an_omitted_vector_primary_key() {
    let server = TestServer::start().await;

    server
        .exec("CREATE SEQUENCE seq_vector_default;")
        .await
        .unwrap();
    create_vector_primary(
        &server,
        "def_vec_seq",
        "BIGINT DEFAULT nextval('seq_vector_default')",
    )
    .await;

    server
        .exec(
            "INSERT INTO def_vec_seq (vec, owner) \
             VALUES (ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert omitting a defaulted key must succeed");
    server
        .exec(
            "INSERT INTO def_vec_seq (vec, owner) \
             VALUES (ARRAY[0.0, 1.0, 0.0], 'bob')",
        )
        .await
        .expect("second vector-primary insert must succeed");

    let ids = sorted_ids(&server, "def_vec_seq").await;
    assert_eq!(
        ids,
        vec!["1".to_string(), "2".to_string()],
        "nextval must allocate 1 then 2, got {ids:?}"
    );
}

/// A `DEFAULT UUID_V7()` key column fills a distinct value per vector-primary
/// insert that omits it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uuid_default_fills_an_omitted_vector_primary_key() {
    let server = TestServer::start().await;

    create_vector_primary(&server, "def_vec_uuid", "STRING DEFAULT UUID_V7()").await;

    server
        .exec(
            "INSERT INTO def_vec_uuid (vec, owner) \
             VALUES (ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await
        .expect("vector-primary insert omitting a defaulted key must succeed");
    server
        .exec(
            "INSERT INTO def_vec_uuid (vec, owner) \
             VALUES (ARRAY[0.0, 1.0, 0.0], 'bob')",
        )
        .await
        .expect("second vector-primary insert must succeed");

    let ids = sorted_ids(&server, "def_vec_uuid").await;
    assert_eq!(ids.len(), 2, "two rows expected: {ids:?}");
    let distinct: std::collections::HashSet<&str> = ids.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        distinct.len(),
        2,
        "each row must carry a distinct id, got {ids:?}"
    );
}

/// A non-key column DEFAULT fills on a vector-primary insert that omits it.
///
/// The payload map carries every column but the vector field, so a defaulted
/// payload column shares the key column's materialization path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_default_fills_an_omitted_vector_primary_payload_column() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION def_vec_payload (\
                id STRING PRIMARY KEY, \
                vec VECTOR(3), \
                owner STRING DEFAULT 'unassigned') \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .unwrap();

    server
        .exec("INSERT INTO def_vec_payload (id, vec) VALUES ('r1', ARRAY[1.0, 0.0, 0.0])")
        .await
        .expect("vector-primary insert omitting a defaulted payload column must succeed");

    let rows = server
        .query_named_rows("SELECT * FROM def_vec_payload")
        .await
        .expect("SELECT * FROM def_vec_payload");
    assert_eq!(rows.len(), 1, "one row expected: {rows:?}");
    let owner = rows[0]
        .get("owner")
        .unwrap_or_else(|| panic!("row must carry an `owner` column, got {:?}", rows[0]));
    assert_eq!(
        owner.trim(),
        "unassigned",
        "the declared DEFAULT must fill the omitted column, got `{owner}`"
    );
}
