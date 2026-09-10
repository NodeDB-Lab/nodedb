// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for UPDATE RETURNING and DELETE RETURNING via the pgwire
//! simple-query and extended-query protocols.
//!
//! Each test spins up a fresh single-core server, performs DML with a
//! RETURNING clause, and asserts that the correct columns and values come
//! back as a multi-column row result.

use crate::harness::TestServer;
use tokio_postgres::types::Type;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Set up a schemaless collection with a single document {id, name, score}.
async fn seed_docs(server: &TestServer) {
    server
        .exec("CREATE COLLECTION items TYPE DOCUMENT (id STRING, name STRING, score INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('a', 'alpha', 10)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('b', 'beta', 20)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO items (id, name, score) VALUES ('c', 'gamma', 30)")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// UPDATE RETURNING *
// ---------------------------------------------------------------------------

/// Point UPDATE with `RETURNING *` must return all fields of the post-update
/// document as a multi-column row.
#[tokio::test]
async fn point_update_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 99 WHERE id = 'a' RETURNING *")
        .await
        .expect("UPDATE RETURNING * should succeed");

    assert_eq!(rows.len(), 1, "expected exactly one returned row");

    // The row must contain the updated score.
    let row = &rows[0];
    let joined = row.join(",");
    assert!(
        joined.contains("99"),
        "returned row must reflect updated score=99, got: {joined}"
    );
    assert!(
        joined.contains("alpha"),
        "returned row must retain name=alpha, got: {joined}"
    );
}

// ---------------------------------------------------------------------------
// UPDATE RETURNING named columns
// ---------------------------------------------------------------------------

/// Point UPDATE with named column list must return only the requested columns,
/// in spec order, using the alias where provided.
#[tokio::test]
async fn point_update_returning_named_columns() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 55 WHERE id = 'b' RETURNING id, score AS new_score")
        .await
        .expect("UPDATE RETURNING named should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    // Two columns: id, new_score.
    let row = &rows[0];
    assert_eq!(row.len(), 2, "expected 2 columns, got {}", row.len());
    assert_eq!(row[0], "b", "first column (id) must be 'b'");
    assert_eq!(row[1], "55", "second column (new_score) must be '55'");
}

// ---------------------------------------------------------------------------
// DELETE RETURNING *
// ---------------------------------------------------------------------------

/// Point DELETE with `RETURNING *` must return the pre-deletion document as a
/// multi-column row.
#[tokio::test]
async fn point_delete_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("DELETE FROM items WHERE id = 'c' RETURNING *")
        .await
        .expect("DELETE RETURNING * should succeed");

    assert_eq!(rows.len(), 1, "expected one returned row for deleted doc");
    let row = &rows[0];
    let joined = row.join(",");
    assert!(
        joined.contains("gamma"),
        "returned row must contain pre-deletion name=gamma, got: {joined}"
    );
    assert!(
        joined.contains("30"),
        "returned row must contain pre-deletion score=30, got: {joined}"
    );
}

// ---------------------------------------------------------------------------
// DELETE RETURNING named columns
// ---------------------------------------------------------------------------

/// Point DELETE with named RETURNING columns must return only those columns.
#[tokio::test]
async fn point_delete_returning_named_columns() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("DELETE FROM items WHERE id = 'a' RETURNING id, name")
        .await
        .expect("DELETE RETURNING named should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    let row = &rows[0];
    assert_eq!(row.len(), 2, "expected 2 columns, got {}", row.len());
    assert_eq!(row[0], "a");
    assert_eq!(row[1], "alpha");
}

// ---------------------------------------------------------------------------
// Bulk UPDATE RETURNING
// ---------------------------------------------------------------------------

/// Bulk UPDATE (no WHERE clause) with RETURNING * must return one row per
/// matched document, each reflecting the post-update value.
#[tokio::test]
async fn bulk_update_returning() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows("UPDATE items SET score = 0 RETURNING id, score")
        .await
        .expect("bulk UPDATE RETURNING should succeed");

    // All three documents must be returned.
    assert_eq!(
        rows.len(),
        3,
        "expected 3 returned rows, got {}",
        rows.len()
    );

    // Every returned row must show score = 0.
    for row in &rows {
        assert_eq!(
            row.len(),
            2,
            "each row must have 2 columns, got {}",
            row.len()
        );
        assert_eq!(
            row[1], "0",
            "updated score must be 0 in every row, got {}",
            row[1]
        );
    }
}

// ---------------------------------------------------------------------------
// Bulk DELETE RETURNING
// ---------------------------------------------------------------------------

/// Bulk DELETE with a WHERE clause that matches two rows must return two
/// pre-deletion documents.
#[tokio::test]
async fn bulk_delete_returning() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    // Delete the two rows with score >= 20.
    let rows = server
        .query_rows("DELETE FROM items WHERE score >= 20 RETURNING id, score")
        .await
        .expect("bulk DELETE RETURNING should succeed");

    assert_eq!(rows.len(), 2, "expected 2 deleted rows, got {}", rows.len());

    // Collect returned ids.
    let mut ids: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["b", "c"], "returned ids must be b and c");
}

// ---------------------------------------------------------------------------
// MERGE RETURNING
// ---------------------------------------------------------------------------

/// Target holds 'a' and 'b'; source holds 'a' (matched) and 'c' (unmatched),
/// so one statement can exercise the matched and not-matched arms separately.
async fn seed_merge(server: &TestServer) {
    for name in ["merge_tgt", "merge_src"] {
        server
            .exec(&format!(
                "CREATE COLLECTION {name} (\
                     id TEXT PRIMARY KEY, name TEXT, score INT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }
    for (id, name, score) in [("a", "alpha", 10i64), ("b", "beta", 20)] {
        server
            .exec(&format!(
                "INSERT INTO merge_tgt (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap();
    }
    for (id, name, score) in [("a", "ALPHA_UPD", 99i64), ("c", "gamma", 30)] {
        server
            .exec(&format!(
                "INSERT INTO merge_src (id, name, score) VALUES ('{id}', '{name}', {score})"
            ))
            .await
            .unwrap();
    }
}

/// The NOT-MATCHED INSERT arm must return the post-image of each inserted row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_insert_arm_returning() {
    let server = TestServer::start().await;
    seed_merge(&server).await;

    let rows = server
        .query_rows(
            "MERGE INTO merge_tgt t USING merge_src s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score) \
             RETURNING id, score",
        )
        .await
        .expect("MERGE INSERT arm RETURNING should succeed");

    assert_eq!(rows.len(), 1, "only 'c' is unmatched: {rows:?}");
    assert_eq!(rows[0][0], "c");
    assert_eq!(
        rows[0][1], "30",
        "insert arm must return the new row's value"
    );
}

/// The MATCHED UPDATE arm must return the POST-image, not the pre-update row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_arm_returning() {
    let server = TestServer::start().await;
    seed_merge(&server).await;

    let rows = server
        .query_rows(
            "MERGE INTO merge_tgt t USING merge_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score \
             RETURNING id, name, score",
        )
        .await
        .expect("MERGE UPDATE arm RETURNING should succeed");

    assert_eq!(rows.len(), 1, "only 'a' is matched: {rows:?}");
    assert_eq!(rows[0][0], "a");
    assert_eq!(rows[0][1], "ALPHA_UPD");
    assert_eq!(rows[0][2], "99", "update arm must return the post-image");
}

/// The DELETE arm has no post-image, so it must return the PRE-image of the
/// removed row — the row as it stood when the merge classified it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_delete_arm_returns_pre_image() {
    let server = TestServer::start().await;
    seed_merge(&server).await;

    let rows = server
        .query_rows(
            "MERGE INTO merge_tgt t USING merge_src s ON t.id = s.id \
             WHEN MATCHED THEN DELETE RETURNING id, name, score",
        )
        .await
        .expect("MERGE DELETE arm RETURNING should succeed");

    assert_eq!(rows.len(), 1, "only 'a' is matched: {rows:?}");
    assert_eq!(rows[0][0], "a");
    assert_eq!(rows[0][1], "alpha", "pre-image name");
    assert_eq!(rows[0][2], "10", "pre-image score");

    // The row really is gone — the pre-image is not a sign the delete no-oped.
    let remaining = server
        .query_rows("SELECT id FROM merge_tgt ORDER BY id")
        .await
        .unwrap();
    let ids: Vec<&str> = remaining.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(ids, ["b"], "'a' must have been deleted: {remaining:?}");
}

/// Both arms in one statement: `RETURNING *` must surface every row the merge
/// wrote, updated and inserted alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_both_arms_returning_star() {
    let server = TestServer::start().await;
    seed_merge(&server).await;

    let rows = server
        .query_rows(
            "MERGE INTO merge_tgt t USING merge_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score \
             WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score) \
             RETURNING *",
        )
        .await
        .expect("MERGE RETURNING * should succeed");

    assert_eq!(rows.len(), 2, "one updated + one inserted row: {rows:?}");
    let joined: Vec<String> = rows.iter().map(|r| r.join(",")).collect();
    assert!(
        joined.iter().any(|r| r.contains("ALPHA_UPD")),
        "updated row missing: {joined:?}"
    );
    assert!(
        joined.iter().any(|r| r.contains("gamma")),
        "inserted row missing: {joined:?}"
    );
}

/// Without a RETURNING clause the statement still reports its affected count —
/// adding RETURNING support must not change the plain MERGE's response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_without_returning_reports_affected_count() {
    let server = TestServer::start().await;
    seed_merge(&server).await;

    let messages = server
        .client
        .simple_query(
            "MERGE INTO merge_tgt t USING merge_src s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET score = s.score \
             WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)",
        )
        .await
        .expect("plain MERGE should succeed");

    let mut rows = 0usize;
    let mut affected = None;
    for message in &messages {
        match message {
            tokio_postgres::SimpleQueryMessage::Row(_) => rows += 1,
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => affected = Some(*n),
            _ => {}
        }
    }
    assert_eq!(rows, 0, "a plain MERGE returns no rows");
    assert_eq!(
        affected,
        Some(2),
        "one updated + one inserted row must be counted"
    );
}

// ---------------------------------------------------------------------------
// Extended-query (prepared statement) RETURNING *
// ---------------------------------------------------------------------------

/// RETURNING * via the extended-query protocol (parameterised prepared
/// statement) must surface multi-column rows, not a JSON envelope.
#[tokio::test]
async fn extended_query_update_returning_star() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    // Use the extended-query (prepared-statement) path via tokio-postgres
    // `.query()` which drives Parse / Bind / Execute.
    let stmt = server
        .client
        .prepare_typed(
            "UPDATE items SET score = $1 WHERE id = $2 RETURNING *",
            &[Type::UNKNOWN, Type::UNKNOWN],
        )
        .await
        .expect("prepare should succeed");
    let rows = server
        .client
        .query(&stmt, &[&"77", &"b"])
        .await
        .expect("prepared UPDATE RETURNING should succeed");

    assert_eq!(rows.len(), 1, "expected one row");
    // Must have at least 2 columns (id + score at minimum).
    assert!(
        rows[0].len() >= 2,
        "row must expose multiple columns, got {}",
        rows[0].len()
    );
}

/// `RETURNING *` over the extended protocol is the one shape where the two
/// column lists are decided in different places: Describe answers with a
/// RowDescription BEFORE any row exists (from the target's catalog schema),
/// while the rows carry the columns of the row actually stored. pgwire sends
/// no second RowDescription with the DataRows, so any disagreement is
/// unreadable to the client — the statement fails with "DataRow field count
/// does not match the number of columns".
///
/// This pins BOTH sides at once: the announced list is the stored row's own
/// columns, and every DataRow carries exactly one field per announced column,
/// each holding that column's value. A padded or truncated row would keep the
/// counts equal while sliding values under the wrong names, so the values are
/// checked by NAME, not just counted.
#[tokio::test]
async fn extended_query_returning_star_matches_the_announced_row_description() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    // The stored row's own columns, as the row-derived shaping reports them.
    let stored = server
        .query_named_rows("SELECT * FROM items WHERE id = 'b'")
        .await
        .expect("read back the stored row");
    assert_eq!(stored.len(), 1, "one seeded row with id='b'");
    let stored = &stored[0];

    let stmt = server
        .client
        .prepare_typed(
            "UPDATE items SET score = $1 WHERE id = $2 RETURNING *",
            &[Type::UNKNOWN, Type::UNKNOWN],
        )
        .await
        .expect("prepare should succeed");

    // --- Announced side: the RowDescription Describe returned at Parse time.
    let announced: Vec<String> = stmt
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let mut announced_sorted = announced.clone();
    announced_sorted.sort();
    let mut stored_names: Vec<String> = stored.keys().cloned().collect();
    stored_names.sort();
    assert_eq!(
        announced_sorted, stored_names,
        "RETURNING * must announce the stored row's own columns, got {announced:?}"
    );

    let rows = server
        .client
        .query(&stmt, &[&"77", &"b"])
        .await
        .expect("prepared UPDATE RETURNING should succeed");
    assert_eq!(rows.len(), 1, "expected one row");
    let row = &rows[0];

    // --- DataRow side: one field per announced column, no padding, no
    // truncation.
    assert_eq!(
        row.len(),
        announced.len(),
        "DataRow field count must equal the announced column count"
    );

    // --- And each field carries THAT column's value, read in the type the
    // RowDescription announced for it.
    for (i, column) in row.columns().iter().enumerate() {
        let ty = column.type_();
        // One arm per numeric OID the RowDescription can carry. `score` is
        // declared `INT`, so it is announced as int4 and must be read as
        // `i32`: reading it as `String` is a client-side type error, not a
        // fallback. The narrower widths are listed for the same reason.
        let value = if *ty == Type::INT8 {
            row.get::<_, i64>(i).to_string()
        } else if *ty == Type::INT4 {
            row.get::<_, i32>(i).to_string()
        } else if *ty == Type::INT2 {
            row.get::<_, i16>(i).to_string()
        } else if *ty == Type::FLOAT8 {
            row.get::<_, f64>(i).to_string()
        } else if *ty == Type::FLOAT4 {
            row.get::<_, f32>(i).to_string()
        } else {
            row.get::<_, String>(i)
        };
        let expected = match column.name() {
            "id" => "b",
            "name" => "beta",
            // The value this statement just wrote.
            "score" => "77",
            other => panic!("unexpected RETURNING * column {other}"),
        };
        assert_eq!(
            value,
            expected,
            "column {} must carry its own value",
            column.name()
        );
    }
}

// ---------------------------------------------------------------------------
// UTF-8 statement boundaries
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unicode_identifier_before_returning_preserves_connection() {
    let server = TestServer::start().await;

    server
        .expect_error("DELETE FROM missingﬀﬀ RETURNING *", "does not exist")
        .await;
    let rows = server
        .query_text("SELECT 1")
        .await
        .expect("connection must remain usable after the statement error");
    assert_eq!(rows, vec!["1"]);
}

// ---------------------------------------------------------------------------
// Arithmetic expression in RETURNING — error path
// ---------------------------------------------------------------------------

/// A RETURNING clause containing an arithmetic expression must be rejected
/// with a typed error. NodeDB only supports column references and aliases
/// in RETURNING, not computed expressions.
#[tokio::test]
async fn returning_arithmetic_expression_rejected() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    server
        .expect_error(
            "UPDATE items SET score = 1 WHERE id = 'a' RETURNING score + 1",
            "not supported",
        )
        .await;
}

// ---------------------------------------------------------------------------
// INSERT RETURNING
// ---------------------------------------------------------------------------

/// `INSERT ... RETURNING` on a document collection returns the STORED row.
///
/// The clause must not be parsed and discarded — that would apply the write,
/// give the client a command tag, and answer a statement that asked for rows
/// with none and no error. Engines that still cannot carry the clause refuse it by
/// name rather than dropping it — pinned in `dml_returning_insert.rs`.
#[tokio::test]
async fn insert_returning_returns_the_inserted_row() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    let rows = server
        .query_rows(
            "INSERT INTO items (id, name, score) VALUES ('d', 'delta', 40) RETURNING id, score",
        )
        .await
        .expect("INSERT RETURNING must return the inserted row");
    assert_eq!(rows, vec![vec!["d".to_string(), "40".to_string()]]);

    let rows = server
        .query_rows("INSERT INTO items (id, name, score) VALUES ('e', 'epsilon', 50) RETURNING id")
        .await
        .expect("INSERT RETURNING must return the inserted row");
    assert_eq!(rows, vec![vec!["e".to_string()]]);

    // Both statements wrote, on top of the three seeded rows.
    let rows = server
        .query_rows("SELECT id FROM items ORDER BY id")
        .await
        .expect("read back items");
    assert_eq!(
        rows,
        vec![
            vec!["a".to_string()],
            vec!["b".to_string()],
            vec!["c".to_string()],
            vec!["d".to_string()],
            vec!["e".to_string()],
        ],
        "both returning inserts must have written their row: {rows:?}"
    );
}

/// An insert whose data merely contains the word carries no clause: the
/// keyword scan must not truncate the statement at a string literal.
#[tokio::test]
async fn a_plain_insert_still_applies() {
    let server = TestServer::start().await;
    seed_docs(&server).await;

    server
        .exec("INSERT INTO items (id, name, score) VALUES ('d', 'RETURNING soon', 40)")
        .await
        .expect("an insert with no RETURNING clause must apply");

    let rows = server
        .query_rows("SELECT id FROM items ORDER BY id")
        .await
        .expect("read back items");
    assert_eq!(
        rows.len(),
        4,
        "the plain insert must have applied: {rows:?}"
    );
}
