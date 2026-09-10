// SPDX-License-Identifier: BUSL-1.1

//! A declared column type resolves from the type keyword alone.
//!
//! `CREATE COLLECTION` records each column as the raw DDL text after its
//! name, so a column written `n INT DEFAULT 5` is stored as the type string
//! `INT DEFAULT 5`. A resolver that matches that whole string recognizes no
//! keyword and falls through to text, which costs the column its numeric
//! `RowDescription` OID, its numeric comparison semantics, and its exact
//! integer round trip.
//!
//! Every test here pairs a column carrying a `DEFAULT` clause with a control
//! column of the same declared type carrying none, so the `DEFAULT` clause is
//! the only variable.
//!
//! Companion coverage: `ddl_int_width_aliases_strict_kv.rs` for declared
//! integer widths, `sql_default_expressions.rs` for DEFAULT evaluation itself.

use crate::harness::TestServer;

/// PostgreSQL type OID for `int4`.
const OID_INT4: u32 = 23;
/// PostgreSQL type OID for `int8`.
const OID_INT8: u32 = 20;

/// A value above 2^53, which no `f64` and no decimal-rendering text path
/// carries back unchanged.
const BEYOND_F64_MANTISSA: i64 = 9_007_199_254_740_993;

/// Assert the exact `RowDescription` OID of each named column.
///
/// A missing column fails loudly rather than being skipped: a silent skip
/// turns this into a test that passes when the columns vanish.
fn assert_column_oids(row: &tokio_postgres::Row, expected: &[(&str, u32)]) {
    for (col_name, expected_oid) in expected {
        let col = row
            .columns()
            .iter()
            .find(|c| c.name() == *col_name)
            .unwrap_or_else(|| {
                panic!(
                    "column '{col_name}' must appear in RowDescription; got {:?}",
                    row.columns().iter().map(|c| c.name()).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            col.type_().oid(),
            *expected_oid,
            "column '{col_name}' must advertise OID {expected_oid}, got {}",
            col.type_().oid()
        );
    }
}

/// Asserts a rendered row carries a real value in place of an absent or NULL column.
fn assert_not_null(row: &str, label: &str) {
    let trimmed = row.trim();
    assert!(
        !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("null"),
        "{label}: expected a value, got `{row}`"
    );
}

/// Create one collection per engine carrying a defaulted and a control column
/// of each numeric width, then insert the single probe row.
///
/// `n`/`big` declare a `DEFAULT`; `plain`/`plain_big` declare the same type
/// with none. `ts` covers a temporal column with a volatile `DEFAULT`.
async fn create_and_seed(server: &TestServer, name: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (\
                id TEXT PRIMARY KEY, \
                n INT DEFAULT 5, \
                plain INT, \
                big BIGINT DEFAULT {BEYOND_F64_MANTISSA}, \
                plain_big BIGINT, \
                ts TIMESTAMP DEFAULT NOW()) WITH (engine='{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name} on {engine}: {e}"));

    server
        .exec(&format!(
            "INSERT INTO {name} (id, plain, plain_big) \
             VALUES ('k1', 5, {BEYOND_F64_MANTISSA})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert into {name}: {e}"));
}

/// Assert every declared-type behavior on a seeded collection.
///
/// The defaulted column and its control column must agree on all three:
/// advertised OID, numeric comparison, and exact integer round trip.
async fn assert_declared_types_hold(server: &TestServer, name: &str) {
    let stmt = server
        .client
        .prepare_typed(
            &format!("SELECT n, plain, big, plain_big FROM {name} WHERE id = $1"),
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .unwrap_or_else(|e| panic!("prepare select on {name}: {e}"));
    let rows = server
        .client
        .query(&stmt, &[&"k1"])
        .await
        .unwrap_or_else(|e| panic!("execute select on {name}: {e}"));
    assert_eq!(rows.len(), 1, "one row expected from {name}");

    assert_column_oids(
        &rows[0],
        &[
            ("n", OID_INT4),
            ("plain", OID_INT4),
            ("big", OID_INT8),
            ("plain_big", OID_INT8),
        ],
    );

    // Typed getters matching the advertised widths: a wrong OID or a
    // wrong-width binary payload panics inside `get` before the comparison.
    assert_eq!(rows[0].get::<_, i32>("n"), 5);
    assert_eq!(rows[0].get::<_, i32>("plain"), 5);
    assert_eq!(rows[0].get::<_, i64>("big"), BEYOND_F64_MANTISSA);
    assert_eq!(rows[0].get::<_, i64>("plain_big"), BEYOND_F64_MANTISSA);

    for column in ["n", "plain"] {
        let matched = server
            .query_text(&format!("SELECT id FROM {name} WHERE {column} > 4"))
            .await
            .unwrap_or_else(|e| panic!("{name}.{column} > 4: {e}"));
        assert_eq!(
            matched,
            vec!["k1".to_string()],
            "{name}.{column} > 4 must match the row, got {matched:?}"
        );

        let unmatched = server
            .query_text(&format!("SELECT id FROM {name} WHERE {column} > 6"))
            .await
            .unwrap_or_else(|e| panic!("{name}.{column} > 6: {e}"));
        assert!(
            unmatched.is_empty(),
            "{name}.{column} > 6 must match nothing, got {unmatched:?}"
        );
    }

    for column in ["big", "plain_big"] {
        let stored = server
            .query_text(&format!("SELECT {column} FROM {name} WHERE id = 'k1'"))
            .await
            .unwrap_or_else(|e| panic!("{name}.{column} read: {e}"));
        assert_eq!(stored.len(), 1, "one row expected for {name}.{column}");
        assert_eq!(
            stored[0].trim(),
            BEYOND_F64_MANTISSA.to_string(),
            "{name}.{column} must round-trip exactly, got `{}`",
            stored[0]
        );
    }

    let stamps = server
        .query_text(&format!("SELECT ts FROM {name} WHERE id = 'k1'"))
        .await
        .unwrap_or_else(|e| panic!("{name}.ts read: {e}"));
    assert_eq!(stamps.len(), 1, "one row expected for {name}.ts");
    assert_not_null(&stamps[0], "ts");
}

/// A `document_strict` column keeps its declared type when a `DEFAULT`
/// clause follows it, matching a control column that declares none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_columns_keep_their_declared_type_across_a_default_clause() {
    let server = TestServer::start().await;
    create_and_seed(&server, "typed_defaults_strict", "document_strict").await;
    assert_declared_types_hold(&server, "typed_defaults_strict").await;
}

/// A `document_schemaless` column keeps its declared type when a `DEFAULT`
/// clause follows it, matching a control column that declares none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_columns_keep_their_declared_type_across_a_default_clause() {
    let server = TestServer::start().await;
    create_and_seed(&server, "typed_defaults_schemaless", "document_schemaless").await;
    assert_declared_types_hold(&server, "typed_defaults_schemaless").await;
}

/// A `columnar` column keeps its declared type when a `DEFAULT` clause
/// follows it, matching a control column that declares none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_columns_keep_their_declared_type_across_a_default_clause() {
    let server = TestServer::start().await;
    create_and_seed(&server, "typed_defaults_columnar", "columnar").await;
    assert_declared_types_hold(&server, "typed_defaults_columnar").await;
}

/// A `NOT NULL` modifier leaves the declared integer type intact, the same
/// way a `DEFAULT` clause must.
///
/// `NOT NULL` and `DEFAULT` are both trailing modifiers on the same stored
/// type string, so they share one resolution path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_not_null_modifier_leaves_the_declared_integer_type_intact() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION typed_not_null (\
                id TEXT PRIMARY KEY, \
                n INT NOT NULL, \
                plain INT) WITH (engine='document_schemaless')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO typed_not_null (id, n, plain) VALUES ('k1', 5, 5)")
        .await
        .unwrap();

    let stmt = server
        .client
        .prepare_typed(
            "SELECT n, plain FROM typed_not_null WHERE id = $1",
            &[tokio_postgres::types::Type::TEXT],
        )
        .await
        .expect("prepare typed_not_null select");
    let rows = server
        .client
        .query(&stmt, &[&"k1"])
        .await
        .expect("execute typed_not_null select");
    assert_eq!(rows.len(), 1, "one row expected from typed_not_null");

    assert_column_oids(&rows[0], &[("n", OID_INT4), ("plain", OID_INT4)]);
    assert_eq!(rows[0].get::<_, i32>("n"), 5);
    assert_eq!(rows[0].get::<_, i32>("plain"), 5);
}
