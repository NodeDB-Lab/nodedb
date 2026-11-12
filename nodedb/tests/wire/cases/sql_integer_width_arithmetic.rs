// SPDX-License-Identifier: BUSL-1.1

//! Integer arithmetic takes PostgreSQL's result width over pgwire.
//!
//! `integer op integer` is `integer` and `smallint op smallint` is
//! `smallint`. Mixed widths take the wider one. An integer literal is
//! `integer` when it fits. A result past its width raises SQLSTATE 22003
//! with PostgreSQL's message for that width. `SUM` over `integer` is
//! `bigint`.

use crate::harness::TestServer;

/// The type names the extended protocol describes for `sql`'s columns.
async fn column_types(server: &TestServer, sql: &str) -> Vec<String> {
    let statement = server
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    statement
        .columns()
        .iter()
        .map(|column| column.type_().name().to_owned())
        .collect()
}

/// A strict collection `name` with `smallint`, `integer` and `bigint`
/// columns, holding one row `(s, i, b) = (2, 3, 4)`.
async fn widths(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, s SMALLINT, i INT, b BIGINT) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();
    server
        .exec(&format!(
            "INSERT INTO {name} (id, s, i, b) VALUES ('r', 2, 3, 4)"
        ))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn arithmetic_announces_the_postgres_width() {
    let server = TestServer::start().await;
    widths(&server, "iw_types").await;

    for (expr, want) in [
        ("i * 1", "int4"),
        ("i + i", "int4"),
        ("s + i", "int4"),
        ("s + s", "int2"),
        ("s * 2", "int4"),
        ("i + b", "int8"),
        ("b * 1", "int8"),
        ("-i", "int4"),
        ("i / 2", "int4"),
        ("i % 2", "int4"),
        ("i + 3000000000", "int8"),
        ("SUM(i)", "int8"),
        ("SUM(s)", "int8"),
    ] {
        let sql = format!("SELECT {expr} FROM iw_types");
        assert_eq!(column_types(&server, &sql).await, [want], "{sql}");
    }
    assert_eq!(
        server
            .query_rows("SELECT i * 1, s + i, s + s FROM iw_types")
            .await
            .unwrap(),
        [["3", "5", "4"]]
    );
    assert_eq!(
        server
            .query_rows("SELECT SUM(i) FROM iw_types")
            .await
            .unwrap(),
        [["3"]]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literals_are_integer_when_they_fit() {
    let server = TestServer::start().await;
    assert_eq!(column_types(&server, "SELECT 1").await, ["int4"]);
    assert_eq!(column_types(&server, "SELECT 1 + 1").await, ["int4"]);
    assert_eq!(column_types(&server, "SELECT 3000000000").await, ["int8"]);
    assert_eq!(column_types(&server, "SELECT 1::INT8 * 2").await, ["int8"]);
    assert_eq!(
        column_types(&server, "SELECT 1::INT2 + 1::INT2").await,
        ["int2"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constant_overflow_raises_at_the_result_width() {
    let server = TestServer::start().await;
    server
        .expect_error(
            "SELECT 2147483647::int4 + 1",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT 2147483647 + 1",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT 32767::int2 + 1::int2",
            "smallint out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT -(-2147483648)",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT (-2147483648) / -1",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    assert_eq!(
        server
            .query_text("SELECT 2147483647::int8 + 1")
            .await
            .unwrap(),
        ["2147483648"]
    );
    assert_eq!(server.query_text("SELECT 7 / 2").await.unwrap(), ["3"]);
    assert_eq!(server.query_text("SELECT -7 % 2").await.unwrap(), ["-1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn column_overflow_raises_at_the_result_width() {
    let server = TestServer::start().await;
    widths(&server, "iw_over").await;
    server
        .exec("UPDATE iw_over SET i = 2147483647, s = 32767 WHERE id = 'r'")
        .await
        .unwrap();

    server
        .expect_error(
            "SELECT i + 1 FROM iw_over",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT s + s FROM iw_over",
            "smallint out of range (SQLSTATE 22003)",
        )
        .await;
    server
        .expect_error(
            "SELECT id FROM iw_over WHERE i * 2 > 0",
            "integer out of range (SQLSTATE 22003)",
        )
        .await;
    assert_eq!(
        server
            .query_rows("SELECT i + b, s + b FROM iw_over")
            .await
            .unwrap(),
        [["2147483651", "32771"]]
    );
}

/// A bound integer parameter keeps its parameter type: an `INT8` `$1` is
/// `bigint` arithmetic, and an `INT4` one is checked at `integer` width.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bound_parameter_keeps_its_type() {
    use tokio_postgres::types::Type;
    let server = TestServer::start().await;

    let int8 = server
        .client
        .prepare_typed("SELECT $1 + 1", &[Type::INT8])
        .await
        .expect("prepare an int8 parameter");
    assert_eq!(int8.columns()[0].type_(), &Type::INT8);
    for (value, want) in [(2_147_483_647i64, 2_147_483_648i64), (5, 6)] {
        let rows = server
            .client
            .query(&int8, &[&value])
            .await
            .unwrap_or_else(|e| panic!("int8 $1 = {value}: {e}"));
        assert_eq!(rows[0].get::<_, i64>(0), want, "int8 $1 = {value}");
    }

    let int4 = server
        .client
        .prepare_typed("SELECT $1 + 1", &[Type::INT4])
        .await
        .expect("prepare an int4 parameter");
    assert_eq!(int4.columns()[0].type_(), &Type::INT4);
    let err = server
        .client
        .query(&int4, &[&2_147_483_647i32])
        .await
        .expect_err("int4 2147483647 + 1 overflows");
    let db = err.as_db_error().expect("a server error");
    assert_eq!(db.code().code(), "22003");
    assert!(
        db.message().contains("integer out of range"),
        "{}",
        db.message()
    );
}

/// An untyped `$1` compared to an `INT` column is described and bound as
/// `int4`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_untyped_parameter_takes_its_column_type() {
    use tokio_postgres::types::Type;
    let server = TestServer::start().await;
    widths(&server, "iw_param").await;

    let statement = server
        .client
        .prepare("SELECT id FROM iw_param WHERE i = $1")
        .await
        .expect("prepare");
    assert_eq!(statement.params(), &[Type::INT4]);
    let rows = server
        .client
        .query(&statement, &[&3i32])
        .await
        .expect("an int4 parameter binds");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "r");
}

/// `abs($1)` and `mod($1, …)` keep the parameter's type: `abs` of the
/// least `integer` overflows it, and of the least `bigint` value that fits
/// an `integer` does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abs_and_mod_parameters_keep_their_type() {
    use tokio_postgres::types::Type;
    let server = TestServer::start().await;

    let int4 = server
        .client
        .prepare_typed("SELECT abs($1)", &[Type::INT4])
        .await
        .expect("prepare");
    assert_eq!(int4.columns()[0].type_(), &Type::INT4);
    let err = server
        .client
        .query(&int4, &[&i32::MIN])
        .await
        .expect_err("abs of the least integer overflows");
    assert_eq!(err.as_db_error().map(|db| db.code().code()), Some("22003"));

    let int8 = server
        .client
        .prepare_typed("SELECT abs($1), mod($1, 3)", &[Type::INT8])
        .await
        .expect("prepare");
    assert_eq!(int8.columns()[0].type_(), &Type::INT8);
    assert_eq!(int8.columns()[1].type_(), &Type::INT8);
    let rows = server
        .client
        .query(&int8, &[&i64::from(i32::MIN)])
        .await
        .expect("abs of a bigint");
    assert_eq!(rows[0].get::<_, i64>(0), 2_147_483_648);
    assert_eq!(rows[0].get::<_, i64>(1), -2);
}
