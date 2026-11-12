// SPDX-License-Identifier: BUSL-1.1

//! A native integer parameter carries no declared width, so it is `bigint`
//! wherever its width decides the result: an arithmetic operand, an `abs`
//! or `mod` argument, a SELECT item. `$1 + 1` with 2147483647 is the
//! `bigint` 2147483648, never "integer out of range". A comparison keeps
//! the plain literal.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;

/// The one cell `sql` with `params` answers over the native protocol.
async fn cell(native: &NativeClient, sql: &str, params: &[Value]) -> Value {
    let result = native
        .execute_sql(sql, params)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let [row] = result.rows.as_slice() else {
        panic!("{sql}: one row, got {:?}", result.rows);
    };
    let [value] = row.as_slice() else {
        panic!("{sql}: one cell, got {row:?}");
    };
    value.clone()
}

#[tokio::test]
async fn a_native_integer_parameter_is_bigint() {
    let server = TestServer::start().await;
    let native = NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ));
    let max = [Value::Integer(i64::from(i32::MAX))];
    let min = [Value::Integer(i64::from(i32::MIN))];

    assert_eq!(
        cell(&native, "SELECT $1 + 1 AS n", &max).await,
        Value::Integer(2_147_483_648)
    );
    assert_eq!(
        cell(&native, "SELECT $1 * 2 AS n", &max).await,
        Value::Integer(4_294_967_294)
    );
    assert_eq!(
        cell(&native, "SELECT -$1 AS n", &min).await,
        Value::Integer(2_147_483_648)
    );
    assert_eq!(
        cell(&native, "SELECT abs($1) AS n", &min).await,
        Value::Integer(2_147_483_648)
    );
    assert_eq!(
        cell(&native, "SELECT mod($1, 2) AS n", &max).await,
        Value::Integer(1)
    );

    native
        .execute_sql(
            "CREATE COLLECTION npw (id TEXT PRIMARY KEY, i INT) WITH (engine='document_strict')",
            &[],
        )
        .await
        .expect("create");
    native
        .execute_sql("INSERT INTO npw (id, i) VALUES ('a', $1)", &max)
        .await
        .expect("insert");
    assert_eq!(
        cell(
            &native,
            "SELECT i + $1 AS n FROM npw WHERE i = $2",
            &[Value::Integer(1), Value::Integer(i64::from(i32::MAX)),]
        )
        .await,
        Value::Integer(2_147_483_648),
        "an int4 column plus a bigint parameter is bigint"
    );

    server.graceful_shutdown().await;
}
