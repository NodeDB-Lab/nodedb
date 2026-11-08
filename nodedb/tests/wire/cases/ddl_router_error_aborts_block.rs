// SPDX-License-Identifier: BUSL-1.1

//! An error from the DDL router inside a transaction block aborts the block,
//! as in PostgreSQL: every later statement gets SQLSTATE 25P02 until
//! ROLLBACK. The rule holds on the native and the pgwire simple-query path.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::protocol::NativeResponse;
use tokio::net::TcpStream;

use crate::harness::TestServer;

/// A DDL statement the router claims and answers with an error.
const ROUTER_ERROR: &str = "DROP COLLECTION router_absent";

const INSERT_A: &str = "INSERT INTO router_t (id, n) VALUES ('a', 1)";
const INSERT_B: &str = "INSERT INTO router_t (id, n) VALUES ('b', 1)";

async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION router_t (id TEXT PRIMARY KEY, n INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap_or_else(|e| panic!("create router_t: {e}"));
    server
}

/// A native session. Seq 1 is the handshake, so requests start at seq 2.
struct Native {
    stream: TcpStream,
    seq: u64,
}

impl Native {
    async fn sql(&mut self, sql: &str) -> NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_router_error_aborts_the_block_until_rollback() {
    let server = fixture().await;
    let mut native = Native {
        stream: open_trust_session(server.native_port, "nodedb").await,
        seq: 1,
    };
    assert!(native.sql("BEGIN").await.error.is_none(), "BEGIN");
    let router_error = native
        .sql(ROUTER_ERROR)
        .await
        .error
        .expect("the router must refuse the statement");
    assert_ne!(router_error.code, "25P02", "{}", router_error.message);
    let after_error = native
        .sql(INSERT_A)
        .await
        .error
        .expect("the aborted block must refuse");
    assert_eq!(after_error.code, "25P02", "{}", after_error.message);
    assert!(native.sql("ROLLBACK").await.error.is_none(), "ROLLBACK");
    let again = native.sql(INSERT_B).await;
    assert!(again.error.is_none(), "a lone write applies after ROLLBACK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_router_error_aborts_the_block_until_rollback() {
    let server = fixture().await;
    let client = &server.client;
    client.simple_query("BEGIN").await.expect("begin");
    let router = client
        .simple_query(ROUTER_ERROR)
        .await
        .expect_err("the router must refuse the statement");
    let router_code = router.as_db_error().expect("server SQLSTATE").code().code();
    assert_ne!(router_code, "25P02");
    let after = client
        .simple_query(INSERT_A)
        .await
        .expect_err("the aborted block must refuse");
    let after = after.as_db_error().expect("server SQLSTATE");
    assert_eq!(after.code().code(), "25P02", "{}", after.message());
    client.simple_query("ROLLBACK").await.expect("rollback");
    client
        .simple_query(INSERT_B)
        .await
        .expect("a lone write applies after ROLLBACK");
}
