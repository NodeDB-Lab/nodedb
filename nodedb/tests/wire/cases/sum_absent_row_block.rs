// SPDX-License-Identifier: BUSL-1.1

//! The absent-row races of [`super::sum_absent_row`] inside a
//! transaction block, on pgwire and on native.
//!
//! The block's statement reads the row and records the read. COMMIT checks it
//! against the rows then. A block whose write a mover raced must commit or
//! abort with SQLSTATE 40001, and the balance must equal the source sum either
//! way. A native block writes through a direct op.

#![cfg(feature = "failpoints")]

use nodedb_test_support::native_harness::{open_trust_session, send_request_within};
use nodedb_types::protocol::{OpCode, TextFields};
use tokio::net::TcpStream;

use super::sum_absent_row::{
    ABSENT_UPDATE, End, PARKED_BOUND, ROUTE_GATE, Race, UNBOUND_UPDATE, assert_settled, exec,
    native_end, native_write, pg_end, sql_fields, write_sql,
};
use super::sum_cell_support::{arm_gate, parked_fixture_at, sum_fixture, wait_parked_at};
use crate::harness::TestServer;

/// A feed row the block inserts. `ptv_feed` homes on another vShard than
/// `ptv_post`, so the block's COMMIT is multi-shard.
const FEED_F0: &str = "INSERT INTO ptv_feed (id, account_id, amount) VALUES ('f0', 'acc2', '2')";

/// One block session: pgwire, or native with its last sequence number.
enum Session {
    Pg(
        tokio_postgres::Client,
        #[expect(
            dead_code,
            reason = "held only so dropping the session ends the connection task"
        )]
        tokio::task::JoinHandle<()>,
    ),
    Native(TcpStream, u64),
}

impl Session {
    async fn open(server: &TestServer, native: bool) -> Self {
        if native {
            return Self::Native(open_trust_session(server.native_port, "nodedb").await, 1);
        }
        let (client, connection) = server
            .connect_as("nodedb", "nodedb")
            .await
            .expect("connect the block session");
        Self::Pg(client, connection)
    }

    /// Run `sql` in the block.
    async fn sql(&mut self, sql: &str) -> End {
        self.send(OpCode::Sql, sql_fields(sql), sql).await
    }

    /// Run `race`'s write in the block: SQL on pgwire, a direct op on native.
    async fn write(&mut self, race: Race) -> End {
        let (op, fields) = native_write(race);
        self.send(op, fields, &write_sql(race)).await
    }

    /// pgwire runs `sql`, and native sends `op` with `fields`.
    async fn send(&mut self, op: OpCode, fields: TextFields, sql: &str) -> End {
        match self {
            Self::Pg(client, _) => pg_end(client.simple_query(sql).await.map(drop)),
            Self::Native(stream, seq) => {
                *seq += 1;
                let bound = Some(PARKED_BOUND);
                native_end(&send_request_within(stream, *seq, op, fields, bound).await)
            }
        }
    }
}

/// Assert that `end` answered. `what` names the statement.
fn assert_answered(end: End, what: &str) {
    assert!(
        matches!(end, End::Answered),
        "{what} must answer, got {end:?}"
    );
}

/// Open a block, park `race`'s write at the route gate, run the mover, release
/// the write, COMMIT, and assert that the block settled.
async fn block_parked(native: bool, race: Race) {
    if native {
        // Native never reaches `stmt::before_route`. The block reads
        // the row at the write, so the mover runs between the write and COMMIT.
        return block_then_mover(true, race, &[]).await;
    }
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release");
    let server = parked_fixture_at(ROUTE_GATE, &release).await;
    for sql in race.setup {
        exec(&server, sql).await;
    }
    let mut session = Session::open(&server, native).await;
    assert_answered(session.sql("BEGIN").await, "BEGIN");
    arm_gate(&release);
    let parked = tokio::spawn(async move {
        match session.write(race).await {
            End::Answered => session.sql("COMMIT").await,
            other => other,
        }
    });
    wait_parked_at(ROUTE_GATE, &release, "write inside the block").await;
    exec(&server, race.mover).await;
    std::fs::write(&release, b"release").expect("release the parked write");
    let end = parked.await.expect("the parked block task");
    assert_settled(&server, &end, &format!("native {native}, parked {race:?}")).await;
}

/// Open a block, run `race`'s write and then `more`, run the mover, COMMIT,
/// and assert that the block settled.
async fn block_then_mover(native: bool, race: Race, more: &[&str]) {
    let server = sum_fixture().await;
    for sql in race.setup {
        exec(&server, sql).await;
    }
    let mut session = Session::open(&server, native).await;
    assert_answered(session.sql("BEGIN").await, "BEGIN");
    assert_answered(session.write(race).await, "the write inside the block");
    for sql in more {
        assert_answered(session.sql(sql).await, sql);
    }
    exec(&server, race.mover).await;
    let end = session.sql("COMMIT").await;
    assert_settled(&server, &end, &format!("native {native}, {race:?}")).await;
}

/// A block whose absent-row update commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_block_update_of_an_absent_row_parked_settles() {
    block_parked(false, ABSENT_UPDATE).await;
}

/// A block whose absent-row update commits after the row returns ends at balance 15, sum 17.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_block_update_of_an_absent_row_parked_settles() {
    block_parked(true, ABSENT_UPDATE).await;
}

/// An empty-match block that ends off the source sum or with an error other than 40001 is broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_block_update_of_an_absent_row_then_mover_settles() {
    block_then_mover(false, ABSENT_UPDATE, &[]).await;
}

/// An empty-match block that ends off the source sum or with an error other than 40001 is broken.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_block_update_of_an_absent_row_then_mover_settles() {
    block_then_mover(true, ABSENT_UPDATE, &[]).await;
}

/// A multi-shard block whose unbound update commits after the insert ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_multi_shard_block_update_of_an_unbound_key_settles() {
    block_then_mover(false, UNBOUND_UPDATE, &[FEED_F0]).await;
}

/// A multi-shard block whose unbound update commits after the insert ends at balance 18, sum 22.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_multi_shard_block_update_of_an_unbound_key_settles() {
    block_then_mover(true, UNBOUND_UPDATE, &[FEED_F0]).await;
}
