// SPDX-License-Identifier: BUSL-1.1

//! A native session updates the source of a cross-shard materialized sum
//! inside a `BEGIN` .. `COMMIT` block, and another session changes the same
//! source row before the `COMMIT`.
//!
//! The update settles a balance delta from the source image it read. The
//! block buffers that delta, and `COMMIT` applies it with the source rows. Only
//! the OCC read versions of the settled images guard the delta, so the block
//! must carry them. A `COMMIT` that applies a delta folded from the old image
//! leaves the balance different from the source sum.
//!
//! Each test checks the balance against the source sum, not the answer of the
//! `COMMIT`. An abort with a serialization error is a valid end.
//!
//! One test drives the update through the `DocumentUpdate` opcode. The other
//! drives it through the SQL opcode.

use nodedb_test_support::native_harness::{open_trust_session, send_request, send_sql};
use nodedb_types::Value;
use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};
use tokio::net::TcpStream;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture};
use crate::harness::TestServer;

/// Sets posting `e0` from 10 to 1. Settlement ships -9 to `acc`.
const SQL_UPDATE: &str = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'";
/// The concurrent change: `e0` goes from 10 to 20, and `acc` from 15 to 25.
const MOVE_SOURCE: &str = "UPDATE ptv_post SET amount = '20' WHERE id = 'e0'";

/// A native session with a running sequence number. Seq 1 is the handshake.
struct Session {
    stream: TcpStream,
    seq: u64,
}

impl Session {
    async fn open(server: &TestServer) -> Self {
        let stream = open_trust_session(server.native_port, "nodedb").await;
        Self { stream, seq: 1 }
    }

    async fn sql(&mut self, sql: &str) -> NativeResponse {
        self.seq += 1;
        send_sql(&mut self.stream, self.seq, sql).await
    }

    /// Set `amount` of posting `e0` to 1 with the `DocumentUpdate` opcode.
    async fn update_e0_by_opcode(&mut self) -> NativeResponse {
        self.seq += 1;
        let amount = nodedb_types::value_to_msgpack(&Value::String("1".into())).expect("encode");
        send_request(
            &mut self.stream,
            self.seq,
            OpCode::DocumentUpdate,
            TextFields {
                collection: Some("ptv_post".to_string()),
                document_id: Some("e0".to_string()),
                updates: Some(vec![("amount".to_string(), amount)]),
                ..Default::default()
            },
        )
        .await
    }
}

/// The sum of the `acc` postings, read from the source rows.
async fn source_sum(server: &TestServer) -> i64 {
    server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc'")
        .await
        .expect("read the source postings")
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

/// How a response ended, for an assert message.
fn describe(response: &NativeResponse) -> String {
    match &response.error {
        Some(error) => format!("{:?} {}: {}", response.status, error.code, error.message),
        None => format!("{:?}", response.status),
    }
}

/// Open a block, run the update, change the source from a second session, and
/// commit. The balance must equal the source sum afterwards.
async fn update_in_block_then_move_source(by_opcode: bool) {
    let server = sum_fixture().await;
    let mut session = Session::open(&server).await;

    let begin = session.sql("BEGIN").await;
    assert_ne!(
        begin.status,
        ResponseStatus::Error,
        "BEGIN: {}",
        describe(&begin)
    );

    let update = if by_opcode {
        session.update_e0_by_opcode().await
    } else {
        session.sql(SQL_UPDATE).await
    };
    assert_ne!(
        update.status,
        ResponseStatus::Error,
        "the update inside the block: {}",
        describe(&update)
    );

    server
        .exec(MOVE_SOURCE)
        .await
        .expect("the concurrent source change");
    assert_eq!(
        balance(&server, "acc").await,
        "25",
        "the concurrent change settles its own delta"
    );

    let commit = session.sql("COMMIT").await;
    let sum = source_sum(&server).await;
    assert_eq!(
        balance(&server, "acc").await,
        sum.to_string(),
        "the balance must equal the source sum: COMMIT answered {}; the block either \
         aborts or settles again, and never applies the delta it computed from the old image",
        describe(&commit)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_direct_update_in_block_keeps_the_sum_when_the_source_moved() {
    update_in_block_then_move_source(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sql_update_in_block_keeps_the_sum_when_the_source_moved() {
    update_in_block_then_move_source(false).await;
}
