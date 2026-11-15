// SPDX-License-Identifier: BUSL-1.1

//! A predicate write or a TRUNCATE beside another write of one
//! materialized-sum source, in one request, on every transport that plans a
//! request as one task set.
//!
//! Correct behaviour on any build: the request is refused whole and changes
//! nothing, or it applies with each balance equal to the sum of its source
//! rows. A red cell is an applied request whose balance drifted, or a refused
//! request that changed a row or a balance.
//!
//! The fixture starts with `e0` = 10 and `e1` = 5 on `acc`, balance 15, and
//! `acc2` at 0.

use super::sum_cell_support::{Transport, balance, source_sum, sum_fixture};
use crate::harness::TestServer;

/// Every source row as its tab-joined `id`, `account_id` and `amount`.
async fn source_rows(server: &TestServer) -> Vec<String> {
    server
        .query_text_joined("SELECT id, account_id, amount FROM ptv_post ORDER BY id")
        .await
        .unwrap_or_else(|e| panic!("read the source rows: {e}"))
}

/// The sum of the `acc2` postings, read from the source rows.
async fn acc2_sum(server: &TestServer) -> i64 {
    server
        .query_text("SELECT amount FROM ptv_post WHERE account_id = 'acc2'")
        .await
        .unwrap_or_else(|e| panic!("read the acc2 postings: {e}"))
        .iter()
        .map(|amount| amount.parse::<i64>().expect("a numeric amount"))
        .sum()
}

/// Each balance equals the sum of its source rows.
async fn assert_balances_match_sources(server: &TestServer, what: &str) {
    assert_eq!(
        balance(server, "acc").await,
        source_sum(server).await.to_string(),
        "acc must equal its source sum after {what}"
    );
    assert_eq!(
        balance(server, "acc2").await,
        acc2_sum(server).await.to_string(),
        "acc2 must equal its source sum after {what}"
    );
}

/// Send `sql` as one request on `transport`. A refusal asks for separate
/// statements, changes no source row and leaves `acc` at 15. An applied
/// request keeps both balances equal to their source sums.
async fn refused_or_settled(transport: Transport, sql: &str) {
    let server = sum_fixture().await;
    let seed = source_rows(&server).await;

    let sent = transport.send(&server, sql).await;

    if sent.errored {
        assert!(
            sent.text.contains("run them as separate statements"),
            "{transport:?}: a refusal names its remedy: {}",
            sent.text
        );
        assert_eq!(
            source_rows(&server).await,
            seed,
            "{transport:?}: a refused request changes no source row: {}",
            sent.text
        );
        assert_eq!(
            balance(&server, "acc").await,
            "15",
            "{transport:?}: acc stays after a refusal: {}",
            sent.text
        );
    }
    assert_balances_match_sources(&server, sql).await;
}

/// A predicate delete of `acc`, then a point update of the row it removed.
const UPDATE_AFTER_DELETE: &str = "DELETE FROM ptv_post WHERE account_id = 'acc'; \
     UPDATE ptv_post SET amount = '7' WHERE id = 'e0'";
/// A predicate delete of `acc`, then a point delete of the row it removed.
const DELETE_AFTER_DELETE: &str = "DELETE FROM ptv_post WHERE account_id = 'acc'; \
     DELETE FROM ptv_post WHERE id = 'e0'";
/// An insert, then a predicate delete that matches the inserted row.
const INSERT_THEN_PREDICATE_DELETE: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc2', '7'); \
     DELETE FROM ptv_post WHERE amount > '0'";
/// A point move into `acc2`, then a predicate delete of `acc2`.
const MOVE_THEN_PREDICATE_DELETE: &str = "UPDATE ptv_post SET account_id = 'acc2' WHERE id = 'e0'; \
     DELETE FROM ptv_post WHERE account_id = 'acc2'";
/// A predicate move into `acc2`, then a predicate delete of `acc2`.
const PREDICATE_MOVE_THEN_DELETE: &str = "UPDATE ptv_post SET account_id = 'acc2' WHERE amount > '8'; \
     DELETE FROM ptv_post WHERE account_id = 'acc2'";
/// An insert on `acc2`, then a TRUNCATE that removes the inserted row.
const INSERT_THEN_TRUNCATE: &str = "INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc2', '7'); \
     TRUNCATE ptv_post";

macro_rules! per_transport {
    ($case:ident, $sql:expr) => {
        mod $case {
            use super::*;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn http() {
                refused_or_settled(Transport::Http, $sql).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ndjson() {
                refused_or_settled(Transport::Ndjson, $sql).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ws() {
                refused_or_settled(Transport::Ws, $sql).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn native() {
                refused_or_settled(Transport::Native, $sql).await;
            }
        }
    };
}

per_transport!(p2_update_after_predicate_delete, UPDATE_AFTER_DELETE);
per_transport!(p3_delete_after_predicate_delete, DELETE_AFTER_DELETE);
per_transport!(insert_then_predicate_delete, INSERT_THEN_PREDICATE_DELETE);
per_transport!(move_then_predicate_delete, MOVE_THEN_PREDICATE_DELETE);
per_transport!(predicate_move_then_delete, PREDICATE_MOVE_THEN_DELETE);
per_transport!(insert_then_truncate, INSERT_THEN_TRUNCATE);

/// Control: a predicate write then a point write of a row the predicate does
/// not match, on another balance. It applies on any build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_predicate_write_then_a_point_write_of_another_row() {
    let server = sum_fixture().await;
    server
        .exec("INSERT INTO ptv_post (id, account_id, amount) VALUES ('e2', 'acc2', '7')")
        .await
        .unwrap_or_else(|e| panic!("seed e2: {e}"));
    let sent = Transport::Http
        .send(
            &server,
            "UPDATE ptv_post SET amount = '2' WHERE account_id = 'acc'; \
             UPDATE ptv_post SET amount = '9' WHERE id = 'e2'",
        )
        .await;
    assert!(!sent.errored, "the request applies: {}", sent.text);
    assert_eq!(balance(&server, "acc").await, "4", "e0 and e1 at 2");
    assert_eq!(balance(&server, "acc2").await, "9", "e2 at 9");
    assert_balances_match_sources(&server, "the control").await;
}

/// Control: a predicate delete, then a re-insert of the removed row, as two
/// requests apply and stay consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_two_requests_apply() {
    let server = sum_fixture().await;
    server
        .http_exec("DELETE FROM ptv_post WHERE account_id = 'acc'")
        .await;
    server
        .http_exec("INSERT INTO ptv_post (id, account_id, amount) VALUES ('e0', 'acc', '99')")
        .await;
    assert_eq!(balance(&server, "acc").await, "99");
    assert_balances_match_sources(&server, "two requests").await;
}
