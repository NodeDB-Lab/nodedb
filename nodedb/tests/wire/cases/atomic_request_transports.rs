// SPDX-License-Identifier: BUSL-1.1

//! An insert into a materialized-sum source, then a read of its target, in
//! one request on every transport. The read answers the balance the insert
//! moved: 18, not the seeded 15.

use super::atomic_transport::Transport;
use super::materialized_sum_cross_shard_expanders::{POSTINGS, sum_fixture_on};
use crate::harness::TestServer;

async fn sum_target_read(transport: Transport) {
    let server = sum_fixture_on(TestServer::start_multicores(2).await).await;
    let sql = format!(
        "INSERT INTO {POSTINGS} (id, account_id, amount) VALUES ('t1', 'acc', '3'); \
         SELECT balance FROM ptv_acct WHERE id = 'acc'"
    );
    let sent = transport.send(&server, &sql).await;
    assert!(!sent.errored, "{transport:?}: {}", sent.text);
    assert!(
        sent.text.contains("18") && !sent.text.contains("15"),
        "{transport:?}: the read after the write must answer 18, got {}",
        sent.text
    );
}

macro_rules! per_transport {
    ($case:ident) => {
        mod $case {
            use super::*;
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn http() {
                super::$case(Transport::Http).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ndjson() {
                super::$case(Transport::Ndjson).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn ws() {
                super::$case(Transport::Ws).await;
            }
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn native() {
                super::$case(Transport::Native).await;
            }
        }
    };
}

per_transport!(sum_target_read);
