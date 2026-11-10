// SPDX-License-Identifier: BUSL-1.1

//! Pgwire coverage for text-conversion faults over a point lookup. A cast in
//! the SELECT list of `WHERE id = '<key>'` runs on the Control Plane, in the
//! response shaper. Its fault keeps PostgreSQL's SQLSTATE: `22P02`
//! (invalid_text_representation) for text that is no value of the target
//! type, and `22007` (invalid_datetime_format) for text that is no
//! timestamp.

use crate::harness::TestServer;

const INVALID_TEXT: &str = "SQLSTATE 22P02";
const INVALID_DATETIME: &str = "SQLSTATE 22007";

async fn seed(srv: &TestServer, collection: &str) {
    srv.exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    srv.exec(&format!(
        "INSERT INTO {collection} (id, txt) VALUES ('a', 'abc')"
    ))
    .await
    .unwrap();
}

/// `txt::INT` over a non-numeric column value raises `22P02`.
#[tokio::test]
async fn point_lookup_int_cast_of_text_errors_22p02() {
    let srv = TestServer::start().await;
    seed(&srv, "text_domain_int").await;
    srv.expect_error(
        "SELECT txt::INT FROM text_domain_int WHERE id = 'a'",
        INVALID_TEXT,
    )
    .await;
}

/// `txt::TIMESTAMP` over a non-date column value raises `22007`.
#[tokio::test]
async fn point_lookup_timestamp_cast_of_text_errors_22007() {
    let srv = TestServer::start().await;
    seed(&srv, "text_domain_ts").await;
    srv.expect_error(
        "SELECT txt::TIMESTAMP FROM text_domain_ts WHERE id = 'a'",
        INVALID_DATETIME,
    )
    .await;
}
