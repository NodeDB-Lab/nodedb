// SPDX-License-Identifier: BUSL-1.1

//! The statement splitter of a pgwire simple query, through the wire.
//!
//! `pgwire/handler/sql_split.rs` splits a simple query at top-level
//! semicolons. PostgreSQL syntax the splitter must read:
//!
//! - An `E'..'` string reads `\` as an escape, so `\'` stays inside the
//!   string. A plain string has no escape.
//! - A block comment nests: `/* a /* b */ ; */` is one comment. `/*/` opens a
//!   comment and does not close it.
//! - A tagged dollar quote `$t$ .. $t$` hides its semicolons.
//!
//! Each cell sends an insert whose literal holds a semicolon, then a select
//! that reads the stored value back. A split inside the literal answers a
//! parse error of the cut fragment or a wrong value, and the cell is red.
//!
//! The planner behind the splitter does not evaluate `E'..'` or `$t$..$t$`
//! literals: it answers `unsupported: value literal`. A cell that sends such
//! a literal accepts that answer, which proves the splitter passed the
//! statement whole. Any other error is red.

use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

async fn fixture() -> TestServer {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION tsp (id TEXT PRIMARY KEY, v TEXT)")
        .await
        .unwrap_or_else(|e| panic!("create tsp: {e}"));
    server
}

/// The text of the planner's refusal of an `E'..'` or dollar-quoted literal.
const PLANNER_LIMIT: &str = "unsupported: value literal";

/// Send `sql` as one simple query. Returns the first column of every row.
async fn try_rows(server: &TestServer, sql: &str) -> Result<Vec<String>, tokio_postgres::Error> {
    let messages = server.client.simple_query(sql).await?;
    Ok(messages
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect())
}

/// Send `sql` as one simple query. Any error fails the cell.
async fn rows_of(server: &TestServer, sql: &str) -> Vec<String> {
    try_rows(server, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql} must run, got {:?} / {e:?}", e.as_db_error()))
}

/// Insert `literal` as the value of row `id`, then read it back, in one query.
/// The read-back must equal `expected`, or the planner must refuse the literal
/// with [`PLANNER_LIMIT`]. Any other error is a split inside the literal.
async fn assert_stored(server: &TestServer, id: &str, literal: &str, expected: &[&str]) {
    let sql = format!(
        "INSERT INTO tsp (id, v) VALUES ('{id}', {literal}); \
         SELECT v FROM tsp WHERE id = '{id}'"
    );
    match try_rows(server, &sql).await {
        Ok(rows) => assert_eq!(rows, expected, "{sql}"),
        Err(e) => {
            let message = e
                .as_db_error()
                .map(|db| db.message().to_string())
                .unwrap_or_default();
            assert!(
                message.contains(PLANNER_LIMIT),
                "{sql} must run or hit the planner limit, got {message:?} / {e:?}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_escape_string_hides_an_escaped_quote_and_its_semicolon() {
    let server = fixture().await;
    assert_stored(&server, "s1", "E'a\\';b'", &["a';b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lowercase_escape_prefix_opens_an_escape_string() {
    let server = fixture().await;
    assert_stored(&server, "s2", "e'a\\';b'", &["a';b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_escape_prefix_after_a_parenthesis_opens_an_escape_string() {
    let server = fixture().await;
    assert_stored(&server, "s3", "(E'a\\';b')", &["a';b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_escaped_backslash_before_the_closing_quote_closes_the_string() {
    let server = fixture().await;
    assert_stored(&server, "s4", "E'a\\\\'", &["a\\"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_doubled_quote_inside_an_escape_string_stays_inside() {
    let server = fixture().await;
    assert_stored(&server, "s5", "E'a''b;c'", &["a'b;c"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plain_string_has_no_backslash_escape() {
    let server = fixture().await;
    assert_stored(&server, "s6", "'a\\'", &["a\\"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tagged_dollar_quote_hides_its_semicolon() {
    let server = fixture().await;
    assert_stored(&server, "s7", "$t$a;b$t$", &["a;b"]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nested_block_comment_hides_its_semicolon() {
    let server = fixture().await;
    let sql = "INSERT INTO tsp (id, v) VALUES ('s8', 'x'); \
               SELECT /* a /* b */ ; */ v FROM tsp WHERE id = 's8'";
    assert_eq!(rows_of(&server, sql).await, ["x"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_comment_open_followed_by_a_slash_stays_open() {
    let server = fixture().await;
    let sql = "INSERT INTO tsp (id, v) VALUES ('s9', 'x'); \
               SELECT /*/ ; */ v FROM tsp WHERE id = 's9'";
    assert_eq!(rows_of(&server, sql).await, ["x"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_comment_does_not_hide_the_next_semicolon() {
    let server = fixture().await;
    let sql = "INSERT INTO tsp (id, v) VALUES ('s10', 'x') /* a */; \
               SELECT v FROM tsp WHERE id = 's10'";
    assert_eq!(rows_of(&server, sql).await, ["x"]);
}

/// The same `E'..'` literal over HTTP, which plans the whole request and has
/// no splitter. A control for the cells above: HTTP answers the planner limit
/// too, so that answer is not a split.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_http_escape_string_with_a_semicolon() {
    let server = fixture().await;
    let (status, body) = server
        .http_query("INSERT INTO tsp (id, v) VALUES ('h1', E'a\\';b')")
        .await;
    if status == 200 {
        assert_eq!(
            server
                .query_text("SELECT v FROM tsp WHERE id = 'h1'")
                .await
                .unwrap_or_else(|e| panic!("read h1: {e}")),
            ["a';b"]
        );
    } else {
        assert!(
            body.contains(PLANNER_LIMIT),
            "an E-string insert over HTTP must run or hit the planner limit: {status} {body}"
        );
    }
}
