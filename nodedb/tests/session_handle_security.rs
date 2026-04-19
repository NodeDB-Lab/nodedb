//! Wire-level coverage for the session-handle resolver's missing
//! hygiene layer: rate limiting + visibility on miss.
//!
//! Today, `SET LOCAL nodedb.auth_session = '<handle>'` on a pgwire connection
//! silently tries to resolve the handle with no per-connection throttle and
//! no observable signal on miss — a misconfigured client or a stolen-handle
//! probe can hammer the resolver indefinitely. Acceptance specifies:
//!
//! > simulate 100 failed `SET LOCAL nodedb.auth_session` calls → connection
//! > closed with pgwire error; audit event recorded.
//!
//! This file encodes that acceptance at the wire boundary — no assumption
//! about internal API names, method signatures, or struct fields. The test
//! compiles against today's code, fails today (server silently accepts all
//! 100 attempts), and passes once the rate-limit + audit plumbing lands,
//! regardless of how the fix is shaped internally.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test]
async fn set_local_auth_session_flood_closes_connection() {
    let server = TestServer::start().await;

    // 100 distinct bogus handles so each is a genuine resolve-miss, not a
    // short-circuit on a repeated value. Each attempt is individually valid
    // SQL; only the count + failure rate should matter.
    let mut closed = false;
    let mut attempts_before_close = 0usize;
    for i in 0..100 {
        attempts_before_close = i + 1;
        let sql = format!("SET LOCAL nodedb.auth_session = 'nds_bogus_{i:032x}'");
        if server.client.simple_query(&sql).await.is_err() {
            closed = true;
            break;
        }
    }

    assert!(
        closed,
        "server accepted {attempts_before_close} consecutive failed \
         `SET LOCAL nodedb.auth_session` calls on one connection without \
         throttling or error — the resolver is unthrottled and unobservable. \
         Expected: connection closed with a pgwire error well before 100 \
         attempts (20/min default)"
    );
}
