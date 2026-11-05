// SPDX-License-Identifier: BUSL-1.1

//! The read consistency of the statement now running on this connection.
//!
//! The mirror read check sits at the dispatch capability gate, below every
//! protocol's signatures, so the statement scope carries the session it reads
//! `default_read_consistency` from. The value resolves at the check, so a
//! `SET` earlier in the same simple-query batch applies to the reads after it.
//!
//! An HTTP or WebSocket request has no session parameters. It names its level
//! in a request field, which [`with_request_consistency`] holds for the
//! request's future. Any other work outside a statement scope reads as
//! `Strong`: a server-owned task owns its reads.

use std::future::Future;
use std::sync::Arc;

use crate::types::ReadConsistency;

use super::conn_scope::with_scope;
use super::connection::SessionId;
use super::store::SessionStore;

/// The session a statement reads its consistency from.
pub(super) type ReadSession = (Arc<SessionStore>, SessionId);

/// Guard installing one statement's session on the current connection scope.
/// Dropping it restores whatever was there before, as a nested statement
/// leaves the outer one's session in place.
pub struct ReadConsistencyScope {
    previous: Option<ReadSession>,
}

impl Drop for ReadConsistencyScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        with_scope((), |scope| *scope.read_session.borrow_mut() = previous);
    }
}

/// Bind the running statement's reads to `session_id` on `sessions`.
pub fn enter(sessions: &Arc<SessionStore>, session_id: SessionId) -> ReadConsistencyScope {
    let installed = Some((Arc::clone(sessions), session_id));
    let previous = with_scope(None, |scope| scope.read_session.replace(installed));
    ReadConsistencyScope { previous }
}

tokio::task_local! {
    static REQUEST_CONSISTENCY: ReadConsistency;
}

/// Run `future`, one request of a session-less protocol, at `consistency`.
///
/// The request gets connection slots of its own, as a connection does.
/// So the statement's DDL audit context follows it across every `.await`.
/// These protocols have no frame for a statement notice, so a notice left at the end is logged.
pub fn with_request_consistency<F: Future>(
    consistency: ReadConsistency,
    future: F,
) -> impl Future<Output = F::Output> {
    REQUEST_CONSISTENCY.scope(
        consistency,
        super::conn_scope::scoped(async move {
            let output = future.await;
            for notice in super::statement_notice::take() {
                tracing::warn!(notice = %notice, "statement notice on a session-less request");
            }
            output
        }),
    )
}

/// Whether a read of the running statement confirms leadership first.
pub fn linearizable() -> bool {
    current().requires_leader()
}

/// The running statement's read consistency.
pub fn current() -> ReadConsistency {
    if let Ok(consistency) = REQUEST_CONSISTENCY.try_with(|consistency| *consistency) {
        return consistency;
    }
    let session = with_scope(None, |scope| scope.read_session.borrow().clone());
    match session {
        Some((sessions, session_id)) => sessions.read_consistency(session_id),
        None => ReadConsistency::Strong,
    }
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope::scoped;
    use super::*;

    #[tokio::test]
    async fn outside_a_scope_reads_are_strong() {
        assert_eq!(current(), ReadConsistency::Strong);
    }

    #[tokio::test]
    async fn a_request_field_applies_for_its_future() {
        let level = with_request_consistency(ReadConsistency::Eventual, async {
            tokio::task::yield_now().await;
            current()
        })
        .await;
        assert_eq!(level, ReadConsistency::Eventual);
        assert_eq!(current(), ReadConsistency::Strong);
    }

    #[tokio::test]
    async fn the_session_value_applies_inside_the_scope() {
        let addr: std::net::SocketAddr = ([127, 0, 0, 1], 5000).into();
        let sessions = Arc::new(SessionStore::new());
        sessions.ensure_session(addr);
        sessions.set_parameter(
            addr,
            super::super::read_consistency::PARAM_KEY.to_string(),
            "eventual".to_string(),
        );
        scoped(async {
            assert_eq!(current(), ReadConsistency::Strong);
            {
                let _scope = enter(&sessions, addr.into());
                assert_eq!(current(), ReadConsistency::Eventual);
            }
            assert_eq!(current(), ReadConsistency::Strong);
        })
        .await;
    }
}
