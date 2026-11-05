// SPDX-License-Identifier: BUSL-1.1

//! Ending a response body when its session is killed.

use futures::{Stream, StreamExt};

use super::binding::SessionBinding;

/// Yield the items of `stream` until `binding` is killed, then end.
///
/// The kill drops `stream`, the path a client disconnect already takes for
/// these read-only bodies. The binding lives as long as the returned stream.
pub fn stream_until_ended<S>(stream: S, mut binding: SessionBinding) -> impl Stream<Item = S::Item>
where
    S: Stream,
{
    async_stream::stream! {
        // A stream serves for as long as it stays connected, so it is never idle.
        binding.set_statement("stream");
        let mut stream = std::pin::pin!(stream);
        loop {
            let next = tokio::select! {
                biased;
                _ = binding.ended() => None,
                item = stream.next() => item,
            };
            match next {
                Some(item) => yield item,
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::identity::Role;
    use crate::control::security::sessions::{
        KillReason, SessionBindRequest, bind_session, stream_until_ended,
    };
    use crate::control::state::SharedState;
    use crate::types::TenantId;
    use crate::wal::WalManager;

    #[tokio::test]
    async fn a_kill_ends_the_stream_and_removes_the_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("t.wal")).expect("wal"));
        let (dispatcher, _) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        state
            .credentials
            .create_user("sse", "pw", TenantId::new(1), vec![Role::ReadOnly])
            .expect("create user");
        let identity =
            crate::control::server::session_auth::trust_identity(&state, "sse").expect("identity");
        let binding = bind_session(
            &state,
            SessionBindRequest::new(&identity, "sse", "127.0.0.1:1"),
        )
        .expect("bind");
        let session_id = binding.session_id().to_string();

        let mut body = Box::pin(stream_until_ended(
            futures::stream::pending::<u8>(),
            binding,
        ));
        state
            .session_registry
            .kill_session_by_id(&session_id, KillReason::AdminKill);

        assert_eq!(body.next().await, None);
        drop(body);
        assert_eq!(state.session_registry.count(None), 0);
    }
}
