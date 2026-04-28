//! In-process token state machine for single-use join-token enforcement.
//!
//! The `JoinTokenStore` tracks every issued token's lifecycle from
//! `Issued` through `InFlight` to `Consumed` (or `Expired`/`Aborted`).
//! In a full distributed deployment this state would be proposed through
//! the metadata Raft group (as `MetadataEntry::JoinTokenTransition`) so
//! all nodes reject a replayed token even after a crash-restart.
//!
//! The trait [`TokenStateBackend`] abstracts the storage so the
//! bootstrap-listener handler can be tested with the in-memory backend
//! and wired to the Raft-backed backend in production.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Lifecycle states for a join token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinTokenLifecycle {
    /// Token has been issued; no joiner has presented it yet.
    Issued,
    /// A joiner at `node_addr` is currently receiving its bundle.
    /// If the joiner times out, the state reverts to `Issued`.
    InFlight { node_addr: SocketAddr },
    /// Bundle was successfully delivered to `node_addr`.
    /// Replay attempts on the same token are rejected.
    Consumed { node_addr: SocketAddr, ts_ms: u64 },
    /// Token's expiry timestamp has passed without being consumed.
    Expired,
    /// Explicitly invalidated (e.g. operator revoke).
    Aborted,
}

/// Complete state record for one token.
#[derive(Debug, Clone)]
pub struct JoinTokenState {
    /// SHA-256 of the token hex string. Never stores the raw token.
    pub token_hash: [u8; 32],
    pub lifecycle: JoinTokenLifecycle,
    /// Absolute unix-ms expiry derived from the token's `expiry_unix_secs`.
    pub expires_at_ms: u64,
    /// Number of times this token moved from `Issued` to `InFlight`
    /// (increments on retry after an in-flight timeout).
    pub attempt: u32,
}

/// Error from token state transitions.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenStateError {
    #[error("join token already consumed")]
    AlreadyConsumed,
    #[error("join token expired")]
    Expired,
    #[error("join token aborted")]
    Aborted,
    #[error("join token is already in-flight from a different address")]
    InFlightConflict,
    #[error("join token not found")]
    NotFound,
    #[error("unexpected lifecycle state for this transition")]
    InvalidTransition,
}

/// Abstraction over where token state is persisted.
///
/// The in-memory implementation ([`InMemoryTokenStore`]) is used in
/// tests and single-node deployments that don't need cross-crash
/// single-use guarantees. Production deployments wire a Raft-backed
/// implementation that proposes each transition through the metadata
/// group.
pub trait TokenStateBackend: Send + Sync + 'static {
    /// Register a freshly issued token with `Issued` state.
    fn register(&self, state: JoinTokenState);
    /// Attempt to transition from `Issued` → `InFlight`.
    /// Returns `Err(AlreadyConsumed)` / `Err(Expired)` / `Err(Aborted)`
    /// if the token is already in a terminal or conflicting state.
    fn begin_inflight(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
    ) -> Result<(), TokenStateError>;
    /// Transition from `InFlight` → `Consumed`. Called after the bundle
    /// has been sent and the peer has acknowledged receipt.
    fn mark_consumed(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
        ts_ms: u64,
    ) -> Result<(), TokenStateError>;
    /// Revert `InFlight` → `Issued` (joiner timed out before ACK).
    fn revert_inflight(&self, token_hash: &[u8; 32]) -> Result<(), TokenStateError>;
    /// Look up the current state.
    fn get(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState>;
}

/// Simple in-memory token store. Thread-safe via `Mutex`.
///
/// Suitable for tests and single-node deployments. Does not survive
/// process restart — a previously-consumed token is invisible after
/// restart, allowing replay on that edge case. Production deployments
/// must use a Raft-backed backend.
#[derive(Default, Clone)]
pub struct InMemoryTokenStore {
    inner: Arc<Mutex<HashMap<[u8; 32], JoinTokenState>>>,
}

impl InMemoryTokenStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TokenStateBackend for InMemoryTokenStore {
    fn register(&self, state: JoinTokenState) {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        map.insert(state.token_hash, state);
    }

    fn begin_inflight(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
    ) -> Result<(), TokenStateError> {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        match &entry.lifecycle {
            JoinTokenLifecycle::Issued => {
                let now_ms = epoch_ms();
                if now_ms > entry.expires_at_ms {
                    entry.lifecycle = JoinTokenLifecycle::Expired;
                    return Err(TokenStateError::Expired);
                }
                entry.lifecycle = JoinTokenLifecycle::InFlight { node_addr };
                entry.attempt += 1;
                Ok(())
            }
            JoinTokenLifecycle::InFlight {
                node_addr: existing,
            } => {
                if *existing == node_addr {
                    // Idempotent: same joiner re-presenting (e.g. reconnect).
                    Ok(())
                } else {
                    Err(TokenStateError::InFlightConflict)
                }
            }
            JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
            JoinTokenLifecycle::Expired => Err(TokenStateError::Expired),
            JoinTokenLifecycle::Aborted => Err(TokenStateError::Aborted),
        }
    }

    fn mark_consumed(
        &self,
        token_hash: &[u8; 32],
        node_addr: SocketAddr,
        ts_ms: u64,
    ) -> Result<(), TokenStateError> {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        match &entry.lifecycle {
            JoinTokenLifecycle::InFlight { .. } => {
                entry.lifecycle = JoinTokenLifecycle::Consumed { node_addr, ts_ms };
                Ok(())
            }
            JoinTokenLifecycle::Consumed { .. } => Err(TokenStateError::AlreadyConsumed),
            _ => Err(TokenStateError::InvalidTransition),
        }
    }

    fn revert_inflight(&self, token_hash: &[u8; 32]) -> Result<(), TokenStateError> {
        let mut map = self.inner.lock().expect("token store lock poisoned");
        let entry = map.get_mut(token_hash).ok_or(TokenStateError::NotFound)?;
        match &entry.lifecycle {
            JoinTokenLifecycle::InFlight { .. } => {
                entry.lifecycle = JoinTokenLifecycle::Issued;
                Ok(())
            }
            _ => Err(TokenStateError::InvalidTransition),
        }
    }

    fn get(&self, token_hash: &[u8; 32]) -> Option<JoinTokenState> {
        let map = self.inner.lock().expect("token store lock poisoned");
        map.get(token_hash).cloned()
    }
}

/// Spawn an async dead-man timer that reverts an `InFlight` token to
/// `Issued` after `timeout` if it has not been consumed. This runs as a
/// detached task; callers call it immediately after `begin_inflight`.
pub fn spawn_inflight_timeout<B: TokenStateBackend>(
    backend: Arc<B>,
    token_hash: [u8; 32],
    timeout: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(timeout).await;
        // If still InFlight, revert — the joiner timed out.
        let _ = backend.revert_inflight(&token_hash);
    });
}

fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn make_state(hash: [u8; 32], expires_in_secs: u64) -> JoinTokenState {
        let expires_at_ms = epoch_ms() + expires_in_secs * 1000;
        JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms,
            attempt: 0,
        }
    }

    #[test]
    fn issued_to_inflight_to_consumed() {
        let store = InMemoryTokenStore::new();
        let hash = [0x01u8; 32];
        store.register(make_state(hash, 60));

        let addr = dummy_addr();
        store.begin_inflight(&hash, addr).unwrap();
        {
            let s = store.get(&hash).unwrap();
            assert_eq!(
                s.lifecycle,
                JoinTokenLifecycle::InFlight { node_addr: addr }
            );
            assert_eq!(s.attempt, 1);
        }

        let ts = epoch_ms();
        store.mark_consumed(&hash, addr, ts).unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(
            s.lifecycle,
            JoinTokenLifecycle::Consumed {
                node_addr: addr,
                ts_ms: ts
            }
        );
    }

    #[test]
    fn replay_on_consumed_token_returns_error() {
        let store = InMemoryTokenStore::new();
        let hash = [0x02u8; 32];
        store.register(make_state(hash, 60));
        let addr = dummy_addr();
        store.begin_inflight(&hash, addr).unwrap();
        store.mark_consumed(&hash, addr, epoch_ms()).unwrap();

        // Second begin_inflight must be rejected.
        assert_eq!(
            store.begin_inflight(&hash, addr).unwrap_err(),
            TokenStateError::AlreadyConsumed
        );
    }

    #[test]
    fn inflight_reverts_to_issued_on_timeout() {
        let store = InMemoryTokenStore::new();
        let hash = [0x03u8; 32];
        store.register(make_state(hash, 60));
        let addr = dummy_addr();
        store.begin_inflight(&hash, addr).unwrap();
        store.revert_inflight(&hash).unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(s.lifecycle, JoinTokenLifecycle::Issued);
        // Second attempt is allowed after revert.
        store.begin_inflight(&hash, addr).unwrap();
        let s = store.get(&hash).unwrap();
        assert_eq!(s.attempt, 2);
    }

    #[test]
    fn expired_token_rejected() {
        let store = InMemoryTokenStore::new();
        let hash = [0x04u8; 32];
        // expires_at_ms in the past
        let state = JoinTokenState {
            token_hash: hash,
            lifecycle: JoinTokenLifecycle::Issued,
            expires_at_ms: 1, // Jan 1, 1970 — long expired
            attempt: 0,
        };
        store.register(state);
        assert_eq!(
            store.begin_inflight(&hash, dummy_addr()).unwrap_err(),
            TokenStateError::Expired
        );
        // State machine must have transitioned to Expired.
        let s = store.get(&hash).unwrap();
        assert_eq!(s.lifecycle, JoinTokenLifecycle::Expired);
    }

    #[test]
    fn aborted_token_rejected() {
        let store = InMemoryTokenStore::new();
        let hash = [0x05u8; 32];
        let mut state = make_state(hash, 60);
        state.lifecycle = JoinTokenLifecycle::Aborted;
        store.register(state);
        assert_eq!(
            store.begin_inflight(&hash, dummy_addr()).unwrap_err(),
            TokenStateError::Aborted
        );
    }

    #[test]
    fn inflight_same_addr_is_idempotent() {
        let store = InMemoryTokenStore::new();
        let hash = [0x06u8; 32];
        store.register(make_state(hash, 60));
        let addr = dummy_addr();
        store.begin_inflight(&hash, addr).unwrap();
        // Same addr: idempotent
        store.begin_inflight(&hash, addr).unwrap();
        let s = store.get(&hash).unwrap();
        // attempt incremented only on the first begin_inflight
        assert_eq!(s.attempt, 1);
    }
}
