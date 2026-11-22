// SPDX-License-Identifier: BUSL-1.1

//! The one process-wide allocator of client connection ids.
//!
//! pgwire, native and WebSocket RPC connections take one id each. A plain
//! HTTP request takes one id of its own. One allocator keeps every id
//! distinct across protocols, so a request stream names one client.

use std::sync::atomic::{AtomicU64, Ordering};

use super::connection::ConnectionId;

/// Failure to allocate a non-wrapping connection identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionAllocationError {
    #[error("connection identifier space exhausted")]
    Exhausted,
}

/// Allocator that never wraps into a previously issued id.
pub struct ConnectionIdAllocator {
    next: AtomicU64,
}

impl ConnectionIdAllocator {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// A fresh id, distinct from every id this allocator issued.
    pub fn allocate(&self) -> Result<ConnectionId, ConnectionAllocationError> {
        let id = self
            .next
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ConnectionAllocationError::Exhausted)?;
        ConnectionId::new(id).map_err(|_| ConnectionAllocationError::Exhausted)
    }
}

impl Default for ConnectionIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_is_nonzero_and_unique() {
        let allocator = ConnectionIdAllocator::new();
        assert_eq!(allocator.allocate().unwrap().get(), 1);
        assert_eq!(allocator.allocate().unwrap().get(), 2);
    }

    /// pgwire and native accepts both draw from the allocator on
    /// `SharedState`. Interleaved accepts of the two protocols never share
    /// an id.
    #[tokio::test]
    async fn pgwire_and_native_connections_draw_distinct_ids() {
        let directory = tempfile::tempdir().expect("temporary WAL directory");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&directory.path().join("ids.wal"))
                .expect("test WAL"),
        );
        let (dispatcher, _sides) = crate::bridge::dispatch::Dispatcher::new(1, 8);
        let state = crate::control::state::SharedState::new(dispatcher, wal).expect("state");
        let mut pgwire = Vec::new();
        let mut native = Vec::new();
        for _ in 0..4 {
            pgwire.push(state.connection_ids.allocate().expect("pgwire id"));
            native.push(state.connection_ids.allocate().expect("native id"));
        }
        assert!(
            pgwire.iter().all(|id| !native.contains(id)),
            "a pgwire id never names a native connection"
        );
        let mut all: Vec<_> = pgwire.iter().chain(&native).copied().collect();
        all.sort();
        all.dedup();
        assert_eq!(all.len(), 8, "every connection id is distinct");
    }

    #[test]
    fn allocation_refuses_to_wrap() {
        let allocator = ConnectionIdAllocator {
            next: AtomicU64::new(u64::MAX),
        };
        assert_eq!(
            allocator.allocate(),
            Err(ConnectionAllocationError::Exhausted)
        );
    }
}
