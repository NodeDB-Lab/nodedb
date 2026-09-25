// SPDX-License-Identifier: BUSL-1.1

//! Sever this node from chosen peers.
//!
//! A severed peer receives no RPC from this node: every send to it fails at
//! once with a transport error, as if the network dropped it. Severing each
//! side from the other models a network partition between them, which is
//! how tests show that a partitioned node loses its leases and catches up
//! once the partition heals.

use crate::error::{ClusterError, Result};

use super::transport::NexarTransport;

impl NexarTransport {
    /// Stop sending to `peer` until [`Self::heal`] is called.
    pub fn sever(&self, peer: u64) {
        self.severed
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(peer);
    }

    /// Resume sending to `peer`.
    pub fn heal(&self, peer: u64) {
        self.severed
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&peer);
    }

    /// Refuse a send to a severed peer.
    pub(super) fn check_not_severed(&self, target: u64) -> Result<()> {
        if self
            .severed
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&target)
        {
            return Err(ClusterError::Transport {
                detail: format!("node {} is severed from node {target}", self.node_id),
            });
        }
        Ok(())
    }
}
