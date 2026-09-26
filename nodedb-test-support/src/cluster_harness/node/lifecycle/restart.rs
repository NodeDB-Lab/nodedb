// SPDX-License-Identifier: BUSL-1.1

//! In-place restart of a [`TestClusterNode`]: the same node id, the same QUIC
//! listen address, and the same data directory.

use std::net::SocketAddr;
use std::time::Duration;

use crate::cluster_harness::cluster::ClusterSpawnConfig;

use super::types::{DataDir, TestClusterNode};

/// A node stopped for an in-place restart. It owns the node's data
/// directory, which outlives the stopped node.
pub(crate) struct StoppedNode {
    node_id: u64,
    listen_addr: SocketAddr,
    data_dir: tempfile::TempDir,
}

impl StoppedNode {
    /// The QUIC address the node listened on, which its peers still hold.
    pub(crate) fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }
}

impl TestClusterNode {
    /// Stop the node, flush its WAL and release every file handle, keeping
    /// its data directory for [`Self::restart`].
    pub(crate) async fn stop_for_restart(
        mut self,
    ) -> Result<StoppedNode, Box<dyn std::error::Error + Send + Sync>> {
        let DataDir::Owned(data_dir) = std::mem::replace(&mut self._data_dir, DataDir::Borrowed)
        else {
            return Err(format!(
                "node {} runs on a caller-supplied data directory, which an in-place \
                 restart does not own",
                self.node_id
            )
            .into());
        };
        let (node_id, listen_addr) = (self.node_id, self.listen_addr);
        self.graceful_shutdown_wal_only().await;
        await_port_released(node_id, listen_addr).await?;
        Ok(StoppedNode {
            node_id,
            listen_addr,
            data_dir,
        })
    }

    /// Bring `stopped` back on its node id, listen address and data. Its
    /// persisted cluster state makes it rejoin as the same member.
    pub(crate) async fn restart(
        stopped: StoppedNode,
        seed_nodes: Vec<SocketAddr>,
        config: &ClusterSpawnConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let StoppedNode {
            node_id,
            listen_addr,
            data_dir,
        } = stopped;
        let mut node = Self::spawn_with_full_config_at(
            node_id,
            seed_nodes,
            config,
            Some(data_dir.path().to_path_buf()),
            Some(listen_addr),
        )
        .await?;
        node._data_dir = DataDir::Owned(data_dir);
        Ok(node)
    }
}

/// Wait until `listen_addr`'s UDP port is free: the stopped node's QUIC
/// endpoint releases its socket once its last handle dropped, after the
/// close drained every connection.
async fn await_port_released(
    node_id: u64,
    listen_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match tokio::net::UdpSocket::bind(listen_addr).await {
            Ok(probe) => {
                drop(probe);
                return Ok(());
            }
            Err(error) if tokio::time::Instant::now() >= deadline => {
                return Err(format!(
                    "node {node_id} stopped, but its QUIC port {listen_addr} is still bound \
                     after 10s ({error}): a task still holds the node's transport"
                )
                .into());
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
}
