// SPDX-License-Identifier: BUSL-1.1

//! The Raft operations a mirror's applier uses, on this node's Raft loop.

use std::sync::{Arc, Weak};

use nodedb_cluster::rpc_codec::{LogProbeItem, LogProbeResponse};
use tokio::time::Instant;

use crate::control::mirror::applier::MirrorRaft;
use crate::control::state::SharedState;
use crate::control::wal_replication::ProposedAt;

use super::loop_build::RaftLoopType;
use super::propose_error::async_propose_error;

/// Weak: `SharedState` holds this, and the loop's tasks keep the loop alive.
struct LoopMirrorRaft {
    raft_loop: Weak<RaftLoopType>,
}

impl LoopMirrorRaft {
    fn live(&self) -> crate::Result<Arc<RaftLoopType>> {
        self.raft_loop
            .upgrade()
            .ok_or_else(|| crate::Error::Internal {
                detail: "mirror applier: the cluster is not running".to_string(),
            })
    }
}

#[async_trait::async_trait]
impl MirrorRaft for LoopMirrorRaft {
    async fn propose(
        &self,
        vshard: u32,
        data: Vec<u8>,
        deadline: Instant,
    ) -> crate::Result<ProposedAt> {
        let (group_id, log_index) = self
            .live()?
            .propose_via_data_leader(vshard, &data, deadline)
            .await
            .map_err(|error| async_propose_error(vshard, error))?;
        Ok(ProposedAt {
            group_id,
            log_index,
        })
    }

    async fn probe(&self, group: u64, items: Vec<LogProbeItem>) -> crate::Result<LogProbeResponse> {
        self.live()?
            .probe_group_log(group, items)
            .await
            .map_err(|error| crate::Error::Dispatch {
                detail: format!("mirror applier: a log probe of group {group}: {error}"),
            })
    }
}

/// Give this node's mirror appliers their Raft operations.
pub(super) fn install_mirror_raft(shared: &Arc<SharedState>, raft_loop: &Arc<RaftLoopType>) {
    let raft: Arc<dyn MirrorRaft> = Arc::new(LoopMirrorRaft {
        raft_loop: Arc::downgrade(raft_loop),
    });
    if shared.mirror.raft.set(raft).is_err() {
        tracing::warn!("the mirror's Raft operations already set: start_raft ran twice");
    }
}
