// SPDX-License-Identifier: BUSL-1.1

use std::time::Duration;

use tracing::{debug, info};

use crate::conf_change::{ConfChange, ConfChangeType};
use crate::error::{ClusterError, Result};
use crate::ghost::GhostStub;
use crate::metadata_group::migration_state::{MigrationCheckpointPayload, MigrationId};
use crate::metadata_group::{MetadataEntry as Entry, RoutingChange};
use crate::migration::MigrationState;

use super::executor::{MigrationExecutor, MigrationRequest};

pub(super) async fn phase1_base_copy(
    ex: &MigrationExecutor,
    state: &mut MigrationState,
    group_id: u64,
    req: &MigrationRequest,
    migration_id: MigrationId,
) -> Result<()> {
    let committed = {
        let mr = ex.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_statuses()
            .iter()
            .find(|s| s.group_id == group_id)
            .map(|s| s.commit_index)
            .unwrap_or(0)
    };
    state.start_base_copy(committed);

    ex.propose_checkpoint(
        migration_id,
        0,
        MigrationCheckpointPayload::AddLearner {
            vshard_id: req.vshard_id,
            source_node: req.source_node,
            target_node: req.target_node,
            source_group: group_id,
            write_pause_budget_us: req.write_pause_budget_us,
            started_at_hlc: nodedb_types::Hlc::default(),
        },
    )
    .await?;

    info!(
        vshard = req.vshard_id,
        group = group_id,
        target = req.target_node,
        entries = committed,
        "phase 1: adding target to raft group"
    );

    let change = ConfChange {
        change_type: ConfChangeType::AddLearner,
        node_id: req.target_node,
    };
    let learner_log_index = {
        let mut mr = ex.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_conf_change(group_id, &change)?;
        mr.group_statuses()
            .iter()
            .find(|s| s.group_id == group_id)
            .map(|s| s.commit_index)
            .unwrap_or(committed)
    };

    if let Some(node_info) = {
        let topo = ex.topology.read().unwrap_or_else(|p| p.into_inner());
        topo.get_node(req.target_node).map(|n| n.addr.clone())
    } && let Ok(addr) = node_info.parse()
    {
        ex.transport.register_peer(req.target_node, addr);
    }

    state.update_base_copy(committed);

    ex.propose_checkpoint(
        migration_id,
        1,
        MigrationCheckpointPayload::CatchUp {
            vshard_id: req.vshard_id,
            learner_log_index_at_add: learner_log_index,
        },
    )
    .await?;

    debug!(
        vshard = req.vshard_id,
        "phase 1 complete: target added as learner"
    );
    Ok(())
}

pub(super) async fn phase2_wal_catchup(
    ex: &MigrationExecutor,
    state: &mut MigrationState,
    group_id: u64,
    req: &MigrationRequest,
    migration_id: MigrationId,
) -> Result<()> {
    let leader_commit = {
        let mr = ex.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.group_statuses()
            .iter()
            .find(|s| s.group_id == group_id)
            .map(|s| s.commit_index)
            .unwrap_or(0)
    };
    state.start_wal_catchup(leader_commit, leader_commit);

    info!(
        vshard = req.vshard_id,
        leader_commit, "phase 2: monitoring replication lag"
    );

    let initial_stable_id = ex.transport.peer_connection_stable_id(req.target_node);
    let initial_target_addr = {
        let topo = ex.topology.read().unwrap_or_else(|p| p.into_inner());
        topo.get_node(req.target_node).map(|n| n.addr.clone())
    };

    let poll_interval = Duration::from_millis(100);
    let timeout = Duration::from_secs(60);
    let deadline = std::time::Instant::now() + timeout;

    loop {
        tokio::time::sleep(poll_interval).await;

        if let Some(initial_id) = initial_stable_id {
            match ex.transport.peer_connection_stable_id(req.target_node) {
                Some(current_id) if current_id != initial_id => {
                    let reason = format!(
                        "peer identity changed mid-migration: stable_id {} -> {} for node {}",
                        initial_id, current_id, req.target_node
                    );
                    state.fail(reason.clone());
                    return Err(ClusterError::Transport { detail: reason });
                }
                None => {
                    let reason = format!(
                        "connection to target node {} lost during migration",
                        req.target_node
                    );
                    state.fail(reason.clone());
                    return Err(ClusterError::Transport { detail: reason });
                }
                _ => {}
            }
        }

        {
            let topo = ex.topology.read().unwrap_or_else(|p| p.into_inner());
            let current_addr = topo.get_node(req.target_node).map(|n| n.addr.clone());
            if current_addr != initial_target_addr {
                let reason = format!(
                    "target node {} address changed: {:?} -> {:?}",
                    req.target_node, initial_target_addr, current_addr
                );
                state.fail(reason.clone());
                return Err(ClusterError::Transport { detail: reason });
            }
        }

        let (leader_commit, target_match) = {
            let mr = ex.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let statuses = mr.group_statuses();
            let commit = statuses
                .iter()
                .find(|s| s.group_id == group_id)
                .map(|s| s.commit_index)
                .unwrap_or(0);
            let target_match = mr.match_index_for(group_id, req.target_node).unwrap_or(0);
            (commit, target_match)
        };
        state.update_wal_catchup(leader_commit, target_match);

        if state.is_catchup_ready() {
            let promote = ConfChange {
                change_type: ConfChangeType::PromoteLearner,
                node_id: req.target_node,
            };
            {
                let mut mr = ex.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
                mr.propose_conf_change(group_id, &promote)?;
            }

            ex.propose_checkpoint(
                migration_id,
                2,
                MigrationCheckpointPayload::PromoteLearner {
                    vshard_id: req.vshard_id,
                    target_node: req.target_node,
                    source_group: group_id,
                },
            )
            .await?;

            debug!(
                vshard = req.vshard_id,
                leader_commit, target_match, "phase 2 complete: target caught up and promoted"
            );
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            let reason = format!(
                "WAL catch-up timed out after {}s (leader={leader_commit}, target={target_match})",
                timeout.as_secs()
            );
            state.fail(reason.clone());
            return Err(ClusterError::Transport { detail: reason });
        }
    }
}

pub(super) async fn phase3_cutover(
    ex: &MigrationExecutor,
    state: &mut MigrationState,
    group_id: u64,
    req: &MigrationRequest,
    migration_id: MigrationId,
) -> Result<()> {
    let estimated_pause_us = 10_000;

    state.start_cutover(estimated_pause_us).map_err(|e| {
        state.fail(format!("cutover rejected: {e}"));
        e
    })?;

    let cutover_start = std::time::Instant::now();

    ex.propose_checkpoint(
        migration_id,
        3,
        MigrationCheckpointPayload::LeadershipTransfer {
            vshard_id: req.vshard_id,
            target_is_voter: true,
            new_leader_node_id: req.target_node,
            source_group: group_id,
        },
    )
    .await?;

    info!(
        vshard = req.vshard_id,
        estimated_pause_us, "phase 3: atomic cut-over"
    );

    if let Some(proposer) = &ex.metadata_proposer {
        let entry = Entry::RoutingChange(RoutingChange::LeadershipTransfer {
            group_id,
            new_leader_node_id: req.target_node,
        });
        proposer.propose_and_wait(entry).await?;
    } else {
        let mut routing = ex.routing.write().unwrap_or_else(|p| p.into_inner());
        // The target is named before its election, so the hint carries no
        // term and fills only a hint that holds none.
        routing.set_leader(group_id, req.target_node);
    }

    ex.propose_checkpoint(
        migration_id,
        4,
        MigrationCheckpointPayload::Cutover {
            vshard_id: req.vshard_id,
            new_leader_node_id: req.target_node,
            source_group: group_id,
        },
    )
    .await?;

    let ghost_stub = GhostStub {
        node_id: format!("vshard-{}", req.vshard_id),
        target_shard: req.vshard_id,
        refcount: 1,
        created_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    };
    {
        let mut ghosts = ex.ghost_table.lock().unwrap_or_else(|p| p.into_inner());
        ghosts.insert(ghost_stub);

        if let Some(catalog) = &ex.catalog {
            catalog.save_ghosts(req.vshard_id, &ghosts)?;
        }
    }

    let actual_pause_us = cutover_start.elapsed().as_micros() as u64;
    state.complete(actual_pause_us);

    ex.propose_checkpoint(
        migration_id,
        5,
        MigrationCheckpointPayload::Complete {
            vshard_id: req.vshard_id,
            actual_pause_us,
            ghost_stub_installed: true,
        },
    )
    .await?;

    if let Some(state_table) = &ex.migration_state {
        let mut guard = state_table.lock().unwrap_or_else(|p| p.into_inner());
        let _ = guard.remove(&migration_id);
    }

    debug!(
        vshard = req.vshard_id,
        actual_pause_us, "phase 3 complete: routing updated"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration_executor::executor::MigrationExecutor;
    use crate::routing::RoutingTable;
    use crate::topology::ClusterTopology;
    use std::sync::{Arc, Mutex, RwLock};
    use uuid::Uuid;

    #[tokio::test]
    async fn migration_executor_phase1() {
        let dir = tempfile::tempdir().unwrap();
        let rt = RoutingTable::uniform(1, &[1], 1);
        let mut mr = crate::multi_raft::MultiRaft::new(1, rt.clone(), dir.path().to_path_buf());
        mr.add_group(0, vec![]).unwrap();

        use std::time::Instant;
        for node in mr.groups_mut().values_mut() {
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        mr.tick().unwrap();
        // The no-op commits once the disk holds it.
        mr.wait_all_durable_blocking().unwrap();
        for (gid, ready) in mr.tick().unwrap().groups {
            if let Some(last) = ready.committed_entries.last() {
                mr.advance_applied(gid, last.index).unwrap();
            }
        }

        let multi_raft = Arc::new(Mutex::new(mr));
        let routing = Arc::new(RwLock::new(rt));
        let topology = Arc::new(RwLock::new(ClusterTopology::new()));
        let transport = Arc::new(
            crate::transport::NexarTransport::new(
                1,
                "127.0.0.1:0".parse().unwrap(),
                crate::transport::credentials::TransportCredentials::Insecure,
            )
            .unwrap(),
        );

        let executor = MigrationExecutor::new(multi_raft.clone(), routing, topology, transport);

        let mut state = crate::migration::MigrationState::new(0, 0, 0, 1, 2, 500_000);
        let req = MigrationRequest {
            vshard_id: 0,
            source_node: 1,
            target_node: 2,
            write_pause_budget_us: 500_000,
        };

        phase1_base_copy(&executor, &mut state, 0, &req, Uuid::new_v4())
            .await
            .unwrap();
    }
}
