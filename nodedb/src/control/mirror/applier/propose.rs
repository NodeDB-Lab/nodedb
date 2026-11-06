// SPDX-License-Identifier: BUSL-1.1

//! The raw commit-wait propose.
//!
//! The usual proposer waits for this node's apply. A mirror's entry applies only
//! after the `MirrorCut` this wait comes before, so that wait never ends. The
//! applier instead waits for the group to commit, and it checks that each index
//! holds the bytes it proposed.
//!
//! Any doubt ends the attempt: a refused propose, an unknown outcome, another
//! entry at an index, or a deadline. Entries can land out of order across a
//! leader change, so an attempt never re-proposes one entry alone. The applier
//! fences a new epoch, every copy of the old epoch skips on every replica, and
//! the new epoch proposes the whole cut again.

use std::time::Duration;

use nodedb_cluster::rpc_codec::{
    LogProbeItem, LogProbeResponse, MAX_LOG_PROBE_ITEMS, log_entry_digest,
};
use tokio::time::Instant;

use crate::control::wal_replication::ProposedAt;

/// How long a wait sleeps between two looks at a group's commit index.
const COMMIT_POLL: Duration = Duration::from_millis(20);

/// The Raft operations the applier needs. `start_raft` installs them.
#[async_trait::async_trait]
pub trait MirrorRaft: Send + Sync {
    /// Propose `data` to `vshard`'s group, forwarding to its leader. Returns
    /// where the entry landed. Landing is not commit.
    async fn propose(
        &self,
        vshard: u32,
        data: Vec<u8>,
        deadline: Instant,
    ) -> crate::Result<ProposedAt>;

    /// Ask a replica of `group` what it committed.
    async fn probe(&self, group: u64, items: Vec<LogProbeItem>) -> crate::Result<LogProbeResponse>;
}

/// The encoded entries of one cut for one mirror group, in proposal order. The
/// last one is the group's seal.
#[derive(Debug)]
pub(super) struct GroupProposal {
    pub group: u64,
    /// Each entry's vShard and its bytes.
    pub entries: Vec<(u32, Vec<u8>)>,
}

/// Where one group's seal landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SealAt {
    pub group: u64,
    pub index: u64,
}

/// Propose one group's entries in order, then wait until the group committed
/// each with the bytes proposed. Returns where the seal landed.
pub(super) async fn propose_group(
    raft: &dyn MirrorRaft,
    proposal: GroupProposal,
    deadline: Instant,
) -> crate::Result<SealAt> {
    let group = proposal.group;
    let mut items = Vec::with_capacity(proposal.entries.len());
    for (vshard, data) in proposal.entries {
        let digest = log_entry_digest(&data);
        let at = raft.propose(vshard, data, deadline).await?;
        if at.group_id != group {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "mirror applier: vShard {vshard} landed in group {}, not {group}",
                    at.group_id
                ),
            });
        }
        items.push(LogProbeItem {
            index: at.log_index,
            digest,
        });
    }
    let Some(seal) = items.last().map(|item| item.index) else {
        return Err(crate::Error::Dispatch {
            detail: format!("mirror applier: group {group} has nothing to propose"),
        });
    };
    await_committed(raft, group, seal, deadline).await?;
    for chunk in items.chunks(MAX_LOG_PROBE_ITEMS) {
        let answer = raft.probe(group, chunk.to_vec()).await?;
        if let Some(index) = answer.mismatch {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "mirror applier: group {group} index {index} holds another entry after a \
                     leader change"
                ),
            });
        }
    }
    Ok(SealAt { group, index: seal })
}

/// Wait until a replica of `group` committed `index`.
async fn await_committed(
    raft: &dyn MirrorRaft,
    group: u64,
    index: u64,
    deadline: Instant,
) -> crate::Result<()> {
    loop {
        match raft.probe(group, Vec::new()).await {
            Ok(answer) if answer.commit_index >= index => return Ok(()),
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(group, index, %error, "mirror applier: a commit probe failed");
            }
        }
        if Instant::now() >= deadline {
            return Err(crate::Error::Dispatch {
                detail: format!(
                    "mirror applier: group {group} did not commit index {index} by the deadline"
                ),
            });
        }
        tokio::time::sleep(COMMIT_POLL).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use nodedb_cluster::rpc_codec::log_entry_digest;

    use super::*;

    const GROUP: u64 = 4;

    /// A group whose log and commit index the test sets.
    #[derive(Default)]
    struct FakeRaft {
        log: Mutex<Vec<Vec<u8>>>,
        /// Entries a new leader placed over what was proposed, by index.
        overwritten: HashMap<u64, Vec<u8>>,
        refuse_at: Option<usize>,
        commit_stalls: bool,
    }

    #[async_trait::async_trait]
    impl MirrorRaft for FakeRaft {
        async fn propose(
            &self,
            _vshard: u32,
            data: Vec<u8>,
            _deadline: Instant,
        ) -> crate::Result<ProposedAt> {
            let mut log = self.log.lock().expect("log");
            if self.refuse_at == Some(log.len()) {
                return Err(crate::Error::Dispatch {
                    detail: "no leader".to_string(),
                });
            }
            log.push(data);
            Ok(ProposedAt {
                group_id: GROUP,
                log_index: log.len() as u64,
            })
        }

        async fn probe(
            &self,
            _group: u64,
            items: Vec<LogProbeItem>,
        ) -> crate::Result<LogProbeResponse> {
            let log = self.log.lock().expect("log");
            let held = |index: u64| {
                self.overwritten
                    .get(&index)
                    .or_else(|| log.get(index as usize - 1))
                    .map(|data| log_entry_digest(data))
            };
            let mismatch = items
                .iter()
                .find(|item| held(item.index) != Some(item.digest))
                .map(|item| item.index);
            Ok(LogProbeResponse {
                hosted: true,
                commit_index: if self.commit_stalls {
                    0
                } else {
                    log.len() as u64
                },
                durable_applied: 0,
                mismatch,
            })
        }
    }

    fn proposal() -> GroupProposal {
        GroupProposal {
            group: GROUP,
            entries: vec![
                (1, b"a".to_vec()),
                (1, b"b".to_vec()),
                (1, b"seal".to_vec()),
            ],
        }
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_millis(200)
    }

    #[tokio::test]
    async fn every_entry_lands_in_order_and_the_seal_is_named() {
        let raft = FakeRaft::default();
        let seal = propose_group(&raft, proposal(), soon())
            .await
            .expect("committed");
        assert_eq!(
            seal,
            SealAt {
                group: GROUP,
                index: 3
            }
        );
        let log = raft.log.lock().expect("log");
        assert_eq!(log[0], b"a");
        assert_eq!(log[2], b"seal");
    }

    /// The new leader kept the later entries and lost an earlier one, so the
    /// log order is no longer the source's.
    #[tokio::test]
    async fn an_entry_a_new_leader_replaced_ends_the_attempt() {
        let raft = FakeRaft {
            overwritten: HashMap::from([(1, b"no-op".to_vec())]),
            ..FakeRaft::default()
        };
        let error = propose_group(&raft, proposal(), soon())
            .await
            .expect_err("a mismatch");
        assert!(error.to_string().contains("index 1"), "{error}");
    }

    #[tokio::test]
    async fn a_refused_propose_ends_the_attempt_with_the_rest_unproposed() {
        let raft = FakeRaft {
            refuse_at: Some(1),
            ..FakeRaft::default()
        };
        propose_group(&raft, proposal(), soon())
            .await
            .expect_err("refused");
        assert_eq!(raft.log.lock().expect("log").len(), 1);
    }

    #[tokio::test]
    async fn a_commit_that_never_comes_ends_at_the_deadline() {
        let raft = FakeRaft {
            commit_stalls: true,
            ..FakeRaft::default()
        };
        let error = propose_group(&raft, proposal(), soon())
            .await
            .expect_err("a timeout");
        assert!(error.to_string().contains("did not commit"), "{error}");
    }
}
