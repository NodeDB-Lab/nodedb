// SPDX-License-Identifier: BUSL-1.1

//! Ask what a group committed, from this node's replica or from a member's.

use crate::error::{ClusterError, Result};
use crate::forward::PlanExecutor;
use crate::multi_raft::LogProbeView;
use crate::rpc_codec::{
    LogProbeItem, LogProbeRequest, LogProbeResponse, MAX_LOG_PROBE_ITEMS, RaftRpc, log_entry_digest,
};

use super::loop_core::{CommitApplier, RaftLoop};

/// Compare `items` against what a replica's log holds. An index above the
/// replica's commit index waits for a later probe.
fn judge(view: &LogProbeView, items: &[LogProbeItem]) -> LogProbeResponse {
    let mismatch = items
        .iter()
        .filter(|item| item.index <= view.commit_index)
        .find(|item| {
            let held = view.entries.iter().find(|entry| entry.index == item.index);
            held.is_none_or(|entry| log_entry_digest(&entry.data) != item.digest)
        })
        .map(|item| item.index);
    LogProbeResponse {
        hosted: true,
        commit_index: view.commit_index,
        durable_applied: view.durable_applied,
        mismatch,
    }
}

impl<A: CommitApplier, P: PlanExecutor> RaftLoop<A, P> {
    /// This node's answer about `group_id`. The digests run outside the Raft
    /// lock: the view holds its own copy of the entries.
    fn probe_local(&self, group_id: u64, items: &[LogProbeItem]) -> LogProbeResponse {
        let lo = items.iter().map(|item| item.index).min();
        let hi = items.iter().map(|item| item.index).max();
        let view = self
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .log_probe_view(group_id, lo.zip(hi));
        view.map_or_else(LogProbeResponse::not_hosted, |view| judge(&view, items))
    }

    /// Answer a member's probe of a group this node can host.
    pub(super) fn handle_log_probe_rpc(&self, req: LogProbeRequest) -> Result<RaftRpc> {
        if req.items.len() > MAX_LOG_PROBE_ITEMS {
            return Err(ClusterError::Codec {
                detail: format!(
                    "a log probe of group {} names {} entries, past the bound of {MAX_LOG_PROBE_ITEMS}",
                    req.group_id,
                    req.items.len()
                ),
            });
        }
        Ok(RaftRpc::LogProbeResponse(
            self.probe_local(req.group_id, &req.items),
        ))
    }

    /// Ask this node's replica of `group_id`, or the first member that answers.
    ///
    /// No member reachable fails with [`ClusterError::Transport`], so the caller
    /// knows nothing about the entries and asks again.
    pub async fn probe_group_log(
        &self,
        group_id: u64,
        items: Vec<LogProbeItem>,
    ) -> Result<LogProbeResponse> {
        let local = self.probe_local(group_id, &items);
        if local.hosted {
            return Ok(local);
        }
        let members = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            let routing = mr.routing();
            let table = routing.read().unwrap_or_else(|p| p.into_inner());
            table
                .group_info(group_id)
                .map_or_else(Vec::new, |info| info.members.clone())
        };
        let mut last_error = None;
        for member in members.into_iter().filter(|member| *member != self.node_id) {
            if let Err(error) = self.register_peer_addr(member) {
                last_error = Some(error);
                continue;
            }
            let request = RaftRpc::LogProbeRequest(LogProbeRequest {
                group_id,
                items: items.clone(),
            });
            match self.transport.send_rpc(member, request).await {
                Ok(RaftRpc::LogProbeResponse(answer)) if answer.hosted => return Ok(answer),
                Ok(RaftRpc::LogProbeResponse(_)) => {}
                Ok(other) => {
                    last_error = Some(ClusterError::Transport {
                        detail: format!("log probe: unexpected response variant {other:?}"),
                    });
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| ClusterError::Transport {
            detail: format!("log probe: no member of group {group_id} hosts a replica"),
        }))
    }
}

#[cfg(test)]
mod tests {
    use nodedb_raft::message::LogEntry;

    use super::*;

    fn view(commit_index: u64, entries: &[(u64, &[u8])]) -> LogProbeView {
        LogProbeView {
            commit_index,
            durable_applied: 1,
            entries: entries
                .iter()
                .map(|(index, data)| LogEntry {
                    term: 1,
                    index: *index,
                    data: data.to_vec(),
                })
                .collect(),
        }
    }

    fn item(index: u64, data: &[u8]) -> LogProbeItem {
        LogProbeItem {
            index,
            digest: log_entry_digest(data),
        }
    }

    #[test]
    fn matching_committed_entries_report_no_mismatch() {
        let answer = judge(
            &view(5, &[(4, b"a"), (5, b"b")]),
            &[item(4, b"a"), item(5, b"b")],
        );
        assert!(answer.hosted);
        assert_eq!(answer.commit_index, 5);
        assert_eq!(answer.mismatch, None);
    }

    /// A new leader placed another entry at the index.
    #[test]
    fn an_overwritten_entry_is_a_mismatch() {
        let answer = judge(
            &view(5, &[(4, b"a"), (5, b"other")]),
            &[item(4, b"a"), item(5, b"b")],
        );
        assert_eq!(answer.mismatch, Some(5));
    }

    /// The replica has not committed the index, so it says nothing yet.
    #[test]
    fn an_entry_above_the_commit_index_waits() {
        let answer = judge(&view(4, &[(4, b"a")]), &[item(4, b"a"), item(5, b"b")]);
        assert_eq!(answer.mismatch, None);
        assert_eq!(answer.commit_index, 4);
    }

    /// The replica compacted the index, so it cannot vouch for the bytes.
    #[test]
    fn a_compacted_entry_is_a_mismatch() {
        let answer = judge(&view(9, &[]), &[item(4, b"a")]);
        assert_eq!(answer.mismatch, Some(4));
    }
}
