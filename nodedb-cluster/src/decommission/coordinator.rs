//! `DecommissionCoordinator` — drives a [`DecommissionPlan`] through
//! the metadata Raft group one entry at a time.
//!
//! The coordinator is a stateless-looking actor: it owns the plan,
//! a [`MetadataProposer`] (the injection seam for tests and for
//! whichever Raft driver is wired up at runtime), and an index
//! counter. On every call to [`DecommissionCoordinator::run`] it
//! proposes each entry in order, waiting for each to commit before
//! advancing. A proposer failure aborts the run at the failed step —
//! the caller can retry by constructing a fresh coordinator from
//! the same plan, because every step is idempotent at the metadata
//! layer (the cache and live-state appliers skip already-applied
//! indexes).
//!
//! The coordinator does not own a timer or a shutdown channel — it
//! is a one-shot sequence. Higher-level supervisors handle retries
//! and cancellation.

use async_trait::async_trait;
use tracing::{debug, info};

use crate::error::Result;
use crate::metadata_group::MetadataEntry;

use super::flow::DecommissionPlan;

/// Injection seam: proposes a single metadata entry through the
/// metadata Raft group and waits for it to commit. Returns the
/// applied index on success so the coordinator can tell it apart
/// from older commits.
#[async_trait]
pub trait MetadataProposer: Send + Sync {
    async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64>;
}

// Blanket impl so callers can pass `Arc<T>` wherever a `MetadataProposer`
// is required without having to write a forwarding impl for every
// wrapper type. Defined here (rather than in the consumer crate) to
// avoid orphan-rule issues for downstream test impls.
#[async_trait]
impl<T: MetadataProposer + ?Sized> MetadataProposer for std::sync::Arc<T> {
    async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64> {
        (**self).propose_and_wait(entry).await
    }
}

/// Drives a [`DecommissionPlan`] to completion.
pub struct DecommissionCoordinator<P: MetadataProposer> {
    plan: DecommissionPlan,
    proposer: P,
}

/// Outcome of a successful coordinator run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecommissionRunResult {
    pub node_id: u64,
    pub entries_committed: usize,
    pub last_applied_index: u64,
}

impl<P: MetadataProposer> DecommissionCoordinator<P> {
    pub fn new(plan: DecommissionPlan, proposer: P) -> Self {
        Self { plan, proposer }
    }

    /// Propose every entry in the plan sequentially, waiting for
    /// each commit. Returns the total number of entries committed
    /// and the final applied index.
    pub async fn run(self) -> Result<DecommissionRunResult> {
        let node_id = self.plan.node_id;
        let total = self.plan.entries.len();
        info!(node_id, steps = total, "decommission coordinator starting");
        let mut last_applied = 0u64;
        for (step, entry) in self.plan.entries.into_iter().enumerate() {
            debug!(node_id, step, total, "proposing decommission entry");
            last_applied = self.proposer.propose_and_wait(entry).await?;
        }
        info!(
            node_id,
            entries_committed = total,
            last_applied,
            "decommission coordinator finished"
        );
        Ok(DecommissionRunResult {
            node_id,
            entries_committed: total,
            last_applied_index: last_applied,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decommission::flow::plan_full_decommission;
    use crate::error::ClusterError;
    use crate::metadata_group::{RoutingChange, TopologyChange};
    use crate::routing::RoutingTable;
    use crate::topology::{ClusterTopology, NodeInfo, NodeState};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    struct RecordingProposer {
        committed: Mutex<Vec<MetadataEntry>>,
        counter: AtomicU64,
    }

    impl RecordingProposer {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                committed: Mutex::new(Vec::new()),
                counter: AtomicU64::new(0),
            })
        }
    }

    #[async_trait]
    impl MetadataProposer for RecordingProposer {
        async fn propose_and_wait(&self, entry: MetadataEntry) -> Result<u64> {
            let idx = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
            self.committed.lock().unwrap().push(entry);
            Ok(idx)
        }
    }

    struct FailingProposer {
        fail_after: usize,
        counter: AtomicU64,
    }

    #[async_trait]
    impl MetadataProposer for FailingProposer {
        async fn propose_and_wait(&self, _entry: MetadataEntry) -> Result<u64> {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            if n as usize >= self.fail_after {
                return Err(ClusterError::Transport {
                    detail: "injected failure".into(),
                });
            }
            Ok(n + 1)
        }
    }

    fn three_node_plan() -> DecommissionPlan {
        let mut t = ClusterTopology::new();
        for (i, id) in [1u64, 2, 3].iter().enumerate() {
            let a: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            t.add_node(NodeInfo::new(*id, a, NodeState::Active));
        }
        let routing = RoutingTable::uniform(2, &[1, 2, 3], 3);
        plan_full_decommission(1, &t, &routing, 2).unwrap()
    }

    #[tokio::test]
    async fn coordinator_proposes_every_entry_in_order() {
        let plan = three_node_plan();
        let expected = plan.entries.clone();
        let proposer = RecordingProposer::new();
        let coord = DecommissionCoordinator::new(plan, proposer.clone());
        let result = coord.run().await.unwrap();

        assert_eq!(result.node_id, 1);
        assert_eq!(result.entries_committed, expected.len());
        let committed = proposer.committed.lock().unwrap().clone();
        assert_eq!(committed, expected);
    }

    #[tokio::test]
    async fn coordinator_aborts_on_proposer_error() {
        let plan = three_node_plan();
        let proposer = FailingProposer {
            fail_after: 2,
            counter: AtomicU64::new(0),
        };
        let coord = DecommissionCoordinator::new(plan, proposer);
        let err = coord.run().await.unwrap_err();
        assert!(err.to_string().contains("injected failure"));
    }

    #[tokio::test]
    async fn coordinator_reports_last_applied_index() {
        let plan = three_node_plan();
        let proposer = RecordingProposer::new();
        let coord = DecommissionCoordinator::new(plan, proposer.clone());
        let result = coord.run().await.unwrap();
        // The recording proposer returns monotonically increasing
        // indexes starting from 1; the last one equals the total
        // entry count.
        assert_eq!(result.last_applied_index, result.entries_committed as u64);
    }

    /// Sanity: the plan's shape is preserved end to end — the
    /// recording proposer sees the same `StartDecommission` /
    /// `FinishDecommission` / `Leave` bookends.
    #[tokio::test]
    async fn coordinator_preserves_bookends() {
        let plan = three_node_plan();
        let proposer = RecordingProposer::new();
        let coord = DecommissionCoordinator::new(plan, proposer.clone());
        coord.run().await.unwrap();

        let committed = proposer.committed.lock().unwrap().clone();
        assert!(matches!(
            committed.first(),
            Some(MetadataEntry::TopologyChange(
                TopologyChange::StartDecommission { node_id: 1 }
            ))
        ));
        assert!(matches!(
            committed.last(),
            Some(MetadataEntry::TopologyChange(TopologyChange::Leave {
                node_id: 1
            }))
        ));
        // At least one RemoveMember for the target.
        assert!(committed.iter().any(|e| matches!(
            e,
            MetadataEntry::RoutingChange(RoutingChange::RemoveMember { node_id: 1, .. })
        )));
    }
}
