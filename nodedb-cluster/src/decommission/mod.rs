//! Decommission flow — graceful removal of a node from the cluster.
//!
//! Decommission is a multi-step, metadata-raft-replicated process:
//!
//! 1. **Safety gate** — [`safety::check_can_decommission`] refuses the
//!    decommission if any Raft group the target is in would drop below
//!    the configured replication factor after its removal. This is
//!    the only correctness-critical check — once it passes, every
//!    subsequent step is just routing/topology bookkeeping.
//! 2. **Plan** — [`flow::plan_full_decommission`] emits the full ordered
//!    sequence of [`MetadataEntry`](crate::metadata_group::MetadataEntry)
//!    values the coordinator will propose: `StartDecommission`, any
//!    required leadership transfers, a `RemoveMember` per group, then
//!    `FinishDecommission` and `Leave`.
//! 3. **Propose** (future batch: `coordinator.rs`) — stateful actor
//!    proposes each entry in order through a `MetadataProposer` trait,
//!    waiting for the applied index to advance past each commit before
//!    advancing its own state.
//! 4. **Observe** (future batch: `observer.rs`) — the target node
//!    watches its own topology state and fires a cooperative shutdown
//!    signal when it transitions to `Decommissioned`.
//!
//! This sub-batch ships steps 1 and 2 as pure, side-effect-free
//! functions so the flow can be exhaustively unit-tested before the
//! stateful coordinator is wired up.

pub mod coordinator;
pub mod flow;
pub mod observer;
pub mod safety;

pub use coordinator::{DecommissionCoordinator, DecommissionRunResult, MetadataProposer};
pub use flow::{DecommissionPlan, plan_full_decommission};
pub use observer::DecommissionObserver;
pub use safety::{DecommissionSafetyError, check_can_decommission};
