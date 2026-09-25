// SPDX-License-Identifier: BUSL-1.1

pub mod applied_gate;
pub mod applied_mirror;
pub mod driver;
pub mod lock;
pub mod metrics;
mod metrics_flow;
pub mod recovery;

pub use applied_gate::AppliedGate;
pub use applied_mirror::{AppliedMirror, AppliedMirrors};
pub use driver::{
    CalvinReadResultProposal, RaftSequencerProposer, ReadResultEvent, Scheduler, SchedulerConfig,
    SchedulerParams, SequencerProposer, propose_calvin_read_result,
};
pub use lock::{AcquireOutcome, HotKeyTable, LockKey, LockManager, LockMode, TxnId};
// Existing call sites reference this module as `scheduler::lock_manager::…`;
// keep that path stable via an alias while the module lives under `lock/`.
pub use lock as lock_manager;
pub use metrics::SchedulerMetrics;
pub use recovery::{AppliedRecovery, NOT_YET_APPLIED_EPOCH, read_applied_recovery};
