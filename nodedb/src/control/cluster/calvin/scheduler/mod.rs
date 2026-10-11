// SPDX-License-Identifier: BUSL-1.1

pub mod applied_gate;
pub mod applied_ledger;
pub mod barrier_log;
pub mod barrier_store;
pub mod caught_up;
pub mod cut_floor;
pub mod cut_hold;
pub mod driver;
pub mod inbox;
pub mod lock;
pub mod metrics;
mod metrics_flow;
pub mod read_results;
pub mod recovery;

pub use applied_gate::AppliedGate;
pub use applied_ledger::{CalvinAppliedLedger, CalvinAppliedLedgers, ClaimRefusal};
pub use barrier_log::{BarrierEvent, BarrierLog, BarrierOutcome, ReceivedReads};
pub use caught_up::{CaughtUpHandle, CaughtUpRegistry};
pub use driver::{
    CalvinReadResultProposal, RaftSequencerProposer, Scheduler, SchedulerConfig, SchedulerParams,
    SequencerProposer, propose_calvin_read_result,
};
pub use inbox::{CalvinApplyEvent, CalvinInboxes, CalvinVShardInbox, InboxHandle};
pub use lock::{AcquireOutcome, HotKeyTable, LockKey, LockManager, LockMode, TxnId};
// Existing call sites reference this module as `scheduler::lock_manager::…`;
// keep that path stable via an alias while the module lives under `lock/`.
pub use lock as lock_manager;
pub use metrics::SchedulerMetrics;
pub use read_results::{BufferedEvents, CalvinReadResults, VShardReadResults};
pub use recovery::{AppliedRecovery, NOT_YET_APPLIED_EPOCH, recover_all_applied};
