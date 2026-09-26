// SPDX-License-Identifier: BUSL-1.1

//! What every entry's apply borrows from the loop, and the futures the loop
//! collects while it prepares later entries.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;

use super::proposal_gate::EntryOutcome;

/// Senders to each local Calvin scheduler's read-result channel, by vShard.
pub(super) type CalvinReadResultSenders = Arc<Mutex<BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>>;

/// The loop-owned handles an entry's apply borrows.
#[derive(Clone, Copy)]
pub(super) struct ApplyContext<'a> {
    pub state: &'a Arc<SharedState>,
    pub tracker: &'a Arc<ProposeTracker>,
    pub calvin_read_result_senders: &'a CalvinReadResultSenders,
}

/// An entry whose apply finished: its waiter is resolved, and its outcome
/// waits for every earlier entry of its group before it settles.
pub(super) struct FinishedApply {
    pub group_id: u64,
    pub log_index: u64,
    pub outcome: EntryOutcome,
}

/// An apply the loop collects while it starts later entries.
pub(super) type ApplyFuture<'a> = Pin<Box<dyn Future<Output = FinishedApply> + Send + 'a>>;

/// How a write continues once its enqueue returned.
pub(super) enum Started<'a> {
    /// The write is on its core. The apply collects its outcome.
    Running(ApplyFuture<'a>),
    /// The write concluded without reaching a core.
    Concluded(EntryOutcome),
}

/// A write past its enqueue, the collection its plan named, and whether its
/// plan writes user data: only such a write raises its tenant's write mark,
/// as the write funnel decides for every write it records.
pub(super) struct StartedEntry<'a> {
    pub started: Started<'a>,
    pub collection: Option<String>,
    pub user_write: bool,
}

impl StartedEntry<'_> {
    pub fn concluded(outcome: EntryOutcome) -> Self {
        Self {
            started: Started::Concluded(outcome),
            collection: None,
            user_write: false,
        }
    }
}

/// A write's enqueue. The next entry of its group starts once it returns.
pub(super) type EnqueueFuture<'a> = Pin<Box<dyn Future<Output = StartedEntry<'a>> + Send + 'a>>;

/// What the loop collects: an enqueue that returned, or an apply that
/// finished.
pub(super) enum LoopEvent<'a> {
    Enqueued {
        group_id: u64,
        log_index: u64,
        entry: StartedEntry<'a>,
    },
    Finished(FinishedApply),
}

/// A future the loop collects.
pub(super) type LoopFuture<'a> = Pin<Box<dyn Future<Output = LoopEvent<'a>> + Send + 'a>>;
