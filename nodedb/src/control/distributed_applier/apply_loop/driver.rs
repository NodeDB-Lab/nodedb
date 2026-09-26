// SPDX-License-Identifier: BUSL-1.1

//! Loop driver: takes batches off the apply channel into the pipeline, and
//! collects the enqueues and applies that finish, until the channel closes
//! and every started entry concluded.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::distributed_applier::applier::ApplyBatch;
use crate::control::distributed_applier::proposal_ledger::{
    PROPOSAL_LEDGER_CAPACITY, ProposalLedger,
};
use crate::control::distributed_applier::propose_tracker::ProposeTracker;
use crate::control::state::SharedState;

use super::context::ApplyContext;
use super::pipeline::Pipeline;

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
    calvin_read_result_senders: Arc<
        std::sync::Mutex<std::collections::BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>,
    >,
) {
    // Proposals this node already applied, recovered from its WAL before any
    // entry is delivered: every record an entry's apply appended carries the
    // entry's idempotency key in its header.
    let records = match state.wal.replay() {
        Ok(records) => records,
        Err(error) => {
            // Without the keys an entry re-delivered above the durable floor,
            // or a second committed copy of a proposal, would apply a second
            // time. Refuse to apply anything rather than risk it: the loop
            // stops, and every propose waiter surfaces the stall.
            tracing::error!(
                %error,
                "data-group apply loop cannot read its WAL to recover applied proposals; \
                 refusing to apply committed entries"
            );
            return;
        }
    };
    let ledger = ProposalLedger::from_records(&records, PROPOSAL_LEDGER_CAPACITY);
    drop(records);

    let ctx = ApplyContext {
        state: &state,
        tracker: &tracker,
        calvin_read_result_senders: &calvin_read_result_senders,
    };
    let mut pipeline = Pipeline::new(ctx, ledger);
    let mut accepting = true;
    loop {
        if !accepting && !pipeline.has_running() {
            // The channel closed and every started entry concluded. The
            // pump started every entry that can start, so none is queued.
            return;
        }
        let running = pipeline.has_running();
        tokio::select! {
            biased;
            Some(event) = pipeline.next_event(), if running => {
                pipeline.handle(event);
            }
            batch = apply_rx.recv(), if accepting => match batch {
                Some(batch) => {
                    pipeline.accept(batch);
                    // Take every batch already queued, so one pass starts
                    // them all and one floor save covers them.
                    while let Ok(batch) = apply_rx.try_recv() {
                        pipeline.accept(batch);
                    }
                }
                None => accepting = false,
            },
        }
        pipeline.pump();
        pipeline.settle();
    }
}
