// SPDX-License-Identifier: BUSL-1.1

//! The Calvin part of a stamped `TransactionRedo` entry's apply.
//!
//! A committed Calvin slice installs only from its vShard's data-group log.
//! Every replica applies its entries in log order, so every replica decides
//! each copy of a slice's redo alike:
//!
//! 1. At entry start, in log order, the apply claims `(epoch, position)` in
//!    the vShard's applied ledger. A claim refused because the position is
//!    applied, or another copy holds it, concludes the entry applied and
//!    installs nothing: a re-proposed copy never installs twice.
//! 2. The install runs like every committed redo's.
//! 3. When the install is durable, the ledger marks the position, the
//!    position's open chunk streams drop, and the vShard's scheduler hears
//!    `RedoApplied`. All of it happens before the entry concludes, so the
//!    ledger holds the position before the group's applied index passes
//!    the entry.
//! 4. A final refusal keeps the claim: every replica refuses the same entry,
//!    and no later copy installs. An install that is not durable releases
//!    the claim: a restart applies the entry again.
//!
//! A full inbox makes the apply wait for room. The entry holds its own
//! place in the group's lane meanwhile, as a parked write does. A vShard
//! with no running scheduler gets no event: the next scheduler seeds from
//! the ledger.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::control::cluster::calvin::scheduler::{CalvinAppliedLedger, CalvinApplyEvent};
use crate::control::server::dispatch_utils::{SubmitOutcome, refusal_is_final};
use crate::control::state::SharedState;
use crate::control::wal_replication::types::CalvinRedoMeta;
use crate::types::{Lsn, RequestId};
use crate::wal::RedoStreamId;

/// A Calvin position this entry holds the claim on.
pub(super) struct CalvinClaim {
    vshard_id: u32,
    epoch: u64,
    position: u32,
    primary_write: bool,
    returning: bool,
    ledger: Arc<CalvinAppliedLedger>,
}

/// What a stamped redo entry's claim decided.
pub(super) enum ClaimOutcome {
    /// The entry carries no Calvin slice.
    NotCalvin,
    /// The entry installs the position.
    Claimed(CalvinClaim),
    /// The position is applied, or another copy holds it: the entry
    /// installs nothing.
    Refused,
    /// The entry names a Calvin slice but no position, so no replica can
    /// install it.
    Malformed(crate::Error),
}

/// Claim the position of an inline Calvin redo: its record's stamp.
pub(super) fn claim_inline(
    state: &SharedState,
    meta: Option<&CalvinRedoMeta>,
    stamp: Option<&crate::wal::CalvinStamp>,
) -> ClaimOutcome {
    let Some(meta) = meta else {
        return ClaimOutcome::NotCalvin;
    };
    match stamp {
        Some(stamp) => claim(state, meta, stamp.vshard_id, stamp.epoch, stamp.position),
        None => ClaimOutcome::Malformed(crate::Error::Internal {
            detail: "a Calvin redo entry carries no calvin_stamp, so it names no position".into(),
        }),
    }
}

/// Claim the position of a chunked Calvin redo: its stream's.
pub(super) fn claim_chunked(
    state: &SharedState,
    meta: Option<&CalvinRedoMeta>,
    stream: &RedoStreamId,
) -> ClaimOutcome {
    let Some(meta) = meta else {
        return ClaimOutcome::NotCalvin;
    };
    match *stream {
        RedoStreamId::Calvin {
            vshard,
            epoch,
            position,
            ..
        } => claim(state, meta, vshard, epoch, position),
        RedoStreamId::Session { .. } => ClaimOutcome::Malformed(crate::Error::Internal {
            detail: format!("a Calvin redo entry names the session stream {stream:?}"),
        }),
    }
}

fn claim(
    state: &SharedState,
    meta: &CalvinRedoMeta,
    vshard_id: u32,
    epoch: u64,
    position: u32,
) -> ClaimOutcome {
    let ledger = state.calvin.applied.get_or_create(vshard_id);
    match ledger.claim(epoch, position) {
        Ok(()) => ClaimOutcome::Claimed(CalvinClaim {
            vshard_id,
            epoch,
            position,
            primary_write: meta.primary_write,
            returning: meta.returning,
            ledger,
        }),
        Err(refusal) => {
            state
                .calvin
                .counters
                .redo_copies_skipped
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                vshard_id,
                epoch,
                position,
                ?refusal,
                "calvin redo: a copy of an applied or claimed position installs nothing"
            );
            ClaimOutcome::Refused
        }
    }
}

impl CalvinClaim {
    /// Conclude the claim from the install's outcome, and return what the
    /// vShard's scheduler hears.
    pub(super) fn settle(
        &self,
        state: &SharedState,
        submitted: &crate::Result<SubmitOutcome>,
    ) -> CalvinApplyEvent {
        match submitted {
            Ok(outcome) if outcome.response.status == Status::Ok => {
                self.applied(state, outcome.response.clone())
            }
            Ok(outcome)
                if outcome
                    .response
                    .error_code
                    .as_deref()
                    .is_some_and(refusal_is_final) =>
            {
                CalvinApplyEvent::RedoRefused {
                    error: format!("{:?}", outcome.response.error_code.as_deref()),
                }
            }
            Ok(outcome) => self.not_applied(format!(
                "the install answered {:?} with {:?}",
                outcome.response.status,
                outcome.response.error_code.as_deref()
            )),
            Err(error) => self.not_applied(error.to_string()),
        }
    }

    /// The entry's collection is gone on this replica: nothing installs,
    /// and the position is done.
    pub(super) fn superseded(&self, state: &SharedState) -> CalvinApplyEvent {
        let reply = Response {
            request_id: RequestId::new(0),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(Box::new(ErrorCode::NotFound)),
            stage_vote: None,
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        };
        self.applied(state, reply)
    }

    /// The install did not run, or did not become durable.
    pub(super) fn not_applied(&self, error: String) -> CalvinApplyEvent {
        self.ledger.release_claim(self.epoch, self.position);
        CalvinApplyEvent::RedoNotApplied { error }
    }

    fn applied(&self, state: &SharedState, reply: Response) -> CalvinApplyEvent {
        self.ledger.mark_applied(self.epoch, self.position);
        state
            .redo_chunks
            .drop_calvin_position(self.vshard_id, self.epoch, self.position);
        // The install recorded the slice's write versions.
        state
            .calvin
            .counters
            .write_versions_recorded
            .fetch_add(1, Ordering::Relaxed);
        CalvinApplyEvent::RedoApplied {
            reply,
            primary_write: self.primary_write,
            returning: self.returning,
        }
    }

    /// Hand `event` to the vShard's scheduler, waiting for room in its
    /// inbox. A vShard with no running scheduler takes nothing.
    pub(super) async fn report(&self, state: &SharedState, event: CalvinApplyEvent) {
        let Some(inbox) = state.calvin.inboxes.get(self.vshard_id) else {
            return;
        };
        inbox.push(self.epoch, self.position, event).await;
    }

    /// Hand an install's `event` to the vShard's scheduler. `collections`
    /// are the slice's collections.
    ///
    /// With fail points, the gate `calvin::before_redo_applied_push::<c>` on
    /// one of them withholds a `RedoApplied`. The entry concludes, and its
    /// own task pushes the event once the gate releases. Until then the
    /// leader sees its group apply past the redo with no install heard, and
    /// proposes the redo again.
    pub(super) async fn report_install(
        &self,
        state: &SharedState,
        event: CalvinApplyEvent,
        collections: &[String],
    ) {
        #[cfg(feature = "failpoints")]
        let Some(event) = self.withhold_applied(state, event, collections) else {
            return;
        };
        #[cfg(not(feature = "failpoints"))]
        let _ = collections;
        self.report(state, event).await;
    }

    /// Hand a held `RedoApplied` to a task that pushes it once its gate
    /// releases. Returns any other event, or one no gate holds.
    #[cfg(feature = "failpoints")]
    fn withhold_applied(
        &self,
        state: &SharedState,
        event: CalvinApplyEvent,
        collections: &[String],
    ) -> Option<CalvinApplyEvent> {
        if !matches!(event, CalvinApplyEvent::RedoApplied { .. }) {
            return Some(event);
        }
        let Some(gate) = crate::control::fail_gate::holds_redo_applied_push(collections) else {
            return Some(event);
        };
        let Some(inbox) = state.calvin.inboxes.get(self.vshard_id) else {
            return Some(event);
        };
        let (epoch, position) = (self.epoch, self.position);
        tracing::info!(
            vshard_id = self.vshard_id,
            epoch,
            position,
            gate = %gate,
            "calvin: RedoApplied withheld at a fail point"
        );
        tokio::spawn(async move {
            crate::control::fail_gate::released(&gate).await;
            inbox.push(epoch, position, event).await;
        });
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;
    use nodedb_physical::physical_plan::CalvinReplySpec;

    fn state() -> (Arc<SharedState>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        (SharedState::new(dispatcher, wal).expect("state"), dir)
    }

    fn meta() -> CalvinRedoMeta {
        CalvinRedoMeta {
            epoch_system_ms: 0,
            reply: CalvinReplySpec::Count(Vec::new()),
            primary_write: true,
            user_write: true,
            returning: false,
        }
    }

    fn stamp(position: u32) -> crate::wal::CalvinStamp {
        crate::wal::CalvinStamp {
            epoch: 4,
            position,
            vshard_id: 3,
        }
    }

    fn ok() -> crate::Result<SubmitOutcome> {
        Ok(SubmitOutcome {
            response: Response {
                request_id: RequestId::new(1),
                status: Status::Ok,
                attempt: 1,
                partial: false,
                payload: Payload::empty(),
                watermark_lsn: Lsn::ZERO,
                error_code: None,
                stage_vote: None,
                read_versions: crate::types::ReadVersions::new(),
                write_set: Vec::new(),
            },
        })
    }

    /// A second stamped copy of a position finds the claim of the first, or
    /// the position applied, and installs nothing.
    #[test]
    fn a_second_stamped_copy_installs_nothing() {
        let (state, _dir) = state();
        let ClaimOutcome::Claimed(first) = claim_inline(&state, Some(&meta()), Some(&stamp(1)))
        else {
            panic!("the first copy claims the position");
        };
        assert!(matches!(
            claim_inline(&state, Some(&meta()), Some(&stamp(1))),
            ClaimOutcome::Refused
        ));
        let event = first.settle(&state, &ok());
        assert!(matches!(event, CalvinApplyEvent::RedoApplied { .. }));
        assert!(matches!(
            claim_inline(&state, Some(&meta()), Some(&stamp(1))),
            ClaimOutcome::Refused
        ));
        assert!(matches!(
            claim_chunked(
                &state,
                Some(&meta()),
                &RedoStreamId::Calvin {
                    vshard: 3,
                    epoch: 4,
                    position: 1,
                    attempt: 2,
                }
            ),
            ClaimOutcome::Refused
        ));
    }

    /// The ledger holds a durable install's position by the time its
    /// settle returns, before the entry concludes and the group's applied
    /// index passes it. An install that is not durable releases the claim.
    #[test]
    fn the_ledger_marks_before_the_applied_index_passes() {
        let (state, _dir) = state();
        let ledger = state.calvin.applied.get_or_create(3);
        let ClaimOutcome::Claimed(claim) = claim_inline(&state, Some(&meta()), Some(&stamp(2)))
        else {
            panic!("claims");
        };
        assert!(
            !ledger.is_applied(4, 2),
            "a claim is not an applied position"
        );
        claim.settle(&state, &ok());
        assert!(ledger.is_applied(4, 2));

        let ClaimOutcome::Claimed(failing) = claim_inline(&state, Some(&meta()), Some(&stamp(5)))
        else {
            panic!("claims");
        };
        let event = failing.settle(
            &state,
            &Err(crate::Error::Internal {
                detail: "fsync failed".into(),
            }),
        );
        assert!(matches!(event, CalvinApplyEvent::RedoNotApplied { .. }));
        assert!(!ledger.is_applied(4, 5));
        assert!(
            matches!(
                claim_inline(&state, Some(&meta()), Some(&stamp(5))),
                ClaimOutcome::Claimed(_)
            ),
            "a released claim lets the next copy install"
        );
    }

    /// An entry with no Calvin meta claims nothing, and a Calvin meta with
    /// no position is malformed.
    #[test]
    fn only_a_stamped_calvin_entry_claims() {
        let (state, _dir) = state();
        assert!(matches!(
            claim_inline(&state, None, Some(&stamp(1))),
            ClaimOutcome::NotCalvin
        ));
        assert!(matches!(
            claim_inline(&state, Some(&meta()), None),
            ClaimOutcome::Malformed(_)
        ));
    }

    /// A vShard with no running scheduler hears nothing, and its ledger
    /// still holds the position.
    #[tokio::test]
    async fn no_scheduler_takes_no_event() {
        let (state, _dir) = state();
        let ClaimOutcome::Claimed(claim) = claim_inline(&state, Some(&meta()), Some(&stamp(3)))
        else {
            panic!("claims");
        };
        let event = claim.settle(&state, &ok());
        claim.report(&state, event).await;
        assert!(state.calvin.applied.get_or_create(3).is_applied(4, 3));

        let handle = state.calvin.inboxes.register(3, 4);
        let ClaimOutcome::Claimed(next) = claim_inline(&state, Some(&meta()), Some(&stamp(4)))
        else {
            panic!("claims");
        };
        let event = next.settle(&state, &ok());
        next.report(&state, event).await;
        assert_eq!(handle.inbox().take_all().len(), 1);
    }
}
