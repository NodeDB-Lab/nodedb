// SPDX-License-Identifier: BUSL-1.1

//! The receiving side of a forwarded feed: a node that does not replicate a
//! data group takes the group's runs from the group's leader.
//!
//! A run is taken only once the journal holds it, so a crash never loses an
//! event this node acked. A run the journal does not write is refused
//! whole: the ring takes none of it, and the leader sends the stretch again
//! from this node's position. Every answer carries that position: the node
//! holds every event of the partition up to it, durably.
//!
//! The ring skips every change at or below the feed's `through`, and the
//! journal keys each row by its position, so a run that arrives twice
//! publishes once. A leader feeds from the position this node reported, so
//! a run starts at or below it, except the one hole a leader sends when it
//! no longer holds the events above that position.

use nodedb_types::RowIdentity;

use crate::control::change_stream::journal::codec::JournalRow;
use crate::control::cluster::metadata_applier::MetadataApplyWedge;
use crate::event::cdc::CdcOffset;
use crate::event::cross_shard::types::{NotifyAnswer, NotifyBroadcastMsg, NotifyBroadcastReply};
use crate::types::{DatabaseId, Lsn, TenantId};

use super::bus::ChangeStream;
use super::ring::{ChangeRun, PositionedChange};
use super::{ChangeEvent, ChangeOperation, ChangePartition, RunOutcome};

impl ChangeStream {
    /// Answer a run or a probe the leader of a partition forwarded. The
    /// reply carries this node's position in the partition. A halt of the
    /// partition records itself on `wedge`.
    pub fn deliver_remote_notify(
        &self,
        msg: &NotifyBroadcastMsg,
        wedge: &MetadataApplyWedge,
    ) -> NotifyBroadcastReply {
        let Some(partition) = msg.partition() else {
            tracing::warn!(
                kind = msg.partition_kind,
                "change run names an unknown partition kind"
            );
            return NotifyBroadcastReply {
                answer: NotifyAnswer::Rejected {
                    detail: format!("unknown partition kind {}", msg.partition_kind),
                },
                through: None,
            };
        };
        let answer = if msg.probe {
            NotifyAnswer::Taken
        } else {
            match self.publish_run(run_of(partition, msg), wedge) {
                RunOutcome::Durable => NotifyAnswer::Taken,
                RunOutcome::Refused(detail) => NotifyAnswer::Refused { detail },
            }
        };
        match self.durable_through(partition) {
            Ok(through) => NotifyBroadcastReply { answer, through },
            Err(error) => {
                let ChangePartition(group_id) = partition;
                crate::diag::change_journal_failed(group_id, "forwarded position read", &error);
                NotifyBroadcastReply {
                    answer: NotifyAnswer::Refused {
                        detail: format!("the change journal's position did not read: {error}"),
                    },
                    through: None,
                }
            }
        }
    }

    /// The position up to which this node's journal holds every event of
    /// `partition`. `None` when it never journaled the partition.
    ///
    /// A node that replicated the group before holds settled events in its
    /// ring that its journal can lack: the floor save that journals them
    /// never came. Its ring position can therefore lie above this one, and
    /// a leader that feeds from this one sends those events again.
    pub(crate) fn durable_through(
        &self,
        partition: ChangePartition,
    ) -> crate::Result<Option<CdcOffset>> {
        match self.journal.get() {
            Some(journal) => journal.through(partition),
            None => Ok(None),
        }
    }

    /// Publish a run the leader of its partition forwarded, once the
    /// journal holds it.
    ///
    /// - The journal holds every change of the run already: the run is
    ///   [`RunOutcome::Durable`], and nothing is written again.
    /// - The write succeeds: the ring takes the changes it lacks, the
    ///   settled events pending for the journal that the run covers leave
    ///   memory, a halt the write paid clears from `wedge`, and the run is
    ///   [`RunOutcome::Durable`].
    /// - The write fails: the ring takes nothing, and the run is
    ///   [`RunOutcome::Refused`]. The failure counts toward the partition's
    ///   halt with the run's new events (see [`super::journal_debt`]).
    pub(crate) fn publish_run(&self, run: ChangeRun, wedge: &MetadataApplyWedge) -> RunOutcome {
        let ChangePartition(group_id) = run.partition;
        let Some(journal) = self.journal.get() else {
            return RunOutcome::Refused("the change journal is not attached yet".into());
        };
        let durable = match journal.through(run.partition) {
            Ok(durable) => durable,
            Err(error) => {
                crate::diag::change_journal_failed(group_id, "forwarded position read", &error);
                return RunOutcome::Refused(error.to_string());
            }
        };
        let held = durable.is_some_and(|through| {
            run.after.is_some_and(|after| after <= through) && run.through <= through
        });
        if held {
            return RunOutcome::Durable;
        }
        let (feed_after, fresh) = {
            let state = self.lock();
            let fresh = state.ring.fresh_in(&run);
            let fresh = fresh.first().map(|first| (fresh.len(), first.position));
            (state.ring.floor_after(&run), fresh)
        };
        let written = journal.persist(
            run.partition,
            feed_after,
            run.through,
            run.changes.iter().map(JournalRow::of_positioned),
        );
        match written {
            Ok(()) => {
                {
                    let mut guard = self.lock();
                    let state = &mut *guard;
                    state.debt.paid(group_id);
                    // Events this node settled as a replica, which the run
                    // journaled: they no longer wait for a floor save.
                    if let Some(pending) = state.group_pending.get_mut(&group_id) {
                        let covered =
                            pending.partition_point(|event| event.position() <= run.through);
                        pending.drain(..covered);
                        state.held.sub(covered);
                    }
                    self.append_locked(state, run);
                }
                self.clear_journal_halts(wedge);
                RunOutcome::Durable
            }
            Err(error) => {
                crate::diag::change_journal_failed(group_id, "forwarded run write", &error);
                self.lock().debt.failed(group_id, &error);
                self.journal_halts_with(group_id, fresh, wedge);
                tracing::error!(
                    group_id,
                    %error,
                    "journaling a forwarded change run failed; the run is refused, and its \
                     leader sends it again"
                );
                RunOutcome::Refused(error.to_string())
            }
        }
    }
}

/// The run `msg` carries, of `partition`.
fn run_of(partition: ChangePartition, msg: &NotifyBroadcastMsg) -> ChangeRun {
    let changes = msg
        .changes
        .iter()
        .map(|change| PositionedChange {
            position: change.position,
            database_id: DatabaseId::new(change.database_id),
            event: ChangeEvent {
                lsn: Lsn::new(change.lsn),
                tenant_id: TenantId::new(change.tenant_id),
                collection: change.collection.clone(),
                // The wire carries the identity as text; wrap it verbatim.
                document_id: RowIdentity::from_user_key(change.document_id.clone()),
                operation: parse_operation(&change.operation),
                timestamp_ms: change.timestamp_ms,
                after: None,
            },
        })
        .collect();
    ChangeRun {
        partition,
        after: Some(msg.after),
        through: msg.through,
        changes,
    }
}

fn parse_operation(operation: &str) -> ChangeOperation {
    match operation {
        "UPDATE" => ChangeOperation::Update,
        "DELETE" => ChangeOperation::Delete,
        _ => ChangeOperation::Insert,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::change_stream::CursorStep;
    use crate::control::change_stream::journal::ChangeJournal;
    use crate::control::change_stream::stream::forward_read::FORWARD_RUN_EVENTS;

    const GROUP: ChangePartition = ChangePartition(3);

    fn event(index: u64, document: &str) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(index),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    fn journaled(dir: &std::path::Path, capacity: usize) -> (ChangeStream, Arc<ChangeJournal>) {
        let stream = ChangeStream::new(capacity);
        let journal = Arc::new(ChangeJournal::open(dir, 1 << 16).expect("open journal"));
        stream.attach_journal(Arc::clone(&journal)).expect("attach");
        (stream, journal)
    }

    /// The leader's next run of [`GROUP`] past `after`, as the wire carries
    /// it.
    fn next_run(leader: &ChangeStream, after: CdcOffset) -> NotifyBroadcastMsg {
        let run = leader
            .forward_run(GROUP, after, FORWARD_RUN_EVENTS)
            .expect("read the leader")
            .expect("a run past the position");
        NotifyBroadcastMsg::from_run(1, &run)
    }

    fn journal_positions(journal: &ChangeJournal, upto: u64) -> Vec<CdcOffset> {
        journal
            .read(
                GROUP,
                CdcOffset::ZERO,
                CdcOffset::whole_index(upto),
                None,
                1 << 16,
            )
            .expect("read the journal")
            .changes
            .iter()
            .map(|change| change.position)
            .collect()
    }

    /// A run answers `Taken` only once the journal holds it. A failed write
    /// answers `Refused`: the ring, the live feed, and the reported
    /// position take nothing of the run. The run sent again answers
    /// `Taken`, and the journal already holds its events as it answers.
    #[test]
    fn no_run_is_taken_before_the_journal_holds_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let leader = ChangeStream::new(16);
        let (follower, journal) = journaled(dir.path(), 16);
        let mut live = follower.subscribe(None, Some(TenantId::new(1)));
        let wedge = MetadataApplyWedge::default();
        leader.settle_entry(3, 1, DatabaseId::DEFAULT, vec![event(1, "a")]);
        let run = next_run(&leader, CdcOffset::ZERO);

        let probe = follower.deliver_remote_notify(&NotifyBroadcastMsg::probe(1, GROUP), &wedge);
        assert_eq!(probe.answer, NotifyAnswer::Taken);
        assert_eq!(probe.through, None, "the follower never saw the group");

        journal.fail_next_persists_for_test(1);
        let refused = follower.deliver_remote_notify(&run, &wedge);
        assert!(matches!(refused.answer, NotifyAnswer::Refused { .. }));
        assert_eq!(refused.through, None, "the position did not move");
        assert!(live.try_recv_sequenced().is_err(), "the ring took nothing");
        assert_eq!(follower.events_published(), 0);
        assert!(journal_positions(&journal, 1).is_empty());

        let taken = follower.deliver_remote_notify(&run, &wedge);
        assert_eq!(taken.answer, NotifyAnswer::Taken);
        assert_eq!(taken.through, Some(CdcOffset::whole_index(1)));
        assert_eq!(
            journal_positions(&journal, 1),
            vec![CdcOffset::data_event(0, 1, 1)],
            "the journal holds the run it acked"
        );
        let delivered = live.try_recv_sequenced().expect("the taken run");
        assert_eq!(delivered.document_id.as_str(), "a");
    }

    /// A run that repeats a stretch the follower holds answers `Taken` and
    /// publishes nothing, even while the journal refuses writes. A run that
    /// starts above the follower's position records a hole, and every
    /// cursor below it resets.
    #[test]
    fn forwarded_runs_deduplicate_and_report_a_hole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let leader = ChangeStream::new(16);
        let (follower, journal) = journaled(dir.path(), 16);
        let mut live = follower.subscribe(None, Some(TenantId::new(1)));
        let mut cursor = live.start_cursor().clone();
        let wedge = MetadataApplyWedge::default();

        leader.settle_entry(3, 1, DatabaseId::DEFAULT, vec![event(1, "a")]);
        let first = next_run(&leader, CdcOffset::ZERO);
        let taken = follower.deliver_remote_notify(&first, &wedge);
        assert_eq!(taken.answer, NotifyAnswer::Taken);
        journal.fail_next_persists_for_test(1);
        let repeat = follower.deliver_remote_notify(&first, &wedge);
        assert_eq!(
            repeat.answer,
            NotifyAnswer::Taken,
            "a held run needs no write"
        );
        journal.fail_next_persists_for_test(0);
        let delivered = live.try_recv_sequenced().expect("the first run");
        assert_eq!(cursor.accept(&delivered), CursorStep::Deliver);
        assert!(
            live.try_recv_sequenced().is_err(),
            "the repeat appended nothing"
        );

        // The leader no longer holds entry 2 when it feeds the follower.
        leader.settle_entry(3, 2, DatabaseId::DEFAULT, vec![event(2, "lost")]);
        leader.install_group_floor(3, 2);
        leader.settle_entry(3, 3, DatabaseId::DEFAULT, vec![event(3, "c")]);
        let holed = next_run(&leader, CdcOffset::whole_index(1));
        assert_eq!(
            holed.after,
            CdcOffset::whole_index(2),
            "the run starts at the floor"
        );
        assert_eq!(
            follower.deliver_remote_notify(&holed, &wedge).answer,
            NotifyAnswer::Taken
        );
        let after_hole = live.try_recv_sequenced().expect("the run past the hole");
        assert_eq!(cursor.accept(&after_hole), CursorStep::Reset);
        assert_eq!(follower.events_published(), 2);
    }
}
