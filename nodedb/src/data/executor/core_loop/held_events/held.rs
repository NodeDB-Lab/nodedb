// SPDX-License-Identifier: BUSL-1.1

//! The events a journalled task holds until its write set is stored, in a
//! spool that spills past `[tuning.query] max_txn_overlay_bytes`.
//!
//! The spool spills into its own directory,
//! `data_dir/txn-spill/core-<core_id>/held-events-<n>/`. The path is built,
//! and the directory created, on the first spill. A held set that never
//! spilled allocates and touches no file. One that spilled removes its
//! directory when it drops: after its events were sent, or unsent when the
//! journal ends unhealthy.
//!
//! A spill that fails puts the set in a failed state for good: it drops
//! what it holds and every later event, and names the failure, so the
//! journal answers the task with an error.

use std::path::Path;

use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_dir};
use crate::data::executor::handlers::spill::tiered::SpillSpace;
use crate::event::WriteEvent;

use super::event_spool::EventSpool;

/// Where a held set spills, before its first spill names the directory.
#[derive(Debug, Clone, Copy)]
pub(in crate::data::executor) struct HeldSpillAt {
    pub core_id: usize,
    /// The `<n>` of `held-events-<n>`.
    pub number: u64,
    /// Hot bytes past which the set spills.
    pub threshold: usize,
}

/// The held events of the running journalled tasks. See the module doc.
pub(in crate::data::executor) struct HeldEvents {
    spool: EventSpool,
    at: HeldSpillAt,
    /// `None` until the first spill.
    space: Option<SpillSpace>,
    /// The first spill error, as text. Set, it stays set.
    failure: Option<String>,
}

impl HeldEvents {
    /// An empty held set that spills as `at` names.
    pub(in crate::data::executor) fn new(at: HeldSpillAt) -> Self {
        Self {
            spool: EventSpool::default(),
            at,
            space: None,
            failure: None,
        }
    }

    /// Hold `event`, spilling the held tail under `data_dir` once it passed
    /// the threshold. `Err` once, at the spill that fails: the set then
    /// drops what it holds, and every later event.
    pub(in crate::data::executor) fn push(
        &mut self,
        event: WriteEvent,
        data_dir: &Path,
    ) -> crate::Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        self.spool.push(event);
        if self.spool.tail_bytes() <= self.at.threshold {
            return Ok(());
        }
        let spilled = self.spill(data_dir);
        if let Err(error) = &spilled {
            self.failure = Some(error.to_string());
            self.spool = EventSpool::default();
        }
        spilled
    }

    fn spill(&mut self, data_dir: &Path) -> crate::Result<()> {
        let at = self.at;
        let space = self.space.get_or_insert_with(|| {
            let dir = SpillKind::Txn
                .core_dir(data_dir, at.core_id)
                .join(format!("held-events-{}", at.number));
            SpillSpace::new(dir, "held", at.threshold)
        });
        self.spool.spill(space, "events")
    }

    /// The spill error that put the set in its failed state.
    pub(in crate::data::executor) fn failure(&self) -> Option<&str> {
        self.failure.as_deref()
    }

    /// Hand every held event to `send`, in hold order. A failed set sends
    /// nothing. The first event that does not read back stops the drain,
    /// and its error is returned.
    pub(in crate::data::executor) fn drain(
        &mut self,
        mut send: impl FnMut(WriteEvent),
    ) -> crate::Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        std::mem::take(&mut self.spool).drain(|event| {
            send(event);
            Ok(())
        })
    }

    /// Events the spill file holds.
    #[cfg(test)]
    pub(in crate::data::executor) fn spilled_len(&self) -> u64 {
        self.spool.spilled_len()
    }

    /// The spill directory, once a spill named it.
    #[cfg(test)]
    pub(in crate::data::executor) fn dir(&self) -> Option<&Path> {
        self.space.as_ref().map(SpillSpace::dir)
    }
}

impl Drop for HeldEvents {
    fn drop(&mut self) {
        let Some(space) = &self.space else {
            return;
        };
        if !space.created() {
            return;
        }
        // A removal that fails files its own faultbox report, and the boot
        // sweep removes the directory at the next open.
        let _removed = remove_spill_dir(SpillKind::Txn, space.dir());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::event::types::RowId;
    use crate::event::{EventSource, WriteOp};
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

    fn event(index: u32) -> WriteEvent {
        WriteEvent {
            sequence: 0,
            collection: Arc::from("orders"),
            op: WriteOp::Insert,
            row_id: RowId::Batch,
            lsn: Lsn::new(u64::from(index)),
            record: None,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(index.to_le_bytes().as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: None,
            image_fault: None,
        }
    }

    fn at(threshold: usize) -> HeldSpillAt {
        HeldSpillAt {
            core_id: 0,
            number: 7,
            threshold,
        }
    }

    fn spill_dir(data_dir: &Path) -> std::path::PathBuf {
        SpillKind::Txn.core_dir(data_dir, 0).join("held-events-7")
    }

    fn drained(held: &mut HeldEvents) -> Vec<u64> {
        let mut lsns = Vec::new();
        held.drain(|event| lsns.push(event.lsn.as_u64()))
            .expect("drain");
        lsns
    }

    #[test]
    fn a_held_set_that_never_spills_touches_no_file() {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let mut held = HeldEvents::new(at(usize::MAX));
        for index in 0..100 {
            held.push(event(index), data_dir.path()).expect("hold");
        }
        assert!(held.space.is_none(), "no spill space was built");
        assert!(!spill_dir(data_dir.path()).exists());
        assert_eq!(drained(&mut held), (0..100).collect::<Vec<u64>>());
        drop(held);
        assert!(!spill_dir(data_dir.path()).exists());
    }

    #[test]
    fn spilled_and_held_events_drain_in_hold_order() {
        let data_dir = tempfile::tempdir().expect("tempdir");
        // Every second push spills the tail, so the last event stays in RAM.
        let threshold = super::super::spooled_event::held_event_weight(&event(0));
        let mut held = HeldEvents::new(at(threshold));
        for index in 0..21 {
            held.push(event(index), data_dir.path()).expect("hold");
        }
        assert_eq!(held.spilled_len(), 20);
        assert!(held.spool.tail_bytes() > 0, "one event stays in RAM");
        assert!(spill_dir(data_dir.path()).is_dir());
        assert_eq!(drained(&mut held), (0..21).collect::<Vec<u64>>());
        drop(held);
        assert!(
            !spill_dir(data_dir.path()).exists(),
            "the spilled set removes its directory"
        );
    }

    #[test]
    fn a_failed_spill_drops_every_event_after_one_error() {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let dir = spill_dir(data_dir.path());
        // A file where the spill directory goes: the spill cannot create it.
        std::fs::create_dir_all(dir.parent().expect("parent")).expect("mkdir");
        std::fs::write(&dir, b"in the way").expect("block the directory");
        let mut held = HeldEvents::new(at(0));
        assert!(
            held.push(event(0), data_dir.path()).is_err(),
            "the spill fails"
        );
        assert!(held.failure().is_some());
        for index in 1..50 {
            held.push(event(index), data_dir.path())
                .expect("a failed set drops the event");
        }
        assert_eq!(held.spool.tail_bytes(), 0, "a failed set holds nothing");
        assert!(drained(&mut held).is_empty());
    }

    #[test]
    fn a_spill_file_that_does_not_read_back_stops_the_drain() {
        let data_dir = tempfile::tempdir().expect("tempdir");
        let mut held = HeldEvents::new(at(0));
        for index in 0..5 {
            held.push(event(index), data_dir.path()).expect("hold");
        }
        for file in std::fs::read_dir(spill_dir(data_dir.path())).expect("spill dir") {
            let path = file.expect("entry").path();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .and_then(|file| file.set_len(2))
                .expect("cut the spill file");
        }
        let mut sent = 0;
        assert!(held.drain(|_| sent += 1).is_err());
        assert_eq!(sent, 0);
    }
}
