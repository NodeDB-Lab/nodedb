// SPDX-License-Identifier: BUSL-1.1

//! The pins restart replay derives a pinned record's row bodies under.
//!
//! Replay derives each row body of a committed record again. Its live
//! install derived the body under the record's pins, so replay opens the
//! same view from the record's WAL copy: a whole record's `pins`, or a
//! stream's content header. A live install has its own view open, so
//! nothing here runs while a redo-apply scope is open.
//!
//! A split record's continuations take its origin's LSN, so their rows find
//! the origin's pins in any later slice of the replay. Each continuation
//! carries the origin's pins too, so its rows find them when the WAL holds
//! no origin record. A record's pins stay until replay ends, or until the
//! last continuation of a record whole at append replays. Records with
//! equal pins share one copy.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_physical::physical_plan::{ConfigPinKey, RedoPins};

use crate::data::executor::core_loop::CoreLoop;

use super::config_pins::ConfigPinError;
use super::judged_configs::JudgedConfigs;

/// What makes two records' pins equal: each config pin's key and digest,
/// and the CRS pin's digest. Replay reads records their live validate
/// judged, so equal digests hold equal bytes.
type PinSetKey = (Vec<(ConfigPinKey, u64)>, Option<u64>);

/// The pins of the records one restart replay holds.
#[derive(Default)]
pub(in crate::data::executor) struct ReplayPins {
    /// The pins of each pinned record, by its LSN.
    by_lsn: HashMap<u64, Arc<RedoPins>>,
    /// One copy of each distinct pin set `by_lsn` holds.
    shared: HashMap<PinSetKey, Arc<RedoPins>>,
    /// The view of the last record replay applied a row of, by its LSN.
    /// The record's next row reads it with no new decode.
    open: Option<(u64, JudgedConfigs)>,
}

impl CoreLoop {
    /// Keep the pins of each pinned record among `pins`, by its LSN. An
    /// origin record and each of its continuations carry the same pins.
    /// `Err` when two copies of one record's pins differ. A live install
    /// keeps none.
    pub(crate) fn keep_replay_pins(&mut self, pins: Vec<(u64, RedoPins)>) -> crate::Result<()> {
        if self.redo_apply.scope.is_some() {
            return Ok(());
        }
        let replay = &mut self.redo_apply.replay_pins;
        for (lsn, record_pins) in pins {
            if let Some(kept) = replay.by_lsn.get(&lsn) {
                if **kept != record_pins {
                    return Err(crate::Error::SegmentCorrupted {
                        detail: format!(
                            "the committed record at lsn {lsn} and its continuation carry \
                             different config pins"
                        ),
                    });
                }
                continue;
            }
            let key: PinSetKey = (
                record_pins
                    .configs
                    .iter()
                    .map(|pin| (pin.key.clone(), pin.digest))
                    .collect(),
                record_pins.crs.as_ref().map(|pin| pin.digest),
            );
            let shared = replay
                .shared
                .entry(key)
                .or_insert_with(|| Arc::new(record_pins));
            replay.by_lsn.insert(lsn, Arc::clone(shared));
        }
        Ok(())
    }

    /// Drop the pins of each record at `origins`: no row of it replays
    /// after this.
    pub(crate) fn release_replay_pins(&mut self, origins: &[u64]) {
        let replay = &mut self.redo_apply.replay_pins;
        for origin in origins {
            replay.by_lsn.remove(origin);
            if replay.open.as_ref().is_some_and(|(open, _)| open == origin) {
                replay.open = None;
            }
        }
        // A pin set that no record holds drops too.
        if !origins.is_empty() {
            replay
                .shared
                .retain(|_, record_pins| Arc::strong_count(record_pins) > 1);
        }
    }

    /// How many records' pins replay holds.
    #[cfg(test)]
    pub(in crate::data::executor) fn held_replay_pins(&self) -> usize {
        self.redo_apply.replay_pins.by_lsn.len()
    }

    /// Forget every record's pins: replay ended.
    pub(crate) fn forget_replay_pins(&mut self) {
        self.redo_apply.replay_pins = ReplayPins::default();
    }

    /// Run `apply`, which replays one row of the record at `lsn`, under the
    /// record's pins, and return what it returns. A record with no pins,
    /// and a row a live install applies, run with no change. A pin that does
    /// not decode halts replay: `apply` does not run, and `unapplied` is
    /// returned.
    pub(in crate::data::executor) fn under_replay_pins<R>(
        &mut self,
        lsn: u64,
        unapplied: R,
        apply: impl FnOnce(&mut Self) -> R,
    ) -> R {
        if self.redo_apply.scope.is_some() {
            return apply(self);
        }
        let view = match self.replay_view(lsn) {
            Ok(Some(view)) => view,
            Ok(None) => return apply(self),
            Err(error) => {
                self.replay_record_unapplied("redo", "config_pins", lsn, &error.to_string());
                return unapplied;
            }
        };
        // The view is open with no validate running: the row derives its
        // body under the pin's install config.
        let held = self.hold_judging();
        self.apply_scope.judged = Some(view);
        let applied = apply(self);
        if let Some(view) = self.apply_scope.judged.take() {
            self.redo_apply.replay_pins.open = Some((lsn, view));
        }
        self.restore_judging(held);
        applied
    }

    /// The view of the record at `lsn`: the kept one when it is that
    /// record's, else one opened from its pins. `None` for a record with no
    /// pins. `Err` when one of its pins does not decode.
    fn replay_view(&mut self, lsn: u64) -> Result<Option<JudgedConfigs>, ConfigPinError> {
        let pins = &mut self.redo_apply.replay_pins;
        if pins.open.as_ref().is_some_and(|(open, _)| *open == lsn) {
            return Ok(pins.open.take().map(|(_, view)| view));
        }
        let Some(record_pins) = pins.by_lsn.get(&lsn) else {
            return Ok(None);
        };
        let Some(mut view) = JudgedConfigs::open(record_pins, &self.crs_set, self.crs_digest)?
        else {
            return Ok(None);
        };
        // Every pin decodes before the record's first row, as the live
        // validate checks.
        view.prime_install(&self.doc_configs);
        match view.take_failure() {
            Some(error) => Err(error),
            None => Ok(Some(view)),
        }
    }
}
