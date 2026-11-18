// SPDX-License-Identifier: BUSL-1.1

//! A spool of held events. Its tail keeps each [`WriteEvent`] as it was
//! raised, sharing its images. Only a spill writes the events it moves as
//! [`SpooledEvent`]s, and a drain reads those back before the tail.

use crate::data::executor::handlers::spill::run::record::{decode_value, encode_value};
use crate::data::executor::handlers::spill::tiered::{SpillSpace, Spool};
use crate::event::WriteEvent;

use super::spooled_event::{SpooledEvent, held_event_weight};

/// Held events, in raise order. See the module doc.
#[derive(Default)]
pub(in crate::data::executor) struct EventSpool {
    spool: Spool<WriteEvent>,
}

impl EventSpool {
    /// Hold `event`.
    pub(in crate::data::executor) fn push(&mut self, event: WriteEvent) {
        let weight = held_event_weight(&event);
        self.spool.push_weighed(event, weight);
    }

    /// RAM the tail events hold.
    pub(in crate::data::executor) fn tail_bytes(&self) -> usize {
        self.spool.tail_bytes()
    }

    /// Events the spill file holds.
    #[cfg(test)]
    pub(in crate::data::executor) fn spilled_len(&self) -> u64 {
        self.spool.spilled_len()
    }

    /// Move every tail event to the spill file in `space`, tagged `tag`.
    pub(in crate::data::executor) fn spill(
        &mut self,
        space: &mut SpillSpace,
        tag: &str,
    ) -> crate::Result<()> {
        self.spool.spill_with(space, tag, |path, event| {
            Ok(encode_value(path, &SpooledEvent::from(event))?)
        })
    }

    /// Hand every event to `visit` in raise order: the spilled ones, then
    /// the tail. The first error stops the drain and is returned.
    pub(in crate::data::executor) fn drain(
        self,
        visit: impl FnMut(WriteEvent) -> crate::Result<()>,
    ) -> crate::Result<()> {
        self.spool.drain_with(
            |path, bytes| decode_value::<SpooledEvent>(path, bytes)?.into_event(),
            visit,
        )
    }
}
