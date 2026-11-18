// SPDX-License-Identifier: BUSL-1.1

//! What an install holds for after its arms, in spools that spill.
//!
//! The validate fills the KV prior images, the node-label deltas, the row
//! sources and the publishes. The install's arms add the document rows
//! they wrote, the events their handlers raised and the write notes. Once
//! the record settled, the install reads each back in write order.
//!
//! A batched install checks the hot bytes after each batch, the validate
//! after each batch it walks. Past the threshold every spool spills, so
//! RAM holds at most the threshold plus one batch's records. A whole
//! record sits in RAM already, so its install never spills its spools.

use crate::data::executor::core_loop::held_events::EventSpool;
use crate::data::executor::handlers::spill::tiered::{SpillSpace, Spool};
use crate::wal::RedoPublish;

use super::super::events::KvPriorImage;
use super::super::sub_ops::RedoLabelOp;
use super::notes::CollapsedNotes;
use super::row_sources::HeldRowSources;
use super::spooled::SpooledDocWrite;

/// The held state of one install. See the module doc.
#[derive(Default)]
pub(in crate::data::executor::handlers::transaction::redo_apply) struct InstallSpools {
    /// The value each KV key the record writes held before the install, in
    /// op order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) kv_priors: Spool<KvPriorImage>,
    /// The record's node-label deltas, in op order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) labels: Spool<RedoLabelOp>,
    pub(in crate::data::executor::handlers::transaction::redo_apply) row_sources: HeldRowSources,
    /// The record's messages, in record order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) publishes: Spool<RedoPublish>,
    /// Document rows written, source rows and fold targets alike, in write
    /// order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) doc_writes:
        Spool<SpooledDocWrite>,
    /// Events the install's handlers raised, in raise order.
    pub(in crate::data::executor::handlers::transaction::redo_apply) pending_events: EventSpool,
    pub(in crate::data::executor::handlers::transaction::redo_apply) notes: CollapsedNotes,
    /// The most hot bytes a check found, before it spilled.
    #[cfg(test)]
    peak_hot_bytes: usize,
}

impl InstallSpools {
    /// Hot bytes every spool holds in RAM.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn hot_bytes(&self) -> usize {
        self.kv_priors.tail_bytes()
            + self.labels.tail_bytes()
            + self.row_sources.hot_bytes()
            + self.publishes.tail_bytes()
            + self.doc_writes.tail_bytes()
            + self.pending_events.tail_bytes()
            + self.notes.hot_bytes()
    }

    /// The most hot bytes any check found.
    #[cfg(test)]
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn peak_hot_bytes(
        &self,
    ) -> usize {
        self.peak_hot_bytes
    }

    /// Entries the spools spilled, notes and row sources aside.
    #[cfg(test)]
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn spilled_entries(
        &self,
    ) -> u64 {
        self.kv_priors.spilled_len()
            + self.labels.spilled_len()
            + self.publishes.spilled_len()
            + self.doc_writes.spilled_len()
            + self.pending_events.spilled_len()
    }

    /// Spill every spool into `space` once their hot bytes, with `pinned`
    /// bytes the install holds in RAM beside them, passed its threshold.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn spill_past_threshold(
        &mut self,
        space: &mut SpillSpace,
        pinned: usize,
    ) -> crate::Result<()> {
        let hot = self.hot_bytes().saturating_add(pinned);
        #[cfg(test)]
        {
            self.peak_hot_bytes = self.peak_hot_bytes.max(hot);
        }
        if hot <= space.threshold() {
            return Ok(());
        }
        self.kv_priors.spill(space, "kv-priors")?;
        self.labels.spill(space, "labels")?;
        self.row_sources.spill(space)?;
        self.publishes.spill(space, "publishes")?;
        self.doc_writes.spill(space, "doc-writes")?;
        self.pending_events.spill(space, "events")?;
        self.notes.spill(space)
    }
}
