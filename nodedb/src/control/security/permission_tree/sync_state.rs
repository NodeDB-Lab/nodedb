// SPDX-License-Identifier: BUSL-1.1

//! How far the permission cache reflects each core's writes.
//!
//! Every Data Plane core numbers the events it emits with a contiguous
//! sequence. The Event Plane passes every event it takes off a core's ring
//! through the permission step, in ring order. For each core the cache keeps
//! `applied`: every event numbered at or below it was either applied to the
//! cache, or its write is covered by a reload.
//!
//! `applied` only advances by one. A dropped event leaves a gap the step can
//! never close, so the core stays behind until a reload covers it. A reload
//! reads the emitted counters first, then scans the source collections. Each
//! scan runs on its core after every write whose event was counted, so the
//! reload covers every event numbered at or below the counter it read.
//!
//! A reload also names, per core, the highest event it covers. An event at or
//! below that number reaching the step later is not applied again. Its write
//! is in the reload, and a later write the ring dropped may have overwritten
//! it.

/// Apply progress of one core.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CoreApply {
    /// Every event at or below this number is reflected in the cache.
    applied: u64,
    /// Every event at or below this number is covered by the last reload.
    reloaded_through: u64,
    /// An event above `applied + 1` passed the step: one was dropped.
    gap: bool,
}

/// Apply progress of the permission cache, per core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyProgress {
    cores: Vec<CoreApply>,
    /// No reload has covered the registered trees yet. Set at startup, before
    /// the durable grants are loaded, and whenever a tree definition changes.
    needs_reload: bool,
}

impl Default for ApplyProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplyProgress {
    /// Progress of a cache that holds no source data yet.
    pub fn new() -> Self {
        Self {
            cores: Vec::new(),
            needs_reload: true,
        }
    }

    fn core_mut(&mut self, core_id: usize) -> &mut CoreApply {
        if self.cores.len() <= core_id {
            self.cores.resize(core_id + 1, CoreApply::default());
        }
        &mut self.cores[core_id]
    }

    fn core(&self, core_id: usize) -> CoreApply {
        self.cores.get(core_id).copied().unwrap_or_default()
    }

    /// Whether the last reload covers event `sequence` of `core_id`. Such an
    /// event is never applied again.
    pub fn covered_by_reload(&self, core_id: usize, sequence: u64) -> bool {
        sequence <= self.core(core_id).reloaded_through
    }

    /// Record that event `sequence` of `core_id` passed the permission step.
    pub fn note_consumed(&mut self, core_id: usize, sequence: u64) {
        let core = self.core_mut(core_id);
        if sequence <= core.applied {
            return;
        }
        if sequence == core.applied + 1 {
            core.applied = sequence;
        } else {
            core.gap = true;
        }
    }

    /// Whether planning must reload before it reads the cache: no reload
    /// covers the registered trees, or a core lost an event the step can
    /// never apply.
    pub fn is_stale(&self) -> bool {
        self.needs_reload || self.cores.iter().any(|core| core.gap)
    }

    /// Record that the source set changed: a tree definition was registered
    /// or removed.
    pub fn mark_reload_needed(&mut self) {
        self.needs_reload = true;
    }

    /// Whether the cache reflects every event numbered at or below
    /// `targets[core]` on each core.
    pub fn caught_up(&self, targets: &[u64]) -> bool {
        !self.needs_reload
            && targets
                .iter()
                .enumerate()
                .all(|(core_id, target)| self.core(core_id).applied >= *target)
    }

    /// Whether waiting cannot reach `targets`: no reload covered the sources
    /// yet, or a core that is behind its target lost an event.
    pub fn needs_reload_for(&self, targets: &[u64]) -> bool {
        self.needs_reload
            || targets.iter().enumerate().any(|(core_id, target)| {
                let core = self.core(core_id);
                core.gap && core.applied < *target
            })
    }

    /// Record a reload that covers every event at or below `emitted[core]`.
    pub fn install_reload(&mut self, emitted: &[u64]) {
        for (core_id, through) in emitted.iter().enumerate() {
            let core = self.core_mut(core_id);
            core.applied = core.applied.max(*through);
            core.reloaded_through = core.reloaded_through.max(*through);
            // Every event the step saw was emitted before the counter was
            // read, so the reload covers any gap among them.
            core.gap = false;
        }
        self.needs_reload = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reloaded(emitted: &[u64]) -> ApplyProgress {
        let mut progress = ApplyProgress::new();
        progress.install_reload(emitted);
        progress
    }

    #[test]
    fn a_fresh_cache_needs_a_reload_before_it_is_caught_up() {
        let progress = ApplyProgress::new();
        assert!(!progress.caught_up(&[]));
        assert!(progress.needs_reload_for(&[]));
        assert!(reloaded(&[0]).caught_up(&[0]));
    }

    #[test]
    fn contiguous_events_advance_the_core() {
        let mut progress = reloaded(&[0, 0]);
        progress.note_consumed(1, 1);
        progress.note_consumed(1, 2);
        assert!(progress.caught_up(&[0, 2]));
        assert!(!progress.caught_up(&[0, 3]));
        assert!(!progress.needs_reload_for(&[0, 3]));
    }

    #[test]
    fn a_dropped_event_holds_the_core_until_a_reload() {
        let mut progress = reloaded(&[0]);
        progress.note_consumed(0, 1);
        progress.note_consumed(0, 3);
        progress.note_consumed(0, 4);
        assert!(!progress.caught_up(&[4]));
        assert!(progress.needs_reload_for(&[4]));
        // A target the core reached before the gap needs nothing.
        assert!(!progress.needs_reload_for(&[1]));

        progress.install_reload(&[4]);
        assert!(progress.caught_up(&[4]));
        assert!(progress.covered_by_reload(0, 3));
        assert!(!progress.covered_by_reload(0, 5));
        progress.note_consumed(0, 5);
        assert!(progress.caught_up(&[5]));
    }

    #[test]
    fn a_tree_change_requires_a_new_reload() {
        let mut progress = reloaded(&[2]);
        progress.mark_reload_needed();
        assert!(!progress.caught_up(&[2]));
        assert!(progress.needs_reload_for(&[2]));
    }

    #[test]
    fn a_lost_event_makes_the_cache_stale_until_a_reload() {
        let mut progress = reloaded(&[0]);
        assert!(!progress.is_stale());
        progress.note_consumed(0, 2);
        assert!(progress.is_stale());
        progress.install_reload(&[2]);
        assert!(!progress.is_stale());
    }
}
