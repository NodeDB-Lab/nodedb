// SPDX-License-Identifier: BUSL-1.1

//! Outside bounds on log compaction, one per owner.
//!
//! An owner that needs entries to stay holds a named ceiling. The log never
//! discards an entry above the lowest ceiling, so one owner never releases
//! entries another owner still needs.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The named ceilings on one log's compaction.
#[derive(Debug, Clone, Default)]
pub struct CompactionCeilings {
    by_owner: Vec<(&'static str, Arc<AtomicU64>)>,
}

impl CompactionCeilings {
    /// Set `owner`'s ceiling. A second call for `owner` replaces its first.
    pub fn set(&mut self, owner: &'static str, ceiling: Arc<AtomicU64>) {
        match self.by_owner.iter_mut().find(|(name, _)| *name == owner) {
            Some((_, held)) => *held = ceiling,
            None => self.by_owner.push((owner, ceiling)),
        }
    }

    /// `index`, lowered to the lowest ceiling.
    pub fn clamp(&self, index: u64) -> u64 {
        self.by_owner
            .iter()
            .map(|(_, ceiling)| ceiling.load(Ordering::Acquire))
            .fold(index, u64::min)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_ceiling_leaves_the_index() {
        assert_eq!(CompactionCeilings::default().clamp(9), 9);
    }

    #[test]
    fn the_lowest_ceiling_binds() {
        let mut ceilings = CompactionCeilings::default();
        let (archive, pin) = (Arc::new(AtomicU64::new(7)), Arc::new(AtomicU64::new(3)));
        ceilings.set("archive", Arc::clone(&archive));
        ceilings.set("pin", Arc::clone(&pin));
        assert_eq!(ceilings.clamp(9), 3);
        pin.store(u64::MAX, Ordering::Release);
        assert_eq!(ceilings.clamp(9), 7);
        archive.store(u64::MAX, Ordering::Release);
        assert_eq!(ceilings.clamp(9), 9);
    }

    #[test]
    fn a_second_set_replaces_the_owners_ceiling() {
        let mut ceilings = CompactionCeilings::default();
        ceilings.set("archive", Arc::new(AtomicU64::new(1)));
        ceilings.set("archive", Arc::new(AtomicU64::new(5)));
        assert_eq!(ceilings.clamp(9), 5);
    }
}
