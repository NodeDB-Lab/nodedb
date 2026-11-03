// SPDX-License-Identifier: BUSL-1.1

//! A node's leadership, published where a reader needs no lock on the node.
//!
//! The node writes its mark at each role change. A reader holds the mark and
//! reads it without taking the lock that guards the node itself.

use std::sync::Mutex;

/// A leadership term this node holds in a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Leadership {
    /// The term this node leads.
    pub term: u64,
    /// Index of the no-op this node appended when it won the term.
    pub term_start: Option<u64>,
}

/// The leadership a node currently holds, or `None` while it leads nothing.
#[derive(Debug, Default)]
pub struct LeaderMark(Mutex<Option<Leadership>>);

impl LeaderMark {
    /// The leadership held now.
    pub fn get(&self) -> Option<Leadership> {
        *self.0.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn set(&self, leadership: Option<Leadership>) {
        *self.0.lock().unwrap_or_else(|p| p.into_inner()) = leadership;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_mark_holds_what_was_set() {
        let mark = LeaderMark::default();
        assert_eq!(mark.get(), None);
        let held = Leadership {
            term: 4,
            term_start: Some(9),
        };
        mark.set(Some(held));
        assert_eq!(mark.get(), Some(held));
        mark.set(None);
        assert_eq!(mark.get(), None);
    }
}
