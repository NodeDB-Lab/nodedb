// SPDX-License-Identifier: BUSL-1.1

//! The leadership of each mounted group, readable without the `MultiRaft` lock.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_raft::{LeaderMark, Leadership};

/// Each mounted group's leader mark. A clone shares the same marks.
#[derive(Debug, Clone, Default)]
pub struct LeaderBoard {
    marks: Arc<RwLock<HashMap<u64, Arc<LeaderMark>>>>,
}

impl LeaderBoard {
    pub(super) fn mount(&self, group_id: u64, mark: Arc<LeaderMark>) {
        self.marks
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(group_id, mark);
    }

    pub(super) fn unmount(&self, group_id: u64) {
        self.marks
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&group_id);
    }

    /// The term this node leads in `group_id`, or `None` while it leads
    /// nothing there or the group is not mounted.
    pub fn leadership(&self, group_id: u64) -> Option<Leadership> {
        self.marks
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)?
            .get()
    }
}
