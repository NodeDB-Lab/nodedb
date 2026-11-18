// SPDX-License-Identifier: BUSL-1.1

//! The spill directory of one stream marker's install, from its validate
//! to its last event.

use std::path::{Path, PathBuf};

use crate::data::executor::handlers::spill::cleanup::{SpillKind, remove_spill_dir};
use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::handlers::spill::tiered::SpillSpace;
use crate::types::Lsn;

/// Where the spools of one install spill:
/// `data_dir/txn-spill/core-<core_id>/redo-install-<lsn>/`. The directory
/// is created on the first spill, and goes when this drops. A space that
/// never spilled touches no file.
pub(in crate::data::executor::handlers::transaction::redo_apply) struct InstallSpace {
    space: SpillSpace,
    dir: PathBuf,
}

impl InstallSpace {
    /// The space of the install at `lsn` on core `core_id`. Its spools
    /// spill past `threshold` hot bytes.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn new(
        data_dir: &Path,
        core_id: usize,
        lsn: Lsn,
        threshold: usize,
    ) -> Self {
        let dir = SpillKind::Txn
            .core_dir(data_dir, core_id)
            .join(format!("redo-install-{}", lsn.as_u64()));
        Self {
            space: SpillSpace::new(dir.clone(), "held", threshold),
            dir,
        }
    }

    pub(in crate::data::executor::handlers::transaction::redo_apply) fn space_mut(
        &mut self,
    ) -> &mut SpillSpace {
        &mut self.space
    }

    pub(in crate::data::executor::handlers::transaction::redo_apply) fn reader(
        &self,
    ) -> &BlockReader {
        self.space.reader()
    }
}

impl Drop for InstallSpace {
    fn drop(&mut self) {
        if !self.space.created() {
            return;
        }
        // A removal that fails files its own faultbox report, and the boot
        // sweep removes the directory at the next open.
        let _removed = remove_spill_dir(SpillKind::Txn, &self.dir);
    }
}
