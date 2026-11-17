// SPDX-License-Identifier: BUSL-1.1

//! Removal of spill directories and files.
//!
//! Spill files are never durable state. A removal that fails files a
//! faultbox report at this site and returns [`SpillError::NotRemoved`]: a
//! leaked spill directory is never a log line only. Directory and unlink
//! calls are bounded metadata operations, so they stay `std::fs` on the
//! Data Plane.

use std::path::{Path, PathBuf};

use super::error::SpillError;

/// The owner of a spill directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::data::executor) enum SpillKind {
    /// A transaction's staging overlay.
    Txn,
    /// A grace-hash join.
    Join,
    /// An external sort.
    Sort,
    /// A spilled GROUP BY.
    GroupBy,
    /// A full-text rebuild's scratch database.
    TextRebuild,
}

impl SpillKind {
    /// Every spill owner, in sweep order.
    pub(in crate::data::executor) const ALL: [SpillKind; 5] = [
        Self::Txn,
        Self::Join,
        Self::Sort,
        Self::GroupBy,
        Self::TextRebuild,
    ];

    /// The owner's label in reports.
    pub(in crate::data::executor) fn as_str(self) -> &'static str {
        match self {
            Self::Txn => "txn",
            Self::Join => "join",
            Self::Sort => "sort",
            Self::GroupBy => "groupby",
            Self::TextRebuild => "text-rebuild",
        }
    }

    /// The directory under the data directory that holds every core's spill
    /// directories of this owner.
    pub(in crate::data::executor) fn root(self) -> &'static str {
        match self {
            Self::Txn => "txn-spill",
            Self::Join => "join-spill",
            Self::Sort => "sort-spill",
            Self::GroupBy => "groupby-spill",
            Self::TextRebuild => "text-rebuild-spill",
        }
    }

    /// The spill directory of this owner for `core_id`.
    pub(in crate::data::executor) fn core_dir(self, data_dir: &Path, core_id: usize) -> PathBuf {
        data_dir.join(self.root()).join(format!("core-{core_id}"))
    }
}

/// Remove `dir` and everything under it. A directory that does not exist is
/// already removed.
pub(in crate::data::executor) fn remove_spill_dir(
    kind: SpillKind,
    dir: &Path,
) -> Result<(), SpillError> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            crate::diag::spill_cleanup_failed(&e, kind.as_str(), "dir", dir);
            Err(SpillError::NotRemoved {
                path: dir.to_path_buf(),
                source: e,
            })
        }
    }
}

/// Remove `dir` when it holds no entry. A directory that does not exist is
/// already removed. A directory that still holds an entry is an error.
pub(in crate::data::executor) fn remove_empty_spill_dir(
    kind: SpillKind,
    dir: &Path,
) -> Result<(), SpillError> {
    match std::fs::remove_dir(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            crate::diag::spill_cleanup_failed(&e, kind.as_str(), "dir", dir);
            Err(SpillError::NotRemoved {
                path: dir.to_path_buf(),
                source: e,
            })
        }
    }
}

/// Remove one spill file. A file that does not exist is already removed.
pub(in crate::data::executor) fn remove_spill_file(
    kind: SpillKind,
    path: &Path,
) -> Result<(), SpillError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            crate::diag::spill_cleanup_failed(&e, kind.as_str(), "file", path);
            Err(SpillError::NotRemoved {
                path: path.to_path_buf(),
                source: e,
            })
        }
    }
}

/// Remove every spill directory `core_id` owns. Runs at core open: no spill
/// file outlives the process that wrote it.
pub(in crate::data::executor) fn sweep_core_spill_dirs(
    data_dir: &Path,
    core_id: usize,
) -> Result<(), SpillError> {
    for kind in SpillKind::ALL {
        remove_spill_dir(kind, &kind.core_dir(data_dir, core_id))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_removes_every_spill_root_of_the_core_only() {
        let data = tempfile::tempdir().unwrap();
        for kind in SpillKind::ALL {
            let mine = kind.core_dir(data.path(), 3).join("leftover");
            std::fs::create_dir_all(&mine).unwrap();
            std::fs::write(mine.join("run-0.spill"), b"x").unwrap();
            std::fs::create_dir_all(kind.core_dir(data.path(), 4)).unwrap();
        }

        sweep_core_spill_dirs(data.path(), 3).unwrap();

        for kind in SpillKind::ALL {
            assert!(!kind.core_dir(data.path(), 3).exists());
            assert!(kind.core_dir(data.path(), 4).exists());
        }
    }

    #[test]
    fn sweep_of_a_core_with_no_spill_dirs_succeeds() {
        let data = tempfile::tempdir().unwrap();
        sweep_core_spill_dirs(data.path(), 0).unwrap();
    }

    #[test]
    fn a_cleanup_that_cannot_remove_returns_an_error() {
        let data = tempfile::tempdir().unwrap();
        // A non-empty directory cannot be removed as an empty one.
        let dir = data.path().join("d");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("f"), b"x").unwrap();
        let err = remove_empty_spill_dir(SpillKind::Sort, &dir).unwrap_err();
        assert!(matches!(err, SpillError::NotRemoved { .. }));
        // A directory where a file is expected cannot be unlinked.
        let err = remove_spill_file(SpillKind::Txn, &dir).unwrap_err();
        assert!(matches!(err, SpillError::NotRemoved { .. }));
        assert!(
            crate::Error::from(err)
                .to_string()
                .contains("spill cleanup")
        );
    }
}
