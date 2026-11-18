// SPDX-License-Identifier: BUSL-1.1

//! The spill directory of one transaction overlay, its hot-tier threshold,
//! and the block reader its lookups share.
//!
//! The directory is `data_dir/txn-spill/core-N/<txn_id>/`. It is created on
//! the first spill. The overlay teardown removes it.

use std::path::PathBuf;

use crate::data::executor::handlers::spill::error::SpillError;
use crate::data::executor::handlers::spill::run::BlockReader;

/// Where one overlay writes its spill files.
#[derive(Debug)]
pub(in crate::data::executor) struct SpillSpace {
    dir: PathBuf,
    /// File-name prefix that keeps overlays of one transaction apart.
    prefix: &'static str,
    next_file: u64,
    created: bool,
    /// Hot-tier bytes past which the overlay spills.
    threshold: usize,
    reader: BlockReader,
}

impl SpillSpace {
    /// A space under `dir` that spills past `threshold` hot bytes.
    pub(in crate::data::executor) fn new(
        dir: PathBuf,
        prefix: &'static str,
        threshold: usize,
    ) -> Self {
        Self {
            dir,
            prefix,
            next_file: 0,
            created: false,
            threshold,
            reader: BlockReader::default(),
        }
    }

    /// A space that never spills: its threshold is `usize::MAX`, so its
    /// directory is never created.
    #[cfg(test)]
    pub(in crate::data::executor) fn never_spills() -> Self {
        Self::new(
            std::env::temp_dir().join("nodedb-overlay-never-spills"),
            "test",
            usize::MAX,
        )
    }

    /// Whether a spill created the directory.
    pub(in crate::data::executor) fn created(&self) -> bool {
        self.created
    }

    /// The directory the spills write into.
    pub(in crate::data::executor) fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    pub(in crate::data::executor) fn threshold(&self) -> usize {
        self.threshold
    }

    pub(in crate::data::executor) fn reader(&self) -> &BlockReader {
        &self.reader
    }

    /// The path of the next spill file, tagged `tag`. Creates the
    /// directory on first use.
    pub(in crate::data::executor) fn next_path(&mut self, tag: &str) -> crate::Result<PathBuf> {
        if !self.created {
            std::fs::create_dir_all(&self.dir).map_err(|e| SpillError::Io {
                action: "create directory",
                path: self.dir.clone(),
                source: e,
            })?;
            self.created = true;
        }
        let path = self
            .dir
            .join(format!("{}-{tag}-{}.spill", self.prefix, self.next_file));
        self.next_file += 1;
        Ok(path)
    }
}
