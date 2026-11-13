// SPDX-License-Identifier: BUSL-1.1

//! What a WAL reader outside the manager opens: the directory and the key
//! ring the segments were written under.

use std::path::PathBuf;

/// The WAL directory and key ring of one node.
#[derive(Clone)]
pub struct WalReadSource {
    pub dir: PathBuf,
    /// The ring every encrypted segment opens with. `None` for a WAL that
    /// never encrypted.
    pub ring: Option<nodedb_wal::crypto::KeyRing>,
}

impl WalReadSource {
    /// The source of a WAL in `dir` that never encrypted.
    pub fn unencrypted(dir: PathBuf) -> Self {
        Self { dir, ring: None }
    }
}

impl std::fmt::Debug for WalReadSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WalReadSource")
            .field("dir", &self.dir)
            .field("encrypted", &self.ring.is_some())
            .finish()
    }
}
