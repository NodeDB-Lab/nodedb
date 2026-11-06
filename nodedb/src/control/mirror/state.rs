// SPDX-License-Identifier: BUSL-1.1

//! The Control Plane's live mirror state.

use std::sync::Arc;

use std::sync::OnceLock;

use super::cut_watch::MirrorCutWatch;
use super::fence::MirrorFence;
use super::registry::MirrorLinkRegistry;
use super::source::pins::MirrorPinTable;
use crate::control::security::catalog::SystemCatalog;

/// The live state of this node's mirror databases.
#[derive(Default)]
pub struct MirrorState {
    /// One observer link per mirror following a source cluster.
    /// `ALTER DATABASE PROMOTE` and `DROP DATABASE` tear one down.
    pub links: Arc<MirrorLinkRegistry>,
    /// The databases that refuse client writes.
    pub fence: Arc<MirrorFence>,
    /// Each mirror's cut state, which decides its held entries.
    pub cuts: Arc<MirrorCutWatch>,
    /// Each link's hold on this cluster's Raft logs, as this cluster's mirrors
    /// follow it.
    pub pins: Arc<MirrorPinTable>,
    /// This cluster's id, once Raft started. A mirror verifies it at hello.
    pub cluster_id: OnceLock<u64>,
}

impl MirrorState {
    /// Rebuild what follows the catalog. A read error fails the caller: a
    /// fence that misses a mirror admits client writes to it, and a cut watch
    /// that misses one skips its entries.
    pub fn load_from_catalog(&self, catalog: &SystemCatalog) -> crate::Result<()> {
        self.fence.load_from_catalog(catalog)?;
        self.cuts.load_from_catalog(catalog)?;
        self.pins.load_from_catalog(catalog)
    }
}
