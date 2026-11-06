// SPDX-License-Identifier: BUSL-1.1

//! The Control Plane's live mirror state.

use std::sync::Arc;

use super::fence::MirrorFence;
use super::registry::MirrorLinkRegistry;

/// The live state of this node's mirror databases.
#[derive(Default)]
pub struct MirrorState {
    /// One observer link per mirror following a source cluster.
    /// `ALTER DATABASE PROMOTE` and `DROP DATABASE` tear one down.
    pub links: Arc<MirrorLinkRegistry>,
    /// The databases that refuse client writes.
    pub fence: Arc<MirrorFence>,
}
