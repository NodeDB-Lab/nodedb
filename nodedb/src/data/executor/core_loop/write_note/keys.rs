// SPDX-License-Identifier: BUSL-1.1

//! The stamp a committed write is noted with.

use crate::types::{Lsn, VShardId};

/// Row identity type, re-exported from its plane-neutral home
/// ([`crate::types::KeyRepr`]) so Data-Plane call sites can keep referring to
/// it through this module. Read keys and write keys share this one namespace.
pub use crate::types::KeyRepr;

/// What one committed write is noted with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteStamp {
    /// The vShard the write applied on.
    pub vshard: VShardId,
    /// The LSN of the write's WAL record on this node.
    pub lsn: Lsn,
}
