// SPDX-License-Identifier: BUSL-1.1

//! The keys the write-version index records versions under, and the stamp a
//! committed write records them with.

use nodedb_types::{DatabaseId, TenantId, WriteVersion};

use crate::types::{Lsn, VShardId};

/// Row identity type, re-exported from its plane-neutral home
/// ([`crate::types::KeyRepr`]) so Data-Plane call sites can keep referring to
/// it through this module. Read keys and write keys share this one namespace.
pub use crate::types::KeyRepr;

/// Fully-qualified per-key version-index key. Scoped by `(database, tenant)`
/// exactly like the write path, so two tenants (or databases) never alias.
/// A version positions a write within one vShard, so the vShard is part of
/// the key: an edge written on two endpoint vShards holds one version on
/// each.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteKey {
    pub vshard: VShardId,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
    pub key: KeyRepr,
}

/// Fully-qualified per-collection version-index key, on one vShard.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollKey {
    pub vshard: VShardId,
    pub db: DatabaseId,
    pub tenant: TenantId,
    pub collection: Box<str>,
}

/// What the version index records one committed write under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteStamp {
    /// The vShard whose history the write extends.
    pub vshard: VShardId,
    /// The LSN of the write's WAL record on this node.
    pub lsn: Lsn,
    /// The log position of the data-group entry the write applies. `None`
    /// for a write that applies no entry.
    pub entry: Option<WriteVersion>,
}
