// SPDX-License-Identifier: BUSL-1.1

//! Scoped identity types for array CRDT sync.
//!
//! Array sync state is keyed by the full `(database, tenant, array)` identity
//! so two arrays sharing a name in different databases can never alias each
//! other. These aliases name that key once instead of repeating the tuple at
//! every map, field, and signature that carries it.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use nodedb_array::sync::hlc::Hlc;

use crate::types::{DatabaseId, TenantId, VShardId};

/// Full scoped identity of an array: `(database, tenant, array name)`.
///
/// Owned key form, for use as a `HashMap` key.
pub type ArrayScopeKey = (DatabaseId, u64, String);

/// Per-array GC boundary HLC, keyed by scoped identity.
///
/// `Hlc::ZERO` means no GC has occurred for that array. Shared between the GC
/// task (writer) and the outbound fan-out (reader, to decide when a lagging
/// subscriber must fall back to catch-up).
pub type ArraySnapshotHlcs = Arc<RwLock<HashMap<ArrayScopeKey, Hlc>>>;

/// Borrowed scoped identity of the array a replicated op targets.
///
/// Borrowed rather than owned so the Raft apply path does not allocate a
/// `String` per applied op.
#[derive(Debug, Clone, Copy)]
pub struct ArrayOpTarget<'a> {
    /// Authenticated tenant that owns the array.
    pub tenant_id: TenantId,
    /// Database the array lives in.
    pub database_id: DatabaseId,
    /// Array name, unique within `(database_id, tenant_id)`.
    pub array: &'a str,
    /// The vShard the entry was proposed to: the tile route of the op's
    /// cell. Every replica applies the cell on it and places it there.
    pub vshard: VShardId,
}

/// Authenticated `(database, tenant)` scope an array sync server serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrayServerScope {
    /// Database the server is bound to.
    pub database_id: DatabaseId,
    /// Tenant the server is bound to.
    pub tenant_id: u64,
}

impl ArrayServerScope {
    /// Construct a scope from its parts.
    pub fn new(database_id: DatabaseId, tenant_id: u64) -> Self {
        Self {
            database_id,
            tenant_id,
        }
    }
}
