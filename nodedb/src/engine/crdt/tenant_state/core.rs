// SPDX-License-Identifier: BUSL-1.1

//! TenantCrdtEngine core: the struct, its tenant-wide constraint lookup view,
//! construction, and per-collection state access.
//!
//! Each `(tenant, collection)` owns its own `LoroDoc` (one [`CrdtState`] per
//! collection). The validator, dead-letter queue and the cross-engine array
//! surrogate registry stay tenant-wide because UNIQUE / FK constraints are
//! cross-collection (and FK referents may be array-engine rows).
//!
//! Behaviour lives in sibling modules: `apply` (delta apply), `snapshot_io`
//! (snapshot export/import), `constraints` (constraint installation and
//! fencing), and `rows` (row reads, history, purge).

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::row_lookup::RowLookup;
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::Validator;

use crate::types::TenantId;

/// Tenant-wide row/field lookup view passed to the constraint validator.
///
/// Row existence (FK / BiTemporalFK) is satisfied by ANY collection's doc OR
/// by the tenant's array-surrogate registry (cross-engine FK referents).
/// Field-value uniqueness probes are per-collection only — array surrogates are
/// not document rows and never participate in UNIQUE checks.
pub(super) struct TenantRowLookup<'a> {
    pub(super) collections: &'a HashMap<String, CrdtState>,
    pub(super) array_surrogate_ids: &'a HashSet<String>,
}

impl RowLookup for TenantRowLookup<'_> {
    fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.row_exists(collection, row_id))
            || self.array_surrogate_ids.contains(row_id)
    }

    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.field_value_exists(collection, field, value, exclude_row_id))
    }

    fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.field_value_exists_live(collection, field, value, exclude_row_id))
    }
}

/// Per-tenant CRDT engine state.
pub struct TenantCrdtEngine {
    pub(super) tenant_id: TenantId,

    /// Peer ID used to construct each per-collection [`CrdtState`] lazily.
    pub(super) peer_id: u64,

    /// Tenant-wide cross-engine FK registry: array-engine surrogate IDs that
    /// count as live referents for `ForeignKey` / `BiTemporalFK` checks.
    pub(super) array_surrogate_ids: HashSet<String>,

    /// Constraint validator with DLQ and policy registry (tenant-wide).
    pub(crate) validator: Validator,

    /// Per-collection committed CRDT state — one `LoroDoc` per collection.
    pub(super) collections: HashMap<String, CrdtState>,

    /// Last constraint-set version installed per collection. Acts as a
    /// monotonic fence on constraint installs: a constraint change is applied
    /// only when its `constraint_version` is `>=` the version last installed
    /// for the collection. This makes proposer-ordering races harmless — a
    /// stale set re-proposed at a higher data-log index can never clobber a
    /// newer one. Collections absent from the map are treated as version `0`.
    pub(super) constraint_versions: HashMap<String, u64>,

    /// Per-collection validation candidate retained between applies.
    ///
    /// A delta is validated by importing it into a copy of the collection, so
    /// a rejection can be dropped without touching authoritative state. Copying
    /// costs a full encode and decode of the collection, so the copy is kept
    /// alive across a run of deltas and rebuilt only when a delta is refused.
    /// Cleared by `clear_apply_candidates` when the run ends.
    pub(super) apply_candidates: HashMap<String, CrdtState>,

    /// The dead-letter entry the latest validated apply enqueued. The applier
    /// binds it to the record that carried the delta.
    pub(super) last_dead_letter: Option<u64>,
}

impl TenantCrdtEngine {
    /// Create a new engine for a tenant with the given peer ID and constraints.
    pub fn new(
        tenant_id: TenantId,
        peer_id: u64,
        constraints: ConstraintSet,
    ) -> crate::Result<Self> {
        Ok(Self {
            tenant_id,
            peer_id,
            array_surrogate_ids: HashSet::new(),
            validator: Validator::new(constraints, 1000),
            collections: HashMap::new(),
            constraint_versions: HashMap::new(),
            apply_candidates: HashMap::new(),
            last_dead_letter: None,
        })
    }

    /// Get the peer ID for this CRDT engine.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Derive this collection's Loro peer id from the node's peer id.
    ///
    /// Loro operation identity is `(peer_id, counter)`, and each collection's
    /// document counts its own counter from zero. Giving every collection the
    /// node's peer id verbatim therefore mints the SAME operation id for
    /// unrelated writes in different collections. Any consumer that merges two
    /// of this tenant's collections into one document — which is exactly how an
    /// embedded peer stores them — sees the second operation as a replay of the
    /// first and silently drops one of the rows.
    ///
    /// The derivation is a pure function of `(base_peer_id, collection)`, so
    /// every replica computes the identical id for the identical collection and
    /// the Raft-applied path stays deterministic. Zero is avoided because Loro
    /// treats it as an unset peer.
    pub(super) fn collection_peer_id(base_peer_id: u64, collection: &str) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
        const FNV_PRIME: u64 = 1099511628211;

        let mut hash = FNV_OFFSET_BASIS;
        for byte in base_peer_id.to_le_bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        for byte in collection.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        // Loro reserves the top bit of a peer id, and 0 reads as "unset".
        let id = hash & ((1u64 << 63) - 1);
        if id == 0 { 1 } else { id }
    }

    /// Lazily get (creating if absent) the per-collection state. Propagates the
    /// `CrdtState::new` error rather than panicking.
    pub(super) fn state_mut(&mut self, collection: &str) -> crate::Result<&mut CrdtState> {
        let peer_id = Self::collection_peer_id(self.peer_id, collection);
        match self.collections.entry(collection.to_string()) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let state = CrdtState::new(peer_id).map_err(crate::Error::Crdt)?;
                Ok(e.insert(state))
            }
        }
    }

    /// Names of every collection that currently has local CRDT state.
    pub fn collection_names(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Register an array-engine surrogate ID as a valid cross-engine FK
    /// referent for this tenant.
    pub fn register_array_surrogate(&mut self, id: impl Into<String>) {
        self.array_surrogate_ids.insert(id.into());
    }
}
