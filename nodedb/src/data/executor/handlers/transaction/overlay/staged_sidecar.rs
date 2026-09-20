// SPDX-License-Identifier: BUSL-1.1

//! Per-row sidecars of the staging overlay: the KV TTL delta and the
//! bitemporal stamp. Both live beside [`Staged`](super::Staged) rather than
//! inside it because only one engine reads each.

use nodedb_types::RowIdentity;

use super::staged::TxnOverlay;
use crate::types::{DatabaseId, TenantId};

/// A staged TTL delta for one KV row, kept OUTSIDE `Staged` because TTL is
/// KV-specific (only KV entries carry `expire_at_ms`,
/// `engine/kv/entry.rs::KvEntry.expire_at_ms`) while `Staged` is shared by
/// every engine's read-merge. Only KV reads ever consult this map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagedTtl {
    /// `EXPIRE` staged: the row expires at this absolute epoch-ms instant.
    ExpireAt(u64),
    /// `PERSIST` staged: any base TTL is cleared, the row never expires.
    Persist,
}

/// The bitemporal system/valid-time stamp assigned to one staged document
/// `Put` at COMMIT resolve time, kept OUTSIDE `Staged` because it is only
/// meaningful for a `bitemporal=true` document collection (like [`StagedTtl`]
/// is only meaningful for KV).
///
/// Assigning it ONCE at resolve — rather than re-deriving it at both the
/// commit-time base install and WAL replay — is what keeps a normal restart
/// from writing a SECOND version of the same row: the redo sub-record carries
/// this stamp verbatim, and the base install reads the identical stamp back
/// out of the overlay sidecar so both agree on the version key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitemporalStamp {
    /// System-time key (`_ts_system`) the version row is appended at.
    pub sys_from_ms: i64,
    /// Valid-time lower bound (`i64::MIN` = unbounded).
    pub valid_from_ms: i64,
    /// Valid-time upper bound (`i64::MAX` = unbounded).
    pub valid_until_ms: i64,
}

impl TxnOverlay {
    /// Stage a KV TTL delta (`EXPIRE` / `PERSIST`) for `surrogate` in the
    /// given collection, binding `doc_id` to `surrogate` the same way
    /// `insert_put` / `insert_tombstone` do — a `GetTtl` (or a later
    /// `Expire`/`Persist`/`Incr` in the same transaction) resolves the same
    /// slot by the KV row's identity.
    pub fn set_ttl(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &RowIdentity,
        ttl: StagedTtl,
    ) {
        self.record_undo(&coll_key, surrogate, doc_id);
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.ttl_by_surrogate.insert(surrogate, ttl);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate);
    }

    /// Look up the staged TTL delta for `surrogate` in the given collection.
    pub fn get_ttl(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<StagedTtl> {
        self.collections
            .get(coll_key)?
            .ttl_by_surrogate
            .get(&surrogate)
            .copied()
    }

    /// Look up the staged TTL delta for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_ttl_by_doc_id(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &RowIdentity,
    ) -> Option<StagedTtl> {
        let overlay = self.collections.get(coll_key)?;
        let surrogate = overlay.doc_id_to_surrogate.get(doc_id)?;
        overlay.ttl_by_surrogate.get(surrogate).copied()
    }

    /// Record the resolve-time bitemporal stamp for `surrogate` in the given
    /// collection. Assigned exactly once, at COMMIT resolve, after all
    /// savepoint activity for the transaction has completed — so no undo
    /// journalling is needed (it is never rolled back mid-statement).
    pub fn set_bitemporal(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        stamp: BitemporalStamp,
    ) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .bitemporal_by_surrogate
            .insert(surrogate, stamp);
    }

    /// Look up the resolve-time bitemporal stamp for `surrogate` in the given
    /// collection. `Some` only for a `bitemporal=true` collection's staged
    /// `Put` whose stamp was assigned at resolve.
    pub fn get_bitemporal(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<BitemporalStamp> {
        self.collections
            .get(coll_key)?
            .bitemporal_by_surrogate
            .get(&surrogate)
            .copied()
    }

    /// Iterate every `(surrogate, BitemporalStamp)` staged across all
    /// collections in this overlay. Surrogates are globally unique, so the
    /// commit-time install flattens these into one per-core scratch map.
    pub fn all_bitemporal_stamps(&self) -> impl Iterator<Item = (u32, BitemporalStamp)> + '_ {
        self.collections.values().flat_map(|overlay| {
            overlay
                .bitemporal_by_surrogate
                .iter()
                .map(|(surrogate, stamp)| (*surrogate, *stamp))
        })
    }
}
