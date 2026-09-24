// SPDX-License-Identifier: BUSL-1.1

//! Per-row sidecars of the staging overlay: the KV TTL delta, the
//! bitemporal stamp, the displaced columnar base key and a timeseries
//! batch's default timestamp. Each lives beside
//! [`Staged`](super::Staged) rather than inside it because only one engine
//! reads it.

use nodedb_types::RowIdentity;

use super::staged::{JournalEntry, TxnOverlay};
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

    /// Every staged TTL delta of `coll_key` whose row has no staged value:
    /// an `EXPIRE` or `PERSIST` of a base row. Yields the row's client
    /// identity and its delta.
    pub fn iter_ttl_only_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (&'a RowIdentity, StagedTtl)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| {
                overlay
                    .doc_id_to_surrogate
                    .iter()
                    .filter(move |(_, surrogate)| !overlay.by_surrogate.contains_key(surrogate))
                    .filter_map(move |(doc_id, surrogate)| {
                        overlay
                            .ttl_by_surrogate
                            .get(surrogate)
                            .map(|ttl| (doc_id, *ttl))
                    })
            })
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

    /// Record the primary key of the base row `surrogate` names, the first
    /// time a statement stages that base row. A base row's key does not change
    /// inside the transaction, so a later record for the same surrogate is
    /// dropped and no undo journalling is needed: after a savepoint rollback
    /// the recorded key still names the same base row.
    pub fn note_base_pk(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
        pk_msgpack: Vec<u8>,
    ) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .base_pk_by_surrogate
            .entry(surrogate)
            .or_insert(pk_msgpack);
    }

    /// The primary key (MessagePack) of the base row a staged columnar write
    /// displaced for `surrogate`. `None` when the transaction staged no base
    /// row under that surrogate.
    pub fn base_pk(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<&[u8]> {
        self.collections
            .get(coll_key)?
            .base_pk_by_surrogate
            .get(&surrogate)
            .map(Vec::as_slice)
    }

    /// Record the instant a staged timeseries ingest read as its default row
    /// timestamp, under the batch's first surrogate. Surrogates are fresh per
    /// staged row, so no later statement records the same key and no undo
    /// journalling is needed.
    pub fn note_ingest_now(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        first_surrogate: u32,
        now_ms: i64,
    ) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .ingest_now_by_surrogate
            .insert(first_surrogate, now_ms);
    }

    /// The default row timestamp the staged ingest whose first row is
    /// `first_surrogate` read at its statement.
    pub fn ingest_now(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        first_surrogate: u32,
    ) -> Option<i64> {
        self.collections
            .get(coll_key)?
            .ingest_now_by_surrogate
            .get(&first_surrogate)
            .copied()
    }

    /// Record the instant a staged unkeyed timeseries ingest read as its
    /// default row timestamp. A savepoint rollback removes it.
    pub fn note_unkeyed_ingest_now(
        &mut self,
        coll_key: &(DatabaseId, TenantId, String),
        now_ms: i64,
    ) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .unkeyed_ingest_now
            .push(now_ms);
        self.journal.push(JournalEntry::UnkeyedIngest {
            coll_key: coll_key.clone(),
        });
    }

    /// The instant the `ordinal`-th unkeyed ingest into `coll_key` read.
    pub fn unkeyed_ingest_now(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        ordinal: usize,
    ) -> Option<i64> {
        self.collections
            .get(coll_key)?
            .unkeyed_ingest_now
            .get(ordinal)
            .copied()
    }
}
