// SPDX-License-Identifier: BUSL-1.1

//! Per-row sidecars of the staging overlay: the KV TTL delta, the
//! bitemporal stamp, the displaced columnar base key and a timeseries
//! batch's default timestamp. Each lives beside
//! [`Staged`](super::Staged) rather than inside it because only one engine
//! reads it.

use std::borrow::Cow;

use nodedb_types::RowIdentity;
use zerompk::{Read, Write};

use super::staged::{CollKey, TxnOverlay};
use super::staged_journal::JournalEntry;
use crate::data::executor::handlers::spill::tiered::SpillWeight;
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;

/// A staged TTL delta for one KV row, kept OUTSIDE `Staged` because TTL is
/// KV-specific (only KV entries carry `expire_at_ms`,
/// `engine/kv/entry.rs::KvEntry.expire_at_ms`) while `Staged` is shared by
/// every engine's read-merge. Only KV reads ever consult this map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct BitemporalStamp {
    /// System-time ordinal the version row is keyed at. `_ts_system`
    /// renders its millisecond.
    pub sys_from: i64,
    /// Valid-time lower bound (`i64::MIN` = unbounded).
    pub valid_from_ms: i64,
    /// Valid-time upper bound (`i64::MAX` = unbounded).
    pub valid_until_ms: i64,
}

impl SpillWeight for StagedTtl {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<StagedTtl>()
    }
}

impl SpillWeight for BitemporalStamp {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<BitemporalStamp>()
    }
}

/// The MessagePack primary key of a displaced columnar base row. Spills as
/// MessagePack `bin`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PkBytes(pub(super) Vec<u8>);

impl zerompk::ToMessagePack for PkBytes {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_binary(&self.0)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for PkBytes {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        Ok(Self(reader.read_binary()?.into_owned()))
    }
}

impl SpillWeight for PkBytes {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<Vec<u8>>() + self.0.len()
    }
}

impl TxnOverlay {
    /// Stage a KV TTL delta (`EXPIRE` / `PERSIST`) for `surrogate` in the
    /// given collection, binding `doc_id` to `surrogate` the same way
    /// `insert_put` / `insert_tombstone` do — a `GetTtl` (or a later
    /// `Expire`/`Persist`/`Incr` in the same transaction) resolves the same
    /// slot by the KV row's identity.
    pub fn set_ttl(
        &mut self,
        coll_key: CollKey,
        surrogate: u32,
        doc_id: &RowIdentity,
        ttl: StagedTtl,
    ) -> crate::Result<()> {
        let seq = self.record_undo(&coll_key, surrogate, doc_id)?;
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.ttl_by_surrogate.insert(surrogate, ttl, seq);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.clone(), surrogate, seq);
        self.spill_past_threshold()
    }

    /// Look up the staged TTL delta for `surrogate` in the given collection.
    pub fn get_ttl(&self, coll_key: &CollKey, surrogate: u32) -> crate::Result<Option<StagedTtl>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        Ok(overlay
            .ttl_by_surrogate
            .get(&surrogate, self.space.reader())?
            .map(Cow::into_owned))
    }

    /// Look up the staged TTL delta for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_ttl_by_doc_id(
        &self,
        coll_key: &CollKey,
        doc_id: &RowIdentity,
    ) -> crate::Result<Option<StagedTtl>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        let reader = self.space.reader();
        let Some(surrogate) = overlay.doc_id_to_surrogate.get(doc_id, reader)? else {
            return Ok(None);
        };
        Ok(overlay
            .ttl_by_surrogate
            .get(&*surrogate, reader)?
            .map(Cow::into_owned))
    }

    /// Every staged TTL delta of `coll_key` whose row has no staged value:
    /// an `EXPIRE` or `PERSIST` of a base row. Yields the row's client
    /// identity and its delta.
    pub fn iter_ttl_only_for_collection<'a>(
        &'a self,
        coll_key: &CollKey,
    ) -> impl Iterator<Item = crate::Result<(RowIdentity, StagedTtl)>> + 'a {
        let poisoned = self.guard().err();
        let overlay = match poisoned {
            Some(_) => None,
            None => self.collections.get(coll_key),
        };
        let reader = self.space.reader();
        let entries = overlay.into_iter().flat_map(move |overlay| {
            overlay
                .doc_id_to_surrogate
                .iter()
                .filter_map(move |binding| {
                    let ttl_only = |(doc_id, surrogate): (RowIdentity, u32)| -> crate::Result<Option<(RowIdentity, StagedTtl)>> {
                        if overlay.by_surrogate.contains(&surrogate, reader)? {
                            return Ok(None);
                        }
                        Ok(overlay
                            .ttl_by_surrogate
                            .get(&surrogate, reader)?
                            .map(|ttl| (doc_id, ttl.into_owned())))
                    };
                    match binding.and_then(|(doc_id, surrogate)| {
                        ttl_only((doc_id, surrogate.into_owned()))
                    }) {
                        Ok(Some(entry)) => Some(Ok(entry)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }
                })
        });
        poisoned.map(Err).into_iter().chain(entries)
    }

    /// Record the resolve-time bitemporal stamp for `surrogate` in the given
    /// collection. Assigned exactly once, at COMMIT resolve, after all
    /// savepoint activity for the transaction has completed — so no undo
    /// journalling is needed (it is never rolled back mid-statement).
    pub fn set_bitemporal(
        &mut self,
        coll_key: &CollKey,
        surrogate: u32,
        stamp: BitemporalStamp,
    ) -> crate::Result<()> {
        self.guard()?;
        let seq = self.journal.next_seq() as u64;
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .bitemporal_by_surrogate
            .insert(surrogate, stamp, seq);
        self.spill_past_threshold()
    }

    /// Look up the resolve-time version stamp for `surrogate` in the given
    /// collection. `Some` for every staged put and tombstone once its
    /// resolve pinned the stamp.
    pub fn get_bitemporal(
        &self,
        coll_key: &CollKey,
        surrogate: u32,
    ) -> crate::Result<Option<BitemporalStamp>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        Ok(overlay
            .bitemporal_by_surrogate
            .get(&surrogate, self.space.reader())?
            .map(Cow::into_owned))
    }

    /// Record the primary key of the base row `surrogate` names, the first
    /// time a statement stages that base row. A base row's key does not change
    /// inside the transaction, so a later record for the same surrogate is
    /// dropped and no undo journalling is needed. The record carries the
    /// journal position, so a rollback past the statement that recorded it
    /// drops it, and the next statement that stages the row records it again.
    pub fn note_base_pk(
        &mut self,
        coll_key: &CollKey,
        surrogate: u32,
        pk_msgpack: Vec<u8>,
    ) -> crate::Result<()> {
        self.guard()?;
        let seq = self.journal.next_seq() as u64;
        let reader = self.space.reader();
        let overlay = self.collections.entry(coll_key.clone()).or_default();
        if overlay.base_pk_by_surrogate.contains(&surrogate, reader)? {
            return Ok(());
        }
        overlay
            .base_pk_by_surrogate
            .insert(surrogate, PkBytes(pk_msgpack), seq);
        self.spill_past_threshold()
    }

    /// The primary key (MessagePack) of the base row a staged columnar write
    /// displaced for `surrogate`. `None` when the transaction staged no base
    /// row under that surrogate.
    pub fn base_pk(&self, coll_key: &CollKey, surrogate: u32) -> crate::Result<Option<Vec<u8>>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        Ok(overlay
            .base_pk_by_surrogate
            .get(&surrogate, self.space.reader())?
            .map(|pk| pk.into_owned().0))
    }

    /// Record the instant a staged timeseries ingest read as its default row
    /// timestamp, under the batch's first surrogate. Surrogates are fresh per
    /// staged row, so no later statement records the same key and no undo
    /// journalling is needed.
    pub fn note_ingest_now(&mut self, coll_key: &CollKey, first_surrogate: u32, now_ms: i64) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .ingest_now_by_surrogate
            .insert(first_surrogate, now_ms);
    }

    /// The default row timestamp the staged ingest whose first row is
    /// `first_surrogate` read at its statement.
    pub fn ingest_now(&self, coll_key: &CollKey, first_surrogate: u32) -> Option<i64> {
        self.collections
            .get(coll_key)?
            .ingest_now_by_surrogate
            .get(&first_surrogate)
            .copied()
    }

    /// Record the instant a staged unkeyed timeseries ingest read as its
    /// default row timestamp. A savepoint rollback removes it.
    pub fn note_unkeyed_ingest_now(&mut self, coll_key: &CollKey, now_ms: i64) {
        self.collections
            .entry(coll_key.clone())
            .or_default()
            .unkeyed_ingest_now
            .push(now_ms);
        self.journal.push(JournalEntry::UnkeyedIngest {
            coll_key: coll_key.clone(),
        });
    }

    /// Record the schema a staged timeseries ingest into `coll_key`
    /// previewed to. A savepoint rollback restores the schema it replaced.
    pub fn note_ts_preview_schema(&mut self, coll_key: &CollKey, schema: ColumnarSchema) {
        let overlay = self.collections.entry(coll_key.clone()).or_default();
        let prev = overlay.ts_preview_schema.replace(schema);
        self.journal.push(JournalEntry::PreviewSchema {
            coll_key: coll_key.clone(),
            prev,
        });
    }

    /// The schema the transaction's last staged timeseries ingest into
    /// `coll_key` previewed to, if one did.
    pub fn ts_preview_schema(&self, coll_key: &CollKey) -> Option<&ColumnarSchema> {
        self.collections.get(coll_key)?.ts_preview_schema.as_ref()
    }

    /// The instant the `ordinal`-th unkeyed ingest into `coll_key` read.
    pub fn unkeyed_ingest_now(&self, coll_key: &CollKey, ordinal: usize) -> Option<i64> {
        self.collections
            .get(coll_key)?
            .unkeyed_ingest_now
            .get(ordinal)
            .copied()
    }
}
