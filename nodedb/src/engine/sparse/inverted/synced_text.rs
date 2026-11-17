// SPDX-License-Identifier: BUSL-1.1

//! Text sync frames delivered, kept so a rebuild can re-derive them.
//!
//! A sync `FtsIndex` frame carries a document's `(field, text)` pairs. The
//! document store does not always hold the body they came from: a Lite
//! columnar row lands in the columnar engine, and a schemaless row's CRDT
//! delta travels apart from its frame and can arrive later or be refused.
//! A rebuild that read only stored bodies drops that text.
//!
//! Each frame stores every pair it carried, whatever fields the collection
//! declares, as one version at the frame's system-time ordinal. A frame
//! with no pairs and a sync delete store a tombstone version. A rebuild
//! replays every version of a document the document store holds no
//! version of, in ordinal order, so the text history survives with it.
//!
//! A collection purge and a tenant purge drop the rows. A rebuild's
//! data-only purge keeps them. The text-history purge drops the versions
//! below its horizon, with the inverted index's own history.

use redb::{ReadableDatabase as _, ReadableTable, WriteTransaction};

use nodedb_types::{Surrogate, TenantId};

use super::core::InvertedIndex;
use super::errors::inverted_err;
use super::indexing::IndexDocScope;
use crate::engine::sparse::btree::storage_bytes::SPARSE_COUNTER;
use crate::engine::sparse::fts_redb::backend::synced_rows::{put_synced_row, remove_synced_row};
use crate::engine::sparse::fts_redb::keys::{KeyOwner, SyncedKey};
use crate::engine::sparse::fts_redb::tables::{SYNCED_TEXT, SyncedTextKey};
use crate::engine::storage_bytes::StorageDelta;

/// One stored frame version, as a rebuild, a snapshot and a restore read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedDocText {
    pub collection: String,
    pub surrogate: Surrogate,
    /// The system-time ordinal the frame indexed at.
    pub ordinal: i64,
    /// Every `(field, text)` pair the frame carried. Empty for a tombstone.
    pub fields: Vec<(String, String)>,
}

impl SyncedDocText {
    /// Whether this version is a tombstone: a sync delete or a frame with
    /// no pairs.
    pub fn is_tombstone(&self) -> bool {
        self.fields.is_empty()
    }
}

/// The newest stored frame version of one document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncedTextHead {
    /// The system-time ordinal the version is stored at.
    pub ordinal: i64,
    /// Whether the version is a tombstone.
    pub tombstone: bool,
}

impl SyncedTextHead {
    fn of((ordinal, value): (i64, Vec<u8>)) -> Self {
        Self {
            ordinal,
            tombstone: value.is_empty(),
        }
    }
}

/// The stored frame versions of one document at one moment, as an undo
/// puts them back: the newest version's ordinal and stored value, and the
/// value at the ordinal a frame write names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncedTextMark {
    newest: Option<(i64, Vec<u8>)>,
    /// The ordinal a re-issued frame version writes, and the value stored
    /// there before it. `None` for a live frame, which writes above
    /// `newest`.
    at: Option<(i64, Option<Vec<u8>>)>,
}

impl InvertedIndex {
    /// Mark the stored frame versions of `surrogate`, for an undo of a
    /// frame write that runs after it. `at` is the source ordinal a
    /// re-issued frame version writes: it can sit below the newest one.
    pub fn synced_text_mark(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        at: Option<i64>,
    ) -> crate::Result<SyncedTextMark> {
        let txn = self
            .inner
            .backend()
            .db()
            .begin_read()
            .map_err(|e| inverted_err("synced text read txn", e))?;
        let table = txn
            .open_table(SYNCED_TEXT)
            .map_err(|e| inverted_err("open synced text", e))?;
        let (lo, hi) = doc_bounds(database_id, tid, collection, surrogate);
        let newest = newest_version(&table, lo, hi)?;
        let at = match at {
            Some(ordinal) => {
                let prior = table
                    .get((lo.0, lo.1, collection, lo.3, ordinal))
                    .map_err(|e| inverted_err("synced text get", e))?
                    .map(|value| value.value().to_vec());
                Some((ordinal, prior))
            }
            None => None,
        };
        Ok(SyncedTextMark { newest, at })
    }

    /// The newest stored frame version of `surrogate`, from one read
    /// snapshot. `None` when the document has no stored version.
    pub fn synced_text_head(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
    ) -> crate::Result<Option<SyncedTextHead>> {
        let txn = self
            .inner
            .backend()
            .db()
            .begin_read()
            .map_err(|e| inverted_err("synced text read txn", e))?;
        let table = txn
            .open_table(SYNCED_TEXT)
            .map_err(|e| inverted_err("open synced text", e))?;
        let (lo, hi) = doc_bounds(database_id, tid, collection, surrogate);
        Ok(newest_version(&table, lo, hi)?.map(SyncedTextHead::of))
    }

    /// [`Self::synced_text_head`] as `txn` sees it.
    pub fn synced_text_head_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
    ) -> crate::Result<Option<SyncedTextHead>> {
        let table = txn
            .open_table(SYNCED_TEXT)
            .map_err(|e| inverted_err("open synced text", e))?;
        let (lo, hi) = doc_bounds(doc.database_id, doc.tid, doc.collection, doc.surrogate);
        Ok(newest_version(&table, lo, hi)?.map(SyncedTextHead::of))
    }

    /// Put the stored frame versions of `surrogate` back to `mark`, in one
    /// write transaction: every version above the marked newest one goes,
    /// the marked write ordinal holds its marked value or nothing, and the
    /// marked newest one holds its marked value again. The undo of a frame
    /// write.
    pub fn restore_synced_text_mark(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        surrogate: Surrogate,
        mark: &SyncedTextMark,
    ) -> crate::Result<()> {
        // The frames a rebuild derives the document's text from change.
        self.note_doc_write(IndexDocScope {
            database_id,
            tid,
            collection,
            surrogate,
            ordinal: super::UNVERSIONED,
        });
        let txn = self
            .inner
            .backend()
            .db()
            .begin_write()
            .map_err(|e| inverted_err("synced text write txn", e))?;
        let mut delta = StorageDelta::default();
        {
            let mut table = txn
                .open_table(SYNCED_TEXT)
                .map_err(|e| inverted_err("open synced text", e))?;
            let (lo, hi) = doc_bounds(database_id, tid, collection, surrogate);
            let floor = mark.newest.as_ref().map(|(ordinal, _)| *ordinal);
            let mut above: Vec<i64> = Vec::new();
            for entry in table
                .range(lo..=hi)
                .map_err(|e| inverted_err("synced text range", e))?
            {
                let (key, _) = entry.map_err(|e| inverted_err("synced text entry", e))?;
                let ordinal = key.value().4;
                if floor.is_none_or(|floor| ordinal > floor) {
                    above.push(ordinal);
                }
            }
            for ordinal in above {
                remove_synced_row(
                    &mut table,
                    (lo.0, lo.1, collection, lo.3, ordinal),
                    &mut delta,
                )
                .map_err(|e| inverted_err("remove synced text", e))?;
            }
            // A re-issued version below the newest one leaves its slot as
            // the mark found it.
            if let Some((ordinal, prior)) = &mark.at {
                let key = (lo.0, lo.1, collection, lo.3, *ordinal);
                match prior {
                    Some(value) => put_synced_row(&mut table, key, value.as_slice(), &mut delta)
                        .map_err(|e| inverted_err("insert synced text", e))?,
                    None => remove_synced_row(&mut table, key, &mut delta)
                        .map_err(|e| inverted_err("remove synced text", e))?,
                }
            }
            if let Some((ordinal, value)) = &mark.newest {
                put_synced_row(
                    &mut table,
                    (lo.0, lo.1, collection, lo.3, *ordinal),
                    value.as_slice(),
                    &mut delta,
                )
                .map_err(|e| inverted_err("insert synced text", e))?;
            }
        }
        delta.apply(&txn, SPARSE_COUNTER)?;
        txn.commit()
            .map_err(|e| inverted_err("commit synced text undo", e))
    }

    /// Store `fields` as the synced-text version of `doc` at `doc.ordinal`,
    /// inside `txn`. Empty `fields` store a tombstone. A version already at
    /// that ordinal is replaced: a re-applied frame stores itself once.
    /// Does NOT commit.
    pub fn put_synced_text_in_txn(
        &self,
        txn: &WriteTransaction,
        doc: IndexDocScope<'_>,
        fields: &[(String, String)],
    ) -> crate::Result<()> {
        // A stored frame changes the text history a rebuild derives, even a
        // frame the caller stores without indexing it.
        self.note_doc_write(doc);
        let bytes = if fields.is_empty() {
            Vec::new()
        } else {
            zerompk::to_msgpack_vec(&fields.to_vec())
                .map_err(|e| inverted_err("serialize synced text", e))?
        };
        let mut delta = StorageDelta::default();
        {
            let mut table = txn
                .open_table(SYNCED_TEXT)
                .map_err(|e| inverted_err("open synced text", e))?;
            let key = (
                doc.database_id,
                doc.tid.as_u64(),
                doc.collection,
                doc.surrogate.as_u32(),
                doc.ordinal,
            );
            put_synced_row(&mut table, key, bytes.as_slice(), &mut delta)
                .map_err(|e| inverted_err("insert synced text", e))?;
        }
        delta.apply(txn, SPARSE_COUNTER)
    }

    /// Visit every stored frame version of one collection, or of the whole
    /// `(database, tenant)` when `collection` is `None`, from one read
    /// snapshot. Order is collection, then surrogate, then ordinal.
    pub fn for_each_synced_text(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: Option<&str>,
        mut visit: impl FnMut(SyncedDocText) -> crate::Result<()>,
    ) -> crate::Result<()> {
        let owner = match collection {
            Some(collection) => KeyOwner::collection(database_id, tid.as_u64(), collection),
            None => KeyOwner::tenant(database_id, tid.as_u64()),
        };
        let txn = self
            .inner
            .backend()
            .db()
            .begin_read()
            .map_err(|e| inverted_err("synced text read txn", e))?;
        let table = txn
            .open_table(SYNCED_TEXT)
            .map_err(|e| inverted_err("open synced text", e))?;
        let range = table
            .range(owner.synced_start()..)
            .map_err(|e| inverted_err("synced text range", e))?;
        for entry in range {
            let (key, value) = entry.map_err(|e| inverted_err("synced text entry", e))?;
            let (db, owner_tid, owner_collection, surrogate, ordinal) = key.value();
            if !owner.owns(db, owner_tid, owner_collection, None) {
                break;
            }
            let fields: Vec<(String, String)> = if value.value().is_empty() {
                Vec::new()
            } else {
                zerompk::from_msgpack(value.value())
                    .map_err(|e| inverted_err("decode synced text", e))?
            };
            visit(SyncedDocText {
                collection: owner_collection.to_string(),
                surrogate: Surrogate::new(surrogate),
                ordinal,
                fields,
            })?;
        }
        Ok(())
    }
}

/// The ordinal and stored value of the newest version in `lo..=hi`.
fn newest_version<T>(
    table: &T,
    lo: SyncedKey<'_>,
    hi: SyncedKey<'_>,
) -> crate::Result<Option<(i64, Vec<u8>)>>
where
    T: ReadableTable<SyncedTextKey, &'static [u8]>,
{
    Ok(table
        .range(lo..=hi)
        .map_err(|e| inverted_err("synced text range", e))?
        .next_back()
        .transpose()
        .map_err(|e| inverted_err("synced text entry", e))?
        .map(|(key, value)| (key.value().4, value.value().to_vec())))
}

/// The lowest and highest `SYNCED_TEXT` key of one document.
fn doc_bounds(
    database_id: u64,
    tid: TenantId,
    collection: &str,
    surrogate: Surrogate,
) -> (SyncedKey<'_>, SyncedKey<'_>) {
    let (d, t, s) = (database_id, tid.as_u64(), surrogate.as_u32());
    (
        (d, t, collection, s, i64::MIN),
        (d, t, collection, s, i64::MAX),
    )
}
