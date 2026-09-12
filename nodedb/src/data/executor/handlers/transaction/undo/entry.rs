// SPDX-License-Identifier: BUSL-1.1

//! `UndoEntry` — tracks a single write operation for rollback purposes.

use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::engine::timeseries::columnar_memtable::{ColumnarMemtableConfig, MemtableSnapshot};
use crate::engine::timeseries::last_value_cache::LastValueCache;
use crate::types::TenantId;

/// Complete in-memory pre-image for one transaction-deferred timeseries ingest.
///
/// The token is captured before the ingest can create a memtable, evolve a
/// schema, append rows, mutate tag dictionaries or update the last-value
/// cache. Deferred ingest deliberately leaves timer, checkpoint and reservation
/// accounting untouched; their prior presence/size is still recorded so undo
/// can verify that invariant rather than silently accepting accounting drift.
pub(in crate::data::executor) struct TimeseriesIngestUndo {
    pub collection_key: (nodedb_types::DatabaseId, TenantId, String),
    pub memtable_before: Option<MemtableSnapshot>,
    pub memtable_config_before: Option<ColumnarMemtableConfig>,
    /// The pre-image's reported resident footprint. Snapshot reconstruction
    /// intentionally does not retain `Vec` spare capacity, but the memory
    /// governor's live reservation must still match the pre-transaction
    /// accounting after rollback.
    pub memtable_memory_bytes_before: Option<usize>,
    pub last_value_cache_before: Option<LastValueCache>,
    pub max_ingested_lsn_before: Option<u64>,
    pub last_ts_ingest_before: Option<std::time::Instant>,
    pub reservation_bytes_before: Option<usize>,
}

/// Tracks a write operation for rollback purposes.
pub(in crate::data::executor) enum UndoEntry {
    /// Undo a PointPut by deleting the document (or restoring the old value).
    PutDocument {
        collection: String,
        /// The redb storage key. `.surrogate()` recovers the numeric surrogate
        /// FTS index rollback needs.
        document_id: nodedb_types::StorageKey,
        /// `None` if the document didn't exist before (inserted); `Some(bytes)`
        /// if it was overwritten (updated).
        old_value: Option<Vec<u8>>,
        /// System-time key of the versioned/tombstone row this op appended on a
        /// bitemporal collection. `None` = plain non-bitemporal op → reverse via
        /// the non-versioned table exactly as before. `Some(t)` = physically
        /// remove the version row at `t` (and skip the plain-table reversal).
        bitemporal_sys_from_ms: Option<i64>,
        /// `(field, value)` pairs whose versioned index entries this op wrote at
        /// `bitemporal_sys_from_ms`. Empty = none.
        bitemporal_index_tuples: Vec<(String, String)>,
        /// `(field, value)` pairs this op INSERTED into the plain secondary
        /// index. Reversed by `index_remove` on undo. Empty = none.
        secondary_index_added: Vec<(String, String)>,
        /// `(field, value)` pairs this op REMOVED from the plain secondary index
        /// (stale entries on UPDATE). Restored by `index_put` on undo. Empty = none.
        secondary_index_removed: Vec<(String, String)>,
        /// Pre-image of `chain_hashes[(tenant, collection)]` before this op
        /// mutated it. Outer `None` = op didn't touch the chain (no-op on undo);
        /// `Some(None)` = no prior entry (genesis insert → remove key on undo);
        /// `Some(Some(prev))` = restore the key to `prev` on undo.
        chain_hash_prior: Option<Option<String>>,
    },
    /// Undo a PointDelete by re-inserting the document.
    DeleteDocument {
        collection: String,
        /// The redb storage key. `.surrogate()` recovers the numeric surrogate
        /// the FTS inverted-index rollback re-indexes under: the forward
        /// delete cascade removed this document's postings, and a
        /// rolled-back delete recomputes and re-inserts them under it.
        document_id: nodedb_types::StorageKey,
        old_value: Vec<u8>,
        /// System-time key of the versioned tombstone row this op appended on a
        /// bitemporal collection. `None` = plain op → re-insert via the
        /// non-versioned table as before. `Some(t)` = physically remove the
        /// tombstone row at `t` (and skip the plain-table re-insert).
        bitemporal_sys_from_ms: Option<i64>,
        /// `(field, value)` pairs whose versioned index entries this op wrote at
        /// `bitemporal_sys_from_ms`. Empty = none.
        bitemporal_index_tuples: Vec<(String, String)>,
        /// `(field, value)` pairs the plain secondary-index cascade removed for
        /// this document. Restored by `index_put` on undo, closing the
        /// rolled-back-DELETE secondary-index hole. Empty = none.
        secondary_index_tuples: Vec<(String, String)>,
        /// Pre-image of `chain_hashes[(tenant, collection)]` before this op
        /// mutated it (see [`UndoEntry::PutDocument`] for semantics).
        chain_hash_prior: Option<Option<String>>,
    },
    /// Undo a VectorInsert by soft-deleting the inserted vector and removing
    /// the stale forward-insert `vector_doc_map` entry it created — mirroring
    /// `SpatialInsert`'s reverse-map cleanup. Without this, a rolled-back
    /// insert leaves a `vector_doc_map` entry pointing at a vector id that no
    /// longer represents a live document: an unbounded leak.
    InsertVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
        /// Collection, field, and doc id — the `vector_doc_map` key
        /// components the forward insert wrote, needed to remove them.
        collection: String,
        field: String,
        doc_id: String,
    },
    /// Undo a VectorDelete by un-deleting (clearing tombstone) and restoring
    /// the `vector_doc_map` entry the forward delete removed — mirroring
    /// `SpatialDelete`'s reverse-map restore. Without this, a rolled-back
    /// delete leaves the doc→vector reverse lookup missing: a future delete
    /// of the same document can no longer find its vector, orphaning it
    /// permanently.
    DeleteVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
        /// Collection, field, and doc id — the `vector_doc_map` key
        /// components the forward delete removed, needed to restore them.
        collection: String,
        field: String,
        doc_id: String,
    },
    /// Undo a spatial R-tree insert by removing the entry from the per-field
    /// R-tree and deleting its reverse `spatial_doc_map` record.
    ///
    /// `key` is the `(database, tenant, collection, field)` spatial index key;
    /// `entry_id` is the FNV-1a hash of the substrate row key used as the
    /// R-tree entry id.
    SpatialInsert { key: SpatialIndexKey, entry_id: u64 },
    /// Undo a spatial R-tree removal by re-inserting the entry (with its
    /// captured bounding box) into the per-field R-tree and re-populating the
    /// reverse `spatial_doc_map` record.
    ///
    /// `bbox` is the entry's geometry captured BEFORE the forward `delete`
    /// (the R-tree `delete` does not return it); `document_id` is the reverse
    /// map value removed by the forward cascade.
    SpatialDelete {
        key: SpatialIndexKey,
        entry_id: u64,
        bbox: nodedb_types::BoundingBox,
        document_id: String,
    },
    /// Undo an EdgePut by deleting the edge (or restoring old properties).
    PutEdge {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        /// `None` if edge didn't exist before (inserted); `Some(bytes)` if overwritten.
        old_properties: Option<Vec<u8>>,
    },
    /// Undo an EdgeDelete by re-inserting the edge with its old properties.
    DeleteEdge {
        collection: String,
        src_id: String,
        label: String,
        dst_id: String,
        old_properties: Vec<u8>,
    },
    /// Undo a KV write (Put / Insert / InsertIfAbsent / InsertOnConflictUpdate /
    /// FieldSet / Incr / IncrFloat / Cas / GetSet) by restoring the prior value.
    ///
    /// `prior_value == None` means the key did not exist before — undo deletes it.
    /// `prior_value == Some(bytes)` means the key was overwritten — undo restores it.
    ///
    /// The KV hash table preserves existing non-ZERO surrogate bindings on `put`,
    /// so passing `Surrogate::ZERO` during undo is safe: the original surrogate
    /// remains bound in the entry.
    KvPut {
        collection: String,
        key: Vec<u8>,
        prior_value: Option<Vec<u8>>,
    },
    /// Undo a KV Delete by restoring one key's prior value.
    ///
    /// One entry per key that was actually deleted. If a batch delete removed
    /// N keys, N `KvDelete` entries are pushed.
    KvDelete {
        collection: String,
        key: Vec<u8>,
        prior_value: Vec<u8>,
    },
    /// Undo a KV BatchPut by restoring prior values for all affected keys.
    ///
    /// Each element is `(key, prior_value)` where `prior_value == None`
    /// means the key was newly inserted.
    KvBatchPut {
        collection: String,
        entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
    /// Undo a KV Transfer (fungible) by restoring source and destination prior values.
    KvTransfer {
        collection: String,
        source_key: Vec<u8>,
        source_prior: Vec<u8>,
        dest_key: Vec<u8>,
        dest_prior: Option<Vec<u8>>,
    },
    /// Undo a KV TransferItem by restoring source and destination prior values.
    KvTransferItem {
        source_collection: String,
        dest_collection: String,
        item_key: Vec<u8>,
        dest_key: Vec<u8>,
        source_prior: Vec<u8>,
        dest_prior: Option<Vec<u8>>,
    },
    /// Undo a KV `Expire`/`Persist` by restoring the key's prior TTL state.
    ///
    /// `prior_expiry == None` means the key had no TTL (persistent) before
    /// the forward op; undo calls `KvEngine::persist`. `prior_expiry ==
    /// Some(expire_at_ms)` means the key had a TTL expiring at that exact
    /// absolute instant; undo calls `KvEngine::expire_with_absolute_expiry`
    /// with it verbatim (not a freshly-derived `now_ms + ttl_ms`, which
    /// would drift from the original instant by the elapsed time).
    KvTtl {
        collection: String,
        key: Vec<u8>,
        prior_expiry: Option<u64>,
    },
    /// Undo a KV `RegisterSortedIndex`/`DropSortedIndex` by restoring the
    /// index name's prior definition state.
    ///
    /// `prior_def == None` means no index existed under this name before
    /// the forward op (a fresh `RegisterSortedIndex`); undo drops it.
    /// `prior_def == Some(def)` means an index existed under this name
    /// before the forward op (either overwritten by `RegisterSortedIndex`,
    /// or removed by `DropSortedIndex`); undo re-registers `def`, which
    /// rebuilds the order-statistic tree by backfilling from the KV
    /// collection's CURRENT contents at undo time -- correct regardless of
    /// undo-log ordering relative to sibling KV-write undos, since
    /// `SortedIndexManager::register` always derives the tree fresh from
    /// live table state rather than from a point-in-time snapshot.
    SortedIndexDdl {
        database_id: u64,
        tenant_id: u64,
        index_name: String,
        prior_def: Option<crate::engine::kv::sorted_index::manager::SortedIndexDef>,
    },
    /// Undo a `mark_node_deleted` by removing the node from the in-memory
    /// deleted-nodes set (edge referential-integrity tracker).
    ///
    /// The delete cascade records a deleted document's node id so a later
    /// `EdgePut` to it is rejected as dangling. This tracker is IN-MEMORY, so
    /// an aborted redb txn does not reverse it — a rolled-back tx DELETE must
    /// explicitly un-mark the node. Pushed ONLY when the forward mark newly
    /// inserted the node (`mark_node_deleted` returned `true`); a node a prior
    /// committed op already tombstoned is never un-marked here. `database_id`
    /// and `tid` are captured from the forward op (the rollback driver's own
    /// `did` is the DEFAULT database, not necessarily the op's), keying the
    /// exact `deleted_nodes` partition.
    MarkNodeDeleted {
        database_id: u64,
        tid: u64,
        node_id: String,
    },
    /// Undo a columnar insert by rolling back in-memory state.
    ///
    /// `row_count_before` is the memtable row count snapshot taken before the
    /// insert. `inserted_pks` are the PK bytes of each newly appended row (for
    /// PK index cleanup). `displaced` are `(pk_bytes, prior_location)` pairs for
    /// rows that were tombstoned by an upsert (their PK index entries must be
    /// restored and their tombstone bits cleared).
    ColumnarInsert {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
        row_count_before: usize,
        inserted_pks: Vec<Vec<u8>>,
        displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    /// Undo a columnar predicate UPDATE by rolling back in-memory state.
    ///
    /// The forward UPDATE reverses each matched row via delete-old +
    /// insert-new: the original row is positionally tombstoned (its PK index
    /// entry removed) and the merged replacement is appended to the memtable.
    /// Reversal has two halves, applied in order by `apply_undo_columnar`:
    /// 1. Remove the appended replacement rows — identical to
    ///    [`UndoEntry::ColumnarInsert`]: `row_count_before` is the memtable row
    ///    count before the whole UPDATE statement; `inserted_pks` are the PK
    ///    bytes of each appended replacement; `displaced` are
    ///    `(pk_bytes, prior_location)` pairs for rows a PK-changing update's
    ///    insert half tombstoned.
    /// 2. Restore the tombstoned originals via `restored`: each
    ///    `(pk_bytes, RowLocation)` clears the row's delete-bitmap bit and
    ///    re-binds the PK index to that location.
    ColumnarUpdate {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
        row_count_before: usize,
        inserted_pks: Vec<Vec<u8>>,
        displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
        restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    /// Undo a columnar predicate DELETE by restoring each tombstoned row.
    ///
    /// A columnar DELETE never grows the memtable — it only sets delete-bitmap
    /// bits and removes PK index entries — so reversal needs no truncation.
    /// Each `restored` entry `(pk_bytes, RowLocation)` clears the row's
    /// delete-bitmap bit and re-binds the PK index to that location, mirroring
    /// the displaced-row restore in `ColumnarInsert`.
    ColumnarDelete {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
        restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    /// Undo a transaction-deferred timeseries ingest from its complete
    /// pre-image. Row-count truncation is insufficient: ingest can evolve
    /// schema/dictionaries and update the last-value cache before a later
    /// sub-plan fails.
    TimeseriesIngest(TimeseriesIngestUndo),
    /// Undo a column-stats observe by restoring the pre-image captured before
    /// the read-modify-write.
    ///
    /// Column stats are a READ-MODIFY-WRITE side-effect: each op reads the
    /// stored `ColumnStats` for a `(collection, field)`, merges the new doc's
    /// value, and writes it back. Because each tx sub-plan commits its own
    /// per-row redb txn, an aborted redb txn does NOT reverse a stats mutation a
    /// prior sub-plan already committed — so rollback must restore the EXACT
    /// pre-image, not merely delete.
    ///
    /// `key` is the composed `COLUMN_STATS` key (`"{db}:{tenant}:{coll}:{field}"`)
    /// exactly as `observe_document_in_txn` produced it. `prior = Some(bytes)`
    /// = the serialized `ColumnStats` that existed before (undo rewrites them);
    /// `prior = None` = no stats existed for this `(coll, field)` before (undo
    /// removes the key).
    StatsRestore { key: String, prior: Option<Vec<u8>> },
}
