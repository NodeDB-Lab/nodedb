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
    /// The collection's series catalog. Ingest registers each new series in
    /// it.
    pub series_catalog_before: Option<nodedb_types::timeseries::SeriesCatalog>,
    pub last_ts_ingest_before: Option<std::time::Instant>,
    pub reservation_bytes_before: Option<usize>,
}

/// Complete in-memory pre-image of one columnar or spatial `TRUNCATE`
/// applied inside a transaction batch. Every row-bearing structure the
/// truncate emptied is moved here whole, so a rollback puts back exactly
/// what existed: every bitemporal version, every tombstone, every flushed
/// segment, and every R-tree entry.
pub(in crate::data::executor) struct ColumnarTruncateUndo {
    pub collection_key: (nodedb_types::DatabaseId, TenantId, String),
    /// The mutation engine's rows, taken by `MutationEngine::truncate`.
    pub rows: nodedb_columnar::TruncatedRows,
    /// `columnar_flushed_segments[key]`, when the collection had one.
    pub flushed_segments: Option<Vec<Vec<u8>>>,
    /// `columnar_flushed_surrogates[key]`, in lockstep with the segments.
    pub flushed_surrogates: Option<nodedb_columnar::mutation::snapshot::FlushedSurrogateTable>,
    /// Every per-field R-tree the collection owned.
    pub spatial_indexes: Vec<(SpatialIndexKey, crate::engine::spatial::RTree)>,
    /// Every reverse-map record of those R-trees.
    pub spatial_doc_map: Vec<SpatialDocMapEntry>,
}

/// One `spatial_doc_map` record: its `(database, tenant, collection, field,
/// entry id)` key and the document id it maps to.
pub(in crate::data::executor) type SpatialDocMapEntry = (
    (nodedb_types::DatabaseId, TenantId, String, String, u64),
    String,
);

/// What one timeseries `TRUNCATE` applied inside a transaction batch
/// changed, so a rollback reverses it exactly. Every row stays on disk: the
/// truncate only ended partitions, so the rollback reopens them.
pub(in crate::data::executor) struct TimeseriesTruncateUndo {
    pub collection_key: (nodedb_types::DatabaseId, TenantId, String),
    /// The partitions the truncate ended.
    pub ended: Vec<crate::data::executor::handlers::timeseries::truncate_end::EndedPartition>,
    /// The in-memory collection stamp before the truncate raised it.
    pub replay_stamp: Option<crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp>,
    /// The collection directory's stamp file before the truncate wrote it.
    pub stamp_file: Option<crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp>,
    /// The aggregate image the truncate removed.
    pub cagg_image: Option<Vec<u8>>,
    /// The collection's newest truncate ordinal before this one.
    pub newest_truncate: Option<i64>,
    pub last_value_cache: Option<LastValueCache>,
    pub series_catalog: Option<nodedb_types::timeseries::SeriesCatalog>,
}

/// The ended partition `install_ts_ended_partition` filed, so a rollback
/// removes it from the registry and from disk.
pub(in crate::data::executor) struct TimeseriesEndedInstallUndo {
    pub collection_key: (nodedb_types::DatabaseId, TenantId, String),
    pub partition_key: crate::engine::timeseries::partition_registry::PartitionKey,
}

/// Tracks a write operation for rollback purposes.
pub(in crate::data::executor) enum UndoEntry {
    /// Undo a PointPut by removing the version it appended, which makes the
    /// row's prior version current again.
    PutDocument {
        collection: String,
        /// The redb storage key. `.surrogate()` recovers the numeric surrogate
        /// FTS index rollback needs.
        document_id: nodedb_types::StorageKey,
        /// System-time key of the version this op appended. Undo physically
        /// removes the version at it.
        version_sys_from: i64,
        /// `(field, value)` pairs whose index entries (live or tombstone) this
        /// op wrote at `version_sys_from`. Undo removes them. Empty = none.
        index_tuples: Vec<(String, String)>,
        /// Pre-image of `chain.heads[(tenant, collection)]` before this op
        /// mutated it. Outer `None` = op didn't touch the chain (no-op on undo);
        /// `Some(None)` = no prior entry (genesis insert → remove key on undo);
        /// `Some(Some(prev))` = restore the key to `prev` on undo.
        chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
    },
    /// Undo a put or a delete installed below the row's newest version by
    /// removing the version and every index entry the install wrote.
    HistoryVersion(Box<super::history_version::HistoryVersionUndo>),
    /// Put a collection's hash-chain head back to `prior`, the head before a
    /// write that linked rows without its own document undo entry. `None` =
    /// no head before (genesis).
    ChainHead {
        collection: String,
        prior: Option<crate::types::hash_chain::ChainHead>,
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
        /// System-time key of the tombstone version this op appended. Undo
        /// physically removes it.
        version_sys_from: i64,
        /// `(field, value)` pairs whose index tombstones this op wrote at
        /// `version_sys_from`. Undo removes them. Empty = none.
        index_tuples: Vec<(String, String)>,
        /// Pre-image of `chain.heads[(tenant, collection)]` before this op
        /// mutated it (see [`UndoEntry::PutDocument`] for semantics).
        chain_hash_prior: Option<Option<crate::types::hash_chain::ChainHead>>,
    },
    /// Undo a VectorInsert by soft-deleting the inserted vector and removing
    /// the stale forward-insert `vector_doc_map` entry it created — mirroring
    /// `SpatialInsert`'s reverse-map cleanup. Without this, a rolled-back
    /// insert leaves a `vector_doc_map` entry pointing at a vector id that no
    /// longer represents a live document: an unbounded leak.
    InsertVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
        /// Collection, field, and storage key — the `vector_doc_map` key
        /// components the forward insert wrote, needed to remove them.
        /// `None` marks the direct primary-vector write path
        /// (`PhysicalPlan::Vector`), which never populates `vector_doc_map`.
        collection: String,
        field: String,
        doc_id: Option<nodedb_types::StorageKey>,
    },
    /// Undo the creation of a vector index by a document write: the index did
    /// not exist before the write. An index left behind fixes the vector
    /// width for every later write, so undo removes it.
    VectorCollectionCreated {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
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
        /// Collection, field, and storage key — the `vector_doc_map` key
        /// components the forward delete removed, needed to restore them.
        /// `None` marks the direct primary-vector write path
        /// (`PhysicalPlan::Vector`), which never populates `vector_doc_map`.
        collection: String,
        field: String,
        doc_id: Option<nodedb_types::StorageKey>,
        /// Whether the forward delete tombstoned `vector_id`. `false` when
        /// the node carried a tombstone already: undo then restores only the
        /// `vector_doc_map` entry and leaves the node deleted.
        node_deleted: bool,
    },
    /// Undo the displacement of a node bound to a document's surrogate that
    /// no `vector_doc_map` entry recorded, such as a node a direct vector
    /// write bound. A document put soft-deletes it before it binds its own
    /// node. Undo un-deletes it and binds it to `surrogate` again.
    DisplacedVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        vector_id: u32,
        surrogate: nodedb_types::Surrogate,
    },
    /// Undo the end of a multi-vector document a row write or removal made:
    /// revive the `ended` nodes and restore `members` as the document's
    /// membership in the index.
    EndedMultiVector {
        index_key: (nodedb_types::DatabaseId, TenantId, String),
        document: nodedb_types::Surrogate,
        members: Vec<u32>,
        ended: Vec<u32>,
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
    /// map value removed by the forward cascade. `srid` is the SRID of the
    /// tree the entry left, which the re-insert binds the tree back to.
    /// `sys_to` is the ordinal the forward removal ended the entry at: the
    /// undo takes the ended entry out of the tree's history.
    SpatialDelete {
        key: SpatialIndexKey,
        entry_id: u64,
        bbox: nodedb_types::BoundingBox,
        document_id: String,
        srid: crate::engine::spatial::TreeSrid,
        sys_to: i64,
    },
    /// Undo a graph edge write: remove the version it added and put the CSR
    /// back.
    EdgeWrite(Box<super::edge_write::EdgeWriteUndo>),
    /// Undo a TRUNCATE share's edge cut: remove the cut it recorded and put
    /// the CSR back.
    EdgeCut(Box<super::edge_cut::EdgeCutUndo>),
    /// Undo a KV write (Put / Insert / InsertIfAbsent / InsertOnConflictUpdate /
    /// FieldSet / Incr / IncrFloat / Cas / GetSet / Expire / Persist) by
    /// reversing it exactly.
    ///
    /// `prior == None` means the key stored no live version: undo removes
    /// the version the write inserted and ends nothing. `prior == Some(image)`
    /// removes the write's version and the ended version it appended, and
    /// reinstalls the value, the absolute expiry instant, the surrogate and
    /// the start ordinal the key held.
    KvPut {
        collection: String,
        key: Vec<u8>,
        prior: Option<crate::engine::kv::KvEntryImage>,
    },
    /// Undo a KV Delete by reinstalling one key's prior live version at its
    /// own start ordinal and removing the ended version the delete appended.
    ///
    /// One entry per key that was actually deleted. If a batch delete removed
    /// N keys, N `KvDelete` entries are pushed.
    KvDelete {
        collection: String,
        key: Vec<u8>,
        prior: crate::engine::kv::KvEntryImage,
    },
    /// Undo a restored KV key's history by putting back the ended versions
    /// the key held before. Pushed before the key's `KvPut`, so a rollback
    /// reinstates the live row first and then replaces whatever history that
    /// reinstatement ended.
    KvHistory {
        collection: String,
        key: Vec<u8>,
        prior: Vec<crate::engine::kv::history::HistoryVersion>,
    },
    /// Undo a KV `TRUNCATE` by reinstalling every row the collection held,
    /// each at its own start ordinal, and removing the ended versions the
    /// truncate appended.
    KvTruncate {
        collection: String,
        rows: Vec<crate::engine::kv::hash_table::KvExportEntry>,
    },
    /// Undo a `mark_node_deleted` by removing the node from the in-memory
    /// deleted-nodes set (edge referential-integrity tracker).
    ///
    /// A delete records a deleted document's node id so a later `EdgePut`
    /// of the same collection to it is rejected as dangling. This tracker is IN-MEMORY, so
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
        /// The collection of the deleted row, which scopes the mark.
        collection: String,
        node_id: String,
    },
    /// Undo a CRDT write by putting the collection's Loro document back.
    CrdtCollection(Box<super::crdt_collection::CrdtCollectionUndo>),
    /// Undo a CRDT scalar write to one document row by putting the row's
    /// scalar fields back.
    CrdtRow(Box<super::crdt_row::CrdtRowUndo>),
    /// Undo an array cell write by putting back the memtable tiles it
    /// touched.
    ArrayTiles {
        array_id: nodedb_array::types::ArrayId,
        snapshot: crate::engine::array::ArrayTileSnapshot,
    },
    /// Undo a vector write a committed redo record installed: withdraw the
    /// nodes it inserted and put every binding, tombstone, sidecar and
    /// bitmap entry back.
    VectorWrite(Box<super::vector_write::VectorWriteUndo>),
    /// Undo a sparse-vector write: put the document back to its prior entries
    /// and the index's id counter back. `next_id == None` means the index did
    /// not exist before the write.
    SparseDoc {
        key: (nodedb_types::DatabaseId, TenantId, String, String),
        doc_id: String,
        prior: Option<crate::engine::vector::sparse::SparseDocImage>,
        next_id: Option<u32>,
    },
    /// Undo a vector-primary truncate: put the detached collection and every
    /// sidecar row back.
    VectorTruncate(Box<super::vector_truncate::VectorTruncateUndo>),
    /// Undo a sync-ingested spatial write by reinstalling the row, the R-tree
    /// entry and the reverse-map record it replaced.
    SpatialRow(Box<super::spatial_row::SpatialRowUndo>),
    /// Undo a sync-ingested full-text write by putting the document's index
    /// footprint back.
    FtsDocument(Box<super::fts_doc::FtsDocUndo>),
    /// Undo the sync high-water-mark advance of a sync-ingested write. `prior
    /// == None` means the stream had no mark before.
    SyncHwm {
        producer_id: u64,
        stream_id: u64,
        prior: Option<u64>,
    },
    /// Undo a node-label set or removal: each label the op touched goes back
    /// to whether the node carried it before (`true` = it did). The label
    /// names and the node the op interned are withdrawn.
    NodeLabels {
        database_id: u64,
        tid: u64,
        node_id: String,
        prior: Vec<(String, bool)>,
        /// Label names the op interned, in interning order.
        interned_labels: Vec<String>,
        /// Whether the op created the node in the CSR.
        created_node: bool,
        /// The ordinal of the node-label version the op wrote, `None` when
        /// it wrote none.
        version: Option<i64>,
        /// The label set the version's key held before the op wrote it.
        /// The undo writes it back. `None` removes the version.
        replaced: Option<Vec<String>>,
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
    /// Undo the creation of a columnar engine by the write: the collection
    /// held no engine before it.
    ColumnarEngineCreated {
        collection_key: (nodedb_types::DatabaseId, TenantId, String),
    },
    /// Undo a transaction-deferred timeseries ingest from its complete
    /// pre-image. Row-count truncation is insufficient: ingest can evolve
    /// schema/dictionaries and update the last-value cache before a later
    /// sub-plan fails.
    TimeseriesIngest(TimeseriesIngestUndo),
    /// Undo a columnar or spatial `TRUNCATE` by reinstalling its pre-image.
    ColumnarTruncate(ColumnarTruncateUndo),
    /// Undo a timeseries `TRUNCATE` by reinstalling its pre-image and
    /// renaming the partition directory back.
    TimeseriesTruncate(Box<TimeseriesTruncateUndo>),
    /// Undo the install of an ended timeseries partition by removing it.
    TimeseriesEndedInstall(TimeseriesEndedInstallUndo),
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
