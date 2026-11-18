// SPDX-License-Identifier: BUSL-1.1

//! [`SpilledUndo`]: the undo entries that hold plain data, in the form a
//! spill file stores them.
//!
//! Each variant holds the fields of the [`UndoEntry`] variant of its name.
//! A snapshot of in-memory engine state (a timeseries memtable, a detached
//! vector index, the rows a truncate moved out) has no variant here and
//! stays in RAM.
//!
//! [`UndoEntry`]: super::super::UndoEntry

use nodedb_types::{BoundingBox, StorageKey, Surrogate};

use crate::data::executor::handlers::point::history_install::SuccessorIndexWrites;
use crate::data::executor::handlers::vector_direct_row::VectorIndexKey;
use crate::data::executor::spatial_key::SpatialIndexKey;
use crate::types::hash_chain::ChainHead;
use crate::types::{DatabaseId, TenantId};

use super::super::TimeseriesEndedInstallUndo;
use super::super::array_install_flush::ArrayInstallFlushUndo;
use super::super::crdt_collection::CrdtCollectionUndo;
use super::super::edge_cut::EdgeCutUndo;
use super::super::edge_write::EdgeWriteUndo;
use super::super::fts_doc::FtsDocUndo;
use super::super::spatial_row::SpatialRowUndo;
use super::super::ts_install_flush::TsInstallFlushUndo;
use super::super::vector_payload::VectorPayloadRowsUndo;
use super::super::vector_write::VectorWriteUndo;

/// `(database, tenant, collection)`.
pub(in crate::data::executor) type CollectionKey = (DatabaseId, TenantId, String);

/// A collection's hash-chain head before a write.
///
/// MessagePack writes `None` as nil, so an `Option<Option<ChainHead>>`
/// cannot tell an untouched chain from a genesis one once stored.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) enum ChainPrior {
    /// The write did not touch the chain.
    Untouched,
    /// The chain had no head.
    Genesis,
    /// The chain's head.
    Head(ChainHead),
}

impl From<Option<Option<ChainHead>>> for ChainPrior {
    fn from(prior: Option<Option<ChainHead>>) -> Self {
        match prior {
            None => Self::Untouched,
            Some(None) => Self::Genesis,
            Some(Some(head)) => Self::Head(head),
        }
    }
}

impl From<ChainPrior> for Option<Option<ChainHead>> {
    fn from(prior: ChainPrior) -> Self {
        match prior {
            ChainPrior::Untouched => None,
            ChainPrior::Genesis => Some(None),
            ChainPrior::Head(head) => Some(Some(head)),
        }
    }
}

/// The fields of a `HistoryVersionUndo`, its chain prior as a
/// [`ChainPrior`].
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) struct SpilledHistoryVersion {
    pub collection: String,
    pub document_id: StorageKey,
    pub version_sys_from: i64,
    pub index_tuples: Vec<(String, String)>,
    pub successor: SuccessorIndexWrites,
    pub chain_hash_prior: ChainPrior,
}

/// An undo entry that holds plain data. See the module doc.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(in crate::data::executor) enum SpilledUndo {
    PutDocument {
        collection: String,
        document_id: StorageKey,
        version_sys_from: i64,
        index_tuples: Vec<(String, String)>,
        chain_hash_prior: ChainPrior,
    },
    HistoryVersion(Box<SpilledHistoryVersion>),
    ChainHead {
        collection: String,
        prior: Option<ChainHead>,
    },
    DeleteDocument {
        collection: String,
        document_id: StorageKey,
        old_value: Vec<u8>,
        version_sys_from: i64,
        index_tuples: Vec<(String, String)>,
        chain_hash_prior: ChainPrior,
    },
    InsertVector {
        index_key: VectorIndexKey,
        vector_id: u32,
        collection: String,
        field: String,
        doc_id: Option<StorageKey>,
    },
    VectorCollectionCreated {
        index_key: VectorIndexKey,
    },
    DeleteVector {
        index_key: VectorIndexKey,
        vector_id: u32,
        collection: String,
        field: String,
        doc_id: Option<StorageKey>,
        node_deleted: bool,
    },
    DisplacedVector {
        index_key: VectorIndexKey,
        vector_id: u32,
        surrogate: Surrogate,
    },
    EndedMultiVector {
        index_key: VectorIndexKey,
        document: Surrogate,
        members: Vec<u32>,
        ended: Vec<u32>,
    },
    SpatialInsert {
        key: SpatialIndexKey,
        entry_id: u64,
    },
    SpatialDelete {
        key: SpatialIndexKey,
        entry_id: u64,
        bbox: BoundingBox,
        document_id: String,
        srid: crate::engine::spatial::TreeSrid,
        sys_to: i64,
    },
    EdgeWrite(Box<EdgeWriteUndo>),
    EdgeCut(Box<EdgeCutUndo>),
    KvPut {
        collection: String,
        key: Vec<u8>,
        prior: Option<crate::engine::kv::KvEntryImage>,
    },
    KvDelete {
        collection: String,
        key: Vec<u8>,
        prior: crate::engine::kv::KvEntryImage,
    },
    KvHistory {
        collection: String,
        key: Vec<u8>,
        prior: Vec<crate::engine::kv::history::HistoryVersion>,
    },
    KvTruncate {
        collection: String,
        rows: Vec<crate::engine::kv::hash_table::KvExportEntry>,
    },
    MarkNodeDeleted {
        database_id: u64,
        tid: u64,
        collection: String,
        node_id: String,
    },
    CrdtCollection(Box<CrdtCollectionUndo>),
    ArrayTiles {
        array_id: nodedb_array::types::ArrayId,
        snapshot: crate::engine::array::ArrayTileSnapshot,
    },
    VectorWrite(Box<VectorWriteUndo>),
    VectorPayloadRows(Box<VectorPayloadRowsUndo>),
    SparseDoc {
        key: (DatabaseId, TenantId, String, String),
        doc_id: String,
        prior: Option<crate::engine::vector::sparse::SparseDocImage>,
        next_id: Option<u32>,
    },
    SpatialRow(Box<SpatialRowUndo>),
    FtsDocument(Box<FtsDocUndo>),
    SyncHwm {
        producer_id: u64,
        stream_id: u64,
        prior: Option<u64>,
    },
    NodeLabels {
        database_id: u64,
        tid: u64,
        node_id: String,
        prior: Vec<(String, bool)>,
        interned_labels: Vec<String>,
        created_node: bool,
        version: Option<i64>,
        replaced: Option<Vec<String>>,
    },
    ColumnarInsert {
        collection_key: CollectionKey,
        row_count_before: usize,
        inserted_pks: Vec<Vec<u8>>,
        displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    ColumnarUpdate {
        collection_key: CollectionKey,
        row_count_before: usize,
        inserted_pks: Vec<Vec<u8>>,
        displaced: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
        restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    ColumnarDelete {
        collection_key: CollectionKey,
        restored: Vec<(Vec<u8>, nodedb_columnar::pk_index::RowLocation)>,
    },
    ColumnarEngineCreated {
        collection_key: CollectionKey,
    },
    TimeseriesEndedInstall(TimeseriesEndedInstallUndo),
    ArrayInstallFlush(Box<ArrayInstallFlushUndo>),
    TimeseriesInstallFlush(Box<TsInstallFlushUndo>),
    StatsRestore {
        key: String,
        prior: Option<Vec<u8>>,
    },
}
