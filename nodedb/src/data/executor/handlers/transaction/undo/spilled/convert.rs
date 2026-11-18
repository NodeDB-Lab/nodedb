// SPDX-License-Identifier: BUSL-1.1

//! An [`UndoEntry`] into its [`SpilledUndo`].
//!
//! A snapshot of in-memory engine state has no spilled form. The
//! conversion hands such an entry back unchanged, and it stays in RAM:
//!
//! - `TimeseriesIngest` holds the memtable as it was, and an `Instant`.
//! - `ColumnarTruncate` holds the rows and R-trees the truncate moved out.
//! - `VectorTruncate` holds the detached index. The settle of a committed
//!   install truncates it in place.
//! - `TimeseriesTruncate` holds the last-value cache the truncate moved out.
//! - `TimeseriesTruncateFlush` holds the memtable as it was before the
//!   truncate's flush, and the partition registry.

use super::super::UndoEntry;
use super::super::history_version::HistoryVersionUndo;
use super::entry::{SpilledHistoryVersion, SpilledUndo};

impl TryFrom<UndoEntry> for SpilledUndo {
    /// The entry, when it is a snapshot of in-memory state.
    type Error = UndoEntry;

    fn try_from(entry: UndoEntry) -> Result<Self, UndoEntry> {
        Ok(match entry {
            UndoEntry::PutDocument {
                collection,
                document_id,
                version_sys_from,
                index_tuples,
                chain_hash_prior,
            } => Self::PutDocument {
                collection,
                document_id,
                version_sys_from,
                index_tuples,
                chain_hash_prior: chain_hash_prior.into(),
            },
            UndoEntry::HistoryVersion(undo) => {
                let HistoryVersionUndo {
                    collection,
                    document_id,
                    version_sys_from,
                    index_tuples,
                    successor,
                    chain_hash_prior,
                } = *undo;
                Self::HistoryVersion(Box::new(SpilledHistoryVersion {
                    collection,
                    document_id,
                    version_sys_from,
                    index_tuples,
                    successor,
                    chain_hash_prior: chain_hash_prior.into(),
                }))
            }
            UndoEntry::ChainHead { collection, prior } => Self::ChainHead { collection, prior },
            UndoEntry::DeleteDocument {
                collection,
                document_id,
                old_value,
                version_sys_from,
                index_tuples,
                chain_hash_prior,
            } => Self::DeleteDocument {
                collection,
                document_id,
                old_value,
                version_sys_from,
                index_tuples,
                chain_hash_prior: chain_hash_prior.into(),
            },
            UndoEntry::InsertVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            } => Self::InsertVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
            },
            UndoEntry::VectorCollectionCreated { index_key } => {
                Self::VectorCollectionCreated { index_key }
            }
            UndoEntry::DeleteVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
                node_deleted,
            } => Self::DeleteVector {
                index_key,
                vector_id,
                collection,
                field,
                doc_id,
                node_deleted,
            },
            UndoEntry::DisplacedVector {
                index_key,
                vector_id,
                surrogate,
            } => Self::DisplacedVector {
                index_key,
                vector_id,
                surrogate,
            },
            UndoEntry::EndedMultiVector {
                index_key,
                document,
                members,
                ended,
            } => Self::EndedMultiVector {
                index_key,
                document,
                members,
                ended,
            },
            UndoEntry::SpatialInsert { key, entry_id } => Self::SpatialInsert { key, entry_id },
            UndoEntry::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
                srid,
                sys_to,
            } => Self::SpatialDelete {
                key,
                entry_id,
                bbox,
                document_id,
                srid,
                sys_to,
            },
            UndoEntry::EdgeWrite(undo) => Self::EdgeWrite(undo),
            UndoEntry::EdgeCut(undo) => Self::EdgeCut(undo),
            UndoEntry::KvPut {
                collection,
                key,
                prior,
            } => Self::KvPut {
                collection,
                key,
                prior,
            },
            UndoEntry::KvDelete {
                collection,
                key,
                prior,
            } => Self::KvDelete {
                collection,
                key,
                prior,
            },
            UndoEntry::KvHistory {
                collection,
                key,
                prior,
            } => Self::KvHistory {
                collection,
                key,
                prior,
            },
            UndoEntry::KvTruncate { collection, rows } => Self::KvTruncate { collection, rows },
            UndoEntry::MarkNodeDeleted {
                database_id,
                tid,
                collection,
                node_id,
            } => Self::MarkNodeDeleted {
                database_id,
                tid,
                collection,
                node_id,
            },
            UndoEntry::CrdtCollection(undo) => Self::CrdtCollection(undo),
            UndoEntry::ArrayTiles { array_id, snapshot } => Self::ArrayTiles { array_id, snapshot },
            UndoEntry::VectorWrite(undo) => Self::VectorWrite(undo),
            UndoEntry::VectorPayloadRows(undo) => Self::VectorPayloadRows(undo),
            UndoEntry::SparseDoc {
                key,
                doc_id,
                prior,
                next_id,
            } => Self::SparseDoc {
                key,
                doc_id,
                prior,
                next_id,
            },
            UndoEntry::SpatialRow(undo) => Self::SpatialRow(undo),
            UndoEntry::FtsDocument(undo) => Self::FtsDocument(undo),
            UndoEntry::SyncHwm {
                producer_id,
                stream_id,
                prior,
                written,
            } => Self::SyncHwm {
                producer_id,
                stream_id,
                prior,
                written,
            },
            UndoEntry::NodeLabels {
                database_id,
                tid,
                node_id,
                prior,
                interned_labels,
                created_node,
                version,
                replaced,
            } => Self::NodeLabels {
                database_id,
                tid,
                node_id,
                prior,
                interned_labels,
                created_node,
                version,
                replaced,
            },
            UndoEntry::ColumnarInsert {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
            } => Self::ColumnarInsert {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
            },
            UndoEntry::ColumnarUpdate {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
                restored,
            } => Self::ColumnarUpdate {
                collection_key,
                row_count_before,
                inserted_pks,
                displaced,
                restored,
            },
            UndoEntry::ColumnarDelete {
                collection_key,
                restored,
            } => Self::ColumnarDelete {
                collection_key,
                restored,
            },
            UndoEntry::ColumnarEngineCreated { collection_key } => {
                Self::ColumnarEngineCreated { collection_key }
            }
            UndoEntry::TimeseriesEndedInstall(undo) => Self::TimeseriesEndedInstall(undo),
            UndoEntry::ArrayInstallFlush(undo) => Self::ArrayInstallFlush(undo),
            UndoEntry::TimeseriesInstallFlush(undo) => Self::TimeseriesInstallFlush(undo),
            UndoEntry::StatsRestore { key, prior } => Self::StatsRestore { key, prior },
            snapshot @ (UndoEntry::TimeseriesIngest(_)
            | UndoEntry::ColumnarTruncate(_)
            | UndoEntry::TimeseriesTruncate(_)
            | UndoEntry::TimeseriesTruncateFlush(_)
            | UndoEntry::VectorTruncate(_)) => return Err(snapshot),
        })
    }
}
