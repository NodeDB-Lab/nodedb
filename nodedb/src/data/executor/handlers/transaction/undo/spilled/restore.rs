// SPDX-License-Identifier: BUSL-1.1

//! A [`SpilledUndo`] back into the [`UndoEntry`] a rollback applies.

use super::super::UndoEntry;
use super::super::history_version::HistoryVersionUndo;
use super::entry::{SpilledHistoryVersion, SpilledUndo};

impl From<SpilledUndo> for UndoEntry {
    fn from(spilled: SpilledUndo) -> Self {
        match spilled {
            SpilledUndo::PutDocument {
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
            SpilledUndo::HistoryVersion(undo) => {
                let SpilledHistoryVersion {
                    collection,
                    document_id,
                    version_sys_from,
                    index_tuples,
                    successor,
                    chain_hash_prior,
                } = *undo;
                Self::HistoryVersion(Box::new(HistoryVersionUndo {
                    collection,
                    document_id,
                    version_sys_from,
                    index_tuples,
                    successor,
                    chain_hash_prior: chain_hash_prior.into(),
                }))
            }
            SpilledUndo::ChainHead { collection, prior } => Self::ChainHead { collection, prior },
            SpilledUndo::DeleteDocument {
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
            SpilledUndo::InsertVector {
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
            SpilledUndo::VectorCollectionCreated { index_key } => {
                Self::VectorCollectionCreated { index_key }
            }
            SpilledUndo::DeleteVector {
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
            SpilledUndo::DisplacedVector {
                index_key,
                vector_id,
                surrogate,
            } => Self::DisplacedVector {
                index_key,
                vector_id,
                surrogate,
            },
            SpilledUndo::EndedMultiVector {
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
            SpilledUndo::SpatialInsert { key, entry_id } => Self::SpatialInsert { key, entry_id },
            SpilledUndo::SpatialDelete {
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
            SpilledUndo::EdgeWrite(undo) => Self::EdgeWrite(undo),
            SpilledUndo::EdgeCut(undo) => Self::EdgeCut(undo),
            SpilledUndo::KvPut {
                collection,
                key,
                prior,
            } => Self::KvPut {
                collection,
                key,
                prior,
            },
            SpilledUndo::KvDelete {
                collection,
                key,
                prior,
            } => Self::KvDelete {
                collection,
                key,
                prior,
            },
            SpilledUndo::KvHistory {
                collection,
                key,
                prior,
            } => Self::KvHistory {
                collection,
                key,
                prior,
            },
            SpilledUndo::KvTruncate { collection, rows } => Self::KvTruncate { collection, rows },
            SpilledUndo::MarkNodeDeleted {
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
            SpilledUndo::CrdtCollection(undo) => Self::CrdtCollection(undo),
            SpilledUndo::ArrayTiles { array_id, snapshot } => {
                Self::ArrayTiles { array_id, snapshot }
            }
            SpilledUndo::VectorWrite(undo) => Self::VectorWrite(undo),
            SpilledUndo::VectorPayloadRows(undo) => Self::VectorPayloadRows(undo),
            SpilledUndo::SparseDoc {
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
            SpilledUndo::SpatialRow(undo) => Self::SpatialRow(undo),
            SpilledUndo::FtsDocument(undo) => Self::FtsDocument(undo),
            SpilledUndo::SyncHwm {
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
            SpilledUndo::NodeLabels {
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
            SpilledUndo::ColumnarInsert {
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
            SpilledUndo::ColumnarUpdate {
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
            SpilledUndo::ColumnarDelete {
                collection_key,
                restored,
            } => Self::ColumnarDelete {
                collection_key,
                restored,
            },
            SpilledUndo::ColumnarEngineCreated { collection_key } => {
                Self::ColumnarEngineCreated { collection_key }
            }
            SpilledUndo::TimeseriesEndedInstall(undo) => Self::TimeseriesEndedInstall(undo),
            SpilledUndo::ArrayInstallFlush(undo) => Self::ArrayInstallFlush(undo),
            SpilledUndo::TimeseriesInstallFlush(undo) => Self::TimeseriesInstallFlush(undo),
            SpilledUndo::StatsRestore { key, prior } => Self::StatsRestore { key, prior },
        }
    }
}
