// SPDX-License-Identifier: BUSL-1.1

//! The RAM a [`SpilledUndo`] holds before it spills: its own size and the
//! heap bytes of the fields that grow with the data it reverses.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::data::executor::handlers::spill::tiered::SpillWeight;

use super::super::row_version::RowVersionFloor;
use super::super::vector_payload::PayloadRow;
use super::entry::SpilledUndo;

/// Heap bytes of a list of `(field, value)` pairs.
fn tuple_bytes(tuples: &[(String, String)]) -> usize {
    tuples.iter().map(|(a, b)| a.len() + b.len()).sum()
}

/// Heap bytes of a list of byte strings.
fn byte_lists(lists: &[Vec<u8>]) -> usize {
    lists.iter().map(Vec::len).sum()
}

/// Heap bytes of `(key, location)` pairs.
fn located<T>(pairs: &[(Vec<u8>, T)]) -> usize {
    pairs
        .iter()
        .map(|(key, _)| key.len() + std::mem::size_of::<T>())
        .sum()
}

fn opt_bytes(bytes: Option<&Vec<u8>>) -> usize {
    bytes.map_or(0, Vec::len)
}

/// Heap bytes of an object's fields.
fn fields_bytes(fields: &HashMap<String, Value>) -> usize {
    fields
        .iter()
        .map(|(name, value)| {
            std::mem::size_of::<(String, Value)>() + name.len() + value.heap_bytes()
        })
        .sum()
}

impl SpilledUndo {
    /// Heap bytes of the fields that grow with the data the entry reverses.
    fn heap_bytes(&self) -> usize {
        match self {
            Self::PutDocument {
                collection,
                index_tuples,
                ..
            } => collection.len() + tuple_bytes(index_tuples),
            Self::HistoryVersion(undo) => {
                undo.collection.len()
                    + tuple_bytes(&undo.index_tuples)
                    + tuple_bytes(&undo.successor.tuples)
            }
            Self::ChainHead { collection, prior } => {
                collection.len() + prior.as_ref().map_or(0, |head| head.hash.len())
            }
            Self::DeleteDocument {
                collection,
                old_value,
                index_tuples,
                ..
            } => collection.len() + old_value.len() + tuple_bytes(index_tuples),
            Self::InsertVector {
                collection, field, ..
            }
            | Self::DeleteVector {
                collection, field, ..
            } => collection.len() + field.len(),
            Self::VectorCollectionCreated { index_key }
            | Self::DisplacedVector { index_key, .. } => index_key.2.len(),
            Self::EndedMultiVector { members, ended, .. } => 4 * (members.len() + ended.len()),
            Self::SpatialInsert { key, .. } => key.2.len() + key.3.len(),
            Self::SpatialDelete {
                key, document_id, ..
            } => key.2.len() + key.3.len() + document_id.len(),
            Self::EdgeWrite(undo) => {
                let version = &undo.version;
                undo.collection.len()
                    + undo.src_id.len()
                    + undo.label.len()
                    + undo.dst_id.len()
                    + opt_bytes(version.prior_forward.as_ref())
                    + opt_bytes(version.prior_reverse.as_ref())
                    + opt_bytes(version.current.as_ref())
                    + version
                        .prior_bindings
                        .iter()
                        .map(|(id, _)| id.len() + 8)
                        .sum::<usize>()
            }
            Self::EdgeCut(undo) => {
                let install = &undo.install;
                install.collection.len()
                    + opt_bytes(install.prior_summary.as_ref())
                    + install
                        .flips
                        .iter()
                        .map(|flip| {
                            flip.src.len()
                                + flip.label.len()
                                + flip.dst.len()
                                + opt_bytes(flip.before.as_ref())
                                + opt_bytes(flip.after.as_ref())
                        })
                        .sum::<usize>()
            }
            Self::KvPut {
                collection,
                key,
                prior,
            } => collection.len() + key.len() + prior.as_ref().map_or(0, |p| p.value.len()),
            Self::KvDelete {
                collection,
                key,
                prior,
            } => collection.len() + key.len() + prior.value.len(),
            Self::KvHistory {
                collection,
                key,
                prior,
            } => {
                collection.len()
                    + key.len()
                    + prior.iter().map(|v| v.value.len() + 32).sum::<usize>()
            }
            Self::KvTruncate { collection, rows } => {
                collection.len()
                    + rows
                        .iter()
                        .map(|row| row.key.len() + row.value.len() + 24)
                        .sum::<usize>()
            }
            Self::MarkNodeDeleted {
                collection,
                node_id,
                ..
            } => collection.len() + node_id.len(),
            Self::CrdtCollection(undo) => undo.collection.len() + opt_bytes(undo.snapshot.as_ref()),
            Self::ArrayTiles { array_id, snapshot } => {
                array_id.name.len() + snapshot.payload_bytes()
            }
            Self::VectorWrite(undo) => {
                undo.collection.len()
                    + undo.index_key.2.len()
                    + undo.mark.as_ref().map_or(0, |mark| mark.heap_bytes())
                    + undo.sidecars.len() * std::mem::size_of::<RowVersionFloor>()
                    + undo
                        .payload_rows
                        .iter()
                        .map(|(_, fields)| {
                            std::mem::size_of::<(u32, HashMap<String, Value>)>()
                                + fields_bytes(fields)
                        })
                        .sum::<usize>()
            }
            Self::VectorPayloadRows(undo) => {
                undo.index_key.2.len()
                    + [&undo.removed, &undo.inserted]
                        .into_iter()
                        .flatten()
                        .map(|row| std::mem::size_of::<PayloadRow>() + fields_bytes(&row.fields))
                        .sum::<usize>()
            }
            Self::SparseDoc {
                key, doc_id, prior, ..
            } => {
                key.2.len()
                    + key.3.len()
                    + doc_id.len()
                    + prior.as_ref().map_or(0, |image| image.heap_bytes())
            }
            Self::SpatialRow(undo) => {
                undo.key.2.len()
                    + undo.key.3.len()
                    + undo.prior_entry.as_ref().map_or(0, |(_, id, _)| id.len())
            }
            Self::FtsDocument(undo) => {
                undo.collection.len()
                    + undo.prior.as_ref().map_or(0, |image| image.heap_bytes())
                    + undo.frames.heap_bytes()
            }
            Self::SyncHwm { .. } => 0,
            Self::NodeLabels {
                node_id,
                prior,
                interned_labels,
                replaced,
                ..
            } => {
                node_id.len()
                    + prior
                        .iter()
                        .map(|(label, _)| label.len() + 1)
                        .sum::<usize>()
                    + interned_labels.iter().map(String::len).sum::<usize>()
                    + replaced
                        .as_ref()
                        .map_or(0, |labels| labels.iter().map(String::len).sum())
            }
            Self::ColumnarInsert {
                collection_key,
                inserted_pks,
                displaced,
                ..
            } => collection_key.2.len() + byte_lists(inserted_pks) + located(displaced),
            Self::ColumnarUpdate {
                collection_key,
                inserted_pks,
                displaced,
                restored,
                ..
            } => {
                collection_key.2.len()
                    + byte_lists(inserted_pks)
                    + located(displaced)
                    + located(restored)
            }
            Self::ColumnarDelete {
                collection_key,
                restored,
            } => collection_key.2.len() + located(restored),
            Self::ColumnarEngineCreated { collection_key } => collection_key.2.len(),
            Self::TimeseriesEndedInstall(undo) => {
                undo.collection_key.2.len() + undo.partition_key.dir_name.len()
            }
            Self::ArrayInstallFlush(undo) => undo.held_bytes(),
            Self::TimeseriesInstallFlush(undo) => undo.held_bytes(),
            Self::StatsRestore { key, prior } => key.len() + opt_bytes(prior.as_ref()),
        }
    }
}

impl SpillWeight for SpilledUndo {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_bytes()
    }
}
