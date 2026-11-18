// SPDX-License-Identifier: BUSL-1.1

//! Every plain-data undo entry survives the spill file whole: into its
//! spilled form, through MessagePack, and back.

use std::collections::HashMap;

use nodedb_types::{BoundingBox, StorageKey, Surrogate, Value};

use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::handlers::point::history_install::SuccessorIndexWrites;
use crate::engine::graph::edge_store::{
    EdgeCountChange, EdgeCutInstall, EdgeFlip, EdgeVersionWrite,
};
use crate::engine::kv::KvEntryImage;
use crate::engine::kv::hash_table::KvExportEntry;
use crate::engine::kv::history::HistoryVersion;
use crate::engine::spatial::TreeSrid;
use crate::types::hash_chain::ChainHead;
use crate::types::{DatabaseId, TenantId};

use super::super::crdt_collection::CrdtCollectionUndo;
use super::super::edge_cut::EdgeCutUndo;
use super::super::edge_write::{EdgeCsrPrior, EdgeWriteUndo};
use super::super::fts_doc::FtsDocUndo;
use super::super::history_version::HistoryVersionUndo;
use super::super::row_version::RowVersionFloor;
use super::super::spatial_row::SpatialRowUndo;
use super::super::vector_payload::{PayloadRow, VectorPayloadRowsUndo};
use super::super::vector_write::VectorWriteUndo;
use super::super::{TimeseriesEndedInstallUndo, TimeseriesIngestUndo, UndoEntry};
use super::SpilledUndo;
use super::entry::ChainPrior;

fn key(surrogate: u32) -> StorageKey {
    StorageKey::for_surrogate(Surrogate::new(surrogate))
}

fn collection_key() -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(1), "docs".into())
}

fn spatial_key() -> (DatabaseId, TenantId, String, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(1),
        "places".into(),
        "geom".into(),
    )
}

fn head() -> ChainHead {
    ChainHead {
        seq: 3,
        hash: "ab".into(),
    }
}

fn image() -> KvEntryImage {
    KvEntryImage {
        value: b"v".to_vec(),
        expire_at_ms: 9,
        surrogate: Surrogate::new(4),
        sys_from: 7,
    }
}

fn bbox() -> BoundingBox {
    BoundingBox::new(1.0, 2.0, 3.0, 4.0)
}

fn location() -> nodedb_columnar::pk_index::RowLocation {
    nodedb_columnar::pk_index::RowLocation {
        segment_id: 2,
        row_index: 5,
    }
}

fn fields() -> HashMap<String, Value> {
    HashMap::from([("tag".to_string(), Value::String("a".into()))])
}

fn tuples() -> Vec<(String, String)> {
    vec![("status".into(), "active".into())]
}

/// One entry of every variant that spills, each field set where a field
/// can be empty.
fn plain_entries(frames: crate::engine::sparse::inverted::SyncedTextMark) -> Vec<UndoEntry> {
    let mut sparse = crate::engine::vector::sparse::SparseInvertedIndex::new();
    let vector = nodedb_types::SparseVector::from_entries(vec![(1, 0.5)]).expect("sparse vector");
    sparse.insert("d1", &vector);
    let mark = nodedb_vector::VectorCollection::new(2, Default::default())
        .write_mark(&[Surrogate::new(1)], &[0]);
    vec![
        UndoEntry::PutDocument {
            collection: "c".into(),
            document_id: key(1),
            version_sys_from: 10,
            index_tuples: tuples(),
            chain_hash_prior: Some(None),
        },
        UndoEntry::HistoryVersion(Box::new(HistoryVersionUndo {
            collection: "c".into(),
            document_id: key(2),
            version_sys_from: 11,
            index_tuples: tuples(),
            successor: SuccessorIndexWrites {
                sys_from: 12,
                tuples: tuples(),
            },
            chain_hash_prior: Some(Some(head())),
        })),
        UndoEntry::ChainHead {
            collection: "c".into(),
            prior: Some(head()),
        },
        UndoEntry::DeleteDocument {
            collection: "c".into(),
            document_id: key(3),
            old_value: b"old".to_vec(),
            version_sys_from: 13,
            index_tuples: tuples(),
            chain_hash_prior: None,
        },
        UndoEntry::InsertVector {
            index_key: collection_key(),
            vector_id: 3,
            collection: "docs".into(),
            field: "emb".into(),
            doc_id: Some(key(4)),
        },
        UndoEntry::VectorCollectionCreated {
            index_key: collection_key(),
        },
        UndoEntry::DeleteVector {
            index_key: collection_key(),
            vector_id: 4,
            collection: "docs".into(),
            field: "emb".into(),
            doc_id: None,
            node_deleted: true,
        },
        UndoEntry::DisplacedVector {
            index_key: collection_key(),
            vector_id: 5,
            surrogate: Surrogate::new(8),
        },
        UndoEntry::EndedMultiVector {
            index_key: collection_key(),
            document: Surrogate::new(9),
            members: vec![1, 2],
            ended: vec![3],
        },
        UndoEntry::SpatialInsert {
            key: spatial_key(),
            entry_id: 77,
        },
        UndoEntry::SpatialDelete {
            key: spatial_key(),
            entry_id: 78,
            bbox: bbox(),
            document_id: "p1".into(),
            srid: TreeSrid::WGS84,
            sys_to: 14,
        },
        UndoEntry::EdgeWrite(Box::new(EdgeWriteUndo {
            database_id: 0,
            tid: 1,
            collection: "g".into(),
            src_id: "a".into(),
            label: "l".into(),
            dst_id: "b".into(),
            version: EdgeVersionWrite {
                system_from: 5,
                prior_forward: Some(b"f".to_vec()),
                prior_reverse: None,
                prior_applied: Some(4),
                counted: Some(EdgeCountChange::Added),
                summary_created: true,
                prior_bindings: vec![("a".into(), Some(1)), ("b".into(), None)],
                current: Some(b"p".to_vec()),
                was_live: false,
            },
            csr: EdgeCsrPrior {
                weight: Some(1.5),
                surrogates: vec![("a".into(), 1)],
                created_nodes: vec!["b".into()],
            },
        })),
        UndoEntry::EdgeCut(Box::new(EdgeCutUndo {
            database_id: 0,
            tid: 1,
            install: EdgeCutInstall {
                collection: "g".into(),
                cut: 9,
                inserted: true,
                prior_summary: None,
                flips: vec![EdgeFlip {
                    src: "a".into(),
                    label: "l".into(),
                    dst: "b".into(),
                    before: Some(b"x".to_vec()),
                    after: None,
                }],
            },
        })),
        UndoEntry::KvPut {
            collection: "cache".into(),
            key: b"k".to_vec(),
            prior: Some(image()),
        },
        UndoEntry::KvDelete {
            collection: "cache".into(),
            key: b"k".to_vec(),
            prior: image(),
        },
        UndoEntry::KvHistory {
            collection: "cache".into(),
            key: b"k".to_vec(),
            prior: vec![HistoryVersion {
                sys_from: 1,
                sys_to: 2,
                value: b"h".to_vec(),
                expire_at_ms: 0,
                surrogate: Surrogate::new(4),
            }],
        },
        UndoEntry::KvTruncate {
            collection: "cache".into(),
            rows: vec![KvExportEntry {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                expire_at_ms: 0,
                surrogate: Surrogate::new(4),
                sys_from: 3,
            }],
        },
        UndoEntry::MarkNodeDeleted {
            database_id: 0,
            tid: 1,
            collection: "g".into(),
            node_id: "n".into(),
        },
        UndoEntry::CrdtCollection(Box::new(CrdtCollectionUndo {
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            collection: "tasks".into(),
            engine_existed: true,
            snapshot: Some(vec![1, 2, 3]),
        })),
        UndoEntry::ArrayTiles {
            array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "grid"),
            snapshot: crate::engine::array::ArrayTileSnapshot::of_tiles(vec![
                (
                    nodedb_array::types::TileId::new(1, 0),
                    Some(crate::engine::array::memtable::TileBuffer::new(3)),
                ),
                (nodedb_array::types::TileId::new(2, 5), None),
            ]),
        },
        UndoEntry::VectorWrite(Box::new(VectorWriteUndo {
            index_key: collection_key(),
            tid: 1,
            collection: "docs".into(),
            mark: Some(mark),
            params_absent: true,
            sidecars: vec![RowVersionFloor {
                key: key(5),
                floor: Some(3),
            }],
            payload_rows: vec![(0, fields())],
        })),
        UndoEntry::VectorPayloadRows(Box::new(VectorPayloadRowsUndo {
            index_key: collection_key(),
            removed: Some(PayloadRow {
                node_id: 1,
                fields: fields(),
            }),
            inserted: None,
        })),
        UndoEntry::SparseDoc {
            key: (
                DatabaseId::DEFAULT,
                TenantId::new(1),
                "docs".into(),
                "sv".into(),
            ),
            doc_id: "d1".into(),
            prior: sparse.doc_image("d1"),
            next_id: Some(4),
        },
        UndoEntry::SpatialRow(Box::new(SpatialRowUndo {
            key: spatial_key(),
            row: RowVersionFloor {
                key: key(6),
                floor: None,
            },
            entry_id: 5,
            prior_entry: Some((bbox(), "p1".into(), TreeSrid::WGS84)),
        })),
        UndoEntry::FtsDocument(Box::new(FtsDocUndo {
            database_id: 0,
            tid: 1,
            collection: "docs".into(),
            surrogate: Surrogate::new(1),
            prior: None,
            frames,
        })),
        UndoEntry::SyncHwm {
            producer_id: 1,
            stream_id: 2,
            prior: Some(3),
        },
        UndoEntry::NodeLabels {
            database_id: 0,
            tid: 1,
            node_id: "n".into(),
            prior: vec![("L".into(), true)],
            interned_labels: vec!["L".into()],
            created_node: true,
            version: Some(4),
            replaced: Some(vec!["M".into()]),
        },
        UndoEntry::ColumnarInsert {
            collection_key: collection_key(),
            row_count_before: 2,
            inserted_pks: vec![b"p".to_vec()],
            displaced: vec![(b"q".to_vec(), location())],
        },
        UndoEntry::ColumnarUpdate {
            collection_key: collection_key(),
            row_count_before: 2,
            inserted_pks: vec![b"p".to_vec()],
            displaced: Vec::new(),
            restored: vec![(b"r".to_vec(), location())],
        },
        UndoEntry::ColumnarDelete {
            collection_key: collection_key(),
            restored: vec![(b"r".to_vec(), location())],
        },
        UndoEntry::ColumnarEngineCreated {
            collection_key: collection_key(),
        },
        UndoEntry::TimeseriesEndedInstall(TimeseriesEndedInstallUndo {
            collection_key: collection_key(),
            partition_key: crate::engine::timeseries::partition_registry::PartitionKey {
                min_ts: 1,
                dir_name: "p-1".into(),
            },
        }),
        UndoEntry::ArrayInstallFlush(Box::new(
            super::super::array_install_flush::ArrayInstallFlushUndo {
                array_id: nodedb_array::types::ArrayId::new(TenantId::new(1), "grid"),
                segment: "0000000002.ndas".into(),
                replay_before: crate::types::replay_stamp::ReplayStamp::through(7),
                partial_before: crate::types::partial_record::PartialRecords::naming(
                    crate::types::partial_record::PartialRecord {
                        lsn: 9,
                        ops_through: 3,
                    },
                ),
            },
        )),
        UndoEntry::TimeseriesInstallFlush(Box::new(
            super::super::ts_install_flush::TsInstallFlushUndo {
                collection_key: collection_key(),
                revert: crate::data::executor::handlers::timeseries::flush_commit::FlushRevert {
                    dirs: vec!["ts-1_2-0000000000000009".into(), "pending-9-0".into()],
                    drop_commit: true,
                },
                partition_keys: vec![
                    crate::engine::timeseries::partition_registry::PartitionKey {
                        min_ts: 1,
                        dir_name: "ts-1_2-0000000000000009".into(),
                    },
                ],
                stamp_before: Some(
                    crate::data::executor::timeseries_checkpoint::stamp::TsReplayStamp {
                        rows: crate::types::replay_stamp::ReplayStamp::through(4),
                        truncates: crate::types::replay_stamp::ReplayStamp::naming(6),
                        partial: crate::types::partial_record::PartialRecords::naming(
                            crate::types::partial_record::PartialRecord {
                                lsn: 9,
                                ops_through: 2,
                            },
                        ),
                    },
                ),
                registry_existed: true,
                adaptive_before: Some(
                    crate::engine::timeseries::partition_registry::PartitionRegistry::new(
                        nodedb_types::timeseries::TieredPartitionConfig::origin_defaults(),
                    )
                    .adaptive_state(),
                ),
                reservation_bytes_before: Some(4096),
                files_before: Some(super::super::ts_install_flush::TsFilesBefore {
                    schema: Some(vec![1, 2]),
                    catalog: None,
                }),
            },
        )),
        UndoEntry::StatsRestore {
            key: "0:1:c:status".into(),
            prior: Some(vec![1]),
        },
    ]
}

fn spilled(entry: UndoEntry) -> SpilledUndo {
    match SpilledUndo::try_from(entry) {
        Ok(spilled) => spilled,
        Err(_) => panic!("a plain-data undo entry has a spilled form"),
    }
}

fn encoded(entry: &SpilledUndo) -> Vec<u8> {
    zerompk::to_msgpack_vec(entry).expect("encode spilled undo")
}

fn frames() -> crate::engine::sparse::inverted::SyncedTextMark {
    let dir = tempfile::tempdir().expect("tempdir");
    let (core, _tx, _rx) = make_core_with_dir(dir.path());
    core.inverted
        .synced_text_mark(0, TenantId::new(1), "docs", Surrogate::new(1), Some(5))
        .expect("frame mark")
}

/// Each variant encodes, decodes into the entry it came from, and that
/// entry encodes to the same bytes again.
#[test]
fn every_spilled_variant_round_trips() {
    let entries = plain_entries(frames());
    assert_eq!(entries.len(), 35, "one entry per plain-data variant");
    for (index, entry) in entries.into_iter().enumerate() {
        let bytes = encoded(&spilled(entry));
        let decoded: SpilledUndo = zerompk::from_msgpack(&bytes).expect("decode spilled undo");
        let again = encoded(&spilled(UndoEntry::from(decoded)));
        assert_eq!(again, bytes, "entry {index} changed through the spill");
    }
}

/// An untouched chain, a genesis chain and a chain head stay apart.
#[test]
fn chain_priors_keep_genesis_apart_from_untouched() {
    for prior in [None, Some(None), Some(Some(head()))] {
        let stored = ChainPrior::from(prior.clone());
        let bytes = zerompk::to_msgpack_vec(&stored).expect("encode");
        let read: ChainPrior = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(Option::<Option<ChainHead>>::from(read), prior);
    }
    let put = UndoEntry::PutDocument {
        collection: "c".into(),
        document_id: key(1),
        version_sys_from: 1,
        index_tuples: Vec::new(),
        chain_hash_prior: Some(None),
    };
    let bytes = encoded(&spilled(put));
    let read: SpilledUndo = zerompk::from_msgpack(&bytes).expect("decode");
    match UndoEntry::from(read) {
        UndoEntry::PutDocument {
            chain_hash_prior, ..
        } => assert_eq!(chain_hash_prior, Some(None)),
        _ => panic!("a put document undo reads back as one"),
    }
}

/// A snapshot of in-memory state has no spilled form.
#[test]
fn a_memtable_snapshot_stays_in_ram() {
    let ingest = UndoEntry::TimeseriesIngest(TimeseriesIngestUndo {
        collection_key: collection_key(),
        memtable_before: None,
        memtable_config_before: None,
        memtable_memory_bytes_before: None,
        last_value_cache_before: None,
        series_catalog_before: None,
        last_ts_ingest_before: None,
        reservation_bytes_before: None,
    });
    assert!(matches!(
        SpilledUndo::try_from(ingest),
        Err(UndoEntry::TimeseriesIngest(_))
    ));
}
