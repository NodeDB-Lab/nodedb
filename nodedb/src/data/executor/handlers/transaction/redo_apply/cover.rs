// SPDX-License-Identifier: BUSL-1.1

//! Keep every published engine watermark true after a committed record
//! applies below it.
//!
//! Restart replay skips a record at or below an engine's published
//! watermark: the vector checkpoint's per-collection LSN, the KV and
//! columnar checkpoint floors, an array manifest's durable LSN. The watermark
//! claims that the published artifact holds every record at or below it.
//! Records apply in the order Raft
//! commits them, not in LSN order, so a record can apply after an artifact
//! was published at a higher LSN. That record lives only in memory, and the
//! claim is false for it. Publishing the artifact again, now holding the
//! record, makes the claim true. The install settles a timeseries partition
//! stamp the same way (`settle`).

use nodedb_types::columnar::{COLUMNAR_IMAGE_KIND, ColumnarImageWalRecord};
use nodedb_wal::record::RecordType;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;
use crate::wal::{RedoRecord, RedoSubRecord};

use super::sub_ops::kv_ops;

/// The engine-wide checkpoints a redo record writes into.
pub(super) struct WrittenEngines {
    /// A vector index the vector checkpoint publishes.
    pub vectors: bool,
    /// A KV collection the KV checkpoint publishes.
    pub kv: bool,
    /// A columnar collection the columnar checkpoint publishes.
    pub columnar: bool,
}

impl WrittenEngines {
    pub(super) fn of(redo: &RedoRecord) -> Self {
        Self {
            vectors: redo.ops.iter().any(writes_vector_index),
            kv: !kv_ops(&redo.ops).is_empty()
                || redo
                    .ops
                    .iter()
                    .any(|op| is_kv_truncate(op) || is_kv_ttl(op)),
            columnar: redo.ops.iter().any(writes_columnar),
        }
    }
}

fn writes_vector_index(op: &RedoSubRecord) -> bool {
    matches!(
        RecordType::from_raw(op.record_type),
        Some(
            RecordType::VectorPut
                | RecordType::VectorDelete
                | RecordType::VectorDirectUpsert
                | RecordType::VectorDirectUpdate
                | RecordType::VectorDirectDelete
                | RecordType::VectorDirectTruncate
                | RecordType::VectorResolvedDirectWrite
                | RecordType::MultiVectorPut
                | RecordType::MultiVectorDelete
        )
    )
}

fn is_kv_truncate(op: &RedoSubRecord) -> bool {
    RecordType::from_raw(op.record_type) == Some(RecordType::Delete)
        && zerompk::from_msgpack::<(String, String)>(&op.payload)
            .is_ok_and(|(disc, _)| disc == "kv_truncate")
}

/// A `kv_expire` or `kv_persist` sub-record: both lead with their
/// discriminator, and the collection follows it.
fn is_kv_ttl(op: &RedoSubRecord) -> bool {
    RecordType::from_raw(op.record_type) == Some(RecordType::Put)
        && (zerompk::from_msgpack::<(String, String, Vec<u8>)>(&op.payload)
            .is_ok_and(|(disc, ..)| disc == "kv_persist")
            || zerompk::from_msgpack::<(String, String, Vec<u8>, u64, u64)>(&op.payload)
                .is_ok_and(|(disc, ..)| disc == "kv_expire"))
}

fn writes_columnar(op: &RedoSubRecord) -> bool {
    match RecordType::from_raw(op.record_type) {
        Some(RecordType::ColumnarTruncate) => true,
        Some(RecordType::TimeseriesBatch) => {
            zerompk::from_msgpack::<ColumnarImageWalRecord>(&op.payload)
                .is_ok_and(|record| record.kind == COLUMNAR_IMAGE_KIND)
        }
        _ => false,
    }
}

impl CoreLoop {
    /// Record that the open redo-apply scope wrote cells to `array_id`.
    pub(in crate::data::executor) fn note_redo_array_written(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && !scope.arrays_written.contains(array_id)
        {
            scope.arrays_written.push(array_id.clone());
        }
    }

    /// Publish again every artifact whose watermark covers `lsn` but not the
    /// record just applied at it.
    pub(super) fn cover_applied_record(
        &mut self,
        lsn: Lsn,
        engines: &WrittenEngines,
        arrays: &[nodedb_array::types::ArrayId],
    ) -> Result<(), ErrorCode> {
        if engines.vectors && lsn <= self.floors.vector_published_lsn {
            let published = self.floors.vector_published_lsn;
            self.checkpoint_vector_indexes()
                .map_err(|error| republish_error("vector checkpoint", lsn, published, &error))?;
        }
        if engines.kv && lsn <= self.floors.kv_published_lsn {
            let published = self.floors.kv_published_lsn;
            self.checkpoint_kv_engines()
                .map_err(|error| republish_error("KV checkpoint", lsn, published, &error))?;
        }
        if engines.columnar && lsn <= self.floors.columnar_published_lsn {
            let published = self.floors.columnar_published_lsn;
            self.checkpoint_columnar_engines()
                .map_err(|error| republish_error("columnar checkpoint", lsn, published, &error))?;
        }
        for array_id in arrays {
            let durable = self.array_durable_lsn(array_id);
            if lsn.as_u64() > durable {
                continue;
            }
            self.array_engine
                .flush(array_id, durable)
                .map_err(|error| ErrorCode::Internal {
                    detail: format!(
                        "a record applied at lsn {} below array '{}' durable lsn {durable} \
                         could not be flushed: {error}",
                        lsn.as_u64(),
                        array_id.name
                    ),
                })?;
        }
        Ok(())
    }
}

fn republish_error(artifact: &str, lsn: Lsn, published: Lsn, error: &crate::Error) -> ErrorCode {
    ErrorCode::Internal {
        detail: format!(
            "a record applied at lsn {} below the {artifact} at lsn {} could not be \
             published again: {error}",
            lsn.as_u64(),
            published.as_u64()
        ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{DatabaseId, Surrogate};

    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::data::executor::handlers::transaction::redo_apply::CommittedRedo;
    use crate::engine::vector::collection::VectorCollection;
    use crate::engine::vector::hnsw::HnswParams;
    use crate::wal::RedoSubRecord;

    const TID: u64 = 1;

    fn vector_put(collection: &str, surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: zerompk::to_msgpack_vec(&(
                collection,
                vec![0.5f32, 0.5],
                2usize,
                "",
                None::<String>,
                surrogate,
                None::<SyncProvenance>,
            ))
            .expect("encode vector put"),
        }
    }

    #[test]
    fn a_vector_record_applied_below_a_published_checkpoint_is_published_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), TID, "docs", "");
        let mut collection = VectorCollection::new(2, HnswParams::default());
        collection.insert_with_surrogate(vec![0.1, 0.9], Surrogate::new(1));
        collection.note_checkpoint_lsn(100);
        core.vector_collections.insert(key.clone(), collection);
        core.advance_watermark(Lsn::new(100));
        core.checkpoint_vector_indexes()
            .expect("publish at lsn 100");

        // Committed at lsn 50, applied after the generation at lsn 100.
        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(50));
        let redo = RedoRecord {
            version: 1,
            ops: vec![vector_put("docs", 2)],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let response = core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["docs".to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "apply: {response:?}");
        drop(core);

        // Restart replay skips lsn 50 against the restored collection, so the
        // published generation is the only copy of the second vector.
        let dir_path = dir.path().to_path_buf();
        let (mut restored, _tx2, _rx2) = make_core_with_dir(&dir_path);
        restored.load_vector_checkpoints().expect("load");
        assert_eq!(
            restored.vector_collections.get(&key).map(|c| c.len()),
            Some(2),
            "the vector applied below the checkpoint is in the newest generation"
        );
    }

    fn kv_put(collection: &str, key: &[u8], value: &[u8], surrogate: u32) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                "kv_put",
                collection,
                key.to_vec(),
                value.to_vec(),
                0u64,
                None::<u64>,
                surrogate,
            ))
            .expect("encode kv put"),
        }
    }

    #[test]
    fn a_kv_record_applied_below_a_published_checkpoint_is_published_again() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let _prior = core.kv_engine.put(crate::engine::kv::KvPutParams {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: TID,
            collection: "cache",
            key: b"a",
            value: b"1",
            ttl_ms: 0,
            now_ms: 0,
            surrogate: Surrogate::new(1),
        });
        core.advance_watermark(Lsn::new(100));
        core.checkpoint_kv_engines().expect("publish at lsn 100");

        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(50));
        let redo = RedoRecord {
            version: 1,
            ops: vec![kv_put("cache", b"b", b"2", 2)],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let response = core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["cache".to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "apply: {response:?}");
        drop(core);

        let dir_path = dir.path().to_path_buf();
        let (mut restored, _tx2, _rx2) = make_core_with_dir(&dir_path);
        restored.load_kv_checkpoints().expect("load");
        let now = crate::engine::kv::current_ms();
        assert_eq!(
            restored
                .kv_engine
                .get(DatabaseId::DEFAULT.as_u64(), TID, "cache", b"b", now),
            Some(b"2".to_vec()),
            "the KV row applied below the checkpoint is in the newest generation"
        );
    }

    /// A record applied below a published checkpoint that cannot be published
    /// again leaves the checkpoint's claim false for it. The work cannot be
    /// rolled back, so the core fail-stops and the record's events stay unsent.
    #[test]
    fn a_failed_republish_fail_stops_the_core() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let (mut producers, mut consumers) =
            crate::event::bus::create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        core.floors.kv_published_lsn = Lsn::new(100);
        // A file where the checkpoint directory belongs makes the publish fail.
        let ckpt_dir = core
            .data_dir
            .join("kv-ckpt")
            .join(format!("core-{}", core.core_id));
        std::fs::create_dir_all(ckpt_dir.parent().expect("parent")).expect("kv-ckpt dir");
        std::fs::write(&ckpt_dir, b"not a directory").expect("block the checkpoint dir");

        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(50));
        let redo = RedoRecord {
            version: 1,
            ops: vec![kv_put("cache", b"b", b"2", 2)],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let response = core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["cache".to_string()],
                sum_targets: &[],
            },
        );

        assert_eq!(response.status, Status::Error);
        assert!(core.fail_stop.is_stopped(), "the core fail-stops");
        assert!(
            consumers[0].try_recv().is_none(),
            "no event leaves for a record whose post-install work failed"
        );
    }

    #[test]
    fn a_columnar_record_applied_below_a_published_checkpoint_is_published_again() {
        use nodedb_types::Value;
        use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarImageWalRow, ColumnarSchema};

        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (
            DatabaseId::DEFAULT,
            crate::types::TenantId::new(TID),
            "m".to_string(),
        );
        let schema = ColumnarSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
                ColumnDef::required("v", ColumnType::Int64),
            ],
            version: 1,
        };
        let mut engine = nodedb_columnar::MutationEngine::new("m".to_string(), schema);
        engine
            .insert_with_surrogate(&[Value::Integer(1), Value::Integer(10)], Surrogate::new(1))
            .expect("seed row");
        core.columnar_engines.insert(key.clone(), engine);
        core.advance_watermark(Lsn::new(100));
        core.checkpoint_columnar_engines()
            .expect("publish at lsn 100");

        let mut image = std::collections::HashMap::new();
        image.insert("id".to_string(), Value::Integer(2));
        image.insert("v".to_string(), Value::Integer(20));
        let record = ColumnarImageWalRecord {
            kind: COLUMNAR_IMAGE_KIND.to_string(),
            collection: "m".to_string(),
            schema_bytes: Vec::new(),
            rows: vec![ColumnarImageWalRow {
                surrogate: 2,
                prior_pk_msgpack: Vec::new(),
                image_msgpack: nodedb_types::value_to_msgpack(&Value::Object(image))
                    .expect("encode image"),
            }],
        };
        let mut task = make_default_task();
        task.wal_lsn = Some(Lsn::new(50));
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::TimeseriesBatch as u32,
                payload: zerompk::to_msgpack_vec(&record).expect("encode image record"),
            }],
            calvin_stamp: None,
        }
        .to_bytes()
        .expect("encode redo");
        let response = core.execute_apply_transaction_redo(
            &task,
            TID,
            CommittedRedo {
                redo: &redo,
                collections: &["m".to_string()],
                sum_targets: &[],
            },
        );
        assert_eq!(response.status, Status::Ok, "apply: {response:?}");
        drop(core);

        let dir_path = dir.path().to_path_buf();
        let (mut restored, _tx2, _rx2) = make_core_with_dir(&dir_path);
        restored.load_columnar_checkpoints().expect("load");
        let mut ids: Vec<Value> = restored
            .columnar_engines
            .get(&key)
            .expect("engine restored")
            .scan_memtable_rows()
            .map(|row| row[0].clone())
            .collect();
        ids.sort_by_key(|value| match value {
            Value::Integer(i) => *i,
            _ => i64::MAX,
        });
        assert_eq!(
            ids,
            vec![Value::Integer(1), Value::Integer(2)],
            "the columnar row applied below the checkpoint is in the newest generation"
        );
    }
}
