// SPDX-License-Identifier: BUSL-1.1

//! Install of a transaction's columnar row images (`columnar_image`
//! records), for restart replay and the committed-redo apply alike.
//!
//! Each row names a surrogate, the primary key of the base row the
//! transaction displaced (when it displaced one), and the row's post-image.
//! The install removes every displaced base row by key, then writes every
//! image verbatim under its surrogate, then runs the side effects a live
//! columnar insert runs (memtable flush, geometry index, aggregate cache,
//! checkpoint dirtiness, collection write floor).
//!
//! A bitemporal collection keeps every base row as history. A base key the
//! transaction removes (a delete, or an update that changes the key) closes
//! with a tombstone at the stamp the row carries. A base row an image of the
//! same key replaces stays as the version the image supersedes.
//!
//! The install is all-or-nothing up to engine errors: every row decodes,
//! every image coerces to the schema, and every displaced key is bound before
//! the first mutation.

use nodedb_columnar::pk_index::encode_pk;
use nodedb_physical::physical_plan::{ColumnarInsertIntent, ColumnarOp};
use nodedb_types::columnar::{COLUMNAR_IMAGE_KIND, ColumnarImageWalRecord, ColumnarSchema};
use nodedb_types::{Surrogate, Value};

use super::core_loop::CoreLoop;
use super::wal_replay_columnar_truncate::TruncateFloors;
use crate::bridge::envelope::{ErrorCode, PhysicalPlan};
use crate::data::executor::handlers::columnar_mutation_apply::ColumnarDeleteMode;
use crate::data::executor::handlers::columnar_write::ndb_field_to_value;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn, TenantId};

/// One decoded row of a `columnar_image` record.
struct ImageRow {
    surrogate: Surrogate,
    prior_pk: Option<Value>,
    image: Option<Value>,
    /// The stamp the transaction's resolve fixed, for a bitemporal
    /// collection.
    system_from: Option<i64>,
}

/// How an install removes the base rows its record displaces.
enum Removals {
    /// Mark each base row deleted: a collection that is not bitemporal.
    Erase(Vec<Value>),
    /// Close each base key with a tombstone at its stamp: a bitemporal
    /// collection.
    Close(Vec<(Value, i64)>),
}

impl Removals {
    fn len(&self) -> usize {
        match self {
            Self::Erase(pks) => pks.len(),
            Self::Close(closes) => closes.len(),
        }
    }
}

/// Decode every row, or name the first one that does not decode.
fn decode_rows(record: &ColumnarImageWalRecord) -> crate::Result<Vec<ImageRow>> {
    let decode = |bytes: &[u8], what: &str, surrogate: u32| -> crate::Result<Option<Value>> {
        if bytes.is_empty() {
            return Ok(None);
        }
        nodedb_types::value_from_msgpack(bytes)
            .map(Some)
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("{what} of surrogate {surrogate} does not decode: {e}"),
            })
    };
    record
        .rows
        .iter()
        .map(|row| {
            let image = decode(&row.image_msgpack, "image", row.surrogate)?;
            if let Some(image) = &image
                && !matches!(image, Value::Object(_))
            {
                return Err(crate::Error::Internal {
                    detail: format!("image of surrogate {} is not an object", row.surrogate),
                });
            }
            Ok(ImageRow {
                surrogate: Surrogate::new(row.surrogate),
                prior_pk: decode(&row.prior_pk_msgpack, "base key", row.surrogate)?,
                image,
                system_from: row.system_from,
            })
        })
        .collect()
}

/// The schema-ordered values of `image`, with bitemporal columns taken from
/// the image as written.
fn image_values(schema: &ColumnarSchema, image: &Value) -> Result<Vec<Value>, ErrorCode> {
    let Value::Object(fields) = image else {
        return Err(ErrorCode::Internal {
            detail: "columnar image is not an object".into(),
        });
    };
    schema
        .columns
        .iter()
        .map(|col| ndb_field_to_value(fields.get(&col.name), col))
        .collect::<crate::Result<Vec<Value>>>()
        .map_err(ErrorCode::from)
}

impl CoreLoop {
    /// Try to decode `payload` as a `columnar_image` record and, if it is
    /// one, install it. `None` when the payload is another shape. `Some(n)`
    /// with the number of rows written otherwise, `Some(0)` when a gate
    /// skipped the record or the install failed (reported through
    /// `replay_policy`).
    pub(in crate::data::executor) fn try_replay_columnar_image(
        &mut self,
        payload: &[u8],
        tenant_id: u64,
        database_id: DatabaseId,
        record_lsn: u64,
        tombstones: &nodedb_wal::TombstoneSet,
        truncate_floors: &TruncateFloors,
    ) -> Option<usize> {
        let record: ColumnarImageWalRecord = zerompk::from_msgpack(payload).ok()?;
        if record.kind != COLUMNAR_IMAGE_KIND {
            return None;
        }
        let collection = record.collection.as_str();
        if tombstones.is_tombstoned(database_id.as_u64(), tenant_id, collection, record_lsn) {
            return Some(0);
        }
        let tid = TenantId::new(tenant_id);
        if truncate_floors.covers_collection(database_id, tid, collection, record_lsn) {
            return Some(0);
        }
        // Already folded into the restored checkpoint. Re-installing would
        // append a second version on a `bitemporal=true` collection.
        if self.replay_watermark_skips(self.floors.replay_floors.columnar.covers(record_lsn)) {
            return Some(0);
        }
        let rows = match decode_rows(&record) {
            Ok(rows) => rows,
            Err(error) => {
                self.replay_record_unapplied(
                    "columnar",
                    "image_decode",
                    record_lsn,
                    &format!("image record of '{collection}': {error}"),
                );
                return Some(0);
            }
        };

        if self.claim_for_validation() {
            return Some(0);
        }

        let images: Vec<Value> = rows.iter().filter_map(|r| r.image.clone()).collect();
        let plan_payload = match nodedb_types::value_to_msgpack(&Value::Array(images)) {
            Ok(bytes) => bytes,
            Err(e) => {
                self.replay_record_unapplied(
                    "columnar",
                    "image_encode",
                    record_lsn,
                    &format!("images of '{collection}' do not re-encode: {e}"),
                );
                return Some(0);
            }
        };
        let Some(vshard) = self.replay_vshard("columnar", record_lsn, database_id, collection)
        else {
            return Some(0);
        };
        let task = Self::replay_task(
            tid,
            database_id,
            vshard,
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
                payload: plan_payload,
                format: "msgpack".into(),
                intent: ColumnarInsertIntent::Put,
                on_conflict_updates: Vec::new(),
                surrogates: rows
                    .iter()
                    .filter(|r| r.image.is_some())
                    .map(|r| r.surrogate)
                    .collect(),
                schema_bytes: record.schema_bytes.clone(),
                provenance: None,
                wal_lsn: Some(record_lsn),
                system_from: None,
                rls_write_check: nodedb_types::RlsWriteCheck::already_decided_elsewhere(),
                returning: None,
                rls_filters: Vec::new(),
            }),
            Some(Lsn::new(record_lsn)),
        );
        match self.install_columnar_images(&task, collection, &record.schema_bytes, &rows) {
            Ok(written) => Some(written),
            Err(error) => {
                self.replay_record_rejected(
                    "columnar",
                    record_lsn,
                    Some(Box::new(error)),
                    &format!("columnar image install into '{collection}' failed"),
                );
                Some(0)
            }
        }
    }

    /// Remove every displaced base row, then write every image under its
    /// surrogate. Returns the number of rows removed and written.
    fn install_columnar_images(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        schema_bytes: &[u8],
        rows: &[ImageRow],
    ) -> Result<usize, ErrorCode> {
        let key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let recording = self.recording_redo_undo();
        let schema = match self.columnar_engines.get(&key) {
            Some(engine) => engine.schema().clone(),
            None => {
                if recording {
                    self.record_redo_undo([UndoEntry::ColumnarEngineCreated {
                        collection_key: key.clone(),
                    }]);
                }
                if rows.iter().any(|r| r.prior_pk.is_some()) {
                    return Err(ErrorCode::Internal {
                        detail: format!(
                            "columnar image record removes base rows of '{collection}', which \
                             has no engine on this core"
                        ),
                    });
                }
                let Some(first) = rows.iter().find_map(|r| r.image.as_ref()) else {
                    return Ok(0);
                };
                let bitemporal =
                    self.is_bitemporal(key.0.as_u64(), task.request.tenant_id.as_u64(), collection);
                self.ensure_columnar_engine_schema(
                    &key,
                    collection,
                    bitemporal,
                    first,
                    schema_bytes,
                )
                .map_err(ErrorCode::from)?
            }
        };

        let mut images: Vec<(Surrogate, Vec<Value>, Value)> = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(image) = &row.image {
                let values = image_values(&schema, image)?;
                // The image keeps the ordinal its transaction stamped.
                self.observe_columnar_row_ordinal(&schema, &values);
                images.push((row.surrogate, values, image.clone()));
            }
        }
        let priors: Vec<Value> = rows.iter().filter_map(|r| r.prior_pk.clone()).collect();
        if let Some(engine) = self.columnar_engines.get(&key)
            && let Some(missing) = priors
                .iter()
                .find(|pk| !engine.pk_index().contains(&encode_pk(pk)))
        {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "columnar image record removes base row {missing:?} of '{collection}', \
                     which this core does not hold"
                ),
            });
        }

        let removals = self.image_removals(&key, &schema, rows)?;
        let mut undo = Vec::new();
        let removed = match &removals {
            Removals::Erase(pks) => self.apply_columnar_delete_pks(
                &key,
                &schema,
                pks,
                ColumnarDeleteMode::Erase,
                recording.then_some(&mut undo),
            ),
            Removals::Close(closes) => self.apply_columnar_close_pks_at(
                &key,
                &schema,
                closes,
                recording.then_some(&mut undo),
            ),
        }
        .map_err(ErrorCode::from)?;
        let affected = removed.affected;
        // The removed base rows come back when the install rolls back.
        if recording {
            undo.push(removed.undo_entry(key.clone()));
        }
        self.record_redo_undo(undo);
        if affected != removals.len() as u64 {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "columnar image record removed {affected} of {} base rows of '{collection}'",
                    removals.len()
                ),
            });
        }
        if recording {
            let row_count_before = self
                .columnar_engines
                .get(&key)
                .map_or(0, |engine| engine.memtable().row_count());
            let rows: Vec<Vec<Value>> =
                images.iter().map(|(_, values, _)| values.clone()).collect();
            let (inserted_pks, displaced) =
                self.columnar_insert_undo_state(&key, &rows, ColumnarInsertIntent::Put);
            self.record_redo_undo([UndoEntry::ColumnarInsert {
                collection_key: key.clone(),
                row_count_before,
                inserted_pks,
                displaced,
            }]);
        }
        let Some(engine) = self.columnar_engines.get_mut(&key) else {
            return Err(ErrorCode::Internal {
                detail: format!("columnar engine of '{collection}' vanished during install"),
            });
        };
        for (surrogate, values, _) in &images {
            engine
                .insert_with_surrogate(values, *surrogate)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("columnar image install into '{collection}': {e}"),
                })?;
        }

        // A flush drains the memtable rows the undo would truncate, so the
        // install flushes once the whole record landed.
        if recording {
            self.note_redo_columnar_written(key.clone());
        } else {
            self.flush_columnar_memtable_if_needed(task, &key, collection)
                .map_err(|response| {
                    response.error_code.map_or_else(
                        || ErrorCode::Internal {
                            detail: format!("columnar flush of '{collection}' failed"),
                        },
                        |error| *error,
                    )
                })?;
        }
        let objects: Vec<Value> = images.into_iter().map(|(_, _, image)| image).collect();
        let delta = self
            .index_columnar_geometry_columns(task, &schema, collection, &objects)
            .map_err(ErrorCode::from)?;
        if recording {
            let mut undo = Vec::new();
            Self::push_geometry_index_undo(&mut undo, delta);
            self.record_redo_undo(undo);
        }

        let written = priors.len() + objects.len();
        if written > 0 {
            self.invalidate_aggregate_cache_for_collection(
                key.0.as_u64(),
                task.request.tenant_id.as_u64(),
                collection,
            );
            self.checkpoint_coordinator.mark_dirty("columnar", written);
            self.note_collection_write(task, collection);
        }
        Ok(written)
    }
}

impl CoreLoop {
    /// The base rows the install of `rows` removes. A collection that is not
    /// bitemporal erases every base row a row names. A bitemporal collection
    /// closes a base key the row's image does not keep, at the row's stamp,
    /// and keeps a base row an image of the same key supersedes.
    ///
    /// `Err` when a bitemporal row that closes a key carries no stamp, or an
    /// image carries no primary key.
    fn image_removals(
        &self,
        key: &(DatabaseId, TenantId, String),
        schema: &ColumnarSchema,
        rows: &[ImageRow],
    ) -> Result<Removals, ErrorCode> {
        if !schema.is_bitemporal() {
            return Ok(Removals::Erase(
                rows.iter().filter_map(|r| r.prior_pk.clone()).collect(),
            ));
        }
        let collection = key.2.as_str();
        let mut closes = Vec::new();
        for row in rows {
            let Some(prior) = &row.prior_pk else {
                continue;
            };
            if let Some(image) = &row.image {
                let values = image_values(schema, image)?;
                let image_key = self
                    .columnar_engines
                    .get(key)
                    .map(|engine| engine.encode_pk_from_row(&values))
                    .transpose()
                    .map_err(|e| ErrorCode::from(crate::Error::from(e)))?;
                if image_key.as_deref() == Some(encode_pk(prior).as_slice()) {
                    continue;
                }
            }
            let stamp = row.system_from.ok_or_else(|| ErrorCode::Internal {
                detail: format!(
                    "columnar image record closes base row {prior:?} of bitemporal \
                     '{collection}' without a system-time stamp"
                ),
            })?;
            closes.push((prior.clone(), stamp));
        }
        Ok(Removals::Close(closes))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nodedb_types::columnar::{ColumnDef, ColumnType};

    use super::*;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::VShardId;

    fn schema() -> ColumnarSchema {
        ColumnarSchema::new(vec![
            ColumnDef::required("_ts_system", ColumnType::Int64),
            ColumnDef::required("_ts_valid_from", ColumnType::Int64),
            ColumnDef::required("_ts_valid_until", ColumnType::Int64),
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("v", ColumnType::Int64),
        ])
        .expect("valid")
    }

    fn version(ts: i64, id: i64, v: i64) -> Vec<Value> {
        vec![
            Value::Integer(ts),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(id),
            Value::Integer(v),
        ]
    }

    fn image(ts: i64, id: i64, v: i64) -> Value {
        let names = ["_ts_system", "_ts_valid_from", "_ts_valid_until", "id", "v"];
        Value::Object(
            names
                .iter()
                .map(|n| n.to_string())
                .zip(version(ts, id, v))
                .collect::<HashMap<_, _>>(),
        )
    }

    /// A bitemporal install keeps every base row. An image of the same key
    /// supersedes its base row, a removed key closes with a tombstone at the
    /// row's stamp, and a key-changing image closes the old key and binds
    /// the new one.
    #[test]
    fn a_bitemporal_install_keeps_every_base_row_as_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (DatabaseId::DEFAULT, TenantId::new(1), "bt".to_string());
        let mut engine = nodedb_columnar::MutationEngine::new("bt".to_string(), schema());
        for (s, (ts, id, v)) in [(100, 1, 10), (200, 2, 20), (300, 3, 30)]
            .into_iter()
            .enumerate()
        {
            engine
                .insert_with_surrogate(&version(ts, id, v), Surrogate::new(s as u32 + 1))
                .expect("seed");
        }
        core.columnar_engines.insert(key.clone(), engine);

        let txn = 1_000;
        let rows = vec![
            ImageRow {
                surrogate: Surrogate::new(1),
                prior_pk: Some(Value::Integer(1)),
                image: Some(image(txn, 1, 11)),
                system_from: Some(txn),
            },
            ImageRow {
                surrogate: Surrogate::new(2),
                prior_pk: Some(Value::Integer(2)),
                image: None,
                system_from: Some(txn),
            },
            ImageRow {
                surrogate: Surrogate::new(3),
                prior_pk: Some(Value::Integer(3)),
                image: Some(image(txn, 4, 30)),
                system_from: Some(txn),
            },
        ];
        let task = CoreLoop::replay_task(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Columnar(ColumnarOp::Truncate {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "bt"),
                restart_identity: false,
                system_from: None,
                erase_history: false,
            }),
            None,
        );
        core.install_columnar_images(&task, "bt", &[], &rows)
            .expect("install");

        let engine = core.columnar_engines.get(&key).expect("engine");
        assert_eq!(
            engine.memtable().row_count(),
            7,
            "3 base rows, 2 tombstones, 2 images"
        );
        let bound = |id: i64| engine.pk_index().contains(&encode_pk(&Value::Integer(id)));
        let closed = |id: i64| {
            engine
                .closed_location(&encode_pk(&Value::Integer(id)))
                .is_some()
        };
        assert!(bound(1) && !closed(1));
        assert!(!bound(2) && closed(2));
        assert!(!bound(3) && closed(3));
        assert!(bound(4));

        let stored: Vec<Vec<Value>> = engine
            .scan_memtable_rows()
            .map(|row| row.expect("read"))
            .collect();
        let read = |cut: Option<i64>| -> Vec<(i64, i64)> {
            let gate = core
                .columnar_version_gate(&key, cut, false)
                .expect("gate")
                .expect("bitemporal");
            let mut out: Vec<(i64, i64)> = stored
                .iter()
                .filter(|row| gate.admits(row))
                .map(|row| match (&row[3], &row[4]) {
                    (Value::Integer(id), Value::Integer(v)) => (*id, *v),
                    other => panic!("unexpected cells {other:?}"),
                })
                .collect();
            out.sort_unstable();
            out
        };
        assert_eq!(
            read(None),
            vec![(1, 11), (4, 30)],
            "the state after the commit"
        );
        assert_eq!(
            read(Some(txn - 1)),
            vec![(1, 10), (2, 20), (3, 30)],
            "the state before the commit"
        );
    }
}
