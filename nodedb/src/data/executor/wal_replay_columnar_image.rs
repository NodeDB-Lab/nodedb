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
use crate::data::executor::handlers::columnar_write::ndb_field_to_value;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// One decoded row of a `columnar_image` record.
struct ImageRow {
    surrogate: Surrogate,
    prior_pk: Option<Value>,
    image: Option<Value>,
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
        .map(|col| ndb_field_to_value(fields.get(&col.name), &col.column_type))
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
        let task = Self::replay_task(
            tid,
            database_id,
            VShardId::from_collection_in_database(database_id, collection),
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
            }
        };

        let mut images: Vec<(Surrogate, Vec<Value>, Value)> = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(image) = &row.image {
                images.push((row.surrogate, image_values(&schema, image)?, image.clone()));
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

        let mut undo = Vec::new();
        let removed =
            self.apply_columnar_delete_pks(&key, &schema, &priors, recording.then_some(&mut undo));
        self.record_redo_undo(undo);
        if removed.affected != priors.len() as u64 {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "columnar image record removed {} of {} base rows of '{collection}'",
                    removed.affected,
                    priors.len()
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
        let delta = self.index_columnar_geometry_columns(task, &schema, collection, &objects);
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
            self.note_collection_write_lsn(task, collection);
        }
        Ok(written)
    }
}
