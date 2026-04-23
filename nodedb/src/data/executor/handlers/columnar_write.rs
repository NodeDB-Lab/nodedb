//! Columnar base insert handler.
//!
//! Writes rows to `nodedb-columnar`'s `MutationEngine`. Accepts JSON payload
//! (array of objects). Creates the engine on first insert with schema inferred
//! from the first row.

use nodedb_columnar::MutationEngine;
use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarSchema};
use nodedb_types::value::Value;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::bridge::physical_plan::ColumnarInsertIntent;
use crate::bridge::physical_plan::document::UpdateValue;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::upsert::apply_on_conflict_updates;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a columnar insert: write rows from MessagePack payload to
    /// `MutationEngine`, applying intent-specific semantics on duplicate
    /// PK (upsert-overwrite for `Insert` and `Put`, silent skip for
    /// `InsertIfAbsent`, merge-via-`apply_on_conflict_updates` for `Put`
    /// with non-empty `on_conflict_updates`).
    pub(in crate::data::executor) fn execute_columnar_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        payload: &[u8],
        _format: &str,
        intent: ColumnarInsertIntent,
        on_conflict_updates: &[(String, UpdateValue)],
    ) -> Response {
        // Parse payload: msgpack-encoded nodedb_types::Value (array or object).
        let ndb_rows: Vec<nodedb_types::Value> = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(arr)) => arr,
            Ok(v @ nodedb_types::Value::Object(_)) => vec![v],
            Ok(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar insert: payload must be array or object".into(),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("columnar insert: invalid payload: {e}"),
                    },
                );
            }
        };

        if ndb_rows.is_empty() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "empty payload".into(),
                },
            );
        }

        let engine_key = (task.request.tenant_id, collection.to_string());
        // Ensure MutationEngine exists (auto-create on first write).
        if !self.columnar_engines.contains_key(&engine_key) {
            let schema = infer_schema_from_value(&ndb_rows[0]);
            let engine = MutationEngine::new(collection.to_string(), schema);
            self.columnar_engines.insert(engine_key.clone(), engine);
        }

        let schema = match self.columnar_engines.get(&engine_key) {
            Some(e) => e.schema().clone(),
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar engine missing after create".into(),
                    },
                );
            }
        };
        let mut accepted = 0u64;

        for row in &ndb_rows {
            let obj = match row {
                nodedb_types::Value::Object(m) => m,
                _ => continue,
            };

            // Build Value slice in schema order.
            let values: Vec<Value> = schema
                .columns
                .iter()
                .map(|col| ndb_field_to_value(obj.get(&col.name), &col.column_type))
                .collect();

            // Resolve the actual row to write (merged for ON CONFLICT DO
            // UPDATE, plain otherwise). This runs before the mutable
            // engine borrow needed by the insert call.
            let final_values: Vec<Value> = match intent {
                ColumnarInsertIntent::Put if !on_conflict_updates.is_empty() => {
                    let pk_bytes = {
                        let engine = match self.columnar_engines.get(&engine_key) {
                            Some(e) => e,
                            None => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: "columnar engine vanished during insert".into(),
                                    },
                                );
                            }
                        };
                        match engine.encode_pk_from_row(&values) {
                            Ok(b) => b,
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("columnar insert: pk encode failed: {e}"),
                                    },
                                );
                            }
                        }
                    };

                    let prior_row = self
                        .columnar_engines
                        .get(&engine_key)
                        .and_then(|e| e.lookup_memtable_row_by_pk(&pk_bytes))
                        .or_else(|| self.read_flushed_row_by_pk(&engine_key, &pk_bytes));

                    match prior_row {
                        None => values,
                        Some(prior) => {
                            let existing_val = row_values_to_object(&schema, &prior);
                            let excluded_val = row_values_to_object(&schema, &values);
                            let merged = apply_on_conflict_updates(
                                existing_val,
                                &excluded_val,
                                on_conflict_updates,
                            );
                            let merged_obj = match merged {
                                nodedb_types::Value::Object(m) => m,
                                _ => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: "merged ON CONFLICT value was not an object"
                                                .into(),
                                        },
                                    );
                                }
                            };
                            schema
                                .columns
                                .iter()
                                .map(|col| {
                                    ndb_field_to_value(merged_obj.get(&col.name), &col.column_type)
                                })
                                .collect()
                        }
                    }
                }
                _ => values,
            };

            let engine = match self.columnar_engines.get_mut(&engine_key) {
                Some(e) => e,
                None => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: "columnar engine vanished during insert".into(),
                        },
                    );
                }
            };
            let result = match intent {
                ColumnarInsertIntent::InsertIfAbsent => engine.insert_if_absent(&final_values),
                ColumnarInsertIntent::Insert | ColumnarInsertIntent::Put => {
                    engine.insert(&final_values)
                }
            };

            match result {
                Ok(_) => accepted += 1,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("columnar insert failed: {e}"),
                        },
                    );
                }
            }
        }

        let engine = match self.columnar_engines.get_mut(&engine_key) {
            Some(e) => e,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "columnar engine missing after insert loop".into(),
                    },
                );
            }
        };

        // Flush memtable to a segment if the threshold has been reached.
        if engine.should_flush() {
            let new_segment_id = engine.next_segment_id();
            let (schema, columns, row_count) = engine.memtable_mut().drain_optimized();
            if row_count > 0 {
                match nodedb_columnar::SegmentWriter::plain()
                    .write_segment(&schema, &columns, row_count)
                {
                    Ok(bytes) => {
                        self.columnar_flushed_segments
                            .entry(engine_key.clone())
                            .or_default()
                            .push(bytes);
                        tracing::debug!(
                            core = self.core_id,
                            %collection,
                            new_segment_id,
                            row_count,
                            "columnar memtable flushed and segment bytes retained in memory"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            error = %e,
                            "columnar segment encode failed; flushed rows may be lost"
                        );
                    }
                }
            }
            engine.on_memtable_flushed(new_segment_id);
        }

        // Populate R-tree for geometry columns so spatial predicates work.
        {
            let tid = task.request.tenant_id;
            let schema = engine.schema().clone();
            let geom_cols: Vec<usize> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, c)| c.column_type == ColumnType::Geometry)
                .map(|(i, _)| i)
                .collect();

            if !geom_cols.is_empty() {
                for row in &ndb_rows {
                    let obj = match row {
                        nodedb_types::Value::Object(m) => m,
                        _ => continue,
                    };
                    let doc_id = obj
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if doc_id.is_empty() {
                        continue;
                    }
                    for &col_idx in &geom_cols {
                        let col_def = &schema.columns[col_idx];
                        let field_val = match obj.get(&col_def.name) {
                            Some(v) => v,
                            None => continue,
                        };
                        // Geometry may be stored as Value::Geometry or Value::String (GeoJSON).
                        let geom: nodedb_types::geometry::Geometry = match field_val {
                            Value::Geometry(g) => g.clone(),
                            Value::String(s) => match sonic_rs::from_str(s) {
                                Ok(g) => g,
                                Err(_) => continue,
                            },
                            _ => continue,
                        };
                        let bbox = nodedb_types::bbox::geometry_bbox(&geom);
                        let index_key = (tid, collection.to_string(), col_def.name.clone());
                        let entry_id = crate::util::fnv1a_hash(doc_id.as_bytes());
                        let rtree = self.spatial_indexes.entry(index_key.clone()).or_default();
                        rtree.insert(crate::engine::spatial::RTreeEntry { id: entry_id, bbox });
                        self.spatial_doc_map.insert(
                            (tid, collection.to_string(), col_def.name.clone(), entry_id),
                            doc_id.clone(),
                        );
                    }
                }
            }
        }

        tracing::debug!(
            core = self.core_id,
            %collection,
            accepted,
            total = ndb_rows.len(),
            "columnar insert complete"
        );
        self.checkpoint_coordinator
            .mark_dirty("columnar", accepted as usize);

        let result = serde_json::json!({
            "accepted": accepted,
            "collection": collection,
        });
        let json = match response_codec::encode_json(&result) {
            Ok(b) => b,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        Response {
            request_id: task.request.request_id,
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(json),
            watermark_lsn: self.watermark,
            error_code: None,
        }
    }
}

impl CoreLoop {
    /// Ingest a single JSON document into the columnar engine for a collection.
    ///
    /// Creates the engine on first call. Used by the spatial insert path.
    pub(in crate::data::executor) fn ingest_doc_to_columnar(
        &mut self,
        tid: u32,
        collection: &str,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) {
        let engine_key = (crate::types::TenantId::new(tid), collection.to_string());
        if !self.columnar_engines.contains_key(&engine_key) {
            let schema = infer_schema_from_json(&serde_json::Value::Object(obj.clone()));
            let engine = MutationEngine::new(collection.to_string(), schema);
            self.columnar_engines.insert(engine_key.clone(), engine);
        }

        let Some(engine) = self.columnar_engines.get_mut(&engine_key) else {
            return;
        };
        let schema = engine.schema().clone();

        let ndb_obj: std::collections::HashMap<String, Value> = obj
            .iter()
            .map(|(k, v)| (k.clone(), Value::from(v.clone())))
            .collect();
        let values: Vec<Value> = schema
            .columns
            .iter()
            .map(|col| ndb_field_to_value(ndb_obj.get(&col.name), &col.column_type))
            .collect();

        let _ = engine.insert(&values);
    }
}

/// Build a `nodedb_types::Value::Object` from a schema-ordered row. Used
/// by the ON CONFLICT DO UPDATE path to present `existing` and `EXCLUDED`
/// rows to `apply_on_conflict_updates` in the same shape the document
/// upsert path uses.
pub(super) fn row_values_to_object(schema: &ColumnarSchema, row: &[Value]) -> nodedb_types::Value {
    let mut map = std::collections::HashMap::with_capacity(schema.columns.len());
    for (col, val) in schema.columns.iter().zip(row.iter()) {
        map.insert(col.name.clone(), val.clone());
    }
    nodedb_types::Value::Object(map)
}

impl CoreLoop {
    /// Read a single row from a flushed columnar segment by PK, if the PK
    /// index points to one. Returns `None` when the PK lives in the
    /// memtable, when the segment is not in memory, or when the row was
    /// tombstoned. Used by the `ON CONFLICT DO UPDATE` path to locate a
    /// prior row that has already been flushed out of the memtable.
    pub(super) fn read_flushed_row_by_pk(
        &self,
        engine_key: &(crate::types::TenantId, String),
        pk_bytes: &[u8],
    ) -> Option<Vec<Value>> {
        let engine = self.columnar_engines.get(engine_key)?;
        let loc = engine.pk_index().get(pk_bytes).copied()?;
        // Memtable case is already covered by the engine-side lookup.
        if loc.segment_id == 0 {
            return None;
        }
        // Tombstoned — prior row no longer logically present.
        if engine
            .delete_bitmap(loc.segment_id)
            .is_some_and(|bm| bm.is_deleted(loc.row_index))
        {
            return None;
        }
        let segs = self.columnar_flushed_segments.get(engine_key)?;
        // Segments are pushed in order starting at segment_id=1.
        let seg_idx = (loc.segment_id as usize).checked_sub(1)?;
        let seg_bytes = segs.get(seg_idx)?;
        let reader = nodedb_columnar::SegmentReader::open(seg_bytes).ok()?;
        let schema = engine.schema();
        let mut row = Vec::with_capacity(schema.columns.len());
        for col_idx in 0..schema.columns.len() {
            let decoded = reader.read_column(col_idx).ok()?;
            row.push(crate::data::executor::scan_normalize::decoded_col_to_value(
                &decoded,
                loc.row_index as usize,
            ));
        }
        Some(row)
    }
}

/// Coerce a `nodedb_types::Value` field to match the column type.
fn ndb_field_to_value(val: Option<&Value>, col_type: &ColumnType) -> Value {
    let Some(val) = val else { return Value::Null };
    match (col_type, val) {
        (_, Value::Null) => Value::Null,
        (ColumnType::Int64, Value::Integer(_)) => val.clone(),
        (ColumnType::Int64, Value::Float(f)) => Value::Integer(*f as i64),
        (ColumnType::Int64, Value::String(s)) => {
            s.parse::<i64>().map(Value::Integer).unwrap_or(Value::Null)
        }
        (ColumnType::Float64, Value::Float(_)) => val.clone(),
        (ColumnType::Float64, Value::Integer(n)) => Value::Float(*n as f64),
        (ColumnType::Float64, Value::String(s)) => {
            s.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        (ColumnType::Bool, Value::Bool(_)) => val.clone(),
        (ColumnType::String, Value::String(_)) => val.clone(),
        (ColumnType::Timestamp, Value::Integer(n)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*n))
        }
        (ColumnType::Timestamp, Value::Float(f)) => {
            Value::DateTime(nodedb_types::NdbDateTime::from_millis(*f as i64))
        }
        (ColumnType::Timestamp, Value::String(s)) => nodedb_types::datetime::NdbDateTime::parse(s)
            .map(Value::DateTime)
            .unwrap_or_else(|| Value::String(s.clone())),
        (ColumnType::Uuid, Value::String(_)) => val.clone(),
        // Fallback: integers as floats, strings as strings.
        (ColumnType::Float64, _) => Value::Null,
        (ColumnType::Int64, _) => Value::Null,
        _ => val.clone(),
    }
}

/// Infer a columnar schema from a `nodedb_types::Value::Object` (first row).
fn infer_schema_from_value(row: &Value) -> ColumnarSchema {
    let obj = match row {
        Value::Object(m) => m,
        _ => {
            return ColumnarSchema::new(vec![ColumnDef::required("value", ColumnType::Float64)])
                .expect("single-column schema");
        }
    };

    let mut columns = Vec::new();
    for (key, val) in obj {
        let lower = key.to_lowercase();
        let col_type = if lower == "timestamp" || lower == "ts" || lower == "time" {
            ColumnType::Timestamp
        } else {
            match val {
                Value::Float(_) => ColumnType::Float64,
                Value::Integer(_) => ColumnType::Int64,
                Value::Bool(_) => ColumnType::Bool,
                _ => ColumnType::String,
            }
        };
        if lower == "id" {
            columns.push(ColumnDef::required(key.clone(), col_type).with_primary_key());
        } else {
            columns.push(ColumnDef::nullable(key.clone(), col_type));
        }
    }

    if columns.is_empty() {
        columns.push(ColumnDef::required("value", ColumnType::Float64));
    }

    ColumnarSchema::new(columns).expect("inferred schema must be valid")
}

/// Infer a columnar schema from a JSON object — used by the spatial insert path.
pub(super) fn infer_schema_from_json(row: &serde_json::Value) -> ColumnarSchema {
    let ndb: Value = row.clone().into();
    infer_schema_from_value(&ndb)
}
