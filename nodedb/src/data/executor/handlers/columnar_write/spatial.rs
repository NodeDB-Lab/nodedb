//! Spatial-side columnar ingest: build a row from a JSON object (first-row
//! schema inference on engine creation).

use nodedb_columnar::MutationEngine;
use nodedb_types::value::Value;

use crate::data::executor::core_loop::CoreLoop;

use super::schema::{infer_schema_from_json, ndb_field_to_value, prepend_bitemporal_columns};

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
        let bitemporal = self.is_bitemporal(tid, collection);
        let sys_now = if bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        if !self.columnar_engines.contains_key(&engine_key) {
            let base_schema = infer_schema_from_json(&serde_json::Value::Object(obj.clone()));
            let schema = if bitemporal {
                prepend_bitemporal_columns(base_schema)
            } else {
                base_schema
            };
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
            .map(|col| match col.name.as_str() {
                "_ts_system" if bitemporal => Value::Integer(sys_now),
                "_ts_valid_from" if bitemporal => match ndb_obj.get("_ts_valid_from") {
                    Some(Value::Integer(i)) => Value::Integer(*i),
                    _ => Value::Integer(i64::MIN),
                },
                "_ts_valid_until" if bitemporal => match ndb_obj.get("_ts_valid_until") {
                    Some(Value::Integer(i)) => Value::Integer(*i),
                    _ => Value::Integer(i64::MAX),
                },
                _ => ndb_field_to_value(ndb_obj.get(&col.name), &col.column_type),
            })
            .collect();

        let _ = engine.insert(&values);
    }
}
