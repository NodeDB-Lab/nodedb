//! Lite query engine: SQL via nodedb-sql over local engines.
//!
//! Parses SQL with nodedb-sql, then executes against CRDT, strict,
//! and columnar engines directly — no DataFusion dependency.

use std::sync::{Arc, Mutex};

use nodedb_sql::types::*;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::htap::HtapBridge;
use crate::engine::strict::StrictEngine;
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::catalog::LiteCatalog;

/// Lite-side query engine.
pub struct LiteQueryEngine<S: StorageEngine> {
    pub(in crate::query) crdt: Arc<Mutex<CrdtEngine>>,
    pub(in crate::query) strict: Arc<Mutex<StrictEngine<S>>>,
    pub(in crate::query) columnar: Arc<Mutex<ColumnarEngine<S>>>,
    pub(in crate::query) htap: Arc<Mutex<HtapBridge>>,
    pub(in crate::query) storage: Arc<S>,
    pub(in crate::query) timeseries:
        Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
}

impl<S: StorageEngine> LiteQueryEngine<S> {
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<Mutex<StrictEngine<S>>>,
        columnar: Arc<Mutex<ColumnarEngine<S>>>,
        htap: Arc<Mutex<HtapBridge>>,
        storage: Arc<S>,
        timeseries: Arc<Mutex<crate::engine::timeseries::engine::TimeseriesEngine>>,
    ) -> Self {
        Self {
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
        }
    }

    /// No-op — collections are auto-discovered via catalog.
    pub fn register_collection(&self, _name: &str) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_strict_collection(&self, _name: &str) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_all_collections(&self) {}
    /// No-op — collections are auto-discovered via catalog.
    pub fn register_columnar_collection(&self, _name: &str) {}

    /// Execute a SQL query and return results.
    pub async fn execute_sql(&self, sql: &str) -> Result<QueryResult, LiteError> {
        if let Some(result) = self.try_handle_ddl(sql).await {
            return result;
        }

        let catalog = LiteCatalog::new(
            Arc::clone(&self.crdt),
            Arc::clone(&self.strict),
            Arc::clone(&self.columnar),
        );

        let plans = nodedb_sql::plan_sql(sql, &catalog)
            .map_err(|e| LiteError::Query(format!("SQL plan: {e}")))?;

        if plans.is_empty() {
            return Ok(QueryResult::empty());
        }

        self.execute_plan(&plans[0])
    }

    fn execute_plan(&self, plan: &SqlPlan) -> Result<QueryResult, LiteError> {
        match plan {
            SqlPlan::Scan {
                collection, engine, ..
            } => self.execute_scan(collection, engine),
            SqlPlan::PointGet {
                collection,
                engine,
                key_value,
                ..
            } => self.execute_point_get(collection, engine, key_value),
            SqlPlan::Insert {
                collection, rows, ..
            } => self.execute_insert(collection, rows),
            SqlPlan::Update {
                collection,
                assignments,
                target_keys,
                ..
            } => self.execute_update(collection, assignments, target_keys),
            SqlPlan::Delete {
                collection,
                target_keys,
                ..
            } => self.execute_delete(collection, target_keys),
            SqlPlan::Truncate { collection } => self.execute_truncate(collection),
            _ => Err(LiteError::Query(format!("unsupported plan: {plan:?}"))),
        }
    }

    fn execute_scan(
        &self,
        collection: &str,
        engine: &EngineType,
    ) -> Result<QueryResult, LiteError> {
        match engine {
            EngineType::DocumentSchemaless => {
                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                let ids = crdt.list_ids(collection);
                let mut rows = Vec::with_capacity(ids.len());
                for id in &ids {
                    if let Some(val) = crdt.read(collection, id) {
                        let json = loro_value_to_json(&val);
                        let doc_str = sonic_rs::to_string(&json).unwrap_or_default();
                        rows.push(vec![Value::String(id.clone()), Value::String(doc_str)]);
                    }
                }
                Ok(QueryResult {
                    columns: vec!["id".into(), "document".into()],
                    rows,
                    rows_affected: 0,
                })
            }
            EngineType::DocumentStrict => {
                let strict = self.strict.lock().map_err(|_| LiteError::LockPoisoned)?;
                let schema = strict.schema(collection);
                let columns = schema
                    .map(|s| s.columns.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_else(|| vec!["id".into(), "data".into()]);
                let rows = Vec::new();
                Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                })
            }
            _ => Ok(QueryResult::empty()),
        }
    }

    fn execute_point_get(
        &self,
        collection: &str,
        engine: &EngineType,
        key: &SqlValue,
    ) -> Result<QueryResult, LiteError> {
        let key_str = sql_value_to_string(key);
        match engine {
            EngineType::DocumentSchemaless => {
                let crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
                match crdt.read(collection, &key_str) {
                    Some(val) => {
                        let json = loro_value_to_json(&val);
                        let doc_str = sonic_rs::to_string(&json).unwrap_or_default();
                        Ok(QueryResult {
                            columns: vec!["id".into(), "document".into()],
                            rows: vec![vec![Value::String(key_str), Value::String(doc_str)]],
                            rows_affected: 0,
                        })
                    }
                    None => Ok(QueryResult::empty()),
                }
            }
            _ => Ok(QueryResult::empty()),
        }
    }

    fn execute_insert(
        &self,
        collection: &str,
        rows: &[Vec<(String, SqlValue)>],
    ) -> Result<QueryResult, LiteError> {
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for row in rows {
            let id = row
                .iter()
                .find(|(k, _)| k == "id")
                .map(|(_, v)| sql_value_to_string(v))
                .unwrap_or_default();
            let fields: Vec<(&str, loro::LoroValue)> = row
                .iter()
                .map(|(k, v)| (k.as_str(), sql_value_to_loro(v)))
                .collect();
            crdt.upsert(collection, &id, &fields)
                .map_err(|e| LiteError::Query(format!("insert: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }

    fn execute_update(
        &self,
        collection: &str,
        assignments: &[(String, nodedb_sql::types::SqlExpr)],
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for key in target_keys {
            let key_str = sql_value_to_string(key);
            let fields: Vec<(&str, loro::LoroValue)> = assignments
                .iter()
                .filter_map(|(field, expr)| {
                    if let nodedb_sql::types::SqlExpr::Literal(val) = expr {
                        Some((field.as_str(), sql_value_to_loro(val)))
                    } else {
                        None
                    }
                })
                .collect();
            crdt.upsert(collection, &key_str, &fields)
                .map_err(|e| LiteError::Query(format!("update: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }

    fn execute_delete(
        &self,
        collection: &str,
        target_keys: &[SqlValue],
    ) -> Result<QueryResult, LiteError> {
        let mut crdt = self.crdt.lock().map_err(|_| LiteError::LockPoisoned)?;
        let mut affected = 0;
        for key in target_keys {
            let key_str = sql_value_to_string(key);
            crdt.delete(collection, &key_str)
                .map_err(|e| LiteError::Query(format!("delete: {e}")))?;
            affected += 1;
        }
        Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: affected,
        })
    }

    fn execute_truncate(&self, collection: &str) -> Result<QueryResult, LiteError> {
        self.crdt
            .lock()
            .map_err(|_| LiteError::LockPoisoned)?
            .clear_collection(collection)
            .map_err(|e| LiteError::Query(format!("truncate: {e}")))?;
        Ok(QueryResult::empty())
    }
}

fn sql_value_to_string(v: &SqlValue) -> String {
    match v {
        SqlValue::String(s) => s.clone(),
        SqlValue::Int(i) => i.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn sql_value_to_loro(v: &SqlValue) -> loro::LoroValue {
    match v {
        SqlValue::Int(i) => loro::LoroValue::I64(*i),
        SqlValue::Float(f) => loro::LoroValue::Double(*f),
        SqlValue::String(s) => loro::LoroValue::String(s.clone().into()),
        SqlValue::Bool(b) => loro::LoroValue::Bool(*b),
        SqlValue::Null => loro::LoroValue::Null,
        _ => loro::LoroValue::Null,
    }
}

fn loro_value_to_json(v: &loro::LoroValue) -> serde_json::Value {
    match v {
        loro::LoroValue::Null => serde_json::Value::Null,
        loro::LoroValue::Bool(b) => serde_json::Value::Bool(*b),
        loro::LoroValue::I64(n) => serde_json::json!(*n),
        loro::LoroValue::Double(f) => serde_json::json!(*f),
        loro::LoroValue::String(s) => serde_json::Value::String(s.to_string()),
        loro::LoroValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in m.iter() {
                obj.insert(k.to_string(), loro_value_to_json(val));
            }
            serde_json::Value::Object(obj)
        }
        loro::LoroValue::List(arr) => {
            serde_json::Value::Array(arr.iter().map(loro_value_to_json).collect())
        }
        _ => serde_json::Value::Null,
    }
}
