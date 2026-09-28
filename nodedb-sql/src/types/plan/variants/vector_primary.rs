// SPDX-License-Identifier: Apache-2.0

//! Vector-primary collection write plan payloads.

use crate::types::filter::Filter;
use crate::types_expr::{SqlExpr, SqlValue};

use super::super::row_types::{VectorPrimaryInsertIntent, VectorPrimaryRow};

/// Payload of [`SqlPlan::VectorPrimaryInsert`](crate::types::SqlPlan::VectorPrimaryInsert).
#[derive(Debug, Clone)]
pub struct VectorPrimaryInsertPlan {
    pub collection: String,
    /// Vector column name (matches `VectorPrimaryConfig::vector_field`).
    /// Plumbed to the direct write op so the Data Plane keys its HNSW
    /// index by `(tid, collection, field)` — the same key the SELECT
    /// path uses.
    pub field: String,
    /// Collection-level quantization. Applied via `set_quantization` on
    /// the first DirectUpsert so subsequent seals trigger codec-dispatch
    /// rebuilds against the configured codec.
    pub quantization: nodedb_types::VectorQuantization,
    /// Native storage dtype for vector values (F32 / F16 / BF16).
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    /// Payload field names that get equality bitmap indexes. Registered
    /// via `payload.add_index` on the first DirectUpsert.
    pub payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
    pub rows: Vec<VectorPrimaryRow>,
    /// Whether any row carries a value materialized from a volatile
    /// DEFAULT such as `nextval`. The value was evaluated while the plan
    /// was built, so caching the lowered tasks would replay one
    /// execution's value into every later one.
    pub volatile_defaults: bool,
    /// What an existing primary key means for each row. Mirrors
    /// `KvInsert::intent`.
    pub intent: VectorPrimaryInsertIntent,
    /// `ON CONFLICT (pk) DO UPDATE SET field = expr` assignments, carried
    /// when `intent == Upsert`. Empty means whole-row replace.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
    /// Resolved primary-key column name. See `Insert::primary_key`.
    pub primary_key: Option<String>,
}

/// Payload of [`SqlPlan::VectorPrimaryDelete`](crate::types::SqlPlan::VectorPrimaryDelete).
#[derive(Debug, Clone)]
pub struct VectorPrimaryDeletePlan {
    pub collection: String,
    /// Vector column name; keys the HNSW index the rows live in.
    pub field: String,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    /// Resolved primary-key column name. See `Insert::primary_key`.
    pub primary_key: Option<String>,
}

/// Payload of [`SqlPlan::VectorPrimaryTruncate`](crate::types::SqlPlan::VectorPrimaryTruncate).
#[derive(Debug, Clone)]
pub struct VectorPrimaryTruncatePlan {
    pub collection: String,
    /// Vector column name; keys the HNSW index the rows live in.
    pub field: String,
    pub restart_identity: bool,
}

/// Payload of [`SqlPlan::VectorPrimaryUpdate`](crate::types::SqlPlan::VectorPrimaryUpdate).
#[derive(Debug, Clone)]
pub struct VectorPrimaryUpdatePlan {
    pub collection: String,
    /// Vector column name; keys the HNSW index the rows live in.
    pub field: String,
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: Vec<(String, nodedb_types::PayloadIndexKind)>,
    pub new_vector: Option<Vec<f32>>,
    pub assignments: Vec<(String, SqlExpr)>,
    pub filters: Vec<Filter>,
    pub target_keys: Vec<SqlValue>,
    pub returning: bool,
    /// Resolved primary-key column name. See `Insert::primary_key`.
    pub primary_key: Option<String>,
}
