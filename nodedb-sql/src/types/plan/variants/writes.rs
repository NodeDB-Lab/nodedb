// SPDX-License-Identifier: Apache-2.0

//! Write-plan payloads: `INSERT`, key-value `INSERT`, `UPSERT`.

use crate::types::query::EngineType;
use crate::types_expr::{SqlExpr, SqlValue};

use super::super::row_types::{KvInsertIntent, WriteRoute};

/// Payload of [`SqlPlan::Insert`](crate::types::SqlPlan::Insert).
#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub collection: String,
    pub engine: EngineType,
    /// The lowering these rows take, chosen by the engine's `EngineRules`.
    /// The conversion layer reads it instead of re-deciding from `engine`.
    pub route: WriteRoute,
    /// Every declared DEFAULT already materialized and every literal
    /// coerced to its declared column type by the planner.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Whether a DEFAULT materialized into `rows` was volatile. The
    /// planner evaluates declared defaults while building this plan, so a
    /// cached plan would replay one execution's value; a volatile plan is
    /// never cached.
    pub volatile_defaults: bool,
    /// `ON CONFLICT DO NOTHING` semantics: when true, duplicate-PK rows
    /// are silently skipped instead of raising `unique_violation`. Plain
    /// `INSERT` (no `ON CONFLICT` clause) sets this to `false`.
    pub if_absent: bool,
    /// Raw column type strings from the catalog: `(column_name, type_str)`.
    /// Forwarded from `InsertParams::column_schema`. Used by columnar
    /// converters to reconstruct the exact `ColumnType` for columns whose
    /// `SqlDataType` is ambiguous (e.g. JSON and Bytes both map to Bytes).
    pub column_schema: Vec<(String, String)>,
    /// Declared `PRIMARY KEY` column name (if any), from
    /// `InsertParams::primary_key`. Used by the conversion layer to
    /// extract the document id from the correct column instead of
    /// guessing at `id`/`document_id`/`key`.
    pub primary_key: Option<String>,
}

/// Payload of [`SqlPlan::KvInsert`](crate::types::SqlPlan::KvInsert).
#[derive(Debug, Clone)]
pub struct KvInsertPlan {
    pub collection: String,
    pub entries: Vec<(SqlValue, Vec<(String, SqlValue)>)>,
    /// TTL in seconds (0 = no expiry). Extracted from `ttl` column if present.
    pub ttl_secs: u64,
    /// INSERT-vs-UPSERT distinction. `KvOp::Put` is a Redis-SET-style
    /// upsert by design; to honor SQL `INSERT` semantics the planner must
    /// tell the converter whether a duplicate key should raise (plain
    /// `INSERT`, `Insert`), be silently skipped (`ON CONFLICT DO NOTHING`,
    /// `InsertIfAbsent`), or overwrite (`UPSERT` / `ON CONFLICT DO
    /// UPDATE`, `Put`).
    pub intent: KvInsertIntent,
    /// `ON CONFLICT (key) DO UPDATE SET field = expr` assignments, carried
    /// through when `intent == Put` via the ON-CONFLICT-DO-UPDATE path.
    /// Empty for plain UPSERT (whole-value overwrite) and for INSERT
    /// variants.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
    /// Whether any DEFAULT materialized into `entries` came from a
    /// `Volatile` expression. The key-value planner evaluates declared
    /// defaults while building this plan, so a cached plan would replay
    /// one execution's value; a volatile plan is never cached.
    pub volatile_defaults: bool,
}

/// Payload of [`SqlPlan::Upsert`](crate::types::SqlPlan::Upsert).
#[derive(Debug, Clone)]
pub struct UpsertPlan {
    pub collection: String,
    pub engine: EngineType,
    /// The lowering these rows take. Mirrors `Insert::route`.
    pub route: WriteRoute,
    /// Defaults materialized and literals coerced, as in `Insert::rows`.
    pub rows: Vec<Vec<(String, SqlValue)>>,
    /// Mirrors `Insert::volatile_defaults`.
    pub volatile_defaults: bool,
    /// `ON CONFLICT (...) DO UPDATE SET field = expr` assignments.
    /// When empty, upsert is a plain merge: new columns overwrite existing.
    /// When non-empty, the engine applies these per-row against the
    /// *existing* document instead of merging the inserted values.
    pub on_conflict_updates: Vec<(String, SqlExpr)>,
    /// Raw column type strings from the catalog: `(column_name, type_str)`.
    /// Mirrors `Insert::column_schema` — see that field for rationale.
    pub column_schema: Vec<(String, String)>,
    /// Declared `PRIMARY KEY` column name (if any). See `Insert::primary_key`.
    pub primary_key: Option<String>,
}
