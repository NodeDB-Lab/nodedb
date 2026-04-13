//! Constraint definition types for collections.
//!
//! These are the per-collection constraint descriptors stored inline
//! on a [`super::StoredCollection`] (balanced, period lock, CHECK,
//! state transition, materialized sum, etc.). Extracted out of
//! `types.rs` to keep that file under the 500-line hard limit and
//! to colocate semantically-related constraint types.
//!
//! Field/event definitions for individual columns live here too
//! because they share the same lifecycle (defined at DDL time,
//! carried on the collection record, evaluated at write time).

use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

use crate::bridge::expr_eval::SqlExpr;

// ── Field + event definitions ─────────────────────────────────────────

/// Extended field definition supporting DEFAULT, VALUE, ASSERT, and TYPE constraints.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct FieldDefinition {
    pub name: String,
    /// Type constraint: "int", "float", "string", etc. Empty = any.
    #[serde(default)]
    pub field_type: String,
    /// Default expression (evaluated when field is missing on insert).
    #[serde(default)]
    pub default_expr: String,
    /// Computed value expression (evaluated on every read, not stored).
    #[serde(default)]
    pub value_expr: String,
    /// Assertion expression (must evaluate to true for writes to succeed).
    #[serde(default)]
    pub assert_expr: String,
    /// Whether the field is read-only (cannot be set by user).
    #[serde(default)]
    pub readonly: bool,
    /// Sequence name for auto-generated values on INSERT.
    #[serde(default)]
    pub sequence_name: Option<String>,
    /// If true, this field is a stored generated column (materialized on write).
    /// `value_expr` contains the serialized SqlExpr for write-time evaluation.
    #[serde(default)]
    pub is_generated: bool,
    /// Column names this generated column depends on (for UPDATE recomputation).
    #[serde(default)]
    pub generated_deps: Vec<String>,
}

/// Table event/trigger definition.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct EventDefinition {
    pub name: String,
    pub collection: String,
    pub when_condition: String,
    pub then_action: String,
}

// ── Constraint types ──────────────────────────────────────────────────

/// Double-entry balance constraint.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct BalancedConstraintDef {
    pub group_key_column: String,
    pub debit_value: String,
    pub credit_value: String,
    pub amount_column: String,
    pub entry_type_column: String,
}

/// Period lock: binds a period column to a reference table for status checks.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct PeriodLockDef {
    pub period_column: String,
    pub ref_table: String,
    pub ref_pk: String,
    pub status_column: String,
    pub allowed_statuses: Vec<String>,
}

/// A legal hold tag preventing deletion.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct LegalHold {
    pub tag: String,
    pub created_at: u64,
    pub created_by: String,
}

/// State transition constraint: column value can only change along declared paths.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct StateTransitionDef {
    pub name: String,
    pub column: String,
    pub transitions: Vec<TransitionRule>,
}

/// A single allowed state transition, optionally guarded by a role.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct TransitionRule {
    pub from: String,
    pub to: String,
    pub required_role: Option<String>,
}

/// Transition check predicate: evaluated on UPDATE with OLD and NEW access.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct TransitionCheckDef {
    pub name: String,
    pub predicate: SqlExpr,
}

/// General CHECK constraint: SQL boolean expression evaluated on the Control Plane
/// before writes are dispatched to the Data Plane. May contain subqueries.
///
/// Stored as raw SQL so subqueries can be re-planned at evaluation time.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct CheckConstraintDef {
    /// Constraint name (user-chosen or auto-generated).
    pub name: String,
    /// Raw SQL boolean expression (may contain `NEW.field` references and subqueries).
    pub check_sql: String,
    /// Whether this CHECK contains a subquery (SELECT). Precomputed at DDL time
    /// to skip the heavier evaluation path for simple expressions.
    pub has_subquery: bool,
}

/// Materialized sum: on INSERT to source, atomically add value_expr to target balance.
#[derive(Serialize, Deserialize, ToMessagePack, FromMessagePack, Debug, Clone)]
pub struct MaterializedSumDef {
    pub target_collection: String,
    pub target_column: String,
    pub source_collection: String,
    pub join_column: String,
    pub value_expr: SqlExpr,
}
