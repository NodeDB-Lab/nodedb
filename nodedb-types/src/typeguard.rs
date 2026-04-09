//! Type guard definitions for schemaless collection write-time validation.
//!
//! Type guards are per-field type + value constraints on schemaless collections.
//! They are stored in the collection catalog and evaluated on the Data Plane
//! at write time, before WAL append.

use serde::{Deserialize, Serialize};

/// A single field guard: type check + optional REQUIRED + optional CHECK expression.
///
/// Stored as part of the collection metadata in the catalog.
/// The `check_expr` is stored as a string (the original SQL expression text)
/// and parsed into an evaluable form at enforcement time.
#[derive(
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    Debug,
    Clone,
    PartialEq,
)]
pub struct TypeGuardFieldDef {
    /// Field name. Supports dot-path for nested fields (e.g., "metadata.source").
    pub field: String,
    /// Type expression string (e.g., "STRING", "INT|NULL", "ARRAY<STRING>").
    pub type_expr: String,
    /// Whether the field must be present and non-null on every write.
    pub required: bool,
    /// Optional CHECK expression as SQL text (e.g., "amount > 0").
    /// Stored as text, parsed at enforcement time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_expr: Option<String>,
}
