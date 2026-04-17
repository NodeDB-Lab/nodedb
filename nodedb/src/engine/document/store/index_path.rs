//! Index path declaration for secondary indexes.

use crate::bridge::physical_plan::RegisteredIndexState;

/// Index path declaration for automatic secondary index extraction.
///
/// Carries modifier state (UNIQUE, case-insensitive, build phase) so the
/// write-path handler can enforce constraints and the planner can skip
/// `Building` indexes without losing dual-write coverage.
///
/// Example: `IndexPath::new("$.user.email")` extracts the `user.email` field
/// from each document and indexes it in redb for efficient lookups.
#[derive(Debug, Clone)]
pub struct IndexPath {
    /// Index name (catalog-assigned, used in UNIQUE violation errors).
    pub name: String,
    /// JSON-path-like expression (e.g., `$.user.email`, `$.tags[]`).
    pub path: String,
    /// Whether this path extracts array elements individually.
    pub is_array: bool,
    /// UNIQUE — enforced at write-path pre-commit.
    pub unique: bool,
    /// COLLATE NOCASE — values lowercased before put/lookup.
    pub case_insensitive: bool,
    /// Build state — `Building` indexes get dual-writes but aren't picked
    /// by the planner.
    pub state: RegisteredIndexState,
}

impl IndexPath {
    /// Lightweight constructor used by in-crate tests and registration of
    /// auto-derived per-field indexes. Resulting index is `Ready`, not
    /// unique, case-sensitive, and named after its path.
    pub fn new(path: &str) -> Self {
        let is_array = path.ends_with("[]");
        let path_trimmed = path.strip_suffix("[]").unwrap_or(path).to_string();
        Self {
            name: path_trimmed.clone(),
            path: path_trimmed,
            is_array,
            unique: false,
            case_insensitive: false,
            state: RegisteredIndexState::Ready,
        }
    }

    /// Build from the wire-format [`RegisteredIndex`] sent via
    /// `DocumentOp::Register`.
    pub fn from_registered(spec: &crate::bridge::physical_plan::RegisteredIndex) -> Self {
        let is_array = spec.path.ends_with("[]");
        let path = spec
            .path
            .strip_suffix("[]")
            .unwrap_or(&spec.path)
            .to_string();
        Self {
            name: spec.name.clone(),
            path,
            is_array,
            unique: spec.unique,
            case_insensitive: spec.case_insensitive,
            state: spec.state,
        }
    }
}
