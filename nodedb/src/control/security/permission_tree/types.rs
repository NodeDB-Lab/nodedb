// SPDX-License-Identifier: BUSL-1.1

//! Permission tree types: definitions, levels, and grants.
//!
//! Permission levels are user-defined ordered strings stored in the
//! `PermissionTreeDef`. The default vocabulary is:
//! `["none", "viewer", "commenter", "editor", "owner"]`
//! but users can override with any ordered list.

use serde::{Deserialize, Serialize};

/// Default permission levels (lowest to highest).
pub const DEFAULT_LEVELS: &[&str] = &["none", "viewer", "commenter", "editor", "owner"];

/// Default minimum level required for read operations.
pub const DEFAULT_READ_LEVEL: &str = "viewer";

/// Default minimum level required for write operations.
pub const DEFAULT_WRITE_LEVEL: &str = "editor";

/// Default minimum level required for delete operations.
pub const DEFAULT_DELETE_LEVEL: &str = "owner";

/// Defines how a collection participates in hierarchical permission inheritance.
///
/// Stored as JSON in `StoredCollection.permission_tree_def`.
/// Binds the collection to a resource hierarchy graph and a permission table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionTreeDef {
    /// Column in this collection that serves as the resource identifier.
    /// Used to look up the resource in the permission graph.
    pub resource_column: String,

    /// Name of the graph index that stores the resource hierarchy.
    /// Parent edges: `resource → parent` (outbound direction).
    pub graph_index: String,

    /// Name of the collection that stores permission grants.
    pub permission_table: String,

    /// Ordered permission levels from lowest to highest.
    /// The first level is "no access", the last is "full control".
    /// Default: `["none", "viewer", "commenter", "editor", "owner"]`.
    #[serde(default = "default_levels")]
    pub levels: Vec<String>,

    /// Minimum level required for SELECT queries.
    #[serde(default = "default_read_level")]
    pub read_level: String,

    /// Minimum level required for INSERT/UPDATE queries.
    #[serde(default = "default_write_level")]
    pub write_level: String,

    /// Minimum level required for DELETE queries.
    #[serde(default = "default_delete_level")]
    pub delete_level: String,
}

fn default_levels() -> Vec<String> {
    DEFAULT_LEVELS.iter().map(|s| (*s).to_owned()).collect()
}

fn default_read_level() -> String {
    DEFAULT_READ_LEVEL.to_owned()
}

fn default_write_level() -> String {
    DEFAULT_WRITE_LEVEL.to_owned()
}

fn default_delete_level() -> String {
    DEFAULT_DELETE_LEVEL.to_owned()
}

impl PermissionTreeDef {
    /// Get the ordinal (0-based index) for a level name.
    /// Returns `None` if the level is not in the vocabulary.
    pub fn level_ordinal(&self, level: &str) -> Option<usize> {
        self.levels
            .iter()
            .position(|l| l.eq_ignore_ascii_case(level))
    }

    /// Check if `actual` level meets or exceeds `required` level.
    /// Returns `false` if either level is not in the vocabulary.
    pub fn level_meets_requirement(&self, actual: &str, required: &str) -> bool {
        match (self.level_ordinal(actual), self.level_ordinal(required)) {
            (Some(a), Some(r)) => a >= r,
            _ => false,
        }
    }

    /// Validate that all configured levels and thresholds are consistent.
    pub fn validate(&self) -> crate::Result<()> {
        if self.levels.len() < 2 {
            return Err(crate::Error::BadRequest {
                detail: "permission levels must have at least 2 entries (including 'none')"
                    .to_string(),
            });
        }
        if self.resource_column.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: "resource_column must not be empty".to_string(),
            });
        }
        if self.graph_index.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: "graph_index must not be empty".to_string(),
            });
        }
        if self.permission_table.is_empty() {
            return Err(crate::Error::BadRequest {
                detail: "permission_table must not be empty".to_string(),
            });
        }
        for level in [&self.read_level, &self.write_level, &self.delete_level] {
            if self.level_ordinal(level).is_none() {
                return Err(crate::Error::BadRequest {
                    detail: format!("level '{level}' not found in levels list"),
                });
            }
        }
        Ok(())
    }
}

/// A single permission grant: who has what access on which resource.
///
/// Loaded from the permission table into the in-memory cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionGrant {
    /// Resource ID this grant applies to.
    pub resource_id: String,
    /// Grantee: user ID or role name.
    pub grantee: String,
    /// Permission level (must be in the `PermissionTreeDef.levels` vocabulary).
    pub level: String,
    /// If `true`, this grant is inherited from an ancestor (informational).
    /// If `false`, this is an explicit override at this level in the tree.
    pub inherited: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_def() -> PermissionTreeDef {
        PermissionTreeDef {
            resource_column: "id".into(),
            graph_index: "resource_tree".into(),
            permission_table: "permissions".into(),
            levels: default_levels(),
            read_level: DEFAULT_READ_LEVEL.into(),
            write_level: DEFAULT_WRITE_LEVEL.into(),
            delete_level: DEFAULT_DELETE_LEVEL.into(),
        }
    }

    #[test]
    fn level_ordinal() {
        let def = make_def();
        assert_eq!(def.level_ordinal("none"), Some(0));
        assert_eq!(def.level_ordinal("viewer"), Some(1));
        assert_eq!(def.level_ordinal("owner"), Some(4));
        assert_eq!(def.level_ordinal("admin"), None);
    }

    #[test]
    fn level_meets_requirement() {
        let def = make_def();
        assert!(def.level_meets_requirement("editor", "viewer"));
        assert!(def.level_meets_requirement("viewer", "viewer"));
        assert!(!def.level_meets_requirement("none", "viewer"));
        assert!(!def.level_meets_requirement("viewer", "editor"));
    }

    #[test]
    fn custom_levels() {
        let def = PermissionTreeDef {
            resource_column: "id".into(),
            graph_index: "tree".into(),
            permission_table: "perms".into(),
            levels: vec![
                "denied".into(),
                "guest".into(),
                "member".into(),
                "admin".into(),
                "superadmin".into(),
            ],
            read_level: "guest".into(),
            write_level: "member".into(),
            delete_level: "admin".into(),
        };
        assert!(def.level_meets_requirement("member", "guest"));
        assert!(!def.level_meets_requirement("guest", "member"));
        assert!(def.level_meets_requirement("superadmin", "admin"));
        assert!(def.validate().is_ok());
    }

    #[test]
    fn case_insensitive_levels() {
        let def = make_def();
        assert!(def.level_meets_requirement("Viewer", "viewer"));
        assert!(def.level_meets_requirement("EDITOR", "viewer"));
    }

    #[test]
    fn validate_rejects_bad_config() {
        let mut def = make_def();
        def.levels = vec!["only_one".into()];
        assert!(def.validate().is_err());

        let mut def = make_def();
        def.read_level = "nonexistent".into();
        assert!(def.validate().is_err());

        let mut def = make_def();
        def.resource_column = String::new();
        assert!(def.validate().is_err());
    }
}
