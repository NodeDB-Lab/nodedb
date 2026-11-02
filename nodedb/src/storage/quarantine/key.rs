// SPDX-License-Identifier: BUSL-1.1

//! Identity of a quarantined segment.
//!
//! Each variant carries the full scope the engine stores the segment under.
//! Two tenants or two databases with the same collection name and segment id
//! therefore hold separate quarantine records.

/// Identifies an engine kind, used as a label in metrics and HTTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum QuarantineEngine {
    Columnar,
    Fts,
    Vector,
    Raft,
}

impl QuarantineEngine {
    pub fn as_str(self) -> &'static str {
        match self {
            QuarantineEngine::Columnar => "columnar",
            QuarantineEngine::Fts => "fts",
            QuarantineEngine::Vector => "vector",
            QuarantineEngine::Raft => "raft",
        }
    }
}

impl std::fmt::Display for QuarantineEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The collection that owns a segment, scoped to its database and tenant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionScope {
    pub database_id: u64,
    pub tenant_id: u64,
    pub collection: String,
}

impl CollectionScope {
    pub fn new(database_id: u64, tenant_id: u64, collection: impl Into<String>) -> Self {
        Self {
            database_id,
            tenant_id,
            collection: collection.into(),
        }
    }
}

/// Uniquely identifies a segment across all engines, databases, and tenants.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SegmentKey {
    /// A flushed columnar segment. `segment_id` is 1-based.
    Columnar {
        scope: CollectionScope,
        segment_id: u64,
    },
    /// An FTS LSM segment. `field` is `None` for the whole-document index.
    Fts {
        scope: CollectionScope,
        field: Option<String>,
        segment_id: String,
    },
    /// A vector NDVS segment.
    Vector {
        scope: CollectionScope,
        segment_id: String,
    },
    /// A Raft InstallSnapshot chunk of one group at one log index.
    Raft {
        group_id: u64,
        last_included_index: u64,
    },
}

impl SegmentKey {
    /// The engine that owns the segment.
    pub fn engine(&self) -> QuarantineEngine {
        match self {
            SegmentKey::Columnar { .. } => QuarantineEngine::Columnar,
            SegmentKey::Fts { .. } => QuarantineEngine::Fts,
            SegmentKey::Vector { .. } => QuarantineEngine::Vector,
            SegmentKey::Raft { .. } => QuarantineEngine::Raft,
        }
    }

    /// The owning collection. `None` for a Raft snapshot chunk.
    pub fn scope(&self) -> Option<&CollectionScope> {
        match self {
            SegmentKey::Columnar { scope, .. }
            | SegmentKey::Fts { scope, .. }
            | SegmentKey::Vector { scope, .. } => Some(scope),
            SegmentKey::Raft { .. } => None,
        }
    }

    /// The FTS field. `None` for every other engine and for the
    /// whole-document FTS index.
    pub fn field(&self) -> Option<&str> {
        match self {
            SegmentKey::Fts { field, .. } => field.as_deref(),
            SegmentKey::Columnar { .. } | SegmentKey::Vector { .. } | SegmentKey::Raft { .. } => {
                None
            }
        }
    }

    /// The segment id as text. A Raft chunk renders as `group=<g>:index=<i>`.
    pub fn segment_label(&self) -> String {
        match self {
            SegmentKey::Columnar { segment_id, .. } => segment_id.to_string(),
            SegmentKey::Fts { segment_id, .. } | SegmentKey::Vector { segment_id, .. } => {
                segment_id.clone()
            }
            SegmentKey::Raft {
                group_id,
                last_included_index,
            } => format!("group={group_id}:index={last_included_index}"),
        }
    }
}

impl std::fmt::Display for SegmentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "engine={}", self.engine())?;
        if let Some(scope) = self.scope() {
            write!(
                f,
                " database_id={} tenant_id={} collection={}",
                scope.database_id, scope.tenant_id, scope.collection
            )?;
        }
        if let Some(field) = self.field() {
            write!(f, " field={field}")?;
        }
        write!(f, " segment_id={}", self.segment_label())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_collection_in_two_tenants_is_two_keys() {
        let a = SegmentKey::Columnar {
            scope: CollectionScope::new(1, 10, "orders"),
            segment_id: 1,
        };
        let b = SegmentKey::Columnar {
            scope: CollectionScope::new(1, 11, "orders"),
            segment_id: 1,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn display_names_every_scope_part() {
        let k = SegmentKey::Fts {
            scope: CollectionScope::new(2, 3, "docs"),
            field: Some("body".to_string()),
            segment_id: "s7".to_string(),
        };
        assert_eq!(
            k.to_string(),
            "engine=fts database_id=2 tenant_id=3 collection=docs field=body segment_id=s7"
        );
    }
}
