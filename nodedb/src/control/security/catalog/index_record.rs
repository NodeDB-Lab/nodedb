// SPDX-License-Identifier: BUSL-1.1

//! The catalog record that gives an index its identity.
//!
//! Every index kind — secondary, vector, full-text, spatial, sparse —
//! registers exactly one [`StoredIndexRecord`] under its declared name. The
//! record is what `SHOW INDEXES` lists, what `DROP INDEX` resolves, and what
//! collection teardown enumerates; the kind-specific stores (a collection's
//! `indexes` vector, `_system.vector_index_params`) hold build parameters
//! only. Without this record an index is reachable by no lifecycle
//! operation: it can be created and listed, but never dropped.

/// The engine surface an index is built on.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum IndexKind {
    /// Secondary index on a document / KV field (`CREATE INDEX`).
    Secondary,
    /// HNSW / IVF vector index (`CREATE VECTOR INDEX`).
    Vector,
    /// BM25 inverted index (`CREATE FULLTEXT INDEX`, `CREATE SEARCH INDEX`).
    FullText,
    /// R-tree / geohash index (`CREATE SPATIAL INDEX`).
    Spatial,
    /// Sparse-vector inverted index (`CREATE SPARSE INDEX`).
    Sparse,
    /// Order-statistic leaderboard index (`CREATE SORTED INDEX`).
    ///
    /// The only kind whose reads name the index instead of the collection —
    /// `RANK` / `TOPK` / `RANGE` / `SORTED_COUNT` take an index name and
    /// nothing else — so its record is what resolves those reads back to the
    /// collection they must be authorized against.
    Sorted,
}

impl IndexKind {
    /// Every kind, in the order `SHOW INDEXES` lists them.
    pub const ALL: [IndexKind; 6] = [
        IndexKind::Secondary,
        IndexKind::Vector,
        IndexKind::FullText,
        IndexKind::Spatial,
        IndexKind::Sparse,
        IndexKind::Sorted,
    ];

    /// The `object_type` this kind's ownership row is filed under. Ownership
    /// stays in the owner ledger; the registry never duplicates it.
    pub fn owner_object_type(&self) -> &'static str {
        match self {
            Self::Secondary => "index",
            Self::Vector => "vector_index",
            Self::FullText => "fulltext_index",
            Self::Spatial => "spatial_index",
            Self::Sparse => "sparse_index",
            Self::Sorted => "sorted_index",
        }
    }

    /// The value reported in the `type` column of `SHOW INDEXES`.
    pub fn display_type(&self) -> &'static str {
        match self {
            Self::Secondary => "btree",
            Self::Vector => "vector",
            Self::FullText => "fulltext",
            Self::Spatial => "spatial",
            Self::Sparse => "sparse",
            Self::Sorted => "sorted",
        }
    }

    /// The keyword that qualifies this kind in `DROP <KIND> INDEX`.
    /// `None` for the unqualified `DROP INDEX` form.
    ///
    /// `Sorted` registers no qualifier: `DROP SORTED INDEX` is claimed by the
    /// sorted-index statement family before the generic `DROP <KIND> INDEX`
    /// parser ever sees it, so advertising the keyword here would only give
    /// the same statement two parsers. The unqualified `DROP INDEX <name>`
    /// still resolves a sorted index through the registry.
    pub fn drop_keyword(&self) -> Option<&'static str> {
        match self {
            Self::Secondary | Self::Sorted => None,
            Self::Vector => Some("VECTOR"),
            Self::FullText => Some("FULLTEXT"),
            Self::Spatial => Some("SPATIAL"),
            Self::Sparse => Some("SPARSE"),
        }
    }

    /// Resolve a `DROP <KIND> INDEX` qualifier. Case-insensitive.
    ///
    /// `SEARCH` is accepted as the documented alias of `FULLTEXT`, matching
    /// `CREATE SEARCH INDEX` / `CREATE FULLTEXT INDEX`.
    pub fn from_drop_keyword(keyword: &str) -> Option<Self> {
        let upper = keyword.to_uppercase();
        if upper == "SEARCH" {
            return Some(Self::FullText);
        }
        Self::ALL
            .into_iter()
            .find(|k| k.drop_keyword() == Some(upper.as_str()))
    }
}

/// One index's identity and lifecycle state.
///
/// `fields` holds every column the index covers — one entry for the single
/// column of a vector / spatial / secondary index, several for a multi-column
/// full-text index. It is the key the kind-specific teardown needs to reach
/// engine state (`vector_index_params` is keyed by `(collection, field)`).
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredIndexRecord {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Index name as declared by the statement that created it — the name
    /// `SHOW INDEXES` reports and `DROP INDEX` accepts.
    pub name: String,
    pub kind: IndexKind,
    pub collection: String,
    pub fields: Vec<String>,
    /// Mirrors the owning collection's `is_active`. A soft-dropped collection
    /// hides its indexes; `UNDROP COLLECTION` brings them back.
    pub is_active: bool,
}

impl StoredIndexRecord {
    /// The single field this index covers, or the empty string when the kind
    /// carries none (a collection-default vector field).
    pub fn primary_field(&self) -> &str {
        self.fields.first().map(String::as_str).unwrap_or("")
    }

    /// Whether this index is currently observable — `SHOW INDEXES` lists it
    /// and `DROP INDEX` resolves it. An index of a soft-dropped collection is
    /// retained but hidden until `UNDROP COLLECTION` restores it.
    pub fn is_visible(&self) -> bool {
        self.is_active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each kind files ownership under its own `object_type`.
    #[test]
    fn owner_object_types_are_distinct() {
        for (i, a) in IndexKind::ALL.into_iter().enumerate() {
            for b in IndexKind::ALL.into_iter().skip(i + 1) {
                assert_ne!(a.owner_object_type(), b.owner_object_type());
            }
        }
    }

    #[test]
    fn drop_keywords_round_trip_case_insensitively() {
        for kind in IndexKind::ALL {
            let Some(keyword) = kind.drop_keyword() else {
                continue;
            };
            assert_eq!(IndexKind::from_drop_keyword(keyword), Some(kind));
            assert_eq!(
                IndexKind::from_drop_keyword(&keyword.to_lowercase()),
                Some(kind)
            );
        }
        assert_eq!(IndexKind::from_drop_keyword("NOSUCH"), None);
    }

    #[test]
    fn display_types_are_distinct() {
        let mut seen = std::collections::HashSet::new();
        for kind in IndexKind::ALL {
            assert!(
                seen.insert(kind.display_type()),
                "duplicate display type for {kind:?}"
            );
        }
    }
}
