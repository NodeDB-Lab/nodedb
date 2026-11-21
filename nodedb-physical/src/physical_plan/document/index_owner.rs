// SPDX-License-Identifier: Apache-2.0

//! The owner of one document secondary-index path.
//!
//! A path can hold entries for more than one owner. An explicit index and a
//! declared field's auto-index can name the same path. A build adds its
//! owner to the path, and a retire removes it. The path's entries go only
//! when no owner is left.
//!
//! A schemaless collection keeps every top-level key instead of one path
//! per declared field. Its build and retire name [`DocIndexOwner::DeclaredTopLevel`]
//! with [`TOP_LEVEL_BUILD_PATH`] as the path.

/// The path a top-level build or retire carries. The step covers every
/// top-level key, so the path names no one key.
pub const TOP_LEVEL_BUILD_PATH: &str = "$";

/// Who keeps one document index path maintained.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum DocIndexOwner {
    /// An explicit `CREATE INDEX`, by its catalog name.
    Named(String),
    /// The auto-index a declared field derives.
    Declared,
    /// Every top-level key of a schemaless collection, as non-array,
    /// case-sensitive entries with no predicate. A named owner of one key's
    /// path decides that path's entries.
    DeclaredTopLevel,
}

/// The catalog state a document index step derives from: the collection
/// descriptor's incarnation and descriptor version.
///
/// The order is total and monotonic per collection, with the incarnation as
/// the major key. Each core keeps the highest fence it applied per path and
/// owner. A step whose fence is below it derives from an older descriptor
/// and changes nothing. Steps from one descriptor carry equal fences and all
/// apply. A step of a newer incarnation discards the older incarnation's set.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DocIndexFence {
    /// The collection's incarnation. Field order makes it the major key.
    pub incarnation: nodedb_types::Hlc,
    /// The descriptor version within the incarnation.
    pub descriptor_version: u64,
}

impl DocIndexOwner {
    /// Whether an explicit index owns the path.
    pub fn is_named(&self) -> bool {
        matches!(self, Self::Named(_))
    }
}

impl std::fmt::Display for DocIndexOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Named(name) => write!(f, "index '{name}'"),
            Self::Declared => f.write_str("declared field"),
            Self::DeclaredTopLevel => f.write_str("top-level keys"),
        }
    }
}
