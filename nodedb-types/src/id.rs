//! Core identifier types used across NodeDB Origin and Lite.
//!
//! Strong typing prevents mixing up raw integers/strings. All IDs are
//! `serde` + `rkyv` serializable and safe for WASM targets.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifies a tenant. All data is tenant-scoped by construction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct TenantId(u32);

impl TenantId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tenant:{}", self.0)
    }
}

/// Identifies a collection (table/namespace).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct CollectionId(String);

impl CollectionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a document/row across all engines.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct DocumentId(String);

impl DocumentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a graph node. Separate from `DocumentId` because graph nodes
/// can exist independently of documents (e.g., concept nodes in a knowledge graph).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a graph edge. Returned by `graph_insert_edge`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct EdgeId(String);

impl EdgeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generate an edge ID from source, target, and label.
    pub fn from_components(src: &str, dst: &str, label: &str) -> Self {
        Self(format!("{src}--{label}-->{dst}"))
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Identifies a shape subscription (globally unique per Origin).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ShapeId(String);

impl ShapeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ShapeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ShapeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ShapeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_display() {
        let t = TenantId::new(42);
        assert_eq!(t.to_string(), "tenant:42");
        assert_eq!(t.as_u32(), 42);
    }

    #[test]
    fn collection_id() {
        let c = CollectionId::new("embeddings");
        assert_eq!(c.as_str(), "embeddings");
        assert_eq!(c.to_string(), "embeddings");
    }

    #[test]
    fn document_id_str() {
        let d = DocumentId::new("doc-abc-123");
        assert_eq!(d.as_str(), "doc-abc-123");
    }

    #[test]
    fn node_id() {
        let n = NodeId::new("concept:rust");
        assert_eq!(n.as_str(), "concept:rust");
    }

    #[test]
    fn edge_id_from_components() {
        let e = EdgeId::from_components("alice", "bob", "KNOWS");
        assert_eq!(e.as_str(), "alice--KNOWS-->bob");
    }

    #[test]
    fn shape_id_from_str() {
        let s = ShapeId::from("shape-001");
        assert_eq!(s.as_str(), "shape-001");
    }

    #[test]
    fn serde_roundtrip() {
        let tid = TenantId::new(7);
        let json = sonic_rs::to_string(&tid).unwrap();
        let decoded: TenantId = sonic_rs::from_str(&json).unwrap();
        assert_eq!(tid, decoded);
    }
}
