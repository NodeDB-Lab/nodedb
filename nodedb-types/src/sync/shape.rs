//! Shape definition schema: parameterized boundaries for sync subscriptions.
//!
//! A "shape" defines what subset of the database a Lite client sees.
//! Three shape types:
//!
//! - **Document shape**: `SELECT * FROM collection WHERE predicate`
//! - **Graph shape**: N-hop subgraph from a root node
//! - **Vector shape**: collection + optional namespace filter

use serde::{Deserialize, Serialize};

/// A shape definition: describes which data falls within the subscription.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ShapeDefinition {
    /// Unique shape ID.
    pub shape_id: String,
    /// Tenant scope.
    pub tenant_id: u32,
    /// Shape type with parameters.
    pub shape_type: ShapeType,
    /// Human-readable description (for debugging).
    pub description: String,
    /// Optional field filter: only sync these fields (empty = all fields).
    /// Enables selective field-level sync instead of full document sync.
    #[serde(default)]
    pub field_filter: Vec<String>,
}

/// Coordinate range filter for array shapes.
///
/// Tile-aligned: subscriptions cover whole tiles; sub-tile filtering
/// happens at receive time (per the checklist "Out of scope" declaration).
/// Both `start` and `end` are inclusive, with `end` of `None` meaning
/// "all coords from `start` onwards".
///
/// Defined here in `nodedb-types` (not in `nodedb-array`) to avoid
/// adding a cross-crate dependency from `nodedb-types` on `nodedb-array`.
/// Callers that hold a `nodedb_array::sync::snapshot::CoordRange` convert
/// to this type before constructing a `ShapeType::Array`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCoordRange {
    /// Inclusive lower bound (one element per dimension).
    pub start: Vec<u64>,
    /// Inclusive upper bound (one element per dimension).
    /// `None` = unbounded (all coords from `start` onwards).
    pub end: Option<Vec<u64>>,
}

impl ArrayCoordRange {
    /// Return `true` if `coord` falls within this range.
    ///
    /// Comparison is per-dimension lexicographic. A missing dimension in
    /// `coord` compared to `start`/`end` causes the check to return `false`.
    pub fn contains(&self, coord: &[u64]) -> bool {
        if coord.len() != self.start.len() {
            return false;
        }
        if coord < self.start.as_slice() {
            return false;
        }
        if let Some(end) = &self.end
            && (coord.len() != end.len() || coord > end.as_slice())
        {
            return false;
        }
        true
    }
}

/// Shape type: determines how mutations are evaluated for inclusion.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ShapeType {
    /// Document shape: all documents in a collection matching a predicate.
    ///
    /// Predicate is serialized filter bytes (MessagePack). Empty = all documents.
    Document {
        collection: String,
        /// Serialized predicate bytes. Empty = no filter (all documents).
        predicate: Vec<u8>,
    },

    /// Graph shape: N-hop subgraph from root nodes.
    ///
    /// Includes all nodes and edges reachable within `max_depth` hops
    /// from the root nodes. Edge label filter is optional.
    Graph {
        root_nodes: Vec<String>,
        max_depth: usize,
        edge_label: Option<String>,
    },

    /// Vector shape: all vectors in a collection/namespace.
    ///
    /// Optionally filtered by field_name (named vector fields).
    Vector {
        collection: String,
        field_name: Option<String>,
    },

    /// Array shape: all ops on a named ND-array within an optional
    /// coordinate range.
    ///
    /// `coord_range = None` means "subscribe to all ops on this array".
    /// When set, only ops whose coord falls within the range are delivered.
    Array {
        array_name: String,
        coord_range: Option<ArrayCoordRange>,
    },
}

impl ShapeDefinition {
    /// Check if a mutation on a specific collection/document might match this shape.
    ///
    /// This is a fast pre-check — returns true if the mutation COULD match.
    /// Actual predicate evaluation happens separately for document shapes.
    /// Returns `false` for `ShapeType::Array` (use `matches_array_op` instead).
    pub fn could_match(&self, collection: &str, _doc_id: &str) -> bool {
        match &self.shape_type {
            ShapeType::Document {
                collection: shape_coll,
                ..
            } => shape_coll == collection,
            ShapeType::Graph { root_nodes, .. } => {
                // Conservative: any mutation could affect graph nodes.
                !root_nodes.is_empty()
            }
            ShapeType::Vector {
                collection: shape_coll,
                ..
            } => shape_coll == collection,
            ShapeType::Array { .. } => false,
        }
    }

    /// Check if an array op on `array` at `coord` matches this shape.
    ///
    /// Returns `false` for non-Array shape types — use `could_match` for those.
    /// `coord` is the raw dimension values cast to `u64` (unsigned index space).
    pub fn matches_array_op(&self, array: &str, coord: &[u64]) -> bool {
        match &self.shape_type {
            ShapeType::Array {
                array_name,
                coord_range,
            } => {
                if array_name != array {
                    return false;
                }
                match coord_range {
                    None => true,
                    Some(range) => range.contains(coord),
                }
            }
            _ => false,
        }
    }

    /// Get the primary collection for this shape (if applicable).
    pub fn collection(&self) -> Option<&str> {
        match &self.shape_type {
            ShapeType::Document { collection, .. } => Some(collection),
            ShapeType::Vector { collection, .. } => Some(collection),
            ShapeType::Graph { .. } => None,
            ShapeType::Array { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_shape_matches_collection() {
        let shape = ShapeDefinition {
            shape_id: "s1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "all orders".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("orders", "o1"));
        assert!(!shape.could_match("users", "u1"));
        assert_eq!(shape.collection(), Some("orders"));
    }

    #[test]
    fn graph_shape() {
        let shape = ShapeDefinition {
            shape_id: "g1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Graph {
                root_nodes: vec!["alice".into()],
                max_depth: 2,
                edge_label: Some("KNOWS".into()),
            },
            description: "alice's network".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("any_collection", "any_doc"));
        assert_eq!(shape.collection(), None);
    }

    #[test]
    fn vector_shape() {
        let shape = ShapeDefinition {
            shape_id: "v1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Vector {
                collection: "embeddings".into(),
                field_name: Some("title".into()),
            },
            description: "title embeddings".into(),
            field_filter: vec![],
        };

        assert!(shape.could_match("embeddings", "e1"));
        assert!(!shape.could_match("other", "e1"));
    }

    #[test]
    fn msgpack_roundtrip() {
        let shape = ShapeDefinition {
            shape_id: "test".into(),
            tenant_id: 5,
            shape_type: ShapeType::Document {
                collection: "users".into(),
                predicate: vec![1, 2, 3],
            },
            description: "test shape".into(),
            field_filter: vec![],
        };
        let bytes = zerompk::to_msgpack_vec(&shape).unwrap();
        let decoded: ShapeDefinition = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.shape_id, "test");
        assert_eq!(decoded.tenant_id, 5);
        assert!(matches!(decoded.shape_type, ShapeType::Document { .. }));
    }

    #[test]
    fn array_shape_matches_array_op_no_range() {
        let shape = ShapeDefinition {
            shape_id: "a1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Array {
                array_name: "prices".into(),
                coord_range: None,
            },
            description: "all prices".into(),
            field_filter: vec![],
        };
        assert!(shape.matches_array_op("prices", &[0, 0]));
        assert!(shape.matches_array_op("prices", &[999, 999]));
        assert!(!shape.matches_array_op("other", &[0, 0]));
        // could_match returns false for Array shapes.
        assert!(!shape.could_match("prices", "x"));
        assert_eq!(shape.collection(), None);
    }

    #[test]
    fn array_shape_matches_array_op_with_range() {
        let range = ArrayCoordRange {
            start: vec![10, 10],
            end: Some(vec![20, 20]),
        };
        let shape = ShapeDefinition {
            shape_id: "a2".into(),
            tenant_id: 1,
            shape_type: ShapeType::Array {
                array_name: "temps".into(),
                coord_range: Some(range),
            },
            description: "temps sub-range".into(),
            field_filter: vec![],
        };
        assert!(shape.matches_array_op("temps", &[10, 10]));
        assert!(shape.matches_array_op("temps", &[15, 15]));
        assert!(shape.matches_array_op("temps", &[20, 20]));
        assert!(!shape.matches_array_op("temps", &[9, 9]));
        assert!(!shape.matches_array_op("temps", &[21, 21]));
        assert!(!shape.matches_array_op("temps", &[10])); // wrong ndim
    }

    #[test]
    fn array_coord_range_msgpack_roundtrip() {
        let range = ArrayCoordRange {
            start: vec![1, 2, 3],
            end: Some(vec![4, 5, 6]),
        };
        let bytes = zerompk::to_msgpack_vec(&range).unwrap();
        let decoded: ArrayCoordRange = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.start, vec![1, 2, 3]);
        assert_eq!(decoded.end, Some(vec![4, 5, 6]));
    }

    #[test]
    fn array_shape_msgpack_roundtrip() {
        let shape = ShapeDefinition {
            shape_id: "a3".into(),
            tenant_id: 7,
            shape_type: ShapeType::Array {
                array_name: "sensor_matrix".into(),
                coord_range: Some(ArrayCoordRange {
                    start: vec![0, 0],
                    end: None,
                }),
            },
            description: "half-open range".into(),
            field_filter: vec![],
        };
        let bytes = zerompk::to_msgpack_vec(&shape).unwrap();
        let decoded: ShapeDefinition = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.shape_id, "a3");
        assert!(matches!(
            decoded.shape_type,
            ShapeType::Array { ref array_name, .. } if array_name == "sensor_matrix"
        ));
    }
}
