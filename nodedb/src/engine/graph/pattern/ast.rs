//! MATCH pattern AST — Cypher-compatible graph pattern representations.
//!
//! Represents parsed MATCH queries as structured data for compilation
//! into physical execution plans. Supports:
//! - Node bindings with optional labels: `(a:Person)`
//! - Edge bindings with types and direction: `-[:KNOWS]->`
//! - Variable-length paths: `[:KNOWS*1..3]`
//! - OPTIONAL MATCH (LEFT JOIN semantics)
//! - Anti-join via WHERE NOT EXISTS { MATCH ... }
//! - Multiple comma-separated patterns (self-join)

use serde::{Deserialize, Serialize};

/// A complete MATCH query with clauses, predicates, and return projection.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct MatchQuery {
    /// MATCH and OPTIONAL MATCH clauses.
    pub clauses: Vec<MatchClause>,
    /// WHERE predicates (field = value, comparison operators).
    pub where_predicates: Vec<WherePredicate>,
    /// RETURN column list. Empty = return all bound variables.
    pub return_columns: Vec<ReturnColumn>,
    /// Whether RETURN DISTINCT was specified.
    pub distinct: bool,
    /// LIMIT on returned rows.
    pub limit: Option<usize>,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderByColumn>,
}

/// A single MATCH or OPTIONAL MATCH clause containing one or more patterns.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct MatchClause {
    /// Pattern triples: `(a)-[r]->(b)`.
    /// Multiple patterns in one clause (comma-separated) represent self-joins.
    pub patterns: Vec<PatternChain>,
    /// Whether this is an OPTIONAL MATCH (LEFT JOIN semantics).
    pub optional: bool,
}

/// A chain of connected pattern triples: `(a)-[r1]->(b)-[r2]->(c)`.
///
/// Represented as a sequence of triples sharing intermediate node bindings.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PatternChain {
    pub triples: Vec<PatternTriple>,
}

/// A single pattern element: `(src)-[edge]->(dst)`.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct PatternTriple {
    pub src: NodeBinding,
    pub edge: EdgeBinding,
    pub dst: NodeBinding,
}

/// A node variable binding in a pattern.
///
/// `(a:Person)` → `NodeBinding { name: Some("a"), label: Some("Person") }`
/// `(:Person)` → `NodeBinding { name: None, label: Some("Person") }`
/// `(a)` → `NodeBinding { name: Some("a"), label: None }`
/// `()` → `NodeBinding { name: None, label: None }`
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct NodeBinding {
    /// Variable name (e.g., `a`). None for anonymous nodes.
    pub name: Option<String>,
    /// Required node label (collection/type filter). None = any type.
    pub label: Option<String>,
}

/// An edge variable binding in a pattern.
///
/// `-[:KNOWS]->` → `EdgeBinding { name: None, edge_type: Some("KNOWS"), direction: Right, .. }`
/// `-[r:KNOWS*1..3]->` → variable-length path
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct EdgeBinding {
    /// Variable name (e.g., `r`). None for anonymous edges.
    pub name: Option<String>,
    /// Required edge type/label. None = any type.
    pub edge_type: Option<String>,
    /// Traversal direction.
    pub direction: EdgeDirection,
    /// Minimum hops (1 for fixed-length, >0 for variable-length).
    pub min_hops: usize,
    /// Maximum hops (same as min_hops for fixed-length).
    pub max_hops: usize,
}

impl EdgeBinding {
    /// Whether this is a variable-length path (min != max or > 1 hop).
    pub fn is_variable_length(&self) -> bool {
        self.min_hops != self.max_hops || self.min_hops > 1
    }
}

/// Edge traversal direction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum EdgeDirection {
    /// `->` (outbound)
    Right = 0,
    /// `<-` (inbound)
    Left = 1,
    /// `-` (undirected / both directions)
    Both = 2,
}

impl EdgeDirection {
    /// Convert to CSR Direction.
    pub fn to_csr_direction(self) -> crate::engine::graph::edge_store::Direction {
        match self {
            Self::Right => crate::engine::graph::edge_store::Direction::Out,
            Self::Left => crate::engine::graph::edge_store::Direction::In,
            Self::Both => crate::engine::graph::edge_store::Direction::Both,
        }
    }
}

/// A WHERE predicate.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum WherePredicate {
    /// `a.name = 'Alice'`
    Equals {
        binding: String,
        field: String,
        value: String,
    },
    /// `a.age > 25`
    Comparison {
        binding: String,
        field: String,
        op: ComparisonOp,
        value: String,
    },
    /// `WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) }`
    NotExists { sub_pattern: MatchClause },
}

/// Comparison operator for WHERE predicates.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum ComparisonOp {
    Eq = 0,
    Neq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
}

/// A RETURN column specification.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ReturnColumn {
    /// Expression: `a.name`, `b`, `count(*)`.
    pub expr: String,
    /// Optional alias: `AS alias_name`.
    pub alias: Option<String>,
}

/// ORDER BY column.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct OrderByColumn {
    pub expr: String,
    pub ascending: bool,
}

impl MatchQuery {
    /// Get all unique node variable names bound in the query.
    pub fn bound_node_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for clause in &self.clauses {
            for chain in &clause.patterns {
                for triple in &chain.triples {
                    if let Some(ref n) = triple.src.name
                        && !names.contains(n)
                    {
                        names.push(n.clone());
                    }
                    if let Some(ref n) = triple.dst.name
                        && !names.contains(n)
                    {
                        names.push(n.clone());
                    }
                }
            }
        }
        names
    }

    /// Get all unique edge variable names bound in the query.
    pub fn bound_edge_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for clause in &self.clauses {
            for chain in &clause.patterns {
                for triple in &chain.triples {
                    if let Some(ref n) = triple.edge.name
                        && !names.contains(n)
                    {
                        names.push(n.clone());
                    }
                }
            }
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs;

    #[test]
    fn edge_binding_fixed_length() {
        let e = EdgeBinding {
            name: None,
            edge_type: Some("KNOWS".into()),
            direction: EdgeDirection::Right,
            min_hops: 1,
            max_hops: 1,
        };
        assert!(!e.is_variable_length());
    }

    #[test]
    fn edge_binding_variable_length() {
        let e = EdgeBinding {
            name: None,
            edge_type: Some("KNOWS".into()),
            direction: EdgeDirection::Right,
            min_hops: 1,
            max_hops: 3,
        };
        assert!(e.is_variable_length());
    }

    #[test]
    fn bound_names_extraction() {
        let query = MatchQuery {
            clauses: vec![MatchClause {
                patterns: vec![PatternChain {
                    triples: vec![PatternTriple {
                        src: NodeBinding {
                            name: Some("a".into()),
                            label: Some("Person".into()),
                        },
                        edge: EdgeBinding {
                            name: Some("r".into()),
                            edge_type: Some("KNOWS".into()),
                            direction: EdgeDirection::Right,
                            min_hops: 1,
                            max_hops: 1,
                        },
                        dst: NodeBinding {
                            name: Some("b".into()),
                            label: Some("Person".into()),
                        },
                    }],
                }],
                optional: false,
            }],
            where_predicates: vec![],
            return_columns: vec![],
            distinct: false,
            limit: None,
            order_by: vec![],
        };

        assert_eq!(query.bound_node_names(), vec!["a", "b"]);
        assert_eq!(query.bound_edge_names(), vec!["r"]);
    }

    #[test]
    fn edge_direction_to_csr() {
        assert_eq!(
            EdgeDirection::Right.to_csr_direction(),
            crate::engine::graph::edge_store::Direction::Out
        );
        assert_eq!(
            EdgeDirection::Left.to_csr_direction(),
            crate::engine::graph::edge_store::Direction::In
        );
    }

    #[test]
    fn serde_roundtrip() {
        let query = MatchQuery {
            clauses: vec![MatchClause {
                patterns: vec![PatternChain {
                    triples: vec![PatternTriple {
                        src: NodeBinding {
                            name: Some("a".into()),
                            label: None,
                        },
                        edge: EdgeBinding {
                            name: None,
                            edge_type: Some("LIKES".into()),
                            direction: EdgeDirection::Both,
                            min_hops: 1,
                            max_hops: 3,
                        },
                        dst: NodeBinding {
                            name: Some("b".into()),
                            label: None,
                        },
                    }],
                }],
                optional: true,
            }],
            where_predicates: vec![],
            return_columns: vec![ReturnColumn {
                expr: "a".into(),
                alias: None,
            }],
            distinct: false,
            limit: Some(10),
            order_by: vec![],
        };

        let json = sonic_rs::to_string(&query).unwrap();
        let parsed: MatchQuery = sonic_rs::from_str(&json).unwrap();
        assert_eq!(parsed.clauses.len(), 1);
        assert!(parsed.clauses[0].optional);
        assert_eq!(parsed.limit, Some(10));
    }
}
