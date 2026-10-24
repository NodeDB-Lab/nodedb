// SPDX-License-Identifier: BUSL-1.1

//! The edge list of one batch statement, after the statement guards.
//!
//! [`BoundedEdges`] has one constructor, [`BoundedEdges::new`]. A value exists
//! only for a list that has a collection, holds between one and
//! `MAX_EDGES_PER_BATCH_STATEMENT` edges, carries a valid endpoint pair and
//! label in every triple, and contains no repeat. The resolve step takes this
//! type instead of a bare `Vec`, so it can trust the list it sizes its buffers
//! from, and a new caller cannot reach it without passing the guards.

use std::collections::HashSet;
use std::ops::Deref;

use nodedb_sql::ddl_ast::GraphEdgeTuple;
use nodedb_types::protocol::MAX_EDGES_PER_BATCH_STATEMENT;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// A checked, deduplicated list of batch edges. See the module doc.
#[derive(Debug)]
pub(super) struct BoundedEdges(Vec<GraphEdgeTuple>);

impl BoundedEdges {
    /// Check `edges` for the statement `stmt` and drop repeats of one
    /// `(src, dst, label)`.
    ///
    /// Both batch handlers build this before they dispatch or stage any edge,
    /// so a rejection on a later triple cannot leave the edges before it
    /// written. The parser caps the list as well, but a hand-built AST skips
    /// the parser, so the count is checked here before any work is done.
    ///
    /// Dedup is about cost, not the count: it saves the redundant dispatch. On
    /// the delete side a repeat contributes nothing anyway, because the second
    /// copy reads the first copy's tombstone as a pre-image and counts zero.
    pub(super) fn new(
        collection: &str,
        edges: Vec<GraphEdgeTuple>,
        stmt: &str,
    ) -> Result<Self, DdlError> {
        if collection.is_empty() {
            return Err(ddl_err("42601", format!("{stmt} requires IN <collection>")));
        }
        if edges.is_empty() {
            return Err(ddl_err(
                "42601",
                format!("{stmt} requires at least one edge"),
            ));
        }
        if edges.len() > MAX_EDGES_PER_BATCH_STATEMENT {
            return Err(ddl_err(
                "54000",
                format!(
                    "{stmt} carries at most {MAX_EDGES_PER_BATCH_STATEMENT} edges per statement, got {}",
                    edges.len()
                ),
            ));
        }
        for tuple in &edges {
            if tuple.src.is_empty() || tuple.dst.is_empty() {
                return Err(ddl_err("42601", format!("{stmt} requires FROM and TO")));
            }
            super::edge_parse::validate_edge_label(&tuple.label)?;
        }
        let mut edges = edges;
        let mut seen: HashSet<(String, String, String)> = HashSet::new();
        edges.retain(|t| seen.insert((t.src.clone(), t.dst.clone(), t.label.clone())));
        Ok(Self(edges))
    }
}

impl Deref for BoundedEdges {
    type Target = [GraphEdgeTuple];

    fn deref(&self) -> &[GraphEdgeTuple] {
        &self.0
    }
}

impl IntoIterator for BoundedEdges {
    type Item = GraphEdgeTuple;
    type IntoIter = std::vec::IntoIter<GraphEdgeTuple>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(src: &str, dst: &str, label: &str) -> GraphEdgeTuple {
        GraphEdgeTuple {
            src: src.to_string(),
            dst: dst.to_string(),
            label: label.to_string(),
        }
    }

    fn tuples(n: usize) -> Vec<GraphEdgeTuple> {
        (0..n)
            .map(|i| tuple(&format!("s{i}"), &format!("d{i}"), "L"))
            .collect()
    }

    #[test]
    fn a_batch_at_the_cap_is_built() {
        let edges = BoundedEdges::new(
            "c",
            tuples(MAX_EDGES_PER_BATCH_STATEMENT),
            "GRAPH INSERT EDGES",
        )
        .expect("a list at the cap is valid");
        assert_eq!(edges.len(), MAX_EDGES_PER_BATCH_STATEMENT);
    }

    #[test]
    fn a_batch_over_the_cap_is_refused_before_any_work() {
        let error = BoundedEdges::new(
            "c",
            tuples(MAX_EDGES_PER_BATCH_STATEMENT + 1),
            "GRAPH DELETE EDGES",
        )
        .expect_err("1001 edges must be refused");
        let text = format!("{error:?}");
        assert!(text.contains("54000"), "SQLSTATE 54000 expected: {text}");
        assert!(text.contains("1000"), "the limit must be named: {text}");
    }

    #[test]
    fn empty_and_collectionless_shapes_are_refused() {
        assert!(BoundedEdges::new("", tuples(1), "GRAPH INSERT EDGES").is_err());
        assert!(BoundedEdges::new("c", Vec::new(), "GRAPH INSERT EDGES").is_err());
    }

    /// A repeat of one triple is the same edge, so the insert count must drop
    /// it. The un-deduped form reports 2 for this input.
    #[test]
    fn a_repeated_triple_collapses_and_a_distinct_label_does_not() {
        let edges = BoundedEdges::new(
            "c",
            vec![
                tuple("a", "b", "L"),
                tuple("a", "b", "L"),
                tuple("a", "b", "M"),
            ],
            "GRAPH INSERT EDGES",
        )
        .expect("valid triples");
        assert_eq!(edges.len(), 2);
    }

    /// An empty endpoint is rejected for the whole list, before any caller can
    /// stage the tuples ahead of it.
    #[test]
    fn an_empty_endpoint_rejects_the_whole_list() {
        let error = BoundedEdges::new(
            "c",
            vec![tuple("a", "b", "L"), tuple("", "z", "L")],
            "GRAPH INSERT EDGES",
        )
        .expect_err("an empty endpoint must be rejected");
        assert!(
            error.message.contains("FROM and TO"),
            "the error must name the missing endpoint: {}",
            error.message
        );
    }
}
