//! MATCH query parser — parses Cypher-style pattern syntax into AST.
//!
//! Supported syntax:
//! ```text
//! MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
//! WHERE a.name = 'Alice'
//! RETURN a.name, b.name, c.name
//! LIMIT 10
//!
//! OPTIONAL MATCH (a)-[:LIKES]->(b)
//!
//! MATCH (a)-[:KNOWS*1..3]->(b)   -- variable-length paths
//!
//! MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:KNOWS]->(c:Person)  -- self-join
//! ```

mod bindings;
mod clauses;
mod helpers;

use super::ast::*;
use clauses::{parse_order_by, parse_return, parse_where};
use helpers::{find_next_match_keyword, find_top_level_keyword, split_top_level_commas};

/// Parse a MATCH query string into a `MatchQuery` AST.
///
/// The input should start with `MATCH` or `OPTIONAL MATCH`.
pub fn parse(sql: &str) -> Result<MatchQuery, String> {
    let mut where_predicates = Vec::new();
    let mut return_columns = Vec::new();
    let mut distinct = false;
    let mut limit = None;
    let mut order_by = Vec::new();

    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let where_pos = find_top_level_keyword(&upper, "WHERE");
    let return_pos = find_top_level_keyword(&upper, "RETURN");
    let limit_pos = find_top_level_keyword(&upper, "LIMIT");
    let order_pos = find_top_level_keyword(&upper, "ORDER BY");

    let pattern_end = [where_pos, return_pos, limit_pos, order_pos]
        .iter()
        .filter_map(|&p| p)
        .min()
        .unwrap_or(trimmed.len());

    let pattern_section = &trimmed[..pattern_end];
    let clauses = parse_match_clauses(pattern_section)?;

    if let Some(wp) = where_pos {
        let where_end = [return_pos, limit_pos, order_pos]
            .iter()
            .filter_map(|&p| p)
            .min()
            .unwrap_or(trimmed.len());
        let where_section = &trimmed[wp + 5..where_end].trim();
        where_predicates = parse_where(where_section)?;
    }

    if let Some(rp) = return_pos {
        let return_end = [limit_pos, order_pos]
            .iter()
            .filter_map(|&p| p)
            .min()
            .unwrap_or(trimmed.len());
        let return_section = &trimmed[rp + 6..return_end].trim();
        let (cols, dist) = parse_return(return_section);
        return_columns = cols;
        distinct = dist;
    }

    if let Some(lp) = limit_pos {
        let after = trimmed[lp + 5..].trim();
        let word = after.split_whitespace().next().unwrap_or("");
        limit = word.parse().ok();
    }

    if let Some(op) = order_pos {
        let order_end = limit_pos.unwrap_or(trimmed.len());
        let order_section = &trimmed[op + 8..order_end].trim();
        order_by = parse_order_by(order_section);
    }

    Ok(MatchQuery {
        clauses,
        where_predicates,
        return_columns,
        distinct,
        limit,
        order_by,
    })
}

/// Parse MATCH and OPTIONAL MATCH clauses from the pattern section.
pub(super) fn parse_match_clauses(section: &str) -> Result<Vec<MatchClause>, String> {
    let mut clauses = Vec::new();
    let upper = section.to_uppercase();

    let mut pos = 0;
    while pos < section.len() {
        let remaining_upper = &upper[pos..];
        let remaining = &section[pos..];

        let (optional, match_start) = if remaining_upper.trim_start().starts_with("OPTIONAL MATCH")
        {
            let ws = remaining.len() - remaining.trim_start().len();
            (true, pos + ws + 14)
        } else if remaining_upper.trim_start().starts_with("MATCH") {
            let ws = remaining.len() - remaining.trim_start().len();
            (false, pos + ws + 5)
        } else {
            break;
        };

        let rest_upper = &upper[match_start..];
        let next_match = find_next_match_keyword(rest_upper)
            .map(|offset| match_start + offset)
            .unwrap_or(section.len());

        let pattern_text = section[match_start..next_match].trim();
        let patterns = parse_pattern_chains(pattern_text)?;

        clauses.push(MatchClause { patterns, optional });
        pos = next_match;
    }

    if clauses.is_empty() {
        return Err("no MATCH clause found".into());
    }

    Ok(clauses)
}

/// Parse comma-separated pattern chains.
fn parse_pattern_chains(text: &str) -> Result<Vec<PatternChain>, String> {
    let chain_texts = split_top_level_commas(text);
    let mut chains = Vec::new();
    for chain_text in chain_texts {
        chains.push(parse_single_chain(chain_text.trim())?);
    }
    Ok(chains)
}

/// Parse a single pattern chain: `(a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c)`.
fn parse_single_chain(text: &str) -> Result<PatternChain, String> {
    let mut triples = Vec::new();
    let mut pos = 0;

    let (first_node, consumed) = bindings::parse_node_binding(&text[pos..])?;
    pos += consumed;
    let mut prev_node = first_node;

    while pos < text.len() {
        let remaining = text[pos..].trim_start();
        if remaining.is_empty() {
            break;
        }
        pos = text.len() - remaining.len();

        let (edge, edge_consumed) = bindings::parse_edge_binding(&text[pos..])?;
        pos += edge_consumed;

        let remaining = text[pos..].trim_start();
        pos = text.len() - remaining.len();
        let (next_node, node_consumed) = bindings::parse_node_binding(&text[pos..])?;
        pos += node_consumed;

        triples.push(PatternTriple {
            src: prev_node,
            edge,
            dst: next_node.clone(),
        });
        prev_node = next_node;
    }

    if triples.is_empty() {
        return Err(format!("empty pattern chain: '{text}'"));
    }

    Ok(PatternChain { triples })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_match() {
        let q = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b").unwrap();
        assert_eq!(q.clauses.len(), 1);
        assert!(!q.clauses[0].optional);
        assert_eq!(q.clauses[0].patterns.len(), 1);

        let triple = &q.clauses[0].patterns[0].triples[0];
        assert_eq!(triple.src.name.as_deref(), Some("a"));
        assert_eq!(triple.src.label.as_deref(), Some("Person"));
        assert_eq!(triple.edge.edge_type.as_deref(), Some("KNOWS"));
        assert_eq!(triple.edge.direction, EdgeDirection::Right);
        assert_eq!(triple.dst.name.as_deref(), Some("b"));
        assert_eq!(triple.dst.label.as_deref(), Some("Person"));

        assert_eq!(q.return_columns.len(), 2);
        assert_eq!(q.return_columns[0].expr, "a");
    }

    #[test]
    fn parse_multi_hop() {
        let q = parse("MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN a, c").unwrap();
        assert_eq!(q.clauses[0].patterns[0].triples.len(), 2);
    }

    #[test]
    fn parse_variable_length_path() {
        let q = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN a, b").unwrap();
        let edge = &q.clauses[0].patterns[0].triples[0].edge;
        assert_eq!(edge.min_hops, 1);
        assert_eq!(edge.max_hops, 3);
        assert!(edge.is_variable_length());
    }

    #[test]
    fn parse_optional_match() {
        let q =
            parse("MATCH (a:Person)-[:KNOWS]->(b) OPTIONAL MATCH (a)-[:LIKES]->(c) RETURN a, b, c")
                .unwrap();
        assert_eq!(q.clauses.len(), 2);
        assert!(!q.clauses[0].optional);
        assert!(q.clauses[1].optional);
    }

    #[test]
    fn parse_where_equals() {
        let q = parse("MATCH (a:Person)-[:KNOWS]->(b) WHERE a.name = 'Alice' RETURN b").unwrap();
        assert_eq!(q.where_predicates.len(), 1);
        match &q.where_predicates[0] {
            WherePredicate::Equals {
                binding,
                field,
                value,
            } => {
                assert_eq!(binding, "a");
                assert_eq!(field, "name");
                assert_eq!(value, "Alice");
            }
            _ => panic!("expected Equals predicate"),
        }
    }

    #[test]
    fn parse_where_comparison() {
        let q = parse("MATCH (a)-[:HAS]->(b) WHERE a.age > 25 RETURN a").unwrap();
        match &q.where_predicates[0] {
            WherePredicate::Comparison { op, value, .. } => {
                assert_eq!(*op, ComparisonOp::Gt);
                assert_eq!(value, "25");
            }
            _ => panic!("expected Comparison predicate"),
        }
    }

    #[test]
    fn parse_where_not_exists() {
        let q = parse(
            "MATCH (a)-[:KNOWS]->(b) WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) } RETURN a, b",
        )
        .unwrap();
        assert_eq!(q.where_predicates.len(), 1);
        match &q.where_predicates[0] {
            WherePredicate::NotExists { sub_pattern } => {
                assert_eq!(
                    sub_pattern.patterns[0].triples[0].edge.edge_type.as_deref(),
                    Some("BLOCKED")
                );
            }
            _ => panic!("expected NotExists"),
        }
    }

    #[test]
    fn parse_self_join() {
        let q = parse("MATCH (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c) RETURN a, b, c").unwrap();
        assert_eq!(q.clauses[0].patterns.len(), 2);
    }

    #[test]
    fn parse_left_arrow() {
        let q = parse("MATCH (a)<-[:FOLLOWS]-(b) RETURN a").unwrap();
        assert_eq!(
            q.clauses[0].patterns[0].triples[0].edge.direction,
            EdgeDirection::Left
        );
    }

    #[test]
    fn parse_undirected() {
        let q = parse("MATCH (a)-[:RELATED]-(b) RETURN a").unwrap();
        assert_eq!(
            q.clauses[0].patterns[0].triples[0].edge.direction,
            EdgeDirection::Both
        );
    }

    #[test]
    fn parse_limit() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) RETURN a LIMIT 10").unwrap();
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn parse_anonymous_nodes_and_edges() {
        let q = parse("MATCH (:Person)-[]->(b) RETURN b").unwrap();
        let triple = &q.clauses[0].patterns[0].triples[0];
        assert!(triple.src.name.is_none());
        assert_eq!(triple.src.label.as_deref(), Some("Person"));
        assert!(triple.edge.name.is_none());
        assert!(triple.edge.edge_type.is_none());
    }

    #[test]
    fn parse_named_edge() {
        let q = parse("MATCH (a)-[r:KNOWS]->(b) RETURN r").unwrap();
        assert_eq!(
            q.clauses[0].patterns[0].triples[0].edge.name.as_deref(),
            Some("r")
        );
    }

    #[test]
    fn parse_return_with_alias() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) RETURN a.name AS person_name").unwrap();
        assert_eq!(q.return_columns[0].expr, "a.name");
        assert_eq!(q.return_columns[0].alias.as_deref(), Some("person_name"));
    }

    #[test]
    fn parse_order_by() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) RETURN a ORDER BY a.name DESC LIMIT 5").unwrap();
        assert_eq!(q.order_by.len(), 1);
        assert!(!q.order_by[0].ascending);
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn parse_hop_range_variants() {
        assert_eq!(bindings::parse_hop_range("1..3").unwrap(), (1, 3));
        assert_eq!(bindings::parse_hop_range("2").unwrap(), (2, 2));
        assert_eq!(bindings::parse_hop_range("..5").unwrap(), (1, 5));
        assert_eq!(bindings::parse_hop_range("3..").unwrap(), (3, 10));
        assert!(bindings::parse_hop_range("5..2").is_err());
    }

    #[test]
    fn parse_multiple_where_predicates() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) WHERE a.name = 'Alice' AND b.age > 30 RETURN b")
            .unwrap();
        assert_eq!(q.where_predicates.len(), 2);
    }
}
