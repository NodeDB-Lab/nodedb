// SPDX-License-Identifier: BUSL-1.1

//! MATCH query parsing entry points.

use super::super::ast::*;
use super::bindings;
use super::clauses::{parse_order_by, parse_return, parse_where};
use super::helpers::{bad, find_keyword_all, is_word_byte, split_top_level_commas, top_level_mask};

/// The clauses after the patterns, in the order they must appear.
const CLAUSES: [&str; 6] = ["IN", "WHERE", "RETURN", "ORDER BY", "LIMIT", "OFFSET"];

/// Parse a MATCH query string into a `MatchQuery` AST.
///
/// Grammar: `[OPTIONAL] MATCH <patterns> ... [IN <collection>]
/// [WHERE <predicates>] [RETURN <columns>] [ORDER BY <columns>]
/// [LIMIT <n>] [OFFSET <n>]`, with an optional trailing `;`. Each clause appears at most
/// once and in this order. Clause keywords match only at the top level,
/// never inside a literal, a quoted name, or brackets. Any text a clause
/// cannot read is a `BadRequest` error.
pub fn parse(sql: &str) -> crate::Result<MatchQuery> {
    let text = sql.trim();
    let text = text.strip_suffix(';').unwrap_or(text).trim_end();
    let mask = top_level_mask(text);

    // `(order index, keyword start, keyword end)` of every clause present.
    let mut found: Vec<(usize, usize, usize)> = Vec::new();
    for (index, keyword) in CLAUSES.iter().enumerate().skip(1) {
        let hits = find_keyword_all(text, &mask, keyword);
        if hits.len() > 1 {
            return Err(bad(format!("MATCH: {keyword} given more than once")));
        }
        found.extend(hits.into_iter().map(|(s, e)| (index, s, e)));
    }
    // `IN` names the collection only between the patterns and the first
    // other clause. Elsewhere it is part of that clause's text.
    let first_other = found.iter().map(|f| f.1).min().unwrap_or(text.len());
    let in_hits: Vec<_> = find_keyword_all(text, &mask, "IN")
        .into_iter()
        .filter(|(s, _)| *s < first_other)
        .collect();
    if in_hits.len() > 1 {
        return Err(bad("MATCH: IN given more than once"));
    }
    found.extend(in_hits.into_iter().map(|(s, e)| (0, s, e)));
    found.sort_by_key(|f| f.1);
    for pair in found.windows(2) {
        if pair[0].0 > pair[1].0 {
            return Err(bad(format!(
                "MATCH: {} must come before {}",
                CLAUSES[pair[1].0], CLAUSES[pair[0].0]
            )));
        }
    }

    let pattern_end = found.first().map_or(text.len(), |f| f.1);
    let mut query = MatchQuery {
        clauses: parse_match_clauses(&text[..pattern_end])?,
        where_predicates: Vec::new(),
        return_columns: Vec::new(),
        distinct: false,
        limit: None,
        offset: None,
        order_by: Vec::new(),
        collection: None,
    };

    for (n, &(index, _, keyword_end)) in found.iter().enumerate() {
        let section_end = found.get(n + 1).map_or(text.len(), |next| next.1);
        let section = text[keyword_end..section_end].trim();
        match index {
            0 => query.collection = Some(parse_in_target(section)?),
            1 => query.where_predicates = parse_where(section)?,
            2 => {
                let (columns, distinct) = parse_return(section)?;
                query.return_columns = columns;
                query.distinct = distinct;
            }
            3 => query.order_by = parse_order_by(section)?,
            4 => query.limit = Some(parse_count("LIMIT", section)?),
            _ => query.offset = Some(parse_count("OFFSET", section)?),
        }
    }
    check_edge_property_access(&query)?;
    Ok(query)
}

/// The non-negative row count after LIMIT or OFFSET.
fn parse_count(keyword: &str, section: &str) -> crate::Result<usize> {
    section.parse::<usize>().map_err(|_| {
        bad(format!(
            "MATCH: {keyword} needs a non-negative integer, found '{section}'"
        ))
    })
}

/// A relationship property `r.field` reads one edge's property object. It
/// needs a fixed-length relationship, because a variable-length one binds a
/// path of edges.
fn check_edge_property_access(query: &MatchQuery) -> crate::Result<()> {
    let vars = query.edge_property_vars();
    for edge in query.edge_bindings() {
        if let Some(name) = edge.name.as_deref()
            && vars.contains(name)
            && edge.is_variable_length()
        {
            return Err(bad(format!(
                "MATCH: '{name}' is a variable-length relationship, which binds a path; \
                 a property such as '{name}.<field>' needs a fixed-length relationship"
            )));
        }
    }
    Ok(())
}

/// Read the collection after `IN`: one quoted string or one bare word.
fn parse_in_target(section: &str) -> crate::Result<String> {
    let name = match section.chars().next() {
        Some(quote @ ('\'' | '"')) => {
            let rest = section.strip_prefix(quote).unwrap_or(section);
            let (inner, after) = rest.split_once(quote).ok_or_else(|| {
                bad(format!(
                    "MATCH: unclosed quote in IN collection '{section}'"
                ))
            })?;
            if !after.trim().is_empty() {
                return Err(bad(format!(
                    "MATCH: unexpected text after the IN collection: '{section}'"
                )));
            }
            inner
        }
        _ if section.contains(char::is_whitespace) => {
            return Err(bad(format!(
                "MATCH: IN takes one collection name, found '{section}'"
            )));
        }
        _ => section,
    };
    if name.is_empty() {
        return Err(bad("MATCH: IN needs a collection name"));
    }
    Ok(name.to_string())
}

/// Parse MATCH and OPTIONAL MATCH clauses from the pattern section. The
/// section must start with a clause, and every clause needs a pattern.
pub(in crate::engine::graph::pattern) fn parse_match_clauses(
    section: &str,
) -> crate::Result<Vec<MatchClause>> {
    let section = section.trim();
    let mask = top_level_mask(section);

    // `(clause start, pattern start, optional)` per MATCH keyword.
    let mut starts: Vec<(usize, usize, bool)> = Vec::new();
    for (s, e) in find_keyword_all(section, &mask, "MATCH") {
        let before = section[..s].trim_end().as_bytes();
        let optional = before.len() >= 8
            && before[before.len() - 8..].eq_ignore_ascii_case(b"OPTIONAL")
            && (before.len() == 8 || !is_word_byte(before[before.len() - 9]))
            && mask[before.len() - 8];
        let clause_start = if optional { before.len() - 8 } else { s };
        starts.push((clause_start, e, optional));
    }

    match starts.first() {
        None => return Err(bad("no MATCH clause found")),
        Some(&(0, _, _)) => {}
        Some(&(first, _, _)) => {
            return Err(bad(format!(
                "MATCH: unexpected text before MATCH: '{}'",
                section[..first].trim()
            )));
        }
    }

    let mut clauses = Vec::with_capacity(starts.len());
    for (n, &(_, pattern_start, optional)) in starts.iter().enumerate() {
        let pattern_end = starts.get(n + 1).map_or(section.len(), |next| next.0);
        let patterns = parse_pattern_chains(section[pattern_start..pattern_end].trim())?;
        clauses.push(MatchClause { patterns, optional });
    }
    Ok(clauses)
}

/// Parse comma-separated pattern chains.
fn parse_pattern_chains(text: &str) -> crate::Result<Vec<PatternChain>> {
    let chain_texts = split_top_level_commas(text);
    let mut chains = Vec::new();
    for chain_text in chain_texts {
        chains.push(parse_single_chain(chain_text.trim())?);
    }
    Ok(chains)
}

/// Parse a single pattern chain: `(a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c)`.
fn parse_single_chain(text: &str) -> crate::Result<PatternChain> {
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
        return Err(crate::Error::BadRequest {
            detail: format!("empty pattern chain: '{text}'"),
        });
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
    fn optional_match_after_unicode_binding_preserves_original_offsets() {
        let q =
            parse("MATCH (ﬀﬀ:Person)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:LIKES]->(c) RETURN b, c")
                .expect("graph pattern should parse");
        assert_eq!(q.clauses.len(), 2);
        assert!(q.clauses[1].optional);
    }

    #[test]
    fn in_and_predicates_after_unicode_binding_preserve_original_offsets() {
        let q = parse(
            "MATCH (ﬀﬀ:Person)-[:KNOWS]->(b) IN 'people' \
             WHERE ﬀﬀ.name = 'a' AND ﬀﬀ.age > 1 RETURN ﬀﬀ",
        )
        .expect("graph IN and WHERE predicates should parse");
        assert_eq!(q.collection.as_deref(), Some("people"));
        assert_eq!(q.where_predicates.len(), 2);
        assert_eq!(q.return_columns.len(), 1);
        assert_eq!(q.return_columns[0].expr, "ﬀﬀ");
    }

    #[test]
    fn return_alias_after_unicode_expression_preserves_original_offsets() {
        let q = parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN ﬀﬀ AS alias")
            .expect("graph RETURN alias should parse");
        assert_eq!(q.return_columns.len(), 1);
        assert_eq!(q.return_columns[0].expr, "ﬀﬀ");
        assert_eq!(q.return_columns[0].alias.as_deref(), Some("alias"));
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

    fn err(sql: &str) -> String {
        match parse(sql) {
            Err(e) => e.to_string(),
            Ok(q) => panic!("expected an error for {sql}, got {q:?}"),
        }
    }

    #[test]
    fn clauses_out_of_order_are_errors_not_panics() {
        // The old slicer panicked on both: the RETURN and ORDER BY sections
        // ended before they began.
        assert!(err("MATCH (a)-[:K]->(b) RETURN a WHERE a.x = 1").contains("WHERE"));
        assert!(err("MATCH (a)-[:K]->(b) ORDER BY a.x RETURN a").contains("RETURN"));
        assert!(err("MATCH (a)-[:K]->(b) RETURN a LIMIT 3 ORDER BY a").contains("ORDER BY"));
    }

    #[test]
    fn duplicate_clause_is_an_error() {
        assert!(err("MATCH (a)-[:K]->(b) WHERE a.x = 1 WHERE b.y = 2 RETURN a").contains("WHERE"));
    }

    #[test]
    fn bad_limit_is_an_error() {
        // The old parser dropped the limit and returned every row.
        err("MATCH (a)-[:K]->(b) RETURN a LIMIT ten");
        err("MATCH (a)-[:K]->(b) RETURN a LIMIT 5 6");
        err("MATCH (a)-[:K]->(b) RETURN a LIMIT -1");
        assert_eq!(
            parse("MATCH (a)-[:K]->(b) RETURN a LIMIT 7;")
                .expect("parses")
                .limit,
            Some(7)
        );
    }

    #[test]
    fn unclosed_in_quote_is_an_error() {
        err("MATCH (a)-[:K]->(b) IN 'people RETURN a");
        err("MATCH (a)-[:K]->(b) IN people extra RETURN a");
    }

    #[test]
    fn unknown_or_or_joined_predicates_are_errors() {
        // The old parser dropped both and matched every row.
        err("MATCH (a)-[:K]->(b) WHERE a.name LIKE 'x%' RETURN a");
        err("MATCH (a)-[:K]->(b) WHERE a.x = 1 OR a.x = 2 RETURN a");
    }

    #[test]
    fn not_exists_with_two_clauses_is_an_error() {
        err(
            "MATCH (a)-[:K]->(b) WHERE NOT EXISTS { MATCH (a)-[:X]->(b) MATCH (b)-[:Y]->(a) } \
             RETURN a",
        );
    }

    #[test]
    fn keywords_and_operators_inside_literals_are_values() {
        let q =
            parse("MATCH (a)-[:K]->(b) WHERE a.note = 'x<y RETURN z' RETURN a").expect("parses");
        match &q.where_predicates[0] {
            WherePredicate::Equals { field, value, .. } => {
                assert_eq!(field, "note");
                assert_eq!(value, "x<y RETURN z");
            }
            other => panic!("expected Equals, got {other:?}"),
        }
        assert_eq!(q.return_columns.len(), 1);
    }

    #[test]
    fn order_by_direction_is_case_insensitive() {
        let q =
            parse("MATCH (a)-[:K]->(b) RETURN a ORDER BY a.name desc, b.age asc").expect("parses");
        assert_eq!(q.order_by[0].expr, "a.name");
        assert!(!q.order_by[0].ascending);
        assert_eq!(q.order_by[1].expr, "b.age");
        assert!(q.order_by[1].ascending);
    }

    #[test]
    fn empty_clause_sections_are_errors() {
        err("MATCH (a)-[:K]->(b) WHERE RETURN a");
        err("MATCH (a)-[:K]->(b) RETURN");
        err("MATCH (a)-[:K]->(b) RETURN a ORDER BY");
    }

    #[test]
    fn parse_multiple_where_predicates() {
        let q = parse("MATCH (a)-[:KNOWS]->(b) WHERE a.name = 'Alice' AND b.age > 30 RETURN b")
            .unwrap();
        assert_eq!(q.where_predicates.len(), 2);
    }

    /// A relationship property reads a fixed-length relationship:
    /// `r.price` in RETURN, WHERE or ORDER BY.
    #[test]
    fn relationship_properties_need_a_fixed_length_edge() {
        let q = parse("MATCH (a)-[r:KNOWS]->(b) IN 'col' WHERE r.price > 12 RETURN r.price")
            .expect("a fixed-length relationship property parses");
        assert_eq!(
            q.edge_property_vars(),
            std::collections::HashSet::from(["r".to_string()])
        );
        let varlen = parse("MATCH (a)-[r:KNOWS*1..3]->(b) IN 'col' RETURN r.price")
            .expect_err("a path has no single property object");
        assert!(varlen.to_string().contains("variable-length"), "{varlen}");
        // The crossed edge names its own collection, so no IN is needed.
        parse("MATCH (a)-[r:KNOWS]->(b) WHERE r.price > 1 RETURN b")
            .expect("a relationship property without IN");
        // A node property and a bare path variable need no fixed length.
        parse("MATCH (a)-[r:KNOWS*1..3]->(b) RETURN a.name, r").expect("no relationship property");
    }

    #[test]
    fn limit_and_offset_parse_in_order() {
        let q = parse("MATCH (a)-[:K]->(b) RETURN b ORDER BY b DESC LIMIT 5 OFFSET 2").unwrap();
        assert_eq!((q.limit, q.offset), (Some(5), Some(2)));
        assert!(!q.order_by[0].ascending);
        err("MATCH (a)-[:K]->(b) RETURN b OFFSET 2 LIMIT 5");
        err("MATCH (a)-[:K]->(b) RETURN b OFFSET x");
    }
}
