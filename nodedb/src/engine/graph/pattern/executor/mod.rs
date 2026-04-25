//! MATCH pattern executor — runs pattern matching on the CSR index.
//!
//! Takes a parsed `MatchQuery` and produces a result set of bound variable
//! assignments. Each assignment is a row mapping variable names to node/edge IDs.

mod expansion;
mod predicates;

use std::collections::HashMap;

use super::ast::*;
use crate::engine::graph::csr::CsrIndex;
use crate::engine::graph::edge_store::EdgeStore;

/// A single result row: variable bindings.
pub type BindingRow = HashMap<String, String>;

/// Result of running a MATCH query.
///
/// `truncated` is `true` iff a hard cap inside variable-length expansion
/// fired — the binding rows are incomplete. Data Plane handlers MUST set
/// the `partial` flag on the response envelope when this is set so
/// clients can observe the incomplete result.
pub struct MatchOutcome {
    pub rows: Vec<BindingRow>,
    pub truncated: bool,
}

/// Shared mutable state collected during triple execution: the list of
/// binding rows being built + the across-query truncation flag.
#[derive(Default)]
pub(super) struct ExecutionState {
    pub truncated: bool,
}

/// Execute a MATCH query on a CSR index and edge store.
///
/// Applies join order optimization before execution: triples within each
/// PatternChain are reordered by selectivity (lowest edge count first,
/// bound variables preferred).
///
/// `frontier_bitmap`: when `Some`, only nodes whose surrogate is present in the
/// bitmap are eligible as pattern anchors. Bound variables (already resolved
/// from a prior binding row) bypass the bitmap check — only free-variable
/// anchor enumeration is restricted.
pub fn execute(
    query: &MatchQuery,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<MatchOutcome, crate::Error> {
    // Optimize query before execution (reorder triples by selectivity).
    let mut optimized = query.clone();
    super::optimizer::optimize(&mut optimized, csr);
    execute_query(&optimized, csr, edge_store, frontier_bitmap)
}

/// Execute a pre-optimized MATCH query (internal, skip optimizer).
fn execute_query(
    query: &MatchQuery,
    csr: &CsrIndex,
    edge_store: &EdgeStore,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<MatchOutcome, crate::Error> {
    let mut rows: Vec<BindingRow> = vec![HashMap::new()];
    let mut state = ExecutionState::default();

    for clause in &query.clauses {
        let clause_rows = execute_clause(clause, csr, &rows, &mut state, frontier_bitmap)?;
        if clause.optional {
            rows = left_join_rows(&rows, &clause_rows, clause);
        } else {
            rows = clause_rows;
        }
    }

    for predicate in &query.where_predicates {
        rows = predicates::apply_predicate(&rows, predicate, csr, edge_store, frontier_bitmap)?;
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    if !query.return_columns.is_empty() {
        rows = predicates::project_columns(&rows, &query.return_columns);
    }

    if query.distinct {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            let key = format!("{row:?}");
            seen.insert(key)
        });
    }

    Ok(MatchOutcome {
        rows,
        truncated: state.truncated,
    })
}

/// Serialize binding rows to MessagePack for SPSC transport.
///
/// The Data Plane MUST produce MessagePack so that broadcast merge
/// (`extract_msgpack_elements`) can correctly split and re-merge rows
/// from multiple cores. BindingRow is `HashMap<String, String>` — all
/// values are strings, so we write raw msgpack directly.
pub fn rows_to_msgpack(rows: &[BindingRow]) -> Result<Vec<u8>, crate::Error> {
    use nodedb_query::msgpack_scan::{write_array_header, write_map_header, write_str};

    // MATCH bindings now carry user-visible node ids directly. The
    // CSR partition that produced them is tenant-scoped by
    // construction, so there is no `<tid>:` prefix to strip — what
    // the user inserted is what the user sees back.
    let mut buf = Vec::with_capacity(rows.len() * 64);
    write_array_header(&mut buf, rows.len());
    for row in rows {
        write_map_header(&mut buf, row.len());
        for (k, v) in row {
            write_str(&mut buf, k);
            write_str(&mut buf, v);
        }
    }
    Ok(buf)
}

/// Execute a single MATCH clause.
pub(super) fn execute_clause(
    clause: &MatchClause,
    csr: &CsrIndex,
    input_rows: &[BindingRow],
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut result_rows = input_rows.to_vec();

    for chain in &clause.patterns {
        let mut next_rows = Vec::new();
        for row in &result_rows {
            next_rows.extend(execute_chain(chain, csr, row, state, frontier_bitmap)?);
        }
        result_rows = next_rows;
    }

    Ok(result_rows)
}

/// Execute a single pattern chain against a binding row.
fn execute_chain(
    chain: &PatternChain,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let mut rows = vec![input_row.clone()];

    for triple in &chain.triples {
        let mut next_rows = Vec::new();
        for row in &rows {
            next_rows.extend(execute_triple(triple, csr, row, state, frontier_bitmap)?);
        }
        rows = next_rows;
        if rows.is_empty() {
            break;
        }
    }

    Ok(rows)
}

/// Execute a single triple `(src)-[edge]->(dst)` against a binding row.
fn execute_triple(
    triple: &PatternTriple,
    csr: &CsrIndex,
    input_row: &BindingRow,
    state: &mut ExecutionState,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Result<Vec<BindingRow>, crate::Error> {
    let direction = triple.edge.direction.to_csr_direction();
    let label_filter = triple.edge.edge_type.as_deref();
    let src_nodes = resolve_binding(&triple.src, csr, input_row, frontier_bitmap);

    if src_nodes.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::new();

    if triple.edge.is_variable_length() {
        // Path strings are only needed when the edge variable is bound
        // (e.g. `(a)-[e*1..3]->(b) RETURN e`). For anonymous variable
        // expansions skip all `format!`/`String` work in the hot loop.
        let want_path = triple.edge.name.is_some();
        for &src_id in &src_nodes {
            let expansion = expansion::expand_variable_length(
                csr,
                src_id,
                label_filter,
                direction,
                triple.edge.min_hops,
                triple.edge.max_hops,
                want_path,
            );
            if expansion.truncated {
                state.truncated = true;
            }
            for (dst_id, path) in expansion.results {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    row.insert(edge_name.clone(), path);
                }
                results.push(row);
            }
        }
    } else {
        for &src_id in &src_nodes {
            let neighbors = expansion::collect_neighbors(csr, src_id, label_filter, direction);
            for (lid, dst_id) in neighbors {
                if !binding_compatible(&triple.dst, csr, input_row, dst_id) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, csr, src_id);
                bind_node(&mut row, &triple.dst, csr, dst_id);
                if let Some(ref edge_name) = triple.edge.name {
                    let src_name = csr.node_name_raw(src_id);
                    let dst_name = csr.node_name_raw(dst_id);
                    let label_name = csr.label_name(lid);
                    row.insert(
                        edge_name.clone(),
                        format!("{src_name}|{label_name}|{dst_name}"),
                    );
                }
                results.push(row);
            }
        }
    }

    Ok(results)
}

fn resolve_binding(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&nodedb_types::SurrogateBitmap>,
) -> Vec<u32> {
    if let Some(ref name) = binding.name
        && let Some(value) = row.get(name)
    {
        if let Some(id) = csr.node_id_raw(value) {
            // Check label constraint if specified.
            if let Some(ref label) = binding.label
                && !csr.node_has_label(id, label)
            {
                return Vec::new();
            }
            return vec![id];
        }
        return Vec::new();
    }
    // No binding yet — enumerate all nodes, filtering by label and bitmap.
    let all = 0..csr.node_count() as u32;
    all.filter(|&id| {
        let label_ok = binding
            .label
            .as_ref()
            .is_none_or(|l| csr.node_has_label(id, l));
        let bitmap_ok = frontier_bitmap
            .is_none_or(|bm| bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id))));
        label_ok && bitmap_ok
    })
    .collect()
}

fn binding_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    node_id: u32,
) -> bool {
    // Check label constraint.
    if let Some(ref label) = binding.label
        && !csr.node_has_label(node_id, label)
    {
        return false;
    }
    if let Some(ref name) = binding.name
        && let Some(existing) = row.get(name)
    {
        return existing == csr.node_name_raw(node_id);
    }
    true
}

fn bind_node(row: &mut BindingRow, binding: &NodeBinding, csr: &CsrIndex, node_id: u32) {
    if let Some(ref name) = binding.name {
        row.entry(name.clone())
            .or_insert_with(|| csr.node_name_raw(node_id).to_string());
    }
}

/// LEFT JOIN: merge clause results with existing rows.
fn left_join_rows(
    input: &[BindingRow],
    clause_rows: &[BindingRow],
    clause: &MatchClause,
) -> Vec<BindingRow> {
    let new_vars: Vec<String> = clause
        .patterns
        .iter()
        .flat_map(|chain| {
            chain.triples.iter().flat_map(|t| {
                let mut vars = Vec::new();
                if let Some(ref n) = t.src.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.dst.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.edge.name {
                    vars.push(n.clone());
                }
                vars
            })
        })
        .collect();

    let mut result = Vec::new();

    for input_row in input {
        let matches: Vec<&BindingRow> = clause_rows
            .iter()
            .filter(|cr| {
                input_row
                    .iter()
                    .all(|(k, v)| cr.get(k).is_none_or(|cv| cv == v))
            })
            .collect();

        if matches.is_empty() {
            let mut row = input_row.clone();
            for var in &new_vars {
                row.entry(var.clone()).or_insert_with(|| "NULL".to_string());
            }
            result.push(row);
        } else {
            result.extend(matches.into_iter().cloned());
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_social_graph() -> (CsrIndex, EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();

        use crate::engine::graph::edge_store::EdgeRef;
        use nodedb_types::TenantId;
        const T: TenantId = TenantId::new(1);
        let mut ord = 0i64;
        let mut put = |src: &str, label: &str, dst: &str| {
            ord += 1;
            store
                .put_edge_versioned(
                    EdgeRef::new(T, "col", src, label, dst),
                    b"",
                    ord,
                    ord,
                    i64::MAX,
                )
                .unwrap();
        };
        put("alice", "KNOWS", "bob");
        put("bob", "KNOWS", "carol");
        put("carol", "KNOWS", "dave");
        put("alice", "LIKES", "carol");
        put("bob", "BLOCKED", "dave");

        let csr = crate::engine::graph::csr::rebuild::rebuild_from_store(&store).unwrap();
        (csr, store, dir)
    }

    #[test]
    fn execute_simple_one_hop() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) WHERE a = 'alice' RETURN a, b")
                .unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }

    #[test]
    fn execute_two_hops() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a = 'alice' RETURN a, b, c",
        )
        .unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "carol");
    }

    #[test]
    fn execute_optional_match() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b) OPTIONAL MATCH (b)-[:LIKES]->(c) WHERE a = 'alice' RETURN a, b, c",
        ).unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["c"], "NULL");
    }

    #[test]
    fn execute_anti_join() {
        let (csr, store, _dir) = make_social_graph();
        let query = super::super::compiler::parse(
            "MATCH (a)-[:KNOWS]->(b) WHERE NOT EXISTS { MATCH (a)-[:BLOCKED]->(b) } RETURN a, b",
        )
        .unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn execute_with_limit() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b LIMIT 2").unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn execute_empty_result() {
        let (csr, store, _dir) = make_social_graph();
        let query =
            super::super::compiler::parse("MATCH (a)-[:NONEXISTENT]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn execute_with_node_labels() {
        let (mut csr, store, _dir) = make_social_graph();

        // Set labels.
        csr.add_node_label("alice", "Person");
        csr.add_node_label("bob", "Person");
        csr.add_node_label("carol", "Person");
        csr.add_node_label("dave", "Bot");

        // Without label filter — all KNOWS edges.
        let query = super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 3);

        // With label filter — only Person src.
        let query =
            super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        // alice->bob, bob->carol, carol->dave — all 3 srcs are Person.
        assert_eq!(rows.len(), 3);

        // With label filter — only Bot dst.
        let query =
            super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b:Bot) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        // Only carol->dave where dave is Bot.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");
        assert_eq!(rows[0]["b"], "dave");

        // Both labels — Person->Bot.
        let query = super::super::compiler::parse("MATCH (a:Person)-[:KNOWS]->(b:Bot) RETURN a, b")
            .unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "carol");

        // Non-matching labels — should return 0.
        let query = super::super::compiler::parse("MATCH (a:Bot)-[:KNOWS]->(b:Person) RETURN a, b")
            .unwrap();
        let rows = execute(&query, &csr, &store, None).unwrap().rows;
        assert!(rows.is_empty());
    }

    #[test]
    fn rows_to_msgpack_format() {
        let mut row = BindingRow::new();
        row.insert("a".into(), "alice".into());
        row.insert("b".into(), "bob".into());
        let msgpack = rows_to_msgpack(&[row]).unwrap();
        let json = nodedb_types::json_from_msgpack(&msgpack).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr[0]["a"], "alice");
    }

    /// Anchor nodes not in the frontier bitmap are never expanded as sources.
    /// alice (surrogate 1) is in the bitmap; bob and carol (surrogates 2, 3)
    /// are not. A free-variable MATCH should only yield rows where the src
    /// anchor is alice.
    #[test]
    fn match_frontier_bitmap_excludes_non_member_anchors() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let (mut csr, store, _dir) = make_social_graph();
        // Assign surrogates: alice=1, bob=2, carol=3, dave=4.
        csr.set_node_surrogate("alice", Surrogate::new(1));
        csr.set_node_surrogate("bob", Surrogate::new(2));
        csr.set_node_surrogate("carol", Surrogate::new(3));
        csr.set_node_surrogate("dave", Surrogate::new(4));

        // Bitmap contains only alice (surrogate 1).
        let bm = SurrogateBitmap::from_iter([Surrogate::new(1)]);

        let query = super::super::compiler::parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
        let rows = execute(&query, &csr, &store, Some(&bm)).unwrap().rows;

        // Only alice->bob should appear; bob->carol and carol->dave are blocked
        // because the src anchor (bob, carol) is not in the bitmap.
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"], "alice");
        assert_eq!(rows[0]["b"], "bob");
    }
}
